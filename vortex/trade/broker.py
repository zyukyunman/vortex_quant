"""Broker adapter contracts and a local paper broker implementation.

The paper broker is intentionally deterministic and local-only. It lets Vortex
validate order generation, lot constraints, cash checks, partial fills and basic
execution records before a real QMT/MiniQMT bridge is available.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class BrokerHealth:
    ok: bool
    mode: str
    message: str


@dataclass(frozen=True)
class CashSnapshot:
    available_cash: float
    frozen_cash: float
    total_asset: float
    market_value: float


@dataclass(frozen=True)
class Position:
    symbol: str
    shares: int
    available_shares: int
    cost_price: float
    last_price: float

    @property
    def market_value(self) -> float:
        return float(self.shares * self.last_price)


@dataclass(frozen=True)
class Quote:
    symbol: str
    open_price: float
    last_price: float | None = None
    volume: int | None = None
    amount: float | None = None
    is_suspended: bool = False
    is_limit_up: bool = False
    is_limit_down: bool = False

    @property
    def execution_price(self) -> float:
        return float(self.last_price if self.last_price is not None else self.open_price)


@dataclass(frozen=True)
class OrderIntent:
    symbol: str
    side: str
    shares: int
    price_type: str = "limit"
    limit_price: float | None = None
    reason: str = ""
    strategy_version: str = ""
    run_id: str = ""


@dataclass(frozen=True)
class OrderRecord:
    order_id: str
    intent: OrderIntent
    status: str
    filled_shares: int
    remaining_shares: int
    avg_fill_price: float | None
    message: str
    created_at: str


@dataclass(frozen=True)
class FillRecord:
    fill_id: str
    order_id: str
    symbol: str
    side: str
    shares: int
    price: float
    gross_value: float
    fee: float
    created_at: str


@dataclass(frozen=True)
class PaperBrokerConfig:
    initial_cash: float = 1_000_000.0
    lot_size: int = 100
    commission_bps: float = 2.5
    min_commission: float = 5.0
    stamp_duty_sell_bps: float = 5.0
    max_participation_rate: float = 0.05
    allow_trading: bool = True


@dataclass
class _MutablePosition:
    shares: int
    available_shares: int
    cost_price: float
    last_price: float


@dataclass
class PaperBrokerAdapter:
    """A deterministic local broker used before the QMT bridge is available."""

    config: PaperBrokerConfig = field(default_factory=PaperBrokerConfig)
    _cash: float = field(init=False)
    _positions: dict[str, _MutablePosition] = field(default_factory=dict, init=False)
    _quotes: dict[str, Quote] = field(default_factory=dict, init=False)
    _orders: list[OrderRecord] = field(default_factory=list, init=False)
    _fills: list[FillRecord] = field(default_factory=list, init=False)
    _next_order_id: int = field(default=1, init=False)
    _next_fill_id: int = field(default=1, init=False)

    def __post_init__(self) -> None:
        if self.config.initial_cash <= 0:
            raise ValueError("initial_cash must be positive")
        if self.config.lot_size <= 0:
            raise ValueError("lot_size must be positive")
        if not 0 < self.config.max_participation_rate <= 1:
            raise ValueError("max_participation_rate must be in (0, 1]")
        self._cash = float(self.config.initial_cash)

    def health(self) -> BrokerHealth:
        return BrokerHealth(ok=True, mode="paper", message="local paper broker ready")

    def update_quotes(self, quotes: list[Quote]) -> None:
        for quote in quotes:
            if quote.open_price <= 0:
                raise ValueError(f"quote open_price must be positive: {quote.symbol}")
            self._quotes[quote.symbol] = quote
            if quote.symbol in self._positions:
                self._positions[quote.symbol].last_price = quote.execution_price

    def get_quotes(self, symbols: list[str]) -> dict[str, Quote]:
        missing = [symbol for symbol in symbols if symbol not in self._quotes]
        if missing:
            raise KeyError(f"missing quotes: {missing}")
        return {symbol: self._quotes[symbol] for symbol in symbols}

    def get_cash(self) -> CashSnapshot:
        market_value = sum(position.shares * position.last_price for position in self._positions.values())
        return CashSnapshot(
            available_cash=float(self._cash),
            frozen_cash=0.0,
            total_asset=float(self._cash + market_value),
            market_value=float(market_value),
        )

    def get_positions(self) -> list[Position]:
        return [
            Position(
                symbol=symbol,
                shares=position.shares,
                available_shares=position.available_shares,
                cost_price=position.cost_price,
                last_price=position.last_price,
            )
            for symbol, position in sorted(self._positions.items())
            if position.shares > 0
        ]

    def get_orders(self) -> list[OrderRecord]:
        return list(self._orders)

    def get_fills(self) -> list[FillRecord]:
        return list(self._fills)

    def submit_order(self, intent: OrderIntent) -> OrderRecord:
        message = self._validate_intent(intent)
        if message:
            return self._record_order(intent, "rejected", 0, intent.shares, None, message)
        quote = self._quotes.get(intent.symbol)
        if quote is None:
            return self._record_order(intent, "rejected", 0, intent.shares, None, "missing quote")
        tradable_message = self._tradable_message(intent, quote)
        if tradable_message:
            return self._record_order(intent, "rejected", 0, intent.shares, None, tradable_message)

        fill_price = quote.execution_price
        if intent.price_type == "limit":
            assert intent.limit_price is not None
            if intent.side == "buy" and intent.limit_price < fill_price:
                return self._record_order(intent, "rejected", 0, intent.shares, None, "buy limit below execution price")
            if intent.side == "sell" and intent.limit_price > fill_price:
                return self._record_order(intent, "rejected", 0, intent.shares, None, "sell limit above execution price")

        fill_shares = self._fillable_shares(intent, quote, fill_price)
        if fill_shares <= 0:
            return self._record_order(intent, "rejected", 0, intent.shares, None, "no fillable shares")

        fee = self._trade_fee(fill_shares * fill_price, sell=intent.side == "sell")
        self._apply_fill(intent, fill_shares, fill_price, fee)
        status = "filled" if fill_shares == intent.shares else "partial"
        order = self._record_order(
            intent,
            status,
            fill_shares,
            intent.shares - fill_shares,
            fill_price,
            "" if status == "filled" else "partially filled by cash/liquidity constraints",
        )
        self._record_fill(order.order_id, intent, fill_shares, fill_price, fee)
        return order

    def cancel_order(self, order_id: str) -> OrderRecord:
        for index, order in enumerate(self._orders):
            if order.order_id != order_id:
                continue
            if order.status not in {"partial", "open"}:
                return order
            cancelled = OrderRecord(
                order_id=order.order_id,
                intent=order.intent,
                status="cancelled",
                filled_shares=order.filled_shares,
                remaining_shares=order.remaining_shares,
                avg_fill_price=order.avg_fill_price,
                message="cancelled by user",
                created_at=order.created_at,
            )
            self._orders[index] = cancelled
            return cancelled
        raise KeyError(f"unknown order_id: {order_id}")

    def _validate_intent(self, intent: OrderIntent) -> str:
        if not self.config.allow_trading:
            return "trading disabled"
        if intent.side not in {"buy", "sell"}:
            return "side must be buy or sell"
        if intent.price_type not in {"limit", "market"}:
            return "price_type must be limit or market"
        if intent.price_type == "limit" and (intent.limit_price is None or intent.limit_price <= 0):
            return "limit_price must be positive for limit orders"
        if intent.shares <= 0:
            return "shares must be positive"
        if intent.shares % self.config.lot_size != 0:
            return f"shares must be a multiple of {self.config.lot_size}"
        return ""

    def _tradable_message(self, intent: OrderIntent, quote: Quote) -> str:
        if quote.is_suspended:
            return "symbol suspended"
        if intent.side == "buy" and quote.is_limit_up:
            return "limit-up buy blocked"
        if intent.side == "sell" and quote.is_limit_down:
            return "limit-down sell blocked"
        position = self._positions.get(intent.symbol)
        if intent.side == "sell" and (position is None or position.available_shares <= 0):
            return "insufficient position"
        return ""

    def _fillable_shares(self, intent: OrderIntent, quote: Quote, fill_price: float) -> int:
        requested = intent.shares
        if quote.volume is not None:
            liquidity_cap = int((quote.volume * self.config.max_participation_rate) // self.config.lot_size)
            requested = min(requested, liquidity_cap * self.config.lot_size)
        if intent.side == "sell":
            position = self._positions.get(intent.symbol)
            available = position.available_shares if position else 0
            return min(requested, available) // self.config.lot_size * self.config.lot_size

        affordable = int(self._cash // (fill_price * self.config.lot_size)) * self.config.lot_size
        shares = min(requested, affordable)
        while shares > 0 and shares * fill_price + self._trade_fee(shares * fill_price, sell=False) > self._cash:
            shares -= self.config.lot_size
        return max(shares, 0)

    def _apply_fill(self, intent: OrderIntent, shares: int, price: float, fee: float) -> None:
        gross = shares * price
        if intent.side == "buy":
            self._cash -= gross + fee
            position = self._positions.get(intent.symbol)
            if position is None:
                self._positions[intent.symbol] = _MutablePosition(shares, shares, price, price)
                return
            old_value = position.shares * position.cost_price
            new_value = shares * price
            position.shares += shares
            position.available_shares += shares
            position.cost_price = (old_value + new_value) / position.shares
            position.last_price = price
            return

        self._cash += gross - fee
        position = self._positions[intent.symbol]
        position.shares -= shares
        position.available_shares -= shares
        position.last_price = price
        if position.shares <= 0:
            del self._positions[intent.symbol]

    def _trade_fee(self, gross_value: float, *, sell: bool) -> float:
        commission = max(gross_value * self.config.commission_bps / 10_000.0, self.config.min_commission)
        stamp_duty = gross_value * self.config.stamp_duty_sell_bps / 10_000.0 if sell else 0.0
        return float(commission + stamp_duty)

    def _record_order(
        self,
        intent: OrderIntent,
        status: str,
        filled_shares: int,
        remaining_shares: int,
        avg_fill_price: float | None,
        message: str,
    ) -> OrderRecord:
        order = OrderRecord(
            order_id=f"P{self._next_order_id:08d}",
            intent=intent,
            status=status,
            filled_shares=filled_shares,
            remaining_shares=remaining_shares,
            avg_fill_price=avg_fill_price,
            message=message,
            created_at=_now_iso(),
        )
        self._next_order_id += 1
        self._orders.append(order)
        return order

    def _record_fill(self, order_id: str, intent: OrderIntent, shares: int, price: float, fee: float) -> None:
        fill = FillRecord(
            fill_id=f"F{self._next_fill_id:08d}",
            order_id=order_id,
            symbol=intent.symbol,
            side=intent.side,
            shares=shares,
            price=price,
            gross_value=shares * price,
            fee=fee,
            created_at=_now_iso(),
        )
        self._next_fill_id += 1
        self._fills.append(fill)


def _now_iso() -> str:
    return datetime.now().replace(microsecond=0).isoformat()
