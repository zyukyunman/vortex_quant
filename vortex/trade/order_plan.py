"""Generate order plans from frozen target portfolios and broker state."""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass

from vortex.trade.broker import CashSnapshot, OrderIntent, Position, Quote
from vortex.trade.market_rules import min_order_shares, price_tick
from vortex.trade.models import Lineage, OrderPlan, TargetPortfolio


@dataclass(frozen=True)
class OrderPlanConfig:
    buy_limit_bps: float = 30.0
    sell_limit_bps: float = 30.0
    min_order_value: float = 3_000.0
    lot_size: int = 100


def generate_order_plan(
    portfolio: TargetPortfolio,
    *,
    cash: CashSnapshot,
    positions: list[Position],
    quotes: dict[str, Quote],
    config: OrderPlanConfig | None = None,
) -> OrderPlan:
    """Create deterministic sell-first order intents from a target portfolio."""

    config = config or OrderPlanConfig()
    _validate_config(config)
    # 差分以真实持仓 shares 为准，卖出上限再受 available_shares 约束。
    # 否则冻结中的目标内仓位会被误判成“缺口”，在 QMT 执行前反向补买。
    current = {item.symbol: item.shares for item in positions}
    sellable = {item.symbol: item.available_shares for item in positions}
    target = {item.symbol: item.target_shares for item in portfolio.positions}
    symbols = sorted(set(current) | set(target))
    missing_quotes = [symbol for symbol in symbols if symbol not in quotes]
    if missing_quotes:
        raise ValueError(f"missing quotes: {missing_quotes}")

    orders: list[OrderIntent] = []
    available_cash = cash.available_cash
    for symbol in symbols:
        delta = target.get(symbol, 0) - current.get(symbol, 0)
        if delta >= 0:
            continue
        quote = quotes[symbol]
        reference_price = _sell_reference_price(symbol, quote, config)
        shares = _round_lot(min(abs(delta), sellable.get(symbol, 0)), config.lot_size)
        if shares > 0:
            orders.append(
                OrderIntent(
                    symbol=symbol,
                    side="sell",
                    shares=shares,
                    price_type="limit",
                    limit_price=reference_price,
                    reason="rebalance_sell",
                    strategy_version=portfolio.strategy_version,
                    run_id=portfolio.run_id,
                )
            )
            available_cash += shares * reference_price

    for symbol in symbols:
        delta = target.get(symbol, 0) - current.get(symbol, 0)
        if delta <= 0:
            continue
        quote = quotes[symbol]
        reference_price = _buy_reference_price(symbol, quote, config)
        shares = _round_lot(delta, config.lot_size)
        if shares < min_order_shares(symbol, "buy"):
            continue
        max_affordable = _round_lot(int(available_cash // reference_price), config.lot_size)
        shares = min(shares, max_affordable)
        gross = shares * reference_price
        if shares > 0 and gross >= config.min_order_value:
            orders.append(
                OrderIntent(
                    symbol=symbol,
                    side="buy",
                    shares=shares,
                    price_type="limit",
                    limit_price=reference_price,
                    reason="rebalance_buy",
                    strategy_version=portfolio.strategy_version,
                    run_id=portfolio.run_id,
                )
            )
            available_cash -= gross

    exec_id = _exec_id(portfolio, orders)
    return OrderPlan(
        exec_id=exec_id,
        portfolio_id=portfolio.portfolio_id,
        trade_date=portfolio.trade_date,
        orders=orders,
        lineage=Lineage(
            exec_id=exec_id,
            portfolio_id=portfolio.portfolio_id,
            strategy_version=portfolio.strategy_version,
            strategy_run_id=portfolio.run_id,
            snapshot_id=portfolio.snapshot_id,
            gateway_type="paper",
        ),
    )


def _round_lot(shares: int, lot_size: int) -> int:
    return int(shares // lot_size) * lot_size


def _buy_reference_price(symbol: str, quote: Quote, config: OrderPlanConfig) -> float:
    if quote.ask_price_1 is not None and quote.ask_price_1 > 0:
        return quote.ask_price_1
    raw = quote.execution_price * (1 + config.buy_limit_bps / 10_000.0)
    return _round_price_to_tick(symbol, raw, side="buy")


def _sell_reference_price(symbol: str, quote: Quote, config: OrderPlanConfig) -> float:
    if quote.bid_price_1 is not None and quote.bid_price_1 > 0:
        return quote.bid_price_1
    raw = quote.execution_price * (1 - config.sell_limit_bps / 10_000.0)
    return _round_price_to_tick(symbol, raw, side="sell")


def _round_price_to_tick(symbol: str, price: float, *, side: str) -> float:
    tick = price_tick(symbol)
    scaled = price / tick
    rounded = math.ceil(scaled) if side == "buy" else math.floor(scaled)
    return round(max(tick, rounded * tick), 4)


def _validate_config(config: OrderPlanConfig) -> None:
    if config.lot_size <= 0:
        raise ValueError("lot_size must be positive")
    if config.min_order_value < 0:
        raise ValueError("min_order_value must be non-negative")
    if config.buy_limit_bps < 0 or config.sell_limit_bps < 0:
        raise ValueError("limit bps must be non-negative")


def _exec_id(portfolio: TargetPortfolio, orders: list[OrderIntent]) -> str:
    payload = "|".join(
        [portfolio.portfolio_id, portfolio.trade_date]
        + [f"{order.side}:{order.symbol}:{order.shares}:{order.limit_price}" for order in orders]
    )
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:10]
    return f"exec_{portfolio.trade_date}_{digest}"
