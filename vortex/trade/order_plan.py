"""Generate order plans from frozen target portfolios and broker state."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from vortex.trade.broker import CashSnapshot, OrderIntent, Position, Quote
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
    current = {item.symbol: item.available_shares for item in positions}
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
        shares = _round_lot(abs(delta), config.lot_size)
        gross = shares * quote.execution_price
        if shares > 0 and gross >= config.min_order_value:
            orders.append(
                OrderIntent(
                    symbol=symbol,
                    side="sell",
                    shares=shares,
                    price_type="limit",
                    limit_price=round(quote.execution_price * (1 - config.sell_limit_bps / 10_000.0), 4),
                    reason="rebalance_sell",
                    strategy_version=portfolio.strategy_version,
                    run_id=portfolio.run_id,
                )
            )
            available_cash += gross

    for symbol in symbols:
        delta = target.get(symbol, 0) - current.get(symbol, 0)
        if delta <= 0:
            continue
        quote = quotes[symbol]
        shares = _round_lot(delta, config.lot_size)
        max_affordable = _round_lot(int(available_cash // quote.execution_price), config.lot_size)
        shares = min(shares, max_affordable)
        gross = shares * quote.execution_price
        if shares > 0 and gross >= config.min_order_value:
            orders.append(
                OrderIntent(
                    symbol=symbol,
                    side="buy",
                    shares=shares,
                    price_type="limit",
                    limit_price=round(quote.execution_price * (1 + config.buy_limit_bps / 10_000.0), 4),
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
