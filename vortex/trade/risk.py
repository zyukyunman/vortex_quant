"""Pre-trade risk checks for paper and live execution."""

from __future__ import annotations

from dataclasses import dataclass

from vortex.trade.broker import BrokerHealth, CashSnapshot, OrderIntent, Quote
from vortex.trade.models import Lineage, OrderPlan, RiskCheckResult, RiskRuleResult


@dataclass(frozen=True)
class PreTradeRiskConfig:
    mode: str = "paper"
    allow_live: bool = False
    require_st_data: bool = True
    max_order_count: int = 80
    max_single_order_value: float = 100_000.0
    max_daily_order_value: float = 1_000_000.0


def run_pre_trade_risk_check(
    plan: OrderPlan,
    *,
    health: BrokerHealth,
    cash: CashSnapshot,
    quotes: dict[str, Quote],
    st_flags: dict[str, bool] | None = None,
    config: PreTradeRiskConfig | None = None,
) -> RiskCheckResult:
    """Evaluate fail-closed pre-trade rules before any order is submitted."""

    config = config or PreTradeRiskConfig()
    results: list[RiskRuleResult] = []
    _append(results, "broker_health", health.ok, "critical", health.message)
    _append(results, "mode_allowed", config.mode != "live" or config.allow_live, "critical", "live trading disabled")
    _append(results, "order_count", len(plan.orders) <= config.max_order_count, "critical", "too many orders")
    _append(results, "cash_non_negative", cash.available_cash >= 0, "critical", "available cash is negative")

    total_value = 0.0
    for order in plan.orders:
        quote = quotes.get(order.symbol)
        if quote is None:
            _append(results, "quote_available", False, "critical", "missing quote", order.symbol)
            continue
        order_value = order.shares * quote.execution_price
        total_value += order_value
        _append(
            results,
            "single_order_value",
            order_value <= config.max_single_order_value,
            "critical",
            "single order value too large",
            order.symbol,
        )
        _append(results, "not_suspended", not quote.is_suspended, "critical", "symbol suspended", order.symbol)
        _append(
            results,
            "limit_up_buy",
            not (order.side == "buy" and quote.is_limit_up),
            "critical",
            "limit-up buy blocked",
            order.symbol,
        )
        _append(
            results,
            "limit_down_sell",
            not (order.side == "sell" and quote.is_limit_down),
            "critical",
            "limit-down sell blocked",
            order.symbol,
        )
        _append(results, "lot_size", order.shares > 0 and order.shares % 100 == 0, "critical", "not board lot", order.symbol)
        _append(results, "limit_price", _limit_price_ok(order), "critical", "invalid limit price", order.symbol)
        if config.require_st_data:
            if st_flags is None or order.symbol not in st_flags:
                _append(results, "st_data_available", False, "critical", "missing ST flag", order.symbol)
            else:
                _append(results, "not_st", not st_flags[order.symbol], "critical", "ST symbol blocked", order.symbol)

    _append(
        results,
        "daily_order_value",
        total_value <= config.max_daily_order_value,
        "critical",
        "daily order value too large",
    )
    blocking = [item.message for item in results if item.level == "critical" and not item.passed]
    warnings = [item.message for item in results if item.level == "warning" and not item.passed]
    return RiskCheckResult(
        exec_id=plan.exec_id,
        passed=not blocking,
        blocking_reasons=blocking,
        warnings=warnings,
        rule_results=results,
        lineage=Lineage(exec_id=plan.exec_id, portfolio_id=plan.portfolio_id, gateway_type=config.mode),
    )


def _append(
    results: list[RiskRuleResult],
    name: str,
    passed: bool,
    level: str,
    message: str,
    symbol: str | None = None,
) -> None:
    results.append(RiskRuleResult(name=name, passed=passed, level=level, message="" if passed else message, symbol=symbol))


def _limit_price_ok(order: OrderIntent) -> bool:
    if order.price_type == "market":
        return True
    return order.price_type == "limit" and order.limit_price is not None and order.limit_price > 0
