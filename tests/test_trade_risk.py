from __future__ import annotations

from vortex.trade import BrokerHealth, CashSnapshot, OrderIntent, OrderPlan, Quote
from vortex.trade.risk import PreTradeRiskConfig, run_pre_trade_risk_check


def test_pre_trade_risk_passes_clean_paper_plan() -> None:
    plan = OrderPlan(
        exec_id="exec_1",
        portfolio_id="tp_1",
        trade_date="20260501",
        orders=[OrderIntent(symbol="000001.SZ", side="buy", shares=100, limit_price=10.1)],
    )

    result = run_pre_trade_risk_check(
        plan,
        health=BrokerHealth(ok=True, mode="paper", message="ok"),
        cash=CashSnapshot(available_cash=100_000, frozen_cash=0, total_asset=100_000, market_value=0),
        quotes={"000001.SZ": Quote("000001.SZ", open_price=10.0)},
        st_flags={"000001.SZ": False},
    )

    assert result.passed is True
    assert result.blocking_reasons == []


def test_pre_trade_risk_blocks_missing_st_limit_up_and_live_by_default() -> None:
    plan = OrderPlan(
        exec_id="exec_1",
        portfolio_id="tp_1",
        trade_date="20260501",
        orders=[OrderIntent(symbol="000001.SZ", side="buy", shares=100, limit_price=10.1)],
    )

    result = run_pre_trade_risk_check(
        plan,
        health=BrokerHealth(ok=True, mode="qmt", message="ok"),
        cash=CashSnapshot(available_cash=100_000, frozen_cash=0, total_asset=100_000, market_value=0),
        quotes={"000001.SZ": Quote("000001.SZ", open_price=10.0, is_limit_up=True)},
        st_flags=None,
        config=PreTradeRiskConfig(mode="live"),
    )

    assert result.passed is False
    assert "live trading disabled" in result.blocking_reasons
    assert "limit-up buy blocked" in result.blocking_reasons
    assert "missing ST flag" in result.blocking_reasons


def test_pre_trade_risk_blocks_daily_notional() -> None:
    plan = OrderPlan(
        exec_id="exec_1",
        portfolio_id="tp_1",
        trade_date="20260501",
        orders=[OrderIntent(symbol="000001.SZ", side="buy", shares=10_000, limit_price=10.1)],
    )

    result = run_pre_trade_risk_check(
        plan,
        health=BrokerHealth(ok=True, mode="paper", message="ok"),
        cash=CashSnapshot(available_cash=1_000_000, frozen_cash=0, total_asset=1_000_000, market_value=0),
        quotes={"000001.SZ": Quote("000001.SZ", open_price=10.0)},
        st_flags={"000001.SZ": False},
        config=PreTradeRiskConfig(max_single_order_value=200_000, max_daily_order_value=50_000),
    )

    assert result.passed is False
    assert "daily order value too large" in result.blocking_reasons
