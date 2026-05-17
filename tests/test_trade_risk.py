from __future__ import annotations

from vortex.trade import BrokerHealth, CashSnapshot, OrderIntent, OrderPlan, Quote
from vortex.trade.market_rules import MarketPermissionConfig, is_market_allowed, market_board, min_order_shares
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


def test_pre_trade_risk_blocks_star_buy_below_200_shares() -> None:
    plan = OrderPlan(
        exec_id="exec_1",
        portfolio_id="tp_1",
        trade_date="20260501",
        orders=[OrderIntent(symbol="688809.SH", side="buy", shares=100, limit_price=439.99)],
    )

    result = run_pre_trade_risk_check(
        plan,
        health=BrokerHealth(ok=True, mode="paper", message="ok"),
        cash=CashSnapshot(available_cash=100_000, frozen_cash=0, total_asset=100_000, market_value=0),
        quotes={"688809.SH": Quote("688809.SH", open_price=440.0)},
        st_flags={"688809.SH": False},
    )

    assert result.passed is False
    assert "not board lot" in result.blocking_reasons


def test_market_rules_identify_boards_permissions_and_star_min_buy() -> None:
    assert market_board("688809.SH") == "star"
    assert market_board("300750.SZ") == "chinext"
    assert market_board("920001.BJ") == "bse"
    assert market_board("600000.SH") == "main"

    permissions = MarketPermissionConfig(allow_star=False, allow_bse=False)
    assert is_market_allowed("688809.SH", permissions) is False
    assert is_market_allowed("300750.SZ", permissions) is True
    assert min_order_shares("688809.SH", "buy") == 200
    assert min_order_shares("688809.SH", "sell") == 100


def test_pre_trade_risk_blocks_st_buy_but_allows_st_sell_cleanup() -> None:
    buy_plan = OrderPlan(
        exec_id="exec_buy",
        portfolio_id="tp_1",
        trade_date="20260501",
        orders=[OrderIntent(symbol="002082.SZ", side="buy", shares=100, limit_price=10.0)],
    )
    sell_plan = OrderPlan(
        exec_id="exec_sell",
        portfolio_id="tp_1",
        trade_date="20260501",
        orders=[OrderIntent(symbol="002082.SZ", side="sell", shares=100, limit_price=9.9)],
    )
    common_kwargs = {
        "health": BrokerHealth(ok=True, mode="paper", message="ok"),
        "cash": CashSnapshot(available_cash=100_000, frozen_cash=0, total_asset=100_000, market_value=10_000),
        "quotes": {"002082.SZ": Quote("002082.SZ", open_price=10.0)},
        "st_flags": {"002082.SZ": True},
    }

    buy_result = run_pre_trade_risk_check(buy_plan, **common_kwargs)
    sell_result = run_pre_trade_risk_check(sell_plan, **common_kwargs)

    assert buy_result.passed is False
    assert "ST buy blocked" in buy_result.blocking_reasons
    assert sell_result.passed is True
