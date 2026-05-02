from __future__ import annotations

import pandas as pd
import pytest

from vortex.trade import CashSnapshot, Position, Quote
from vortex.trade.order_plan import OrderPlanConfig, generate_order_plan
from vortex.trade.target_portfolio import TargetPortfolioBuildConfig, build_target_portfolio


def test_build_target_portfolio_rounds_to_lots_and_cash_target() -> None:
    targets = pd.DataFrame(
        [
            {"symbol": "000001.SZ", "target_weight": 0.5, "reference_price": 10.0, "reason": "rank1"},
            {"symbol": "000002.SZ", "target_weight": 0.333, "reference_price": 33.0, "reason": "rank2"},
        ]
    )

    portfolio = build_target_portfolio(
        targets,
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
        config=TargetPortfolioBuildConfig(notional=100_000, lot_size=100),
    )

    assert portfolio.portfolio_id.startswith("tp_20260501_")
    assert [item.symbol for item in portfolio.positions] == ["000001.SZ", "000002.SZ"]
    assert portfolio.positions[0].target_shares == 5_000
    assert portfolio.positions[1].target_shares == 1_000
    assert portfolio.cash_target == 17_000


def test_generate_order_plan_sells_before_buys_and_respects_cash() -> None:
    targets = pd.DataFrame(
        [
            {"symbol": "000001.SZ", "target_weight": 0.2, "reference_price": 10.0},
            {"symbol": "000002.SZ", "target_weight": 0.4, "reference_price": 20.0},
        ]
    )
    portfolio = build_target_portfolio(
        targets,
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
        config=TargetPortfolioBuildConfig(notional=100_000),
    )

    plan = generate_order_plan(
        portfolio,
        cash=CashSnapshot(available_cash=5_000, frozen_cash=0, total_asset=100_000, market_value=95_000),
        positions=[Position("000001.SZ", shares=5_000, available_shares=5_000, cost_price=10, last_price=10)],
        quotes={
            "000001.SZ": Quote("000001.SZ", open_price=10.0),
            "000002.SZ": Quote("000002.SZ", open_price=20.0),
        },
        config=OrderPlanConfig(buy_limit_bps=50, sell_limit_bps=20, min_order_value=3_000),
    )

    assert [order.side for order in plan.orders] == ["sell", "buy"]
    assert plan.orders[0].symbol == "000001.SZ"
    assert plan.orders[0].shares == 3_000
    assert plan.orders[0].limit_price == 9.98
    assert plan.orders[1].symbol == "000002.SZ"
    assert plan.orders[1].shares == 1_700
    assert plan.orders[1].limit_price == 20.1


def test_generate_order_plan_rejects_missing_quotes() -> None:
    portfolio = build_target_portfolio(
        pd.DataFrame([{"symbol": "000001.SZ", "target_weight": 1.0, "reference_price": 10.0}]),
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
    )

    with pytest.raises(ValueError, match="missing quotes"):
        generate_order_plan(
            portfolio,
            cash=CashSnapshot(available_cash=100_000, frozen_cash=0, total_asset=100_000, market_value=0),
            positions=[],
            quotes={},
        )
