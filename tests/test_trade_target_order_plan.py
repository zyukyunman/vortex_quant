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


def test_build_target_portfolio_skips_star_positions_below_200_shares() -> None:
    targets = pd.DataFrame(
        [
            {"symbol": "688809.SH", "target_weight": 1.0, "reference_price": 500.0, "reason": "rank1"},
        ]
    )

    portfolio = build_target_portfolio(
        targets,
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
        config=TargetPortfolioBuildConfig(notional=50_000, lot_size=100),
    )

    assert portfolio.positions == []
    assert portfolio.cash_target == 50_000


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
            "000001.SZ": Quote("000001.SZ", open_price=10.0, bid_price_1=9.99),
            "000002.SZ": Quote("000002.SZ", open_price=20.0, ask_price_1=20.01),
        },
        config=OrderPlanConfig(buy_limit_bps=50, sell_limit_bps=20, min_order_value=3_000),
    )

    assert [order.side for order in plan.orders] == ["sell", "buy"]
    assert plan.orders[0].symbol == "000001.SZ"
    assert plan.orders[0].shares == 3_000
    assert plan.orders[0].limit_price == 9.99
    assert plan.orders[1].symbol == "000002.SZ"
    assert plan.orders[1].shares == 1_700
    assert plan.orders[1].limit_price == 20.01


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


def test_generate_order_plan_uses_real_shares_for_diff_and_available_shares_for_sell_cap() -> None:
    portfolio = build_target_portfolio(
        pd.DataFrame([{"symbol": "000001.SZ", "target_weight": 1.0, "reference_price": 10.0}]),
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
        config=TargetPortfolioBuildConfig(notional=50_000),
    )

    plan = generate_order_plan(
        portfolio,
        cash=CashSnapshot(available_cash=30_000, frozen_cash=0, total_asset=100_000, market_value=70_000),
        positions=[
            Position("000001.SZ", shares=5_000, available_shares=3_000, cost_price=10.0, last_price=10.0),
            Position("000002.SZ", shares=2_000, available_shares=1_000, cost_price=10.0, last_price=10.0),
        ],
        quotes={
            "000001.SZ": Quote("000001.SZ", open_price=10.0),
            "000002.SZ": Quote("000002.SZ", open_price=10.0),
        },
    )

    assert [(order.side, order.symbol, order.shares) for order in plan.orders] == [
        ("sell", "000002.SZ", 1_000),
    ]


def test_generate_order_plan_keeps_small_sell_orders_for_cleanup() -> None:
    portfolio = build_target_portfolio(
        pd.DataFrame([{"symbol": "000002.SZ", "target_weight": 1.0, "reference_price": 10.0}]),
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
        config=TargetPortfolioBuildConfig(notional=50_000),
    )

    plan = generate_order_plan(
        portfolio,
        cash=CashSnapshot(available_cash=30_000, frozen_cash=0, total_asset=100_000, market_value=70_000),
        positions=[
            Position("000001.SZ", shares=100, available_shares=100, cost_price=10.0, last_price=10.0),
            Position("000002.SZ", shares=5_000, available_shares=5_000, cost_price=10.0, last_price=10.0),
        ],
        quotes={
            "000001.SZ": Quote("000001.SZ", open_price=10.0, bid_price_1=9.99),
            "000002.SZ": Quote("000002.SZ", open_price=10.0),
        },
        config=OrderPlanConfig(min_order_value=3_000),
    )

    assert ("sell", "000001.SZ", 100) in [(order.side, order.symbol, order.shares) for order in plan.orders]


def test_generate_order_plan_rounds_fallback_limit_price_to_valid_tick() -> None:
    portfolio = build_target_portfolio(
        pd.DataFrame([{"symbol": "000001.SZ", "target_weight": 1.0, "reference_price": 10.0}]),
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
        config=TargetPortfolioBuildConfig(notional=50_000),
    )

    plan = generate_order_plan(
        portfolio,
        cash=CashSnapshot(available_cash=50_000, frozen_cash=0, total_asset=50_000, market_value=0),
        positions=[],
        quotes={"000001.SZ": Quote("000001.SZ", open_price=10.0, last_price=11.31)},
        config=OrderPlanConfig(buy_limit_bps=30, sell_limit_bps=30, min_order_value=3_000),
    )

    assert len(plan.orders) == 1
    assert plan.orders[0].side == "buy"
    assert plan.orders[0].limit_price == 11.35


def test_generate_order_plan_skips_star_buy_below_200_shares() -> None:
    portfolio = build_target_portfolio(
        pd.DataFrame([{"symbol": "688809.SH", "target_weight": 1.0, "reference_price": 500.0}]),
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
        config=TargetPortfolioBuildConfig(notional=50_000),
    )

    plan = generate_order_plan(
        portfolio,
        cash=CashSnapshot(available_cash=50_000, frozen_cash=0, total_asset=50_000, market_value=0),
        positions=[],
        quotes={"688809.SH": Quote("688809.SH", open_price=500.0, ask_price_1=439.99)},
    )

    assert plan.orders == []
