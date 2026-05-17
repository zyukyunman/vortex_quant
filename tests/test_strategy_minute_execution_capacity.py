"""Target-price minute execution capacity tests."""
from __future__ import annotations

import pandas as pd

from vortex.strategy.minute_execution_capacity import (
    TargetPriceCapacityConfig,
    analyze_target_price_minute_capacity,
    build_target_price_buy_share_limits,
)


def test_target_price_capacity_sums_only_minutes_that_touch_price():
    orders = pd.DataFrame(
        [
            {
                "date": "20250102",
                "symbol": "000001.SZ",
                "side": "buy",
                "requested_shares": 1000,
                "requested_notional": 10_000.0,
                "open_price": 10.0,
            }
        ]
    )
    minutes = pd.DataFrame(
        [
            {
                "date": "20250102",
                "symbol": "000001.SZ",
                "open": 9.8,
                "high": 9.9,
                "low": 9.7,
                "close": 9.85,
                "amount": 1_000_000.0,
            },
            {
                "date": "20250102",
                "symbol": "000001.SZ",
                "open": 10.1,
                "high": 10.2,
                "low": 9.95,
                "close": 10.0,
                "amount": 50_000.0,
            },
        ]
    )

    report = analyze_target_price_minute_capacity(
        orders,
        minutes,
        config=TargetPriceCapacityConfig(participation_rates=(0.10,)),
    )

    row = report.order_level.iloc[0]
    assert row["target_price_matched_amount"] == 50_000.0
    assert row["available_notional"] == 5_000.0
    assert bool(row["target_notional_feasible"]) is False
    assert row["filled_notional"] == 5_000.0


def test_target_price_capacity_marks_higher_participation_feasible():
    orders = pd.DataFrame(
        [
            {
                "date": "20250102",
                "symbol": "000001.SZ",
                "side": "buy",
                "requested_shares": 1000,
                "requested_notional": 10_000.0,
                "open_price": 10.0,
            }
        ]
    )
    minutes = pd.DataFrame(
        [
            {
                "date": "20250102",
                "symbol": "000001.SZ",
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "amount": 50_000.0,
            }
        ]
    )

    report = analyze_target_price_minute_capacity(
        orders,
        minutes,
        config=TargetPriceCapacityConfig(participation_rates=(0.10, 0.30)),
    )

    by_rate = report.order_level.set_index("participation_rate")
    assert bool(by_rate.loc[0.10, "target_notional_feasible"]) is False
    assert bool(by_rate.loc[0.30, "target_notional_feasible"]) is True
    assert bool(by_rate.loc[0.30, "target_share_feasible"]) is True


def test_target_price_capacity_summarizes_by_strategy_group():
    orders = pd.DataFrame(
        [
            {
                "variant": "top80",
                "date": "20250102",
                "symbol": "000001.SZ",
                "side": "buy",
                "requested_shares": 1000,
                "requested_notional": 10_000.0,
                "open_price": 10.0,
            },
            {
                "variant": "top80",
                "date": "20250102",
                "symbol": "000002.SZ",
                "side": "buy",
                "requested_shares": 1000,
                "requested_notional": 10_000.0,
                "open_price": 10.0,
            },
        ]
    )
    minutes = pd.DataFrame(
        [
            {
                "date": "20250102",
                "symbol": "000001.SZ",
                "open": 10.0,
                "high": 10.1,
                "low": 9.9,
                "close": 10.0,
                "amount": 100_000.0,
            },
            {
                "date": "20250102",
                "symbol": "000002.SZ",
                "open": 9.0,
                "high": 9.5,
                "low": 8.9,
                "close": 9.2,
                "amount": 100_000.0,
            },
        ]
    )

    report = analyze_target_price_minute_capacity(
        orders,
        minutes,
        config=TargetPriceCapacityConfig(participation_rates=(0.20,)),
        group_fields=("variant",),
    )

    summary = report.summary.iloc[0]
    assert summary["variant"] == "top80"
    assert summary["buy_order_count"] == 2
    assert summary["target_price_touch_rate"] == 0.5
    assert summary["filled_notional_ratio"] == 0.5


def test_build_target_price_buy_share_limits_for_active_target_days():
    weights = pd.DataFrame(
        {"000001.SZ": [0.1, 0.1], "000002.SZ": [0.0, 0.1]},
        index=["20250102", "20250103"],
    )
    prices = pd.DataFrame(
        {"000001.SZ": [10.0, 10.0], "000002.SZ": [20.0, 20.0]},
        index=["20250102", "20250103"],
    )
    minutes = pd.DataFrame(
        [
            {
                "date": "20250102",
                "symbol": "000001.SZ",
                "open": 10.0,
                "high": 10.1,
                "low": 9.9,
                "close": 10.0,
                "amount": 100_000.0,
            },
            {
                "date": "20250103",
                "symbol": "000001.SZ",
                "open": 11.0,
                "high": 11.2,
                "low": 10.8,
                "close": 11.0,
                "amount": 100_000.0,
            },
            {
                "date": "20250103",
                "symbol": "000002.SZ",
                "open": 20.0,
                "high": 20.2,
                "low": 19.8,
                "close": 20.0,
                "amount": 100_000.0,
            },
        ]
    )

    limits = build_target_price_buy_share_limits(
        weights,
        prices,
        minutes,
        participation_rate=0.2,
    )

    assert limits.loc["20250102", "000001.SZ"] == 2000
    assert limits.loc["20250102", "000002.SZ"] == 0
    assert limits.loc["20250103", "000001.SZ"] == 0
    assert limits.loc["20250103", "000002.SZ"] == 1000
