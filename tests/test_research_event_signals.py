from __future__ import annotations

import pandas as pd
import pytest

from vortex.research.event_signals import (
    build_express_quality_signal,
    build_forecast_surprise_signal,
    build_hot_rank_signal,
    build_limit_strength_signal,
    build_top_list_signal,
)


def test_build_limit_strength_signal_applies_delay_and_liquidity_mask():
    dates = pd.Index(["20240102", "20240103", "20240104"])
    symbols = pd.Index(["A", "B"])
    events = pd.DataFrame(
        {
            "date": ["20240102", "20240102"],
            "symbol": ["A", "B"],
            "fd_amount": [100.0, 20.0],
            "float_mv": [1000.0, 1000.0],
            "open_times": [0, 2],
            "limit_times": [2, 1],
            "turnover_ratio": [5.0, 10.0],
        }
    )
    liquidity = pd.DataFrame(True, index=dates, columns=symbols)
    liquidity.loc["20240102", "B"] = False

    signal = build_limit_strength_signal(
        events,
        target_index=dates,
        target_columns=symbols,
        liquidity_mask=liquidity,
        delay_days=1,
    )

    assert pd.isna(signal.loc["20240102", "A"])
    assert signal.loc["20240103", "A"] > 0
    assert pd.isna(signal.loc["20240103", "B"])


def test_build_top_list_signal_can_use_zero_delay_for_intraday_feed():
    dates = pd.Index(["20240102", "20240103"])
    symbols = pd.Index(["A", "B"])
    events = pd.DataFrame(
        {
            "date": ["20240102", "20240102"],
            "symbol": ["A", "B"],
            "net_amount": [100.0, -20.0],
            "amount": [1000.0, 1000.0],
            "net_rate": [10.0, -2.0],
            "pct_change": [8.0, 3.0],
        }
    )

    signal = build_top_list_signal(
        events,
        target_index=dates,
        target_columns=symbols,
        delay_days=0,
    )

    assert signal.loc["20240102", "A"] > signal.loc["20240102", "B"]


def test_event_signal_rejects_negative_delay():
    with pytest.raises(ValueError, match="delay_days"):
        build_top_list_signal(
            pd.DataFrame({"date": ["20240102"], "symbol": ["A"]}),
            target_index=pd.Index(["20240102"]),
            target_columns=pd.Index(["A"]),
            delay_days=-1,
        )


def test_build_forecast_surprise_signal_maps_ann_date_to_next_trade_day():
    dates = pd.Index(["20240105", "20240108", "20240109"])
    symbols = pd.Index(["A", "B"])
    events = pd.DataFrame(
        {
            "ann_date": ["20240105", "20240105"],
            "symbol": ["A", "B"],
            "type": ["预增", "预减"],
            "p_change_min": [100.0, -50.0],
            "p_change_max": [150.0, -30.0],
        }
    )
    liquidity = pd.DataFrame(True, index=dates, columns=symbols)
    liquidity.loc["20240108", "B"] = False

    signal = build_forecast_surprise_signal(
        events,
        target_index=dates,
        target_columns=symbols,
        liquidity_mask=liquidity,
        delay_days=1,
        hold_days=2,
    )

    assert pd.isna(signal.loc["20240105", "A"])
    assert signal.loc["20240108", "A"] > 0
    assert pd.isna(signal.loc["20240108", "B"])
    assert signal.loc["20240109", "A"] == signal.loc["20240108", "A"]


def test_build_forecast_surprise_signal_handles_non_trading_ann_date():
    dates = pd.Index(["20240105", "20240108"])
    symbols = pd.Index(["A"])
    events = pd.DataFrame(
        {
            "ann_date": ["20240106"],
            "symbol": ["A"],
            "type": ["扭亏"],
            "p_change_min": [None],
            "p_change_max": [None],
        }
    )

    signal = build_forecast_surprise_signal(
        events,
        target_index=dates,
        target_columns=symbols,
        delay_days=1,
    )

    assert signal.loc["20240108", "A"] > 0


def test_build_express_quality_signal_uses_growth_roe_and_eps():
    dates = pd.Index(["20240102", "20240103"])
    symbols = pd.Index(["A", "B"])
    events = pd.DataFrame(
        {
            "ann_date": ["20240102", "20240102"],
            "symbol": ["A", "B"],
            "yoy_net_profit": [120.0, -20.0],
            "diluted_roe": [15.0, 2.0],
            "diluted_eps": [1.2, 0.1],
        }
    )

    signal = build_express_quality_signal(
        events,
        target_index=dates,
        target_columns=symbols,
        delay_days=1,
    )

    assert signal.loc["20240103", "A"] > signal.loc["20240103", "B"]


def test_build_hot_rank_signal_delays_after_close_hot_list():
    dates = pd.Index(["20240102", "20240103"])
    symbols = pd.Index(["A", "B"])
    events = pd.DataFrame(
        {
            "date": ["20240102", "20240102"],
            "symbol": ["A", "B"],
            "rank": [1, 20],
            "hot": [100.0, 10.0],
            "pct_change": [3.0, 1.0],
        }
    )

    signal = build_hot_rank_signal(
        events,
        target_index=dates,
        target_columns=symbols,
        delay_days=1,
    )

    assert pd.isna(signal.loc["20240102", "A"])
    assert signal.loc["20240103", "A"] > signal.loc["20240103", "B"]


def test_financial_event_signal_rejects_non_positive_hold_days():
    with pytest.raises(ValueError, match="hold_days"):
        build_forecast_surprise_signal(
            pd.DataFrame({"ann_date": ["20240102"], "symbol": ["A"]}),
            target_index=pd.Index(["20240102"]),
            target_columns=pd.Index(["A"]),
            hold_days=0,
        )
