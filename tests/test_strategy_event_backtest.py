from __future__ import annotations

import pandas as pd
import pytest

from vortex.research.goal_review import ExperimentQuality, GoalCriteria
from vortex.strategy.event_backtest import EventBacktestConfig, run_event_signal_backtest


def _event_signal_and_returns():
    dates = pd.Index(["20240102", "20240103", "20240104", "20240105"])
    symbols = pd.Index(["A", "B"])
    signal = pd.DataFrame(index=dates, columns=symbols, dtype=float)
    signal.loc["20240103", "A"] = 1.0
    signal.loc["20240104", "B"] = 1.0
    returns = pd.DataFrame(0.0, index=dates, columns=symbols)
    returns.loc["20240103", "A"] = 0.10
    returns.loc["20240104", "B"] = 0.05
    returns.loc["20240105", "B"] = -0.02
    return signal, returns


def test_event_signal_backtest_uses_signal_on_same_trade_date():
    signal, returns = _event_signal_and_returns()

    result = run_event_signal_backtest(
        signal,
        returns,
        EventBacktestConfig(top_n=1, max_weight=1.0, transaction_cost_bps=0),
    )

    assert result.metrics.total_return > 0
    assert result.weights.loc["20240103", "A"] == pytest.approx(1.0)
    assert result.weights.loc["20240104", "B"] == pytest.approx(1.0)


def test_event_signal_backtest_market_gate_forces_cash():
    signal, returns = _event_signal_and_returns()
    gate = pd.Series(False, index=signal.index)

    result = run_event_signal_backtest(
        signal,
        returns,
        EventBacktestConfig(top_n=1, max_weight=1.0, transaction_cost_bps=0),
        market_gate=gate,
    )

    assert result.metrics.total_return == pytest.approx(0.0)
    assert result.weights.sum(axis=1).max() == pytest.approx(0.0)


def test_event_signal_backtest_capped_with_cash_keeps_unallocated_cash():
    dates = pd.Index(["20240102", "20240103"])
    symbols = pd.Index(["A", "B"])
    signal = pd.DataFrame({"A": [1.0, 1.0], "B": [0.5, 0.5]}, index=dates)
    returns = pd.DataFrame(0.0, index=dates, columns=symbols)

    result = run_event_signal_backtest(
        signal,
        returns,
        EventBacktestConfig(
            top_n=2,
            max_weight=0.30,
            target_exposure=1.0,
            transaction_cost_bps=0,
            position_mode="capped_with_cash",
        ),
    )

    assert result.weights.loc["20240103", "A"] == pytest.approx(0.30)
    assert result.weights.loc["20240103", "B"] == pytest.approx(0.30)
    assert result.weights.loc["20240103"].sum() == pytest.approx(0.60)


def test_event_signal_backtest_full_equal_selected_stays_fully_invested():
    dates = pd.Index(["20240102", "20240103"])
    symbols = pd.Index(["A", "B"])
    signal = pd.DataFrame({"A": [1.0, 1.0], "B": [0.5, 0.5]}, index=dates)
    returns = pd.DataFrame(0.0, index=dates, columns=symbols)

    result = run_event_signal_backtest(
        signal,
        returns,
        EventBacktestConfig(
            top_n=2,
            max_weight=0.30,
            target_exposure=1.0,
            transaction_cost_bps=0,
            position_mode="full_equal_selected",
        ),
    )

    assert result.weights.loc["20240103", "A"] == pytest.approx(0.50)
    assert result.weights.loc["20240103", "B"] == pytest.approx(0.50)
    assert result.weights.loc["20240103"].sum() == pytest.approx(1.0)


def test_event_signal_backtest_blocked_buy_prevents_new_position():
    signal, returns = _event_signal_and_returns()
    blocked_buy = pd.DataFrame(False, index=signal.index, columns=signal.columns)
    blocked_buy.loc["20240103", "A"] = True

    result = run_event_signal_backtest(
        signal,
        returns,
        EventBacktestConfig(top_n=1, max_weight=1.0, transaction_cost_bps=0),
        blocked_buy_mask=blocked_buy,
    )

    assert result.weights.loc["20240103", "A"] == pytest.approx(0.0)


def test_event_signal_backtest_blocked_sell_keeps_position_without_excess_exposure():
    signal, returns = _event_signal_and_returns()
    blocked_sell = pd.DataFrame(False, index=signal.index, columns=signal.columns)
    blocked_sell.loc["20240104", "A"] = True

    result = run_event_signal_backtest(
        signal,
        returns,
        EventBacktestConfig(top_n=1, max_weight=1.0, transaction_cost_bps=0),
        blocked_sell_mask=blocked_sell,
    )

    assert result.weights.loc["20240104", "A"] == pytest.approx(1.0)
    assert result.weights.loc["20240104"].sum() == pytest.approx(1.0)


def test_event_signal_backtest_cost_reduces_return():
    signal, returns = _event_signal_and_returns()

    no_cost = run_event_signal_backtest(
        signal,
        returns,
        EventBacktestConfig(top_n=1, max_weight=1.0, transaction_cost_bps=0),
    )
    high_cost = run_event_signal_backtest(
        signal,
        returns,
        EventBacktestConfig(top_n=1, max_weight=1.0, transaction_cost_bps=100),
    )

    assert high_cost.metrics.total_return < no_cost.metrics.total_return


def test_event_signal_backtest_includes_goal_review():
    signal, returns = _event_signal_and_returns()

    result = run_event_signal_backtest(
        signal,
        returns,
        EventBacktestConfig(top_n=1, max_weight=1.0, transaction_cost_bps=0),
        quality=ExperimentQuality(
            pit_safe=True,
            adjusted_prices=True,
            cost_included=True,
            no_future_leakage=True,
            out_of_sample_checked=True,
        ),
        goal_criteria=GoalCriteria(min_annual_return=0.01, max_drawdown_floor=-0.50),
    )

    assert result.goal_review.status == "achieved"


def test_event_signal_backtest_rejects_invalid_config():
    signal, returns = _event_signal_and_returns()

    with pytest.raises(ValueError, match="top_n"):
        run_event_signal_backtest(signal, returns, EventBacktestConfig(top_n=0))

    with pytest.raises(ValueError, match="target_exposure"):
        run_event_signal_backtest(signal, returns, EventBacktestConfig(target_exposure=0))

    with pytest.raises(ValueError, match="position_mode"):
        run_event_signal_backtest(
            signal,
            returns,
            EventBacktestConfig(position_mode="bad-mode"),  # type: ignore[arg-type]
        )
