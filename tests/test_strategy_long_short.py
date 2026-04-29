from __future__ import annotations

import pandas as pd
import pytest

from vortex.research.goal_review import ExperimentQuality, GoalCriteria
from vortex.strategy.long_short import LongShortConfig, run_cross_sectional_long_short_backtest


def _price_and_signal(days: int = 80, symbols: int = 20):
    dates = pd.date_range("2020-01-01", periods=days, freq="B")
    cols = [f"S{i:03d}" for i in range(symbols)]
    strength = pd.Series(range(symbols), index=cols, dtype=float)
    close = pd.DataFrame(100.0, index=dates, columns=cols)
    for idx in range(1, days):
        ret = -0.001 + strength / strength.max() * 0.004
        close.iloc[idx] = close.iloc[idx - 1] * (1.0 + ret)
    signal = pd.DataFrame([strength.values] * days, index=dates, columns=cols)
    return signal, close


def test_long_short_backtest_constructs_positive_and_negative_weights():
    signal, close = _price_and_signal()
    result = run_cross_sectional_long_short_backtest(
        signal,
        close,
        LongShortConfig(long_n=5, short_n=5, rebalance_every=5, transaction_cost_bps=0),
    )

    last = result.weights.iloc[-1]
    assert last.gt(0).sum() == 5
    assert last.lt(0).sum() == 5
    assert last.sum() == pytest.approx(0.0)
    assert last.abs().sum() == pytest.approx(1.0)
    assert result.metrics.annual_return > 0


def test_long_short_market_gate_can_force_cash():
    signal, close = _price_and_signal()
    gate = pd.Series(False, index=close.index)

    result = run_cross_sectional_long_short_backtest(
        signal,
        close,
        LongShortConfig(long_n=5, short_n=5, rebalance_every=5, transaction_cost_bps=0),
        market_gate=gate,
    )

    assert result.weights.abs().sum(axis=1).max() == pytest.approx(0.0)
    assert result.metrics.total_return == pytest.approx(0.0)


def test_long_short_goal_review_can_be_achieved_with_custom_criteria():
    signal, close = _price_and_signal()
    result = run_cross_sectional_long_short_backtest(
        signal,
        close,
        LongShortConfig(long_n=5, short_n=5, rebalance_every=5, transaction_cost_bps=0),
        quality=ExperimentQuality(
            pit_safe=True,
            adjusted_prices=True,
            cost_included=True,
            no_future_leakage=True,
            out_of_sample_checked=True,
        ),
        goal_criteria=GoalCriteria(min_annual_return=0.01, max_drawdown_floor=-0.50),
    )

    assert result.goal_review.achieved


def test_long_short_invalid_config_fails_closed():
    signal, close = _price_and_signal()
    with pytest.raises(ValueError, match="gross_exposure"):
        run_cross_sectional_long_short_backtest(signal, close, LongShortConfig(gross_exposure=0))
