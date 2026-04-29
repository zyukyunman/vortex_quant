from __future__ import annotations

import pandas as pd
import pytest

from vortex.research.goal_review import ExperimentQuality, GoalCriteria
from vortex.strategy.technical_pool import (
    TechnicalPoolConfig,
    run_quarterly_pool_technical_backtest,
)


def _panel(days: int = 90, symbols: int = 80):
    dates = pd.date_range("2020-01-01", periods=days, freq="B")
    cols = [f"S{i:03d}" for i in range(symbols)]
    strength = pd.Series(range(symbols), index=cols, dtype=float)
    close = pd.DataFrame(100.0, index=dates, columns=cols)
    for idx in range(1, days):
        ret = -0.001 + strength / strength.max() * 0.004
        close.iloc[idx] = close.iloc[idx - 1] * (1.0 + ret)
    high = close * 1.01
    low = close * 0.99
    selection = pd.DataFrame([strength.values] * days, index=dates, columns=cols)
    return selection, close, high, low


def test_technical_pool_selects_quarterly_pool_and_holds_limit():
    selection, close, high, low = _panel()
    result = run_quarterly_pool_technical_backtest(
        selection,
        close,
        high,
        low,
        TechnicalPoolConfig(
            selection_size=50,
            max_holdings=30,
            selection_every=30,
            rebalance_every=5,
            min_buy_momentum=0.0,
            min_resistance_room=-1.0,
            transaction_cost_bps=0,
            max_weight=1 / 30,
        ),
    )

    assert result.metrics.annual_return > 0
    assert result.weights.gt(0).sum(axis=1).max() <= 30
    first_selection = next(iter(result.selections.values()))
    assert len(first_selection) == 50
    assert "S079" in first_selection


def test_technical_pool_keeps_current_holding_outside_new_top_pool():
    selection, close, high, low = _panel(days=120)
    selection.iloc[:60, :] = list(range(80))
    selection.iloc[60:, :] = list(reversed(range(80)))

    result = run_quarterly_pool_technical_backtest(
        selection,
        close,
        high,
        low,
        TechnicalPoolConfig(
            selection_size=50,
            max_holdings=30,
            selection_every=60,
            rebalance_every=5,
            min_buy_momentum=-1.0,
            sell_momentum=-1.0,
            min_resistance_room=-1.0,
            transaction_cost_bps=0,
        ),
    )

    after_second_selection = result.weights.iloc[70]
    assert after_second_selection.get("S079", 0.0) > 0


def test_technical_pool_invalid_config_fail_closed():
    selection, close, high, low = _panel()
    with pytest.raises(ValueError, match="max_holdings"):
        run_quarterly_pool_technical_backtest(
            selection,
            close,
            high,
            low,
            TechnicalPoolConfig(selection_size=10, max_holdings=30),
        )


def test_technical_pool_market_gate_can_force_cash():
    selection, close, high, low = _panel()
    market_gate = pd.Series(False, index=close.index)

    result = run_quarterly_pool_technical_backtest(
        selection,
        close,
        high,
        low,
        TechnicalPoolConfig(
            selection_size=50,
            max_holdings=30,
            selection_every=30,
            rebalance_every=5,
            min_buy_momentum=-1.0,
            min_resistance_room=-1.0,
            transaction_cost_bps=0,
        ),
        market_gate=market_gate,
    )

    assert result.weights.sum(axis=1).max() == pytest.approx(0.0)
    assert result.metrics.total_return == pytest.approx(0.0)


def test_technical_pool_can_rank_entries_without_hard_support_trigger():
    selection, close, high, low = _panel()

    result = run_quarterly_pool_technical_backtest(
        selection,
        close,
        high,
        low,
        TechnicalPoolConfig(
            selection_size=50,
            max_holdings=30,
            selection_every=30,
            rebalance_every=5,
            min_buy_momentum=0.0,
            min_resistance_room=10.0,
            require_technical_entry=False,
            transaction_cost_bps=0,
            max_weight=1 / 30,
        ),
    )

    assert result.weights.gt(0).sum(axis=1).max() == 30


def test_technical_pool_result_includes_goal_review():
    selection, close, high, low = _panel()

    result = run_quarterly_pool_technical_backtest(
        selection,
        close,
        high,
        low,
        TechnicalPoolConfig(
            selection_size=50,
            max_holdings=30,
            selection_every=30,
            rebalance_every=5,
            min_buy_momentum=0.0,
            min_resistance_room=-1.0,
            transaction_cost_bps=0,
        ),
        quality=ExperimentQuality(
            pit_safe=True,
            adjusted_prices=True,
            cost_included=True,
            no_future_leakage=True,
            out_of_sample_checked=True,
        ),
        goal_criteria=GoalCriteria(min_annual_return=0.01, max_drawdown_floor=-0.50),
    )

    assert result.to_dict()["goal_review"]["status"] == "achieved"
