from __future__ import annotations

import pandas as pd

from vortex.strategy.evaluation_protocol import compute_strategy_protocol_metrics


def test_compute_strategy_protocol_metrics_reports_tail_and_path_metrics():
    dates = pd.bdate_range("2026-01-01", periods=30).strftime("%Y%m%d")
    returns = pd.Series(
        [
            0.01,
            0.02,
            -0.03,
            -0.02,
            0.01,
            -0.04,
            0.03,
            0.01,
            -0.01,
            0.02,
            *([0.005] * 20),
        ],
        index=dates,
    )
    equity = (1.0 + returns).cumprod()
    equity.iloc[0] = 1.0

    metrics = compute_strategy_protocol_metrics(equity, returns)

    assert metrics.sortino != 0.0
    assert metrics.cvar_5pct < 0.0
    assert metrics.worst_5d_return < 0.0
    assert metrics.worst_20d_return < 0.0
    assert metrics.max_drawdown_duration_days > 0
    assert isinstance(metrics.max_drawdown_recovered, bool)
    assert 0.0 <= metrics.positive_month_rate <= 1.0
    assert 0.0 <= metrics.annual_win_rate <= 1.0


def test_compute_strategy_protocol_metrics_handles_duplicate_equity_dates():
    index = ["20260102", "20260102", "20260105", "20260106", "20260107", "20260108"]
    returns = pd.Series([0.0, 0.03, -0.05, -0.04, 0.02, 0.08], index=index)
    equity = (1.0 + returns).cumprod()

    metrics = compute_strategy_protocol_metrics(equity, returns)

    assert metrics.max_drawdown_duration_days == 2
    assert metrics.max_drawdown_recovery_days == 2
    assert metrics.max_drawdown_recovered is True
