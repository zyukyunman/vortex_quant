"""test_backtest.py — 回测引擎单元测试"""
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from vortex.executor.backtest import BacktestEngine, BacktestResult


class TestBacktestResult:
    def test_summary(self):
        result = BacktestResult(
            nav_series=pd.Series([1.0, 1.01]),
            returns_series=pd.Series([0, 0.01]),
            positions_history=[],
            rebalance_dates=["20250131"],
            metrics={
                "total_return": 0.05,
                "annual_return": 0.10,
                "annual_volatility": 0.15,
                "sharpe_ratio": 0.67,
                "max_drawdown": 0.05,
                "max_dd_peak_date": "20250115",
                "max_dd_trough_date": "20250210",
                "max_dd_recovery_date": "20250305",
                "calmar_ratio": 2.0,
                "avg_turnover": 0.3,
                "win_rate": 0.55,
                "start_date": "20250101",
                "end_date": "20250630",
                "n_rebalance": 1,
            },
        )
        text = result.summary()
        assert "回测绩效报告" in text
        assert "夏普" in text

    def test_empty_metrics(self):
        result = BacktestResult(
            nav_series=pd.Series(),
            returns_series=pd.Series(),
            positions_history=[],
            rebalance_dates=[],
            metrics={},
        )
        text = result.summary()
        assert "回测" in text


class TestBacktestEngineMetrics:
    def test_calc_metrics(self):
        nav = pd.Series([1000000, 1010000, 1005000, 1020000, 1030000])
        returns = pd.Series([0, 0.01, -0.005, 0.015, 0.01])
        metrics = BacktestEngine._calc_metrics(
            nav, returns, ["20250131"], [0.2],
        )
        assert "total_return" in metrics
        assert "sharpe_ratio" in metrics
        assert "max_drawdown" in metrics
        assert "max_dd_peak_date" in metrics
        assert "max_dd_trough_date" in metrics
        assert "max_dd_recovery_date" in metrics
        assert "sortino_ratio" in metrics
        assert "profit_factor" in metrics
        assert "max_dd_days" in metrics
        assert metrics["total_return"] == pytest.approx(0.03, abs=1e-6)
        # Sortino 应 >= 0
        assert metrics["sortino_ratio"] >= 0
        # Profit factor: sum(gains)/sum(losses)
        assert metrics["profit_factor"] > 0

    def test_calc_turnover(self):
        old = pd.Series({"A": 0.5, "B": 0.5})
        new = pd.Series({"A": 0.3, "C": 0.7})
        turnover = BacktestEngine._calc_turnover(old, new)
        # |0.3-0.5| + |0-0.5| + |0.7-0| = 0.2 + 0.5 + 0.7 = 1.4 / 2 = 0.7
        assert abs(turnover - 0.7) < 1e-6

    def test_calc_metrics_use_initial_capital_base(self):
        nav = pd.Series([900000, 1000000, 1100000])
        returns = pd.Series([0, 0.1111111111, 0.1])
        metrics = BacktestEngine._calc_metrics(
            nav,
            returns,
            ["20250131"],
            [0.2],
            initial_capital=1_000_000,
        )
        assert metrics["initial_capital"] == pytest.approx(1_000_000)
        assert metrics["start_nav"] == pytest.approx(900000)
        assert metrics["end_nav"] == pytest.approx(1100000)
        assert metrics["total_return"] == pytest.approx(0.10, abs=1e-6)
