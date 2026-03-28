"""test_runner.py — StrategyRunner 单元测试"""
from unittest.mock import MagicMock

import pytest

from app.models import SelectionResult, Signal
from app.strategy.runner import StrategyRunner


def _make_mock_strategy(name, signals=None):
    strategy = MagicMock()
    strategy.name = name
    strategy.description = f"Mock {name}"
    if signals is None:
        signals = [Signal(
            date="20250630", strategy=name, ts_code="000651.SZ",
            name="格力电器", action="buy", weight=0.05, score=0.85,
            reason="mock",
        )]
    strategy.run.return_value = SelectionResult(
        date="20250630", strategy=name, signals=signals,
        universe_size=100, after_filter_size=30, top_n=len(signals),
    )
    return strategy


class TestStrategyRunner:
    @pytest.fixture
    def runner(self):
        ds = MagicMock()
        fh = MagicMock()
        bus = MagicMock()
        return StrategyRunner(ds, fh, bus)

    def test_register(self, runner):
        strategy = _make_mock_strategy("test")
        runner.register(strategy)
        assert "test" in runner._strategies

    def test_list_strategies(self, runner):
        runner.register(_make_mock_strategy("s1"))
        runner.register(_make_mock_strategy("s2"))
        listed = runner.list_strategies()
        assert len(listed) == 2
        names = {s["name"] for s in listed}
        assert "s1" in names
        assert "s2" in names

    def test_run_one(self, runner):
        runner.register(_make_mock_strategy("test"))
        result = runner.run_one("test", "20250630")
        assert result is not None
        assert result.strategy == "test"

    def test_run_one_not_registered(self, runner):
        result = runner.run_one("nonexistent", "20250630")
        assert result is None

    def test_run_all(self, runner):
        runner.register(_make_mock_strategy("s1"))
        runner.register(_make_mock_strategy("s2"))
        results = runner.run_all("20250630", parallel=False)
        assert len(results) == 2

    def test_run_all_one_fails(self, runner):
        good = _make_mock_strategy("good")
        bad = MagicMock()
        bad.name = "bad"
        bad.description = "fails"
        bad.run.side_effect = RuntimeError("boom")
        runner.register(good)
        runner.register(bad)
        results = runner.run_all("20250630", parallel=False)
        assert len(results) == 1
        assert results[0].strategy == "good"
