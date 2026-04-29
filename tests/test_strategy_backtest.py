from __future__ import annotations

import pandas as pd
import pytest

from vortex.research.goal_review import ExperimentQuality, GoalCriteria
from vortex.strategy.backtest import BacktestConfig, run_cross_sectional_backtest
from vortex.strategy.reports import compare_backtest_reports, write_backtest_report_html, write_backtest_report_json


def _price_and_signal(days: int = 80, symbols: int = 20):
    dates = pd.date_range("2020-01-01", periods=days, freq="B")
    cols = [f"S{i:03d}" for i in range(symbols)]
    strength = pd.Series(range(symbols), index=cols, dtype=float)
    close = pd.DataFrame(100.0, index=dates, columns=cols)
    for idx in range(1, days):
        ret = -0.0005 + strength / strength.max() * 0.003
        close.iloc[idx] = close.iloc[idx - 1] * (1.0 + ret)
    signal = pd.DataFrame([strength.values] * days, index=dates, columns=cols)
    return signal, close


def test_cross_sectional_backtest_selects_top_names():
    signal, close = _price_and_signal()
    result = run_cross_sectional_backtest(
        signal,
        close,
        BacktestConfig(top_n=5, rebalance_every=5, max_weight=0.20, transaction_cost_bps=0),
    )
    assert result.metrics.total_return > 0
    assert result.metrics.annual_return > 0
    assert result.metrics.max_drawdown == pytest.approx(0.0)
    assert set(result.weights.iloc[-1][result.weights.iloc[-1] > 0].index) == {
        "S015", "S016", "S017", "S018", "S019"
    }


def test_transaction_cost_reduces_return():
    signal, close = _price_and_signal()
    no_cost = run_cross_sectional_backtest(
        signal,
        close,
        BacktestConfig(top_n=5, rebalance_every=5, max_weight=0.20, transaction_cost_bps=0),
    )
    high_cost = run_cross_sectional_backtest(
        signal,
        close,
        BacktestConfig(top_n=5, rebalance_every=5, max_weight=0.20, transaction_cost_bps=50),
    )
    assert high_cost.metrics.total_return < no_cost.metrics.total_return


def test_target_exposure_keeps_cash():
    signal, close = _price_and_signal()
    result = run_cross_sectional_backtest(
        signal,
        close,
        BacktestConfig(
            top_n=5,
            rebalance_every=5,
            max_weight=0.20,
            target_exposure=0.25,
            transaction_cost_bps=0,
        ),
    )

    assert result.weights.sum(axis=1).max() == pytest.approx(0.25)
    assert result.metrics.total_return > 0


def test_invalid_backtest_inputs_fail_closed():
    signal, close = _price_and_signal()
    with pytest.raises(ValueError, match="top_n"):
        run_cross_sectional_backtest(signal, close, BacktestConfig(top_n=0))

    with pytest.raises(ValueError, match="target_exposure"):
        run_cross_sectional_backtest(signal, close, BacktestConfig(target_exposure=0))


def test_backtest_result_includes_goal_review():
    signal, close = _price_and_signal()
    result = run_cross_sectional_backtest(
        signal,
        close,
        BacktestConfig(top_n=5, rebalance_every=5, max_weight=0.20, transaction_cost_bps=0),
    )

    payload = result.to_dict()
    assert payload["goal_review"]["status"] == "invalid_experiment"
    assert "missing_quality_check" in payload["goal_review"]["failures"]


def test_backtest_goal_review_can_be_achieved_with_quality_and_custom_criteria():
    signal, close = _price_and_signal()
    result = run_cross_sectional_backtest(
        signal,
        close,
        BacktestConfig(top_n=5, rebalance_every=5, max_weight=0.20, transaction_cost_bps=0),
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
    assert result.to_dict()["goal_review"]["status"] == "achieved"


def test_backtest_report_writers_and_compare(tmp_path):
    signal, close = _price_and_signal()
    fast = run_cross_sectional_backtest(
        signal,
        close,
        BacktestConfig(top_n=5, rebalance_every=5, max_weight=0.20, transaction_cost_bps=0),
    )
    slow = run_cross_sectional_backtest(
        signal,
        close,
        BacktestConfig(top_n=3, rebalance_every=20, max_weight=0.20, transaction_cost_bps=0),
    )

    json_path = write_backtest_report_json(fast, tmp_path / "backtest_report.json", strategy_name="demo")
    html_path = write_backtest_report_html(fast, tmp_path / "backtest_report.html", strategy_name="demo")
    comparison = compare_backtest_reports({"fast": fast, "slow": slow})

    assert '"schema": "vortex.backtest_report.v1"' in json_path.read_text(encoding="utf-8")
    assert "demo 回测报告" in html_path.read_text(encoding="utf-8")
    assert {row["name"] for row in comparison} == {"fast", "slow"}
