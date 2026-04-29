from __future__ import annotations

import pandas as pd
import pytest

from vortex.research.evaluation import FactorAdmissionRule, evaluate_factor, evaluate_factor_batch, forward_returns, rank_ic_series
from vortex.research.reports import publish_signal_snapshot, write_factor_report_json, write_factor_tear_sheet_html


def _monotonic_panel(days: int = 80, symbols: int = 40):
    dates = pd.date_range("2020-01-01", periods=days, freq="B")
    cols = [f"S{i:03d}" for i in range(symbols)]
    quality = pd.Series(range(symbols), index=cols, dtype=float)
    close = pd.DataFrame(100.0, index=dates, columns=cols)
    for idx in range(1, days):
        daily_ret = 0.0002 + quality / quality.max() * 0.002
        close.iloc[idx] = close.iloc[idx - 1] * (1.0 + daily_ret)
    factor = pd.DataFrame([quality.values] * days, index=dates, columns=cols)
    return factor, close


def test_forward_returns_uses_future_prices():
    close = pd.DataFrame({"A": [100.0, 110.0, 121.0]})
    returns = forward_returns(close, [1])[1]
    assert returns.iloc[0, 0] == pytest.approx(0.10)
    assert round(returns.iloc[1, 0], 6) == 0.10


def test_rank_ic_detects_monotonic_factor():
    factor, close = _monotonic_panel()
    future = forward_returns(close, [1])[1]
    ic = rank_ic_series(factor, future, min_periods=20)
    assert ic.mean() > 0.99
    assert (ic > 0).mean() == 1.0


def test_evaluate_factor_reports_positive_long_short():
    factor, close = _monotonic_panel()
    result = evaluate_factor(
        factor,
        close,
        horizons=(1, 5, 20),
        long_short_horizon=5,
        min_periods=20,
    )
    assert result.ic_stats[1].ic_mean > 0.99
    assert result.ic_stats[5].positive_rate == 1.0
    assert result.long_short.long_short_mean > 0
    assert result.long_short.count > 0


def test_factor_batch_ranks_and_filters_candidates():
    factor, close = _monotonic_panel()
    weak = -factor

    candidates = evaluate_factor_batch(
        {"strong": factor, "weak": weak},
        close,
        horizons=(1,),
        long_short_horizon=1,
        min_periods=20,
        admission_rule=FactorAdmissionRule(min_ic_mean=0.5, min_positive_rate=0.9),
    )

    assert candidates[0].name == "strong"
    assert candidates[0].admitted
    assert not candidates[1].admitted


def test_factor_report_and_signal_snapshot_writers(tmp_path):
    factor, close = _monotonic_panel()
    result = evaluate_factor(factor, close, horizons=(1,), min_periods=20)

    report = write_factor_report_json(result, tmp_path / "research_report.json", factor_name="quality")
    html = write_factor_tear_sheet_html(result, tmp_path / "tear_sheet.html", factor_name="quality")
    snapshot = publish_signal_snapshot(factor.head(2), tmp_path / "signal_snapshot.json", signal_name="quality")

    assert '"schema": "vortex.research_report.v1"' in report.read_text(encoding="utf-8")
    assert "quality 因子评测" in html.read_text(encoding="utf-8")
    assert '"schema": "vortex.signal_snapshot.v1"' in snapshot.read_text(encoding="utf-8")
