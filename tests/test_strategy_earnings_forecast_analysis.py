from __future__ import annotations

import json

import pandas as pd
import pytest

from vortex.strategy.earnings_forecast_analysis import (
    analyze_launch_months,
    compound_return,
    evaluate_forecast_surprise_factor,
    max_drawdown,
    write_factor_evaluation_report_json,
    write_launch_month_report_html,
    write_launch_month_report_json,
)


def test_compound_return_and_drawdown():
    returns = pd.Series([0.10, -0.05, 0.02])

    assert compound_return(returns) == pytest.approx((1.10 * 0.95 * 1.02) - 1)
    assert max_drawdown(returns) == pytest.approx(-0.05)


def test_analyze_launch_months_groups_by_start_month():
    dates = pd.Index(["20240102", "20240103", "20240201", "20240202", "20250102", "20250203"])
    returns = pd.Series([0.01, 0.02, -0.01, 0.03, 0.04, -0.02], index=dates)
    exposure = pd.Series([1, 1, 0, 1, 1, 0], index=dates)
    holding_count = pd.Series([10, 10, 0, 8, 12, 0], index=dates)

    summary, detail = analyze_launch_months(
        returns,
        exposure=exposure,
        holding_count=holding_count,
        horizons=(2,),
    )

    assert set(summary["start_month"]) == {1, 2}
    assert len(detail) == 4
    january = summary.loc[summary["start_month"] == 1].iloc[0]
    assert january["observations"] == 2
    assert january["win_rate_2d"] == pytest.approx(1.0)
    assert "avg_holding_count_to_year_end" in summary.columns


def test_launch_month_report_writers(tmp_path):
    summary = pd.DataFrame(
        {
            "start_month": [1],
            "observations": [1],
            "win_rate_to_year_end": [1.0],
            "avg_return_to_year_end": [0.2],
        }
    )
    detail = pd.DataFrame({"year": [2024], "start_month": [1], "return_to_year_end": [0.2]})

    json_path = write_launch_month_report_json(summary, detail, tmp_path / "launch.json")
    html_path = write_launch_month_report_html(summary, detail, tmp_path / "launch.html")

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["summary"][0]["start_month"] == 1
    assert "启动月份" in html_path.read_text(encoding="utf-8")


def test_evaluate_forecast_surprise_factor_reports_positive_ic(tmp_path):
    dates = pd.Index(pd.date_range("2024-01-02", periods=80, freq="B").strftime("%Y%m%d"))
    symbols = pd.Index([f"S{i:03d}" for i in range(40)])
    quality = pd.Series(range(40), index=symbols, dtype=float)
    close = pd.DataFrame(100.0, index=dates, columns=symbols)
    for idx in range(1, len(dates)):
        close.iloc[idx] = close.iloc[idx - 1] * (1.0 + quality / quality.max() * 0.002)
    events = []
    for event_date in dates[::5][:10]:
        for symbol in symbols:
            score = quality.loc[symbol]
            events.append(
                {
                    "ann_date": event_date,
                    "symbol": symbol,
                    "type": "预增" if score >= 20 else "预减",
                    "p_change_min": float(score * 5 - 100),
                    "p_change_max": float(score * 5 - 80),
                }
            )
    forecast = pd.DataFrame(events)

    result = evaluate_forecast_surprise_factor(
        forecast,
        close,
        horizons=(1, 5, 20),
        long_short_horizon=5,
        min_periods=20,
    )
    output = write_factor_evaluation_report_json(
        result,
        tmp_path / "factor.json",
        factor_name="forecast_surprise",
    )

    assert result.ic_stats[1].ic_mean > 0.9
    assert result.ic_stats[5].positive_rate == 1.0
    assert result.long_short.long_short_mean > 0
    assert json.loads(output.read_text(encoding="utf-8"))["factor"] == "forecast_surprise"
