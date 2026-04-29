from __future__ import annotations

import argparse
import json

import pandas as pd

import vortex.cli as cli
from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend
from vortex.runtime.workspace import Workspace
from vortex.strategy.earnings_forecast_drift import EarningsForecastDriftConfig
from vortex.strategy.earnings_forecast_runner import (
    load_earnings_forecast_inputs,
    run_earnings_forecast_shadow_plan,
    run_precise_earnings_forecast_review,
)


def test_load_earnings_forecast_inputs_filters_pre_start_events(tmp_path):
    root = _build_earnings_workspace(tmp_path)
    workspace = Workspace(root)

    inputs = load_earnings_forecast_inputs(
        workspace,
        start="20260101",
        end="20260310",
        require_precise_data=True,
    )

    assert set(inputs.forecast["ann_date"]) == {"20260205"}
    assert inputs.stk_limit is not None
    assert inputs.suspend_events is not None


def test_run_precise_earnings_forecast_review_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    artifacts = run_precise_earnings_forecast_review(
        root,
        start="20260101",
        end="20260310",
        output_dir=root / "strategy" / "reports",
        artifact_dir=root / "strategy" / "artifacts",
        label="test-review",
        config=EarningsForecastDriftConfig(hold_days=5, top_n=2),
        cost_grid=(20.0,),
        segments=(),
    )

    assert artifacts.json_path.exists()
    assert artifacts.html_path.exists()
    assert artifacts.holdings_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["strategy"] == "earnings_forecast_drift"
    assert payload["metadata"]["tradability_review"]["data_missing"] == []
    assert payload["metadata"]["safe_3pct_result"]["metrics"]["annual_return"] is not None
    assert "metrics" in artifacts.summary


def test_cmd_strategy_precise_review_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="precise-review",
            root=str(root),
            start="20260101",
            end="20260310",
            output_dir=str(root / "strategy" / "reports"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-review",
            costs="20",
            portfolio_notional=100_000_000,
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-review"
    assert payload["json_path"].endswith("cli-review报告.json")
    assert (root / "strategy" / "reports" / "cli-review报告.json").exists()


def test_run_earnings_forecast_shadow_plan_writes_target_files(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    artifacts = run_earnings_forecast_shadow_plan(
        root,
        start="20260101",
        as_of="20260310",
        output_dir=root / "strategy" / "shadow",
        artifact_dir=root / "strategy" / "artifacts",
        label="shadow-test",
        config=EarningsForecastDriftConfig(hold_days=5, top_n=2),
    )

    assert artifacts.json_path.exists()
    assert artifacts.html_path.exists()
    assert artifacts.target_path.exists()
    target = pd.read_csv(artifacts.target_path)
    assert {"date", "symbol", "weight", "prev_weight", "trade_delta", "action"} <= set(target.columns)
    assert artifacts.summary["requested_as_of"] == "20260310"


def _build_earnings_workspace(tmp_path):
    root = tmp_path / "workspace"
    Workspace(root).initialize()
    storage = ParquetDuckDBBackend(root / "data")
    storage.initialize()
    dates = pd.bdate_range("2026-01-01", periods=48).strftime("%Y%m%d").tolist()
    symbols = ["000001.SZ", "000002.SZ", "000003.SZ"]
    bars_rows = []
    for idx, date in enumerate(dates):
        for symbol_idx, symbol in enumerate(symbols):
            open_price = 10.0 + symbol_idx
            daily_return = 0.002
            if symbol == "000001.SZ" and 26 <= idx <= 32:
                daily_return = 0.03
            bars_rows.append(
                {
                    "date": date,
                    "symbol": symbol,
                    "open": open_price,
                    "close": open_price * (1.0 + daily_return),
                    "amount": 100_000.0,
                }
            )
    _upsert_by_date(storage, "bars", pd.DataFrame(bars_rows))
    index_rows = []
    for idx, date in enumerate(dates):
        for symbol in ["000300.SH", "000905.SH", "000852.SH"]:
            index_rows.append(
                {
                    "date": date,
                    "symbol": symbol,
                    "close": 1000.0 + idx * 5,
                }
            )
    _upsert_by_date(storage, "index_daily", pd.DataFrame(index_rows))
    forecast = pd.DataFrame(
        [
            {
                "symbol": "000003.SZ",
                "ann_date": "20251231",
                "type": "预增",
                "p_change_min": 300.0,
                "p_change_max": 400.0,
                "report_date": "20251231",
            },
            {
                "symbol": "000001.SZ",
                "ann_date": "20260205",
                "type": "预增",
                "p_change_min": 120.0,
                "p_change_max": 180.0,
                "report_date": "20260331",
            },
        ]
    )
    for report_date, group in forecast.groupby("report_date"):
        storage.upsert("forecast", group, {"report_date": str(report_date)})
    limit_rows = [
        {
            "date": date,
            "symbol": symbol,
            "up_limit": 99.0,
            "down_limit": 1.0,
        }
        for date in dates
        for symbol in symbols
    ]
    _upsert_by_date(storage, "stk_limit", pd.DataFrame(limit_rows))
    suspend = pd.DataFrame(
        [
            {
                "date": dates[0],
                "symbol": "000002.SZ",
                "suspend_type": "R",
                "suspend_timing": None,
            }
        ]
    )
    _upsert_by_date(storage, "suspend_d", suspend)
    return root


def _upsert_by_date(storage: ParquetDuckDBBackend, dataset: str, frame: pd.DataFrame) -> None:
    for date, group in frame.groupby("date"):
        storage.upsert(dataset, group, {"date": str(date)})
