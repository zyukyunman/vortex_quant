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
    run_opening_auction_execution_review,
    run_opening_liquidity_review,
    run_earnings_forecast_live_handoff,
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


def test_load_earnings_forecast_inputs_keeps_pre_start_financial_risk(tmp_path):
    root = _build_earnings_workspace(tmp_path)
    workspace = Workspace(root)

    inputs = load_earnings_forecast_inputs(
        workspace,
        start="20260101",
        end="20260310",
        require_precise_data=True,
    )

    assert inputs.st_risk_events is not None
    risk_rows = inputs.st_risk_events.loc[inputs.st_risk_events["symbol"] == "000002.SZ"]
    assert not risk_rows.empty
    assert risk_rows["date"].min() == "20260101"


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


def test_run_earnings_forecast_live_handoff_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    def fake_transport(method, url, payload=None, headers=None):  # noqa: ARG001
        if url == "/api/meta/health":
            return {"status": "ok", "message": "healthy"}
        if url == "/api/meta/connection_status":
            return {"data": {"connected": True, "connected_at": "2026-05-01 09:30:00"}}
        if url == "/api/trading/asset?account_id=99034443":
            return {"data": {"available_cash": 10_000_000.0, "total_asset": 10_000_000.0, "market_value": 0.0}}
        if url == "/api/trading/positions?account_id=99034443":
            return {"data": [{"symbol": "000001.SZ", "volume": 10_000, "can_use_volume": 10_000, "avg_price": 10.0}]}
        if url == "/api/trading/orders?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/trades?account_id=99034443":
            return {"data": []}
        raise AssertionError(f"unexpected request: {method} {url}")

    artifacts = run_earnings_forecast_live_handoff(
        root,
        start="20260101",
        as_of="20260310",
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        output_dir=root / "strategy" / "handoff",
        artifact_dir=root / "strategy" / "artifacts",
        label="handoff-test",
        bridge_transport=fake_transport,
    )

    assert artifacts.json_path.exists()
    assert artifacts.html_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["qmt_ready"] is True
    assert payload["bridge_snapshot"]["cash"]["available_cash"] == 10_000_000.0
    assert payload["target_holding_count"] > 0


def test_run_earnings_forecast_live_handoff_tolerates_known_connection_status_bug(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    def fake_transport(method, url, payload=None, headers=None):  # noqa: ARG001
        if url == "/api/meta/health":
            return {"status": "ok", "message": "healthy"}
        if url == "/api/meta/connection_status":
            return {"data": {"connected": False, "error": "'xtquant.datacenter.IPythonApiClient' object has no attribute 'get_connect_status'"}}
        if url == "/api/trading/asset?account_id=99034443":
            return {"data": {"available_cash": 10_000_000.0, "total_asset": 10_000_000.0, "market_value": 0.0}}
        if url == "/api/trading/positions?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/orders?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/trades?account_id=99034443":
            return {"data": []}
        raise AssertionError(f"unexpected request: {method} {url}")

    artifacts = run_earnings_forecast_live_handoff(
        root,
        start="20260101",
        as_of="20260310",
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        output_dir=root / "strategy" / "handoff",
        artifact_dir=root / "strategy" / "artifacts",
        label="handoff-known-bug",
        bridge_transport=fake_transport,
    )

    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["qmt_ready"] is True
    assert "connection_status_warning" in payload["bridge_snapshot"]
    assert payload["blocking_reasons"] == []


def test_cmd_strategy_live_handoff_outputs_json(tmp_path, capsys, monkeypatch):
    from types import SimpleNamespace

    import vortex.strategy.earnings_forecast_runner as runner

    root = _build_earnings_workspace(tmp_path)

    class FakeQmtBridgeAdapter:
        def __init__(self, config, transport=None):  # noqa: ARG002
            self.config = config

        def health(self):
            return SimpleNamespace(ok=True, message="ok")

        def connection_status(self):
            return {"connected": True}

        def get_cash(self):
            return SimpleNamespace(available_cash=1_000_000.0, frozen_cash=0.0, total_asset=1_200_000.0, market_value=200_000.0)

        def get_positions(self):
            return []

        def get_orders(self):
            return []

        def get_fills(self):
            return []

    monkeypatch.setattr(runner, "QmtBridgeAdapter", FakeQmtBridgeAdapter)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="live-handoff",
            root=str(root),
            start="20260101",
            as_of="20260310",
            qmt_bridge_url="http://bridge.local:8000",
            qmt_bridge_token="token",
            qmt_account_id="99034443",
            output_dir=str(root / "strategy" / "handoff"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-handoff",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-handoff"
    assert payload["qmt_ready"] is True
    assert payload["json_path"].endswith(f"cli-handoff-{payload['as_of']}.json")


def test_run_opening_liquidity_review_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)
    opening_path = _build_opening_snapshot_csv(root / "strategy" / "opening_snapshots.csv")

    artifacts = run_opening_liquidity_review(
        root,
        opening_snapshot_path=opening_path,
        start="20260101",
        end="20260310",
        output_dir=root / "strategy" / "opening-review",
        label="opening-test",
        top_n_values=(2,),
        position_modes=("full_equal_selected",),
        portfolio_notional=100_000.0,
    )

    assert artifacts.json_path.exists()
    assert artifacts.csv_path.exists()
    assert artifacts.md_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["label"] == "opening-test"
    assert payload["summary"]


def test_cmd_strategy_opening_liquidity_review_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)
    opening_path = _build_opening_snapshot_csv(root / "strategy" / "opening_snapshots.csv")

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="opening-liquidity-review",
            root=str(root),
            start="20260101",
            end="20260310",
            opening_snapshots=str(opening_path),
            output_dir=str(root / "strategy" / "opening-review"),
            label="cli-opening",
            top_n_values="2",
            position_modes="full_equal_selected",
            portfolio_notional=100_000.0,
            capped_max_weight=0.05,
            volume_unit="shares",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-opening"
    assert payload["json_path"].endswith("cli-opening.json")
    assert (root / "strategy" / "opening-review" / "cli-opening.json").exists()



def test_run_opening_auction_execution_review_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)
    opening_path = _build_opening_snapshot_csv(root / "strategy" / "auction_snapshots.csv")

    artifacts = run_opening_auction_execution_review(
        root,
        opening_snapshot_path=opening_path,
        start="20260101",
        end="20260310",
        output_dir=root / "strategy" / "auction-review",
        artifact_dir=root / "strategy" / "artifacts",
        label="auction-test",
        top_n=2,
        position_mode="full_equal_selected",
        portfolio_notional=100_000.0,
    )

    assert artifacts.json_path.exists()
    assert artifacts.html_path.exists()
    assert artifacts.holdings_path.exists()
    assert artifacts.trades_path.exists()
    assert artifacts.order_intents_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["strategy"] == "earnings_forecast_drift"
    assert payload["metadata"]["execution"] == "opening_auction_all_or_nothing"
    assert payload["metadata"]["auction_execution_summary"]["buy_order_count"] > 0



def test_cmd_strategy_auction_execution_review_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)
    opening_path = _build_opening_snapshot_csv(root / "strategy" / "auction_snapshots.csv")

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="auction-execution-review",
            root=str(root),
            start="20260101",
            end="20260310",
            opening_snapshots=str(opening_path),
            output_dir=str(root / "strategy" / "auction-review"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-auction",
            top_n=2,
            position_mode="full_equal_selected",
            portfolio_notional=100_000.0,
            capped_max_weight=0.05,
            volume_unit="shares",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-auction"
    assert payload["json_path"].endswith("cli-auction报告.json")
    assert (root / "strategy" / "auction-review" / "cli-auction报告.json").exists()


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
    fina_indicator = pd.DataFrame(
        [
            {
                "symbol": "000002.SZ",
                "ann_date": "20251231",
                "effective_from": "2025-12-31T09:30:00+08:00",
                "bps": -1.0,
                "roe": -5.0,
                "debt_to_assets": 80.0,
                "netprofit_yoy": -20.0,
                "report_date": "20251231",
            }
        ]
    )
    for report_date, group in fina_indicator.groupby("report_date"):
        storage.upsert("fina_indicator", group, {"report_date": str(report_date)})
    return root


def _upsert_by_date(storage: ParquetDuckDBBackend, dataset: str, frame: pd.DataFrame) -> None:
    for date, group in frame.groupby("date"):
        storage.upsert(dataset, group, {"date": str(date)})


def _build_opening_snapshot_csv(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    dates = pd.bdate_range("2026-01-01", periods=48).strftime("%Y%m%d").tolist()
    for date in dates:
        for symbol, price in {"000001.SZ": 10.0, "000002.SZ": 11.0, "000003.SZ": 12.0}.items():
            rows.append(
                {
                    "date": date,
                    "symbol": symbol,
                    "open_price": price,
                    "ask1_price": price,
                    "ask1_volume": 5_000,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path
