from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

import vortex.cli as cli
import vortex.trade as trade_module
from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend
from vortex.runtime.workspace import Workspace
from vortex.trade import CashSnapshot
from vortex.trade.nav import ensure_nav_binding, record_nav_snapshot, summarize_nav


def test_nav_ledger_records_account_equity_and_benchmark(tmp_path: Path) -> None:
    root = _build_nav_workspace(tmp_path)
    binding = ensure_nav_binding(
        root,
        strategy_name="earnings_forecast_auto",
        strategy_version="baseline_top110_large",
        account_id="99034443",
        initial_equity=1_000_000,
        benchmark="000852.SH",
        start_date="20260501",
    )

    record_nav_snapshot(
        root,
        binding=binding,
        trade_date="20260501",
        cash=CashSnapshot(available_cash=800_000, frozen_cash=0, total_asset=1_000_000, market_value=200_000),
    )
    payload = record_nav_snapshot(
        root,
        binding=binding,
        trade_date="20260502",
        cash=CashSnapshot(available_cash=810_000, frozen_cash=0, total_asset=1_020_000, market_value=210_000),
    )

    assert payload["net_value"] == 1.02
    status = summarize_nav(root, binding.run_id)
    assert status["summary"]["snapshot_count"] == 2
    assert abs(status["summary"]["windows"]["since_inception"]["strategy_return"] - 0.02) < 1e-12
    assert status["summary"]["windows"]["since_inception"]["excess_return"] is not None


def test_nav_ledger_uses_subledger_offset_when_account_has_extra_cash(tmp_path: Path) -> None:
    root = _build_nav_workspace(tmp_path)
    binding = ensure_nav_binding(
        root,
        strategy_name="earnings_forecast_auto",
        strategy_version="baseline_top110_large",
        account_id="99034443",
        initial_equity=1_000_000,
        benchmark="000852.SH",
        start_date="20260501",
    )

    first = record_nav_snapshot(
        root,
        binding=binding,
        trade_date="20260501",
        cash=CashSnapshot(available_cash=9_800_000, frozen_cash=0, total_asset=10_000_000, market_value=200_000),
    )
    second = record_nav_snapshot(
        root,
        binding=binding,
        trade_date="20260502",
        cash=CashSnapshot(available_cash=9_810_000, frozen_cash=0, total_asset=10_020_000, market_value=210_000),
    )

    status = summarize_nav(root, binding.run_id)
    assert first["net_value"] == 1.0
    assert first["external_cash_offset"] == 9_000_000
    assert second["net_value"] == 1.02
    assert status["binding"]["external_cash_offset"] == 9_000_000
    assert status["summary"]["latest_total_asset"] == 1_020_000
    assert status["summary"]["latest_account_total_asset"] == 10_020_000


def test_nav_ledger_migrates_legacy_account_level_rows(tmp_path: Path) -> None:
    root = _build_nav_workspace(tmp_path)
    binding = ensure_nav_binding(
        root,
        strategy_name="earnings_forecast_auto",
        strategy_version="baseline_top110_large",
        account_id="99034443",
        initial_equity=1_000_000,
        benchmark="000852.SH",
        start_date="20260501",
    )
    ledger_path = root / "trade" / "nav" / f"{binding.run_id}.csv"
    legacy = pd.DataFrame(
        [
            {
                "trade_date": "20260501",
                "run_id": binding.run_id,
                "strategy_name": binding.strategy_name,
                "strategy_version": binding.strategy_version,
                "account_id": binding.account_id,
                "total_asset": 10_000_000.0,
                "available_cash": 9_800_000.0,
                "frozen_cash": 0.0,
                "market_value": 200_000.0,
                "net_value": 10.0,
                "benchmark": binding.benchmark,
                "benchmark_close": 1000.0,
            }
        ]
    )
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    legacy.to_csv(ledger_path, index=False)

    payload = record_nav_snapshot(
        root,
        binding=binding,
        trade_date="20260502",
        cash=CashSnapshot(available_cash=9_810_000, frozen_cash=0, total_asset=10_020_000, market_value=210_000),
    )

    assert payload["external_cash_offset"] == 9_000_000
    assert payload["net_value"] == 1.02
    status = summarize_nav(root, binding.run_id)
    assert status["summary"]["snapshot_count"] == 2
    assert abs(status["summary"]["windows"]["since_inception"]["strategy_return"] - 0.02) < 1e-12


def test_cmd_trade_nav_snapshot_uses_qmt_cash(monkeypatch, tmp_path: Path, capsys) -> None:
    root = _build_nav_workspace(tmp_path)

    class _FakeAdapter:
        def __init__(self, config):
            self.config = config

        def get_cash(self):
            return CashSnapshot(
                available_cash=900_000,
                frozen_cash=0,
                total_asset=1_050_000,
                market_value=150_000,
            )

    monkeypatch.setattr(trade_module, "QmtBridgeAdapter", _FakeAdapter)

    cli.cmd_trade(
        argparse.Namespace(
            trade_action="nav",
            trade_nav_action="snapshot",
            root=str(root),
            strategy_name="earnings_forecast_auto",
            strategy_version="baseline_top110_large",
            run_id=None,
            initial_equity=1_000_000,
            benchmark="000852.SH",
            start_date="20260501",
            trade_date="20260502",
            qmt_bridge_url="http://bridge.local:8000",
            qmt_bridge_token="token",
            qmt_account_id="99034443",
            reset=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["net_value"] == 1.0
    assert payload["external_cash_offset"] == 50_000
    assert Path(payload["ledger_path"]).exists()


def _build_nav_workspace(tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    Workspace(root).initialize()
    storage = ParquetDuckDBBackend(root / "data")
    storage.initialize()
    frame = pd.DataFrame(
        [
            {"date": "20260501", "symbol": "000852.SH", "close": 1000.0},
            {"date": "20260502", "symbol": "000852.SH", "close": 1010.0},
        ]
    )
    for date, group in frame.groupby("date"):
        storage.upsert("index_daily", group, {"date": str(date)})
    return root
