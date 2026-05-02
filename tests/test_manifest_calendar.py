"""Manifest / Calendar 控制面测试。"""
from __future__ import annotations

from datetime import date

import pandas as pd

from vortex.data.calendar import DataCalendar
from vortex.data.manifest import SyncManifest
from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend


class MockProvider:
    @property
    def name(self) -> str:
        return "mock"

    @property
    def supported_markets(self) -> list[str]:
        return ["cn_stock"]

    @property
    def dataset_registry(self) -> dict[str, dict[str, object]]:
        return {
            "instruments": {"api": "mock_instruments", "description": "标的列表", "phase": "1A"},
            "calendar": {"api": "mock_calendar", "description": "交易日历", "phase": "1A"},
            "bars": {"api": "mock_bars", "description": "行情数据", "phase": "1A"},
        }

    def resolve_dataset(self, dataset: str) -> str:
        return dataset

    def smoke_test(self) -> bool:
        return True

    def fetch_instruments(self, market: str) -> pd.DataFrame:
        return pd.DataFrame()

    def fetch_calendar(self, market: str, start: date, end: date) -> list[date]:
        days = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                days.append(current)
            current = current.replace(day=current.day + 1) if current.day < 28 else date(
                current.year, current.month + 1 if current.month < 12 else 1, 1
            )
        return sorted(days)

    def fetch_bars(self, market, symbols, freq, start, end) -> pd.DataFrame:
        return pd.DataFrame()

    def fetch_fundamental(self, market, symbols, fields, start, end) -> pd.DataFrame:
        return pd.DataFrame()

    def fetch_events(self, market, symbols, start, end) -> pd.DataFrame:
        return pd.DataFrame()

    def fetch_dataset(
        self,
        dataset: str,
        market: str,
        start: date,
        end: date,
        *,
        symbols: list[str] | None = None,
        trading_days: list[date] | None = None,
    ) -> pd.DataFrame:
        if dataset == "calendar":
            days = self.fetch_calendar(market, start, end)
            return pd.DataFrame({"cal_date": [d.strftime("%Y%m%d") for d in days]})
        return pd.DataFrame()


class TestSyncManifest:
    def test_create_and_get_run(self, tmp_path):
        m = SyncManifest(tmp_path / "manifest.db")
        m.create_run("run_001", "test", "bootstrap")
        run = m.get_run("run_001")
        assert run is not None
        assert run["profile"] == "test"
        assert run["status"] in ("pending", "running")

    def test_update_status(self, tmp_path):
        m = SyncManifest(tmp_path / "manifest.db")
        m.create_run("run_002", "test", "update")
        m.update_status("run_002", "success", total_rows=5000)
        run = m.get_run("run_002")
        assert run["status"] == "success"
        assert run["total_rows"] == 5000

    def test_get_latest_run(self, tmp_path):
        m = SyncManifest(tmp_path / "manifest.db")
        m.create_run("run_a", "p1", "bootstrap")
        m.update_status("run_a", "success")
        m.create_run("run_b", "p1", "update")
        m.update_status("run_b", "success")
        latest = m.get_latest_run("p1")
        assert latest["run_id"] == "run_b"

    def test_get_latest_by_action(self, tmp_path):
        m = SyncManifest(tmp_path / "manifest.db")
        m.create_run("r1", "p1", "bootstrap")
        m.update_status("r1", "success")
        m.create_run("r2", "p1", "update")
        m.update_status("r2", "success")
        latest_bootstrap = m.get_latest_run("p1", action="bootstrap")
        assert latest_bootstrap["run_id"] == "r1"

    def test_nonexistent_run(self, tmp_path):
        m = SyncManifest(tmp_path / "manifest.db")
        assert m.get_run("no_such_run") is None

    def test_record_and_query_partition_coverages(self, tmp_path):
        m = SyncManifest(tmp_path / "manifest.db")
        m.create_run("run_cov", "p1", "bootstrap")
        m.record_partition_coverage(
            run_id="run_cov",
            dataset="fundamental",
            partition_key="end_date",
            partition_value="20260331",
            as_of_end="2026-04-08",
            status="pit_blocked",
            source_rows=3,
            materialized_rows=0,
        )
        m.record_partition_coverage(
            run_id="run_cov",
            dataset="fundamental",
            partition_key="end_date",
            partition_value="20260630",
            as_of_end="2026-04-08",
            status="materialized",
            source_rows=10,
            materialized_rows=10,
        )

        covered = m.list_partition_coverages(
            dataset="fundamental",
            partition_key="end_date",
            as_of_end="2026-04-08",
            statuses=("pit_blocked", "source_empty"),
        )

        assert covered == {"20260331"}

    def test_historical_partition_coverages_only_reuse_matured_empty_partitions(self, tmp_path):
        m = SyncManifest(tmp_path / "manifest.db")
        m.create_run("run_cov_hist", "p1", "bootstrap")
        m.record_partition_coverage(
            run_id="run_cov_hist",
            dataset="bars",
            partition_key="date",
            partition_value="20260401",
            as_of_end="2026-04-05",
            status="source_empty",
        )
        m.record_partition_coverage(
            run_id="run_cov_hist",
            dataset="bars",
            partition_key="date",
            partition_value="20260405",
            as_of_end="2026-04-05",
            status="source_empty",
        )

        covered = m.list_historical_partition_coverages(
            dataset="bars",
            partition_key="date",
            as_of_end="2026-04-06",
            statuses=("source_empty",),
            require_as_of_after_partition=True,
        )

        assert covered == {"20260401"}


class TestDataCalendar:
    def test_load_from_provider(self, tmp_path):
        backend = ParquetDuckDBBackend(tmp_path / "data")
        backend.initialize()
        provider = MockProvider()
        cal = DataCalendar(storage=backend, provider=provider)
        days = cal.load_or_fetch("cn_stock", date(2026, 4, 1), date(2026, 4, 10))
        assert len(days) > 0
        assert all(isinstance(d, date) for d in days)

    def test_is_trading_day(self, tmp_path):
        backend = ParquetDuckDBBackend(tmp_path / "data")
        backend.initialize()
        provider = MockProvider()
        cal = DataCalendar(storage=backend, provider=provider)
        cal.load_or_fetch("cn_stock", date(2026, 4, 1), date(2026, 4, 10))
        assert cal.is_trading_day(date(2026, 4, 1))
        assert not cal.is_trading_day(date(2026, 4, 4))
