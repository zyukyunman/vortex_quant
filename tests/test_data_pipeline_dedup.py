"""DataPipeline 去重 / 续跑 / 恢复测试。"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd
import pytest

import vortex.data.pipeline as pipeline_module
from vortex.config.profile.models import DataProfile
from vortex.data.manifest import SyncManifest
from vortex.data.pipeline import DataPipeline
from vortex.data.quality.engine import QualityEngine
from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend
from vortex.shared.errors import DataError


class GenericDatasetProvider:
    """验证 pipeline 走通用 fetch_dataset 入口，而不是继续写死 bars/fundamental。"""

    @property
    def name(self) -> str:
        return "generic"

    @property
    def supported_markets(self) -> list[str]:
        return ["cn_stock"]

    @property
    def dataset_registry(self) -> dict[str, dict[str, object]]:
        return {
            "valuation": {
                "api": "daily_basic",
                "description": "估值指标",
                "phase": "1B",
                "partition_by": "date",
            },
        }

    def resolve_dataset(self, dataset: str) -> str:
        return dataset

    def smoke_test(self) -> bool:
        return True

    def fetch_instruments(self, market: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "symbol": ["600519.SH"],
                "name": ["贵州茅台"],
                "list_date": ["20010827"],
                "delist_date": [None],
                "industry": ["白酒"],
                "market_cap": [None],
            }
        )

    def fetch_calendar(self, market: str, start: date, end: date) -> list[date]:
        return [date(2026, 4, 1)]

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
        if dataset == "valuation":
            return pd.DataFrame(
                {
                    "symbol": ["600519.SH"],
                    "date": ["20260401"],
                    "pe": [22.5],
                    "pb": [8.1],
                }
            )
        raise AssertionError(f"unexpected dataset: {dataset}")


class TestGenericDatasetPipeline:
    def test_bootstrap_writes_non_core_dataset_via_fetch_dataset(self, tmp_path):
        provider = GenericDatasetProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )

        profile = DataProfile(
            name="default",
            datasets=["valuation"],
            history_start="20260401",
        )

        report = pipeline.bootstrap(profile)

        assert report.status == "success"
        valuation = storage.read("valuation")
        assert len(valuation) == 1
        assert valuation.iloc[0]["symbol"] == "600519.SH"
        assert valuation.iloc[0]["pe"] == 22.5

    def test_bootstrap_logs_dataset_elapsed_time(self, tmp_path, caplog):
        provider = GenericDatasetProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )

        profile = DataProfile(
            name="default",
            datasets=["valuation"],
            history_start="20260401",
        )

        caplog.set_level(logging.INFO)
        report = pipeline.bootstrap(profile)

        assert report.status == "success"
        assert "dataset=valuation 完成: fetch_elapsed=" in caplog.text
        assert "write_elapsed=" in caplog.text
        assert "total_elapsed=" in caplog.text

    def test_bootstrap_marks_manifest_cancelled_when_cancel_check_trips(self, tmp_path):
        provider = GenericDatasetProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
            cancel_check=lambda: True,
        )

        profile = DataProfile(
            name="default",
            datasets=["valuation"],
            history_start="20260401",
        )

        with pytest.raises(DataError) as exc_info:
            pipeline.bootstrap(profile, run_id="cancel_test_run")

        assert exc_info.value.code == "DATA_TASK_CANCELLED"
        run = manifest.get_run("cancel_test_run")
        assert run is not None
        assert run["status"] == "cancelled"

    def test_bootstrap_skips_symbol_once_dataset_after_exact_range_scanned(self, tmp_path):
        class EventsRangeProvider(GenericDatasetProvider):
            def __init__(self) -> None:
                self.calls = 0

            @property
            def dataset_registry(self) -> dict[str, dict[str, object]]:
                return {
                    "events": {
                        "api": "dividend",
                        "description": "分红事件",
                        "phase": "1A",
                        "fetch_mode": "symbol_once",
                        "partition_by": "date",
                    },
                }

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
                assert dataset == "events"
                self.calls += 1
                return pd.DataFrame(
                    {
                        "symbol": ["600519.SH", "600519.SH"],
                        "date": ["20240201", "20240301"],
                        "cash_div": [1.0, 1.5],
                    }
                )

        provider = EventsRangeProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )

        profile = DataProfile(
            name="default",
            datasets=["events"],
            history_start="20240101",
        )

        first = pipeline.repair(
            profile,
            (date(2024, 1, 1), date(2024, 4, 8)),
            action="bootstrap",
        )
        second = pipeline.repair(
            profile,
            (date(2024, 1, 1), date(2024, 4, 8)),
            action="bootstrap",
        )

        exact_range = manifest.list_partition_coverages(
            dataset="events",
            partition_key="__range__",
            as_of_end="2024-04-08",
            statuses=("range_complete",),
        )

        assert first.status == "success"
        assert second.status == "success"
        assert provider.calls == 1
        assert exact_range == {"20240101:20240408"}

    def test_bootstrap_skips_existing_trade_day_partitions(self, tmp_path, caplog):
        class ResumeAwareProvider(GenericDatasetProvider):
            def __init__(self) -> None:
                self.seen_trading_days: list[str] = []

            @property
            def dataset_registry(self) -> dict[str, dict[str, object]]:
                return {
                    "valuation": {
                        "api": "daily_basic",
                        "description": "估值指标",
                        "phase": "1B",
                        "fetch_mode": "trade_day_all",
                        "partition_by": "date",
                    },
                }

            def fetch_calendar(self, market: str, start: date, end: date) -> list[date]:
                return [date(2026, 4, 1), date(2026, 4, 2), date(2026, 4, 3)]

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
                assert dataset == "valuation"
                self.seen_trading_days = [
                    day.strftime("%Y%m%d") for day in (trading_days or [])
                ]
                return pd.DataFrame(
                    {
                        "symbol": ["600519.SH"] * len(self.seen_trading_days),
                        "date": self.seen_trading_days,
                        "pe": [20.0 + idx for idx, _ in enumerate(self.seen_trading_days, start=1)],
                        "pb": [8.0] * len(self.seen_trading_days),
                    }
                )

        provider = ResumeAwareProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        storage.upsert(
            "valuation",
            pd.DataFrame(
                {
                    "symbol": ["600519.SH"],
                    "date": ["20260401"],
                    "pe": [19.0],
                    "pb": [8.0],
                }
            ),
            {"date": "20260401"},
        )
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )

        profile = DataProfile(
            name="default",
            datasets=["valuation"],
            history_start="20260401",
        )

        caplog.set_level(logging.INFO)
        report = pipeline.bootstrap(profile)

        assert report.status == "success"
        assert provider.seen_trading_days == ["20260402", "20260403"]
        valuation = storage.read("valuation")
        assert sorted(str(value) for value in valuation["date"].tolist()) == [
            "20260401",
            "20260402",
            "20260403",
        ]
        assert "dataset=valuation 去重判断: partition_key=date, target_partitions=3, existing_partitions=1, missing_partitions=2" in caplog.text
        assert "dataset=valuation 去重决策: 跳过 1 个已存在分区，沿用 0 个已登记覆盖分区，仅抓取 2 个缺失分区" in caplog.text

    def test_bootstrap_reuses_trade_day_source_empty_coverage(self, tmp_path, caplog):
        class EmptyTradeDayProvider(GenericDatasetProvider):
            def __init__(self) -> None:
                self.calls = 0
                self.seen_trading_days: list[list[str]] = []

            @property
            def dataset_registry(self) -> dict[str, dict[str, object]]:
                return {
                    "valuation": {
                        "api": "daily_basic",
                        "description": "估值指标",
                        "phase": "1B",
                        "fetch_mode": "trade_day_all",
                        "partition_by": "date",
                    },
                }

            def fetch_calendar(self, market: str, start: date, end: date) -> list[date]:
                return [date(2026, 4, 1), date(2026, 4, 2)]

            def fetch_dataset(
                self,
                dataset: str,
                market: str,
                start: date,
                end: date,
                *,
                symbols: list[str] | None = None,
                trading_days: list[date] | None = None,
                partition_values: list[str] | None = None,
            ) -> pd.DataFrame:
                assert dataset == "valuation"
                self.calls += 1
                self.seen_trading_days.append(
                    [day.strftime("%Y%m%d") for day in (trading_days or [])]
                )
                return pd.DataFrame(columns=["symbol", "date", "pe", "pb"])

        provider = EmptyTradeDayProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )
        profile = DataProfile(
            name="default",
            datasets=["valuation"],
            history_start="20260401",
        )

        caplog.set_level(logging.INFO)
        first = pipeline.bootstrap(profile)
        second = pipeline.bootstrap(profile)
        coverage = manifest.list_partition_coverages(
            dataset="valuation",
            partition_key="date",
            as_of_end="2026-04-02",
            statuses=("source_empty",),
        )

        assert first.status == "success"
        assert second.status == "success"
        assert provider.calls == 1
        assert provider.seen_trading_days == [["20260401", "20260402"]]
        assert coverage == {"20260401", "20260402"}
        assert "dataset=valuation 去重决策: 目标日期分区已全部存在或已登记覆盖" in caplog.text

    def test_repair_reuses_historical_source_empty_coverage_across_days(self, tmp_path, caplog):
        class HistoricalEmptyTradeDayProvider(GenericDatasetProvider):
            def __init__(self) -> None:
                self.calls = 0
                self.seen_trading_days: list[list[str]] = []

            @property
            def dataset_registry(self) -> dict[str, dict[str, object]]:
                return {
                    "valuation": {
                        "api": "daily_basic",
                        "description": "估值指标",
                        "phase": "1B",
                        "fetch_mode": "trade_day_all",
                        "partition_by": "date",
                    },
                }

            def fetch_calendar(self, market: str, start: date, end: date) -> list[date]:
                return [date(2026, 4, 1), date(2026, 4, 2)]

            def fetch_dataset(
                self,
                dataset: str,
                market: str,
                start: date,
                end: date,
                *,
                symbols: list[str] | None = None,
                trading_days: list[date] | None = None,
                partition_values: list[str] | None = None,
            ) -> pd.DataFrame:
                assert dataset == "valuation"
                self.calls += 1
                self.seen_trading_days.append(
                    [day.strftime("%Y%m%d") for day in (trading_days or [])]
                )
                return pd.DataFrame(columns=["symbol", "date", "pe", "pb"])

        provider = HistoricalEmptyTradeDayProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )
        profile = DataProfile(
            name="default",
            datasets=["valuation"],
            history_start="20260401",
        )

        caplog.set_level(logging.INFO)
        first = pipeline.repair(
            profile,
            (date(2026, 4, 1), date(2026, 4, 5)),
            action="repair",
        )
        second = pipeline.repair(
            profile,
            (date(2026, 4, 1), date(2026, 4, 6)),
            action="repair",
        )

        assert first.status == "success"
        assert second.status == "success"
        assert provider.calls == 1
        assert provider.seen_trading_days == [["20260401", "20260402"]]
        assert "dataset=valuation 去重覆盖: partition_key=date, covered_partitions=2" in caplog.text
        assert "dataset=valuation 去重决策: 目标日期分区已全部存在或已登记覆盖" in caplog.text

    def test_bootstrap_retries_recent_source_empty_for_index_loop_range(self, tmp_path, caplog):
        class RetryRecentIndexProvider(GenericDatasetProvider):
            def __init__(self) -> None:
                self.calls = 0
                self.requested_partition_values: list[list[str]] = []

            @property
            def dataset_registry(self) -> dict[str, dict[str, object]]:
                return {
                    "index_daily": {
                        "api": "index_daily",
                        "description": "指数日线",
                        "phase": "1B",
                        "fetch_mode": "index_loop_range",
                        "partition_by": "date",
                        "source_empty_retry_recent_days": 1,
                    },
                }

            def fetch_calendar(self, market: str, start: date, end: date) -> list[date]:
                return [date(2026, 4, 1), date(2026, 4, 2)]

            def fetch_dataset(
                self,
                dataset: str,
                market: str,
                start: date,
                end: date,
                *,
                symbols: list[str] | None = None,
                trading_days: list[date] | None = None,
                partition_values: list[str] | None = None,
            ) -> pd.DataFrame:
                assert dataset == "index_daily"
                self.calls += 1
                self.requested_partition_values.append(list(partition_values or []))
                return pd.DataFrame(columns=["symbol", "date", "close"])

        provider = RetryRecentIndexProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )
        profile = DataProfile(
            name="default",
            datasets=["index_daily"],
            history_start="20260401",
        )

        caplog.set_level(logging.INFO)
        first = pipeline.bootstrap(profile)
        second = pipeline.bootstrap(profile)

        assert first.status == "success"
        assert second.status == "success"
        assert provider.calls == 2
        assert provider.requested_partition_values == [
            ["20260401", "20260402"],
            ["20260402"],
        ]
        assert "dataset=index_daily 去重覆盖: partition_key=date, covered_partitions=1" in caplog.text

    def test_bootstrap_skips_existing_symbol_range_daily_partitions(self, tmp_path, caplog):
        class SymbolRangeResumeProvider(GenericDatasetProvider):
            def __init__(self) -> None:
                self.requested_ranges: list[tuple[str, str]] = []
                self.requested_partition_values: list[list[str] | None] = []

            @property
            def dataset_registry(self) -> dict[str, dict[str, object]]:
                return {
                    "adj_factor": {
                        "api": "adj_factor",
                        "description": "复权因子",
                        "phase": "1B",
                        "fetch_mode": "symbol_range",
                        "partition_by": "date",
                        "date_partition_mode": "trade_day",
                    },
                }

            def fetch_calendar(self, market: str, start: date, end: date) -> list[date]:
                return [date(2026, 4, 1), date(2026, 4, 2), date(2026, 4, 3)]

            def fetch_dataset(
                self,
                dataset: str,
                market: str,
                start: date,
                end: date,
                *,
                symbols: list[str] | None = None,
                trading_days: list[date] | None = None,
                partition_values: list[str] | None = None,
            ) -> pd.DataFrame:
                assert dataset == "adj_factor"
                self.requested_ranges.append(
                    (start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
                )
                self.requested_partition_values.append(list(partition_values or []))
                values = list(partition_values or [])
                return pd.DataFrame(
                    {
                        "symbol": ["600519.SH"] * len(values),
                        "date": values,
                        "adj_factor": [1.0] * len(values),
                    }
                )

        provider = SymbolRangeResumeProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        storage.upsert(
            "adj_factor",
            pd.DataFrame(
                {
                    "symbol": ["600519.SH"],
                    "date": ["20260401"],
                    "adj_factor": [0.98],
                }
            ),
            {"date": "20260401"},
        )
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )

        profile = DataProfile(
            name="default",
            datasets=["adj_factor"],
            history_start="20260401",
        )

        caplog.set_level(logging.INFO)
        report = pipeline.bootstrap(profile)

        assert report.status == "success"
        assert provider.requested_ranges == [("20260402", "20260403")]
        assert provider.requested_partition_values == [["20260402", "20260403"]]
        result = storage.read("adj_factor")
        assert sorted(str(value) for value in result["date"].tolist()) == [
            "20260401",
            "20260402",
            "20260403",
        ]
        assert "dataset=adj_factor 去重判断: partition_key=date, target_partitions=3, existing_partitions=1, missing_partitions=2" in caplog.text

    def test_bootstrap_skips_existing_symbol_range_week_end_partitions(self, tmp_path, caplog):
        class WeeklyResumeProvider(GenericDatasetProvider):
            def __init__(self) -> None:
                self.requested_ranges: list[tuple[str, str]] = []
                self.requested_partition_values: list[list[str] | None] = []

            @property
            def dataset_registry(self) -> dict[str, dict[str, object]]:
                return {
                    "weekly": {
                        "api": "weekly",
                        "description": "周线行情",
                        "phase": "2",
                        "fetch_mode": "symbol_range",
                        "partition_by": "date",
                        "date_partition_mode": "week_end",
                    },
                }

            def fetch_calendar(self, market: str, start: date, end: date) -> list[date]:
                return [
                    date(2026, 4, 1),
                    date(2026, 4, 2),
                    date(2026, 4, 3),
                    date(2026, 4, 8),
                    date(2026, 4, 9),
                    date(2026, 4, 10),
                ]

            def fetch_dataset(
                self,
                dataset: str,
                market: str,
                start: date,
                end: date,
                *,
                symbols: list[str] | None = None,
                trading_days: list[date] | None = None,
                partition_values: list[str] | None = None,
            ) -> pd.DataFrame:
                assert dataset == "weekly"
                self.requested_ranges.append(
                    (start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
                )
                self.requested_partition_values.append(list(partition_values or []))
                values = list(partition_values or [])
                return pd.DataFrame(
                    {
                        "symbol": ["600519.SH"] * len(values),
                        "date": values,
                        "close": [10.0] * len(values),
                    }
                )

        provider = WeeklyResumeProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        storage.upsert(
            "weekly",
            pd.DataFrame(
                {
                    "symbol": ["600519.SH"],
                    "date": ["20260403"],
                    "close": [9.8],
                }
            ),
            {"date": "20260403"},
        )
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )

        profile = DataProfile(
            name="default",
            datasets=["weekly"],
            history_start="20260401",
        )

        caplog.set_level(logging.INFO)
        report = pipeline.bootstrap(profile)

        assert report.status == "success"
        assert provider.requested_ranges == [("20260410", "20260410")]
        assert provider.requested_partition_values == [["20260410"]]
        result = storage.read("weekly")
        assert sorted(str(value) for value in result["date"].tolist()) == [
            "20260403",
            "20260410",
        ]
        assert "dataset=weekly 去重判断: partition_key=date, target_partitions=2, existing_partitions=1, missing_partitions=1" in caplog.text

    def test_bootstrap_skips_existing_index_loop_week_end_partitions(self, tmp_path, caplog):
        class IndexWeightResumeProvider(GenericDatasetProvider):
            def __init__(self) -> None:
                self.requested_ranges: list[tuple[str, str]] = []
                self.requested_partition_values: list[list[str] | None] = []

            @property
            def dataset_registry(self) -> dict[str, dict[str, object]]:
                return {
                    "index_weight": {
                        "api": "index_weight",
                        "description": "指数权重",
                        "phase": "1B",
                        "fetch_mode": "index_loop_range",
                        "partition_by": "date",
                        "date_partition_mode": "week_end",
                    },
                }

            def fetch_calendar(self, market: str, start: date, end: date) -> list[date]:
                return [
                    date(2026, 4, 1),
                    date(2026, 4, 2),
                    date(2026, 4, 3),
                    date(2026, 4, 8),
                    date(2026, 4, 9),
                    date(2026, 4, 10),
                ]

            def fetch_dataset(
                self,
                dataset: str,
                market: str,
                start: date,
                end: date,
                *,
                symbols: list[str] | None = None,
                trading_days: list[date] | None = None,
                partition_values: list[str] | None = None,
            ) -> pd.DataFrame:
                assert dataset == "index_weight"
                self.requested_ranges.append(
                    (start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
                )
                self.requested_partition_values.append(list(partition_values or []))
                values = list(partition_values or [])
                return pd.DataFrame(
                    {
                        "index_code": ["000300.SH"] * len(values),
                        "con_code": ["600519.SH"] * len(values),
                        "weight": [1.0] * len(values),
                        "date": values,
                    }
                )

        provider = IndexWeightResumeProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        storage.upsert(
            "index_weight",
            pd.DataFrame(
                {
                    "index_code": ["000300.SH"],
                    "con_code": ["600519.SH"],
                    "weight": [0.98],
                    "date": ["20260403"],
                }
            ),
            {"date": "20260403"},
        )
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )

        profile = DataProfile(
            name="default",
            datasets=["index_weight"],
            history_start="20260401",
        )

        caplog.set_level(logging.INFO)
        report = pipeline.bootstrap(profile)

        assert report.status == "success"
        assert provider.requested_ranges == [("20260410", "20260410")]
        assert provider.requested_partition_values == [["20260410"]]
        result = storage.read("index_weight")
        assert sorted(str(value) for value in result["date"].tolist()) == [
            "20260403",
            "20260410",
        ]
        assert "dataset=index_weight 去重判断: partition_key=date, target_partitions=2, existing_partitions=1, missing_partitions=1" in caplog.text

    def test_bootstrap_skips_existing_quarter_partitions(self, tmp_path):
        class QuarterResumeProvider(GenericDatasetProvider):
            def __init__(self) -> None:
                self.requested_ranges: list[tuple[str, str]] = []
                self.requested_partition_values: list[list[str] | None] = []

            @property
            def dataset_registry(self) -> dict[str, dict[str, object]]:
                return {
                    "balancesheet": {
                        "api": "balancesheet",
                        "description": "资产负债表",
                        "phase": "1B",
                        "fetch_mode": "symbol_quarter_range",
                        "partition_by": "report_date",
                    },
                }

            def fetch_dataset(
                self,
                dataset: str,
                market: str,
                start: date,
                end: date,
                *,
                symbols: list[str] | None = None,
                trading_days: list[date] | None = None,
                partition_values: list[str] | None = None,
            ) -> pd.DataFrame:
                assert dataset == "balancesheet"
                self.requested_ranges.append(
                    (start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
                )
                self.requested_partition_values.append(partition_values)
                return pd.DataFrame(
                    {
                        "symbol": ["600519.SH", "600519.SH"],
                        "ann_date": ["20261001", "20270101"],
                        "report_date": ["20260930", "20261231"],
                        "total_assets": [100.0, 110.0],
                    }
                )

        provider = QuarterResumeProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        storage.upsert(
            "balancesheet",
            pd.DataFrame(
                {
                    "symbol": ["600519.SH"],
                    "ann_date": ["20260415"],
                    "report_date": ["20260331"],
                    "total_assets": [90.0],
                }
            ),
            {"report_date": "20260331"},
        )
        storage.upsert(
            "balancesheet",
            pd.DataFrame(
                {
                    "symbol": ["600519.SH"],
                    "ann_date": ["20260715"],
                    "report_date": ["20260630"],
                    "total_assets": [95.0],
                }
            ),
            {"report_date": "20260630"},
        )
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )

        profile = DataProfile(
            name="default",
            datasets=["balancesheet"],
            history_start="20260101",
        )

        report = pipeline.repair(
            profile,
            (date(2026, 1, 1), date(2026, 12, 31)),
            action="repair",
        )

        assert report.status == "success"
        assert provider.requested_ranges == [("20260930", "20261231")]
        assert provider.requested_partition_values == [["20260930", "20261231"]]

    def test_bootstrap_uses_exact_missing_quarter_values(self, tmp_path):
        class SparseQuarterProvider(GenericDatasetProvider):
            def __init__(self) -> None:
                self.requested_ranges: list[tuple[str, str]] = []
                self.requested_partition_values: list[list[str] | None] = []

            @property
            def dataset_registry(self) -> dict[str, dict[str, object]]:
                return {
                    "balancesheet": {
                        "api": "balancesheet",
                        "description": "资产负债表",
                        "phase": "1B",
                        "fetch_mode": "symbol_quarter_range",
                        "partition_by": "report_date",
                    },
                }

            def fetch_dataset(
                self,
                dataset: str,
                market: str,
                start: date,
                end: date,
                *,
                symbols: list[str] | None = None,
                trading_days: list[date] | None = None,
                partition_values: list[str] | None = None,
            ) -> pd.DataFrame:
                self.requested_ranges.append((start.strftime("%Y%m%d"), end.strftime("%Y%m%d")))
                self.requested_partition_values.append(partition_values)
                return pd.DataFrame(
                    {
                        "symbol": ["600519.SH", "600519.SH"],
                        "ann_date": ["20260715", "20270101"],
                        "report_date": ["20260630", "20261231"],
                        "total_assets": [95.0, 110.0],
                    }
                )

        provider = SparseQuarterProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        storage.upsert(
            "balancesheet",
            pd.DataFrame(
                {
                    "symbol": ["600519.SH"],
                    "ann_date": ["20260415"],
                    "report_date": ["20260331"],
                    "total_assets": [90.0],
                }
            ),
            {"report_date": "20260331"},
        )
        storage.upsert(
            "balancesheet",
            pd.DataFrame(
                {
                    "symbol": ["600519.SH"],
                    "ann_date": ["20261001"],
                    "report_date": ["20260930"],
                    "total_assets": [100.0],
                }
            ),
            {"report_date": "20260930"},
        )
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )

        profile = DataProfile(
            name="default",
            datasets=["balancesheet"],
            history_start="20260101",
        )

        report = pipeline.repair(
            profile,
            (date(2026, 1, 1), date(2026, 12, 31)),
            action="repair",
        )

        assert report.status == "success"
        assert provider.requested_ranges == [("20260630", "20261231")]
        assert provider.requested_partition_values == [["20260630", "20261231"]]

    def test_bootstrap_skips_pit_blocked_quarter_on_same_as_of_resume(self, tmp_path):
        class PitBlockedQuarterProvider(GenericDatasetProvider):
            def __init__(self) -> None:
                self.calls = 0

            @property
            def dataset_registry(self) -> dict[str, dict[str, object]]:
                return {
                    "fundamental": {
                        "api": "income",
                        "description": "利润表",
                        "phase": "1B",
                        "fetch_mode": "symbol_quarter_range",
                        "partition_by": "report_date",
                        "pit_required": True,
                    },
                }

            def fetch_calendar(self, market: str, start: date, end: date) -> list[date]:
                return [
                    date(2026, 1, 5),
                    date(2026, 2, 2),
                    date(2026, 3, 2),
                    date(2026, 3, 31),
                    date(2026, 4, 8),
                ]

            def fetch_dataset(
                self,
                dataset: str,
                market: str,
                start: date,
                end: date,
                *,
                symbols: list[str] | None = None,
                trading_days: list[date] | None = None,
                partition_values: list[str] | None = None,
            ) -> pd.DataFrame:
                self.calls += 1
                return pd.DataFrame(
                    {
                        "symbol": ["600519.SH"],
                        "ann_date": ["20260420"],
                        "report_date": ["20260331"],
                        "revenue": [100.0],
                        "net_profit": [10.0],
                        "total_assets": [200.0],
                    }
                )

        provider = PitBlockedQuarterProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )

        profile = DataProfile(
            name="default",
            datasets=["fundamental"],
            history_start="20260101",
        )

        first = pipeline.repair(
            profile,
            (date(2026, 1, 1), date(2026, 4, 8)),
            action="repair",
        )
        second = pipeline.repair(
            profile,
            (date(2026, 1, 1), date(2026, 4, 8)),
            action="repair",
        )

        coverage = manifest.list_partition_coverages(
            dataset="fundamental",
            partition_key="end_date",
            as_of_end="2026-04-08",
            statuses=("pit_blocked",),
        )

        assert first.status == "success"
        assert second.status == "success"
        assert provider.calls == 1
        assert storage.list_partitions("fundamental") == []
        assert coverage == {"20260331"}

    def test_bootstrap_skips_dataset_without_permission_and_returns_partial_success(self, tmp_path):
        class PermissionAwareProvider(GenericDatasetProvider):
            @property
            def dataset_registry(self) -> dict[str, dict[str, object]]:
                return {
                    "valuation": {
                        "api": "daily_basic",
                        "description": "估值指标",
                        "phase": "1B",
                        "partition_by": "date",
                    },
                    "news": {
                        "api": "news",
                        "description": "新闻快讯",
                        "phase": "3A",
                        "partition_by": "date",
                    },
                }

            def describe_dataset_access(self, dataset: str) -> dict[str, object]:
                if dataset == "news":
                    return {"dataset": "news", "allowed": False, "reason": "缺少独立权限: news"}
                return {"dataset": dataset, "allowed": True}

            def fetch_dataset(
                self,
                dataset: str,
                market: str,
                start: date,
                end: date,
                *,
                symbols=None,
                trading_days=None,
            ) -> pd.DataFrame:
                if dataset == "news":
                    raise AssertionError("news 应在 pipeline 层被跳过")
                return super().fetch_dataset(
                    dataset,
                    market,
                    start,
                    end,
                    symbols=symbols,
                    trading_days=trading_days,
                )

        provider = PermissionAwareProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )

        profile = DataProfile(
            name="default",
            datasets=["valuation", "news"],
            history_start="20260401",
        )

        report = pipeline.bootstrap(profile)

        assert report.status == "partial_success"
        assert report.total_rows == 1
        assert report.detail["skipped_datasets"] == [
            {"dataset": "news", "reason": "缺少独立权限: news"}
        ]

    def test_bootstrap_retries_failed_dataset_then_skips_and_continues(self, monkeypatch, tmp_path):
        class FlakyProvider(GenericDatasetProvider):
            def __init__(self) -> None:
                self.calls: dict[str, int] = {"broken": 0, "valuation": 0}

            @property
            def dataset_registry(self) -> dict[str, dict[str, object]]:
                return {
                    "broken": {
                        "api": "broken_api",
                        "description": "故障数据集",
                        "phase": "1B",
                        "partition_by": "date",
                    },
                    "valuation": {
                        "api": "daily_basic",
                        "description": "估值指标",
                        "phase": "1B",
                        "partition_by": "date",
                    },
                }

            def fetch_dataset(
                self,
                dataset: str,
                market: str,
                start: date,
                end: date,
                *,
                symbols=None,
                trading_days=None,
            ) -> pd.DataFrame:
                self.calls[dataset] += 1
                if dataset == "broken":
                    raise DataError(
                        code="DATA_PROVIDER_FETCH_FAILED",
                        message="boom",
                    )
                return super().fetch_dataset(
                    dataset,
                    market,
                    start,
                    end,
                    symbols=symbols,
                    trading_days=trading_days,
                )

        monkeypatch.setattr(pipeline_module.time, "sleep", lambda *_args, **_kwargs: None)

        provider = FlakyProvider()
        storage = ParquetDuckDBBackend(tmp_path / "data")
        storage.initialize()
        manifest = SyncManifest(tmp_path / "manifest.db")
        pipeline = DataPipeline(
            provider=provider,
            storage=storage,
            quality_engine=QualityEngine(rules=[]),
            manifest=manifest,
        )

        profile = DataProfile(
            name="default",
            datasets=["broken", "valuation"],
            history_start="20260401",
        )

        report = pipeline.bootstrap(profile)

        assert provider.calls["broken"] == 3
        assert provider.calls["valuation"] == 1
        assert report.status == "partial_success"
        assert report.total_rows == 1
        assert report.detail["skipped_datasets"] == [
            {"dataset": "broken", "reason": "[DATA_PROVIDER_FETCH_FAILED] boom"}
        ]
