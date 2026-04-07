"""DataPipeline 辅助逻辑测试。"""
from __future__ import annotations

from datetime import date

import pandas as pd

import vortex.data.pipeline as pipeline_module
from vortex.config.profile.models import DataProfile
from vortex.data.manifest import SyncManifest
from vortex.shared.errors import DataError
from vortex.data.pipeline import DataPipeline, _ordered_datasets
from vortex.data.quality.engine import QualityEngine
from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend


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


class TestOrderedDatasets:
    def test_priority_datasets_are_applied_first(self):
        profile = DataProfile(
            name="default",
            datasets=["instruments", "calendar", "bars", "fundamental", "events"],
            priority_datasets=["bars", "calendar"],
        )

        assert _ordered_datasets(profile) == [
            "bars",
            "calendar",
            "instruments",
            "fundamental",
            "events",
        ]

    def test_excluded_datasets_are_removed_before_ordering(self):
        profile = DataProfile(
            name="default",
            datasets=["instruments", "calendar", "bars", "fundamental", "events"],
            exclude_datasets=["calendar", "events"],
            priority_datasets=["events", "bars", "calendar"],
        )

        assert _ordered_datasets(profile) == [
            "bars",
            "instruments",
            "fundamental",
        ]

    def test_unknown_priority_dataset_is_ignored(self):
        profile = DataProfile(
            name="default",
            datasets=["instruments", "calendar", "bars"],
            priority_datasets=["valuation", "bars"],
        )

        assert _ordered_datasets(profile) == [
            "bars",
            "instruments",
            "calendar",
        ]


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

            def fetch_dataset(self, dataset: str, market: str, start: date, end: date, *, symbols=None, trading_days=None) -> pd.DataFrame:
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

            def fetch_dataset(self, dataset: str, market: str, start: date, end: date, *, symbols=None, trading_days=None) -> pd.DataFrame:
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
