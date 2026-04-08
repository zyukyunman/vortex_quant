"""Phase 1A — Provider / Registry / Manifest / Calendar 测试。"""
from __future__ import annotations

import subprocess
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from vortex.data.provider.base import DataProvider
from vortex.data.provider.registry import ProviderRegistry


# ── Mock Provider ──────────────────────────────────────────────────

class MockProvider:
    """测试用的 Mock DataProvider。"""

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
        return pd.DataFrame({
            "symbol": ["600519.SH", "000001.SZ"],
            "name": ["贵州茅台", "平安银行"],
            "list_date": [date(2001, 8, 27), date(1991, 4, 3)],
            "delist_date": [None, None],
            "industry": ["白酒", "银行"],
            "market_cap": [2e12, 3e11],
        })

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
        rows = []
        for s in symbols[:2]:
            rows.append({
                "symbol": s, "date": start.strftime("%Y%m%d"),
                "open": 100.0, "high": 105.0, "low": 98.0,
                "close": 102.0, "volume": 50000.0, "amount": 5e6,
            })
        return pd.DataFrame(rows)

    def fetch_fundamental(self, market, symbols, fields, start, end) -> pd.DataFrame:
        return pd.DataFrame({
            "symbol": ["600519.SH"],
            "ann_date": [date(2026, 4, 1)],
            "report_date": [date(2025, 12, 31)],
            "revenue": [100.0],
        })

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
        if dataset == "instruments":
            return self.fetch_instruments(market)
        if dataset == "calendar":
            days = self.fetch_calendar(market, start, end)
            return pd.DataFrame({"cal_date": [d.strftime("%Y%m%d") for d in days]})
        if dataset == "bars":
            return self.fetch_bars(market, symbols or [], "1d", start, end)
        return pd.DataFrame()


# ── ProviderRegistry ───────────────────────────────────────────────

class TestProviderRegistry:
    def test_register_and_get(self):
        registry = ProviderRegistry()
        mock = MockProvider()
        registry.register(mock)
        assert registry.get("mock") is mock

    def test_get_unknown_raises(self):
        registry = ProviderRegistry()
        with pytest.raises(Exception):
            registry.get("nonexistent")

    def test_list_providers(self):
        registry = ProviderRegistry()
        registry.register(MockProvider())
        names = registry.list_providers()
        assert "mock" in names


class TestProviderDatasetRegistry:
    """Provider 数据集注册表测试。"""

    def test_mock_provider_has_dataset_registry(self):
        mock = MockProvider()
        reg = mock.dataset_registry
        assert isinstance(reg, dict)
        assert "instruments" in reg
        assert "bars" in reg

    def test_dataset_registry_structure(self):
        mock = MockProvider()
        for name, meta in mock.dataset_registry.items():
            assert "api" in meta
            assert "description" in meta
            assert "phase" in meta

    def test_smoke_test(self):
        mock = MockProvider()
        assert mock.smoke_test() is True

    def test_tushare_dataset_registry_exists(self):
        """验证 Tushare registry 至少覆盖核心 1A 数据集，且默认列表已经扩展到全量口径。"""
        from vortex.data.provider.tushare_registry import (
            TUSHARE_DATASET_REGISTRY,
            get_default_tushare_datasets,
        )
        phase_1a = {k for k, v in TUSHARE_DATASET_REGISTRY.items() if v["phase"] == "1A"}
        assert {"instruments", "calendar", "bars", "fundamental", "events"} <= phase_1a
        defaults = get_default_tushare_datasets()
        assert "valuation" in defaults
        assert len(defaults) > 5

    def test_tushare_default_datasets_respect_points_and_extra_permissions(self):
        from vortex.data.provider.tushare_registry import get_default_tushare_datasets

        defaults = get_default_tushare_datasets(points=5000, permission_keys=set())
        assert "valuation" in defaults
        assert "fundamental" in defaults
        assert "anns_d" not in defaults
        assert "news" not in defaults
        assert "hk_daily" not in defaults
        assert "us_daily" not in defaults

        enriched = get_default_tushare_datasets(
            points=5000,
            permission_keys={"news", "announcements"},
        )
        assert "news" in enriched
        assert "major_news" in enriched
        assert "anns_d" in enriched

    def test_tushare_registry_import_does_not_require_pandas(self):
        repo_root = Path(__file__).resolve().parents[1]
        script = """
import builtins

real_import = builtins.__import__

def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name == "pandas" or name.startswith("pandas."):
        raise RuntimeError("unexpected pandas import")
    return real_import(name, globals, locals, fromlist, level)

builtins.__import__ = guarded_import

from vortex.data.provider.tushare_registry import get_default_tushare_datasets

defaults = get_default_tushare_datasets(points=5000, permission_keys=set())
assert "valuation" in defaults
assert "anns_d" not in defaults
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=repo_root,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr

