"""Phase 1A — StorageBackend (Parquet + DuckDB) 测试。"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend


@pytest.fixture
def storage(tmp_path) -> ParquetDuckDBBackend:
    """创建临时存储后端实例。"""
    backend = ParquetDuckDBBackend(tmp_path / "data")
    backend.initialize()
    return backend


class TestStorageInit:
    def test_initialize_creates_dirs(self, storage):
        assert storage._root.is_dir()

    def test_double_init_idempotent(self, storage):
        storage.initialize()  # 再次初始化不应报错


class TestUpsert:
    def test_write_and_read(self, storage):
        df = pd.DataFrame({
            "symbol": ["600519.SH", "000001.SZ"],
            "date": ["20260401", "20260401"],
            "close": [1800.0, 15.0],
        })
        rows = storage.upsert("bars", df, {"date": "20260401"})
        assert rows == 2

        result = storage.read("bars")
        assert len(result) == 2
        assert set(result["symbol"]) == {"600519.SH", "000001.SZ"}

    def test_upsert_idempotent(self, storage):
        """相同分区覆盖写不产生重复。"""
        df = pd.DataFrame({
            "symbol": ["600519.SH"],
            "date": ["20260401"],
            "close": [1800.0],
        })
        storage.upsert("bars", df, {"date": "20260401"})
        # 覆盖写
        df2 = pd.DataFrame({
            "symbol": ["600519.SH"],
            "date": ["20260401"],
            "close": [1850.0],
        })
        storage.upsert("bars", df2, {"date": "20260401"})
        result = storage.read("bars")
        assert len(result) == 1
        assert result.iloc[0]["close"] == 1850.0

    def test_multi_partition_write(self, storage):
        for d in ["20260401", "20260402", "20260403"]:
            df = pd.DataFrame({
                "symbol": ["600519.SH"],
                "date": [d],
                "close": [1800.0],
            })
            storage.upsert("bars", df, {"date": d})
        result = storage.read("bars")
        assert len(result) == 3


class TestRead:
    def test_read_with_filter(self, storage):
        for d in ["20260401", "20260402", "20260403"]:
            df = pd.DataFrame({"symbol": ["600519.SH"], "date": [d], "close": [1800.0]})
            storage.upsert("bars", df, {"date": d})

        result = storage.read("bars", filters={"date": (">=", "20260402")})
        assert len(result) >= 2

    def test_read_empty_dataset(self, storage):
        result = storage.read("nonexistent")
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_read_with_columns(self, storage):
        df = pd.DataFrame({
            "symbol": ["600519.SH"],
            "date": ["20260401"],
            "open": [1780.0], "close": [1800.0],
        })
        storage.upsert("bars", df, {"date": "20260401"})
        result = storage.read("bars", columns=["symbol", "close"])
        assert "close" in result.columns
        # open 可能不在返回列中（取决于实现是否支持 column pruning）


class TestListPartitions:
    def test_empty_partitions(self, storage):
        parts = storage.list_partitions("bars")
        assert parts == []

    def test_partitions_after_write(self, storage):
        for d in ["20260401", "20260402"]:
            df = pd.DataFrame({"symbol": ["600519.SH"], "date": [d], "close": [1800.0]})
            storage.upsert("bars", df, {"date": d})
        parts = storage.list_partitions("bars")
        assert len(parts) == 2
        assert all("2026040" in p for p in parts)


class TestSnapshot:
    def test_snapshot_returns_id(self, storage):
        df = pd.DataFrame({
            "symbol": ["600519.SH"], "date": ["20260401"], "close": [1800.0],
        })
        storage.upsert("bars", df, {"date": "20260401"})
        sid = storage.snapshot("test_profile", date(2026, 4, 1))
        assert sid.startswith("snap_")
        assert "20260401" in sid

    def test_snapshot_id_unique(self, storage):
        df = pd.DataFrame({"symbol": ["X"], "date": ["20260401"], "close": [1.0]})
        storage.upsert("bars", df, {"date": "20260401"})
        import time
        s1 = storage.snapshot("p", date(2026, 4, 1))
        time.sleep(0.01)
        s2 = storage.snapshot("p", date(2026, 4, 1))
        assert s1 != s2  # 不同时间生成的快照 ID 不同
