"""ParquetDuckDBBackend — Parquet 写入 + DuckDB 读取的存储后端（06 §3.3, A.1, A.2）。

数据面使用 Parquet + DuckDB，控制面使用 SQLite — 二者严格分离。

写入规则（06 A.1）：
  - Compression: Snappy
  - Partition key: bars 按 date，fundamental 按 end_date
  - 文件名: 始终 data.parquet
  - 相同 partition 覆盖写

读取模式（06 A.2）：
  - 使用 DuckDB 直查 Parquet，支持 hive_partitioning
  - catalog.duckdb 为可重建的只读视图层
"""
from __future__ import annotations

import secrets
from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from vortex.shared.logging import get_logger

logger = get_logger(__name__)

_SNAPPY = "snappy"


class ParquetDuckDBBackend:
    """Parquet + DuckDB 存储后端实现。

    目录布局：
      {root}/
        {dataset}/{partition_key}={value}/data.parquet
        authoritative/{profile}/{dataset}/...
        catalog.duckdb
    """

    def __init__(self, root: Path) -> None:
        self._root = Path(root)
        self._catalog_path = self._root / "catalog.duckdb"

    # ------------------------------------------------------------------
    # 初始化
    # ------------------------------------------------------------------

    def initialize(self) -> None:
        """创建根目录。catalog.duckdb 按需创建。"""
        self._root.mkdir(parents=True, exist_ok=True)
        logger.info("存储后端初始化: %s", self._root)

    # ------------------------------------------------------------------
    # StorageBackend Protocol 实现
    # ------------------------------------------------------------------

    def upsert(self, dataset: str, df: pd.DataFrame, partition: dict) -> int:
        """写入/覆盖数据到 Parquet 分区。

        partition 示例: {"date": "20260401"}
        写入路径: {root}/{dataset}/date=20260401/data.parquet
        幂等：相同 partition 覆盖写。
        """
        if df.empty:
            return 0

        # 构建分区目录路径
        partition_parts = [f"{k}={v}" for k, v in sorted(partition.items())]
        partition_dir = self._root / dataset / "/".join(partition_parts)
        partition_dir.mkdir(parents=True, exist_ok=True)

        parquet_path = partition_dir / "data.parquet"

        # 使用 pyarrow.parquet 写入（设计规范：写入用 pyarrow）
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(
            table,
            parquet_path,
            compression=_SNAPPY,
            # row_group_size 限制：128MB or 1M rows（取较小者）
            row_group_size=1_000_000,
        )

        row_count = len(df)
        logger.debug(
            "upsert: dataset=%s, partition=%s, rows=%d, path=%s",
            dataset, partition, row_count, parquet_path,
        )
        return row_count

    def read(
        self,
        dataset: str,
        filters: dict | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """使用 DuckDB 直查 Parquet 文件。

        filters 格式：
          {"col": "value"}          → col = 'value'
          {"col": (">=", "value")}  → col >= 'value'
          {"col": (">", "value")}   → col > 'value'
        """
        dataset_dir = self._root / dataset
        if not dataset_dir.exists():
            return pd.DataFrame()

        # 找到所有 data.parquet 文件
        parquet_glob = str(dataset_dir / "**" / "data.parquet")

        # 构建 SQL
        col_clause = ", ".join(columns) if columns else "*"
        where_parts: list[str] = []
        params: list[object] = []

        if filters:
            for col, val in filters.items():
                if isinstance(val, tuple) and len(val) == 2:
                    op, operand = val
                    where_parts.append(f'"{col}" {op} ?')
                    params.append(operand)
                else:
                    where_parts.append(f'"{col}" = ?')
                    params.append(val)

        where_clause = " AND ".join(where_parts) if where_parts else "1=1"

        sql = (
            f"SELECT {col_clause} "
            f"FROM read_parquet('{parquet_glob}', hive_partitioning=true) "
            f"WHERE {where_clause} "
        )

        # 排序：date ASC, symbol ASC（若存在这些列）
        order_parts = []
        # 使用子查询检测列名
        try:
            conn = duckdb.connect(database=":memory:", read_only=False)
            result = conn.execute(sql, params).fetchdf()
            conn.close()
        except duckdb.IOException:
            # 没有匹配的 Parquet 文件
            return pd.DataFrame()
        except Exception as exc:
            logger.error("DuckDB 读取失败: %s", exc)
            raise

        # 排序
        sort_cols = [c for c in ["date", "symbol"] if c in result.columns]
        if sort_cols:
            result = result.sort_values(sort_cols, ascending=True).reset_index(drop=True)

        return result

    def list_partitions(self, dataset: str) -> list[str]:
        """列出已有分区目录路径（排序）。"""
        dataset_dir = self._root / dataset
        if not dataset_dir.exists():
            return []

        partitions: list[str] = []
        for parquet_file in sorted(dataset_dir.rglob("data.parquet")):
            # 取相对于 dataset_dir 的路径，去掉文件名
            rel = parquet_file.parent.relative_to(dataset_dir)
            partitions.append(str(rel))

        return sorted(partitions)

    def snapshot(self, profile: str, as_of: date) -> str:
        """发布快照到 authoritative 目录。

        将当前数据复制到 authoritative/{profile}/ 下。
        同 profile + as_of 可覆盖发布。
        返回 snapshot_id。
        """
        as_of_str = as_of.strftime("%Y%m%d")
        now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        rand_hex = secrets.token_hex(2)
        snapshot_id = f"snap_{as_of_str}_{now_str}_{rand_hex}"

        auth_dir = self._root / "authoritative" / profile
        auth_dir.mkdir(parents=True, exist_ok=True)

        # 遍历所有 dataset 目录（排除 authoritative 和 raw）
        for item in sorted(self._root.iterdir()):
            if item.is_dir() and item.name not in ("authoritative", "raw", "catalog.duckdb"):
                dest = auth_dir / item.name
                # 复制 Parquet 文件到 authoritative
                self._copy_dataset(item, dest)

        logger.info(
            "快照已发布: snapshot_id=%s, profile=%s, as_of=%s",
            snapshot_id, profile, as_of,
        )
        return snapshot_id

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    @staticmethod
    def _copy_dataset(src: Path, dest: Path) -> None:
        """复制 dataset 目录中的所有 Parquet 文件。"""
        import shutil

        for parquet_file in src.rglob("data.parquet"):
            rel = parquet_file.relative_to(src)
            dest_file = dest / rel
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(parquet_file, dest_file)
