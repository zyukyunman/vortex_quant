"""StorageBackend Protocol — 存储后端接口（06 §3.3）。"""
from __future__ import annotations

from datetime import date
from typing import Protocol

import pandas as pd


class StorageBackend(Protocol):
    """存储后端接口。"""

    def upsert(self, dataset: str, df: pd.DataFrame, partition: dict) -> int:
        """写入/覆盖数据。返回写入行数。

        幂等：相同 partition 覆盖写，不产生重复
        不变量：写入后立即可读
        """
        ...

    def read(
        self,
        dataset: str,
        filters: dict | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """读取数据。filters 为 DuckDB WHERE 条件字典。

        filters 格式：
          - {"col": "value"}           → col = 'value'
          - {"col": (">=", "value")}   → col >= 'value'

        返回：按 date ASC, symbol ASC 排序
        """
        ...

    def list_partitions(self, dataset: str) -> list[str]:
        """列出已有分区。返回排序后的分区路径列表。"""
        ...

    def snapshot(self, profile: str, as_of: date) -> str:
        """发布快照。返回 snapshot_id。

        前置条件：质量检查已通过
        不变量：snapshot_id 一旦生成不可变更
        约束：同 profile + as_of 可覆盖发布
        """
        ...
