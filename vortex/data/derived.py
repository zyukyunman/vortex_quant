"""DerivedMetricCalculator — 衍生指标计算器（Phase 1A stub）。

估值百分位、复权价格等衍生指标的计算。
完整实现将在后续阶段交付。
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from vortex.shared.logging import get_logger

if TYPE_CHECKING:
    from vortex.data.storage.base import StorageBackend

logger = get_logger(__name__)


class DerivedMetricCalculator:
    """衍生指标计算器。当前为 Phase 1A stub。

    TODO(Phase 2): 实现估值百分位（PE/PB 历史分位数）
    TODO(Phase 2): 实现前/后复权价格计算
    TODO(Phase 2): 实现流通市值加权指标
    TODO(Phase 2): 集成 DataPipeline 的 derived 阶段
    """

    def __init__(self, storage: StorageBackend) -> None:
        self._storage = storage

    def compute(self, dataset: str, df: pd.DataFrame) -> pd.DataFrame:
        """计算衍生指标。Phase 1A 直接返回原始数据。"""
        logger.info(
            "DerivedMetricCalculator.compute: dataset=%s (stub, 尚未实现)",
            dataset,
        )
        return df

    def compute_all(self) -> None:
        """计算所有衍生指标。Phase 1A 仅记录日志。"""
        logger.info("DerivedMetricCalculator.compute_all (stub, 尚未实现)")
