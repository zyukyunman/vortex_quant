"""QlibViewBuilder — Qlib 格式视图构建器（Phase 1A stub）。

将 Parquet 数据转换为 Qlib bin 格式的衍生研究视图。
完整实现将在后续阶段交付。
"""
from __future__ import annotations

from pathlib import Path

from vortex.shared.logging import get_logger

logger = get_logger(__name__)


class QlibViewBuilder:
    """Qlib 视图构建器。当前为 Phase 1A stub。

    TODO(Phase 2): 实现 Parquet → Qlib bin 格式转换
    TODO(Phase 2): 支持增量更新和全量重建
    TODO(Phase 2): 集成 DataPipeline 的 build 阶段
    """

    def __init__(self, output_dir: Path) -> None:
        self._output_dir = output_dir

    def build(self, dataset: str, source_dir: Path) -> None:
        """构建 Qlib 视图。Phase 1A 仅记录日志。"""
        logger.info(
            "QlibViewBuilder.build: dataset=%s (stub, 尚未实现)",
            dataset,
        )

    def rebuild_all(self, source_dir: Path) -> None:
        """全量重建所有 Qlib 视图。Phase 1A 仅记录日志。"""
        logger.info("QlibViewBuilder.rebuild_all (stub, 尚未实现)")
