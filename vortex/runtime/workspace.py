"""Vortex 工作区目录结构与初始化。"""
from __future__ import annotations

from pathlib import Path

from vortex.shared.errors import RuntimeError_
from vortex.shared.logging import get_logger

logger = get_logger(__name__)


class Workspace:
    """管理 Vortex 工作区目录结构和控制面数据库。"""

    def __init__(self, root: Path) -> None:
        self.root = root

    # ------------------------------------------------------------------
    # 目录属性
    # ------------------------------------------------------------------

    @property
    def state_dir(self) -> Path:
        return self.root / "state"

    @property
    def data_dir(self) -> Path:
        return self.root / "data"

    @property
    def research_dir(self) -> Path:
        return self.root / "research"

    @property
    def strategy_dir(self) -> Path:
        return self.root / "strategy"

    @property
    def trade_dir(self) -> Path:
        return self.root / "trade"

    @property
    def profiles_dir(self) -> Path:
        return self.root / "profiles"

    @property
    def resolved_dir(self) -> Path:
        return self.root / "resolved"

    @property
    def db_path(self) -> Path:
        return self.state_dir / "control.db"

    # ------------------------------------------------------------------
    # 初始化
    # ------------------------------------------------------------------

    def _all_dirs(self) -> list[Path]:
        return [
            self.state_dir,
            self.data_dir,
            self.research_dir,
            self.strategy_dir,
            self.trade_dir,
            self.profiles_dir,
            self.resolved_dir,
        ]

    def initialize(self) -> None:
        """创建所有必需目录 + 初始化 SQLite 表。"""
        from vortex.runtime.database import Database

        for d in self._all_dirs():
            d.mkdir(parents=True, exist_ok=True)
            logger.debug("确保目录存在: %s", d)

        db = Database(self.db_path)
        db.initialize_tables()
        db.close()
        logger.info("工作区已初始化: %s", self.root)

    def ensure_initialized(self) -> None:
        """检查 workspace 是否已初始化，未初始化则抛出错误。"""
        if not self.db_path.exists():
            raise RuntimeError_(
                code="RUNTIME_WORKSPACE_NOT_INITIALIZED",
                message=f"工作区尚未初始化，请先运行 vortex init: {self.root}",
            )
