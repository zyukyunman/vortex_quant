"""
base.py
策略抽象基类

所有策略继承 BaseStrategy:
  - 只依赖 DataStore + FactorHub (只读)
  - 只输出 Signal (通过 SignalBus)
  - 策略之间完全隔离
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import List

from app.core.datastore import DataStore
from app.core.factorhub import FactorHub
from app.core.signalbus import SignalBus
from app.models import Signal, SelectionResult

logger = logging.getLogger(__name__)


class BaseStrategy(ABC):
    """策略基类"""

    name: str = ""
    description: str = ""

    def __init__(self, ds: DataStore, fh: FactorHub, bus: SignalBus):
        self.ds = ds        # 只读
        self.fh = fh        # 只读
        self.bus = bus       # 只写
        self.logger = logging.getLogger(f"strategy.{self.name}")

    @abstractmethod
    def generate(self, date: str) -> SelectionResult:
        """
        执行选股逻辑，返回选股结果。

        Parameters
        ----------
        date : str
            选股基准日期 YYYYMMDD (通常是月末交易日)

        Returns
        -------
        SelectionResult
            包含 Signal 列表 + 统计信息
        """
        ...

    def run(self, date: str) -> SelectionResult:
        """执行策略并发布信号到 SignalBus"""
        self.logger.info("="*50)
        self.logger.info("策略运行: %s @ %s", self.name, date)
        self.logger.info("="*50)

        result = self.generate(date)

        # 发布信号
        self.bus.publish_batch(result.signals)
        self.logger.info(
            "完成: 选出 %d 只 (样本空间 %d → 筛后 %d)",
            result.top_n, result.universe_size, result.after_filter_size,
        )

        return result
