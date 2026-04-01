"""
runner.py
L5 StrategyRunner — 策略注册与并行执行

职责:
  - 注册策略实例
  - 按组或全部执行
  - 隔离单策略错误
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from vortex.core.data.datastore import DataStore
from vortex.core.factorhub import FactorHub
from vortex.core.signalbus import SignalBus
from vortex.models import SelectionResult
from vortex.strategy.base import BaseStrategy

logger = logging.getLogger(__name__)


class StrategyRunner:
    """策略并行执行器"""

    def __init__(self, ds: DataStore, fh: FactorHub, bus: SignalBus):
        self.ds = ds
        self.fh = fh
        self.bus = bus
        self._strategies: Dict[str, BaseStrategy] = {}

    def register(self, strategy: BaseStrategy):
        """注册策略"""
        self._strategies[strategy.name] = strategy
        logger.info("注册策略: %s", strategy.name)

    def list_strategies(self) -> List[Dict]:
        """列出所有已注册策略"""
        return [
            {"name": s.name, "description": s.description}
            for s in self._strategies.values()
        ]

    def run_one(self, name: str, date: str) -> Optional[SelectionResult]:
        """执行单个策略"""
        strategy = self._strategies.get(name)
        if not strategy:
            logger.error("策略 '%s' 未注册", name)
            return None
        try:
            return strategy.run(date)
        except Exception as e:
            logger.error("策略 '%s' 执行失败: %s", name, e, exc_info=True)
            return None

    def run_all(self, date: str, parallel: bool = True) -> List[SelectionResult]:
        """
        执行所有策略

        Parameters
        ----------
        date : str
            选股基准日
        parallel : bool
            True 时多线程并行（DataStore 支持并发读）

        Returns
        -------
        List[SelectionResult]
            成功执行的结果列表
        """
        results: List[SelectionResult] = []
        errors: List[str] = []
        start = time.time()

        logger.info("=" * 60)
        logger.info("StrategyRunner: 执行 %d 个策略 @ %s", len(self._strategies), date)
        logger.info("=" * 60)

        if parallel and len(self._strategies) > 1:
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = {
                    pool.submit(s.run, date): s.name
                    for s in self._strategies.values()
                }
                for future in as_completed(futures):
                    name = futures[future]
                    try:
                        result = future.result()
                        if result:
                            results.append(result)
                    except Exception as e:
                        errors.append(f"{name}: {e}")
                        logger.error("策略 '%s' 并行执行失败: %s", name, e)
        else:
            for name, strategy in self._strategies.items():
                try:
                    result = strategy.run(date)
                    if result:
                        results.append(result)
                except Exception as e:
                    errors.append(f"{name}: {e}")
                    logger.error("策略 '%s' 执行失败: %s", name, e)

        elapsed = time.time() - start
        logger.info(
            "StrategyRunner 完成: %d 成功, %d 失败, 耗时 %.1fs",
            len(results), len(errors), elapsed,
        )

        return results
