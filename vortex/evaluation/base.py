"""
base.py
因子评测策略基类

所有 Evaluator 继承 BaseEvaluator，实现 evaluate() 方法。
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, List

from vortex.evaluation.spec import EvalResult, EvalSpec

logger = logging.getLogger(__name__)


class BaseEvaluator(ABC):
    """因子评测策略基类"""

    @abstractmethod
    def evaluate(
        self,
        spec: EvalSpec,
        analyzer,
        dates: List[str],
    ) -> EvalResult:
        """
        执行评测

        Parameters
        ----------
        spec : EvalSpec
            本因子的评测规格
        analyzer : FactorAnalyzer
            底层计算器
        dates : list[str]
            截面日期列表

        Returns
        -------
        EvalResult
            标准化评测结果
        """
        ...
