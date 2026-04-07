"""质量规则：not_empty — 数据集不为空（critical）。"""
from __future__ import annotations

import pandas as pd

from vortex.data.quality.report import QualityContext, QualityResult


class NotEmptyRule:
    """检查数据集是否为空。空数据集不允许发布。"""

    name: str = "not_empty"
    level: str = "critical"

    def check(self, df: pd.DataFrame, context: QualityContext) -> QualityResult:
        passed = len(df) > 0
        return QualityResult(
            rule_name=self.name,
            level=self.level,
            passed=passed,
            message="" if passed else "数据集为空",
        )
