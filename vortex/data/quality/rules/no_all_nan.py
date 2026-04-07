"""质量规则：no_all_nan — 没有全 NaN 列（critical）。"""
from __future__ import annotations

import pandas as pd

from vortex.data.quality.report import QualityContext, QualityResult


class NoAllNanRule:
    """检查是否存在全 NaN 的列。全 NaN 列意味着数据采集失败。"""

    name: str = "no_all_nan"
    level: str = "critical"

    def check(self, df: pd.DataFrame, context: QualityContext) -> QualityResult:
        if df.empty:
            return QualityResult(
                rule_name=self.name, level=self.level, passed=True,
                message="空 DataFrame，跳过检查",
            )
        all_nan_cols = [c for c in df.columns if df[c].isna().all()]
        passed = len(all_nan_cols) == 0
        return QualityResult(
            rule_name=self.name,
            level=self.level,
            passed=passed,
            message="" if passed else f"存在全 NaN 列: {all_nan_cols}",
            detail={"all_nan_columns": all_nan_cols},
        )
