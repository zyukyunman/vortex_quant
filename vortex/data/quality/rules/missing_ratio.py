"""质量规则：missing_ratio — 缺失比例低于 5%（warning）。

统计各列的缺失率，任一列超过 5% 则发出警告。
"""
from __future__ import annotations

import pandas as pd

from vortex.data.quality.report import QualityContext, QualityResult

_THRESHOLD = 0.05  # 5%


class MissingRatioRule:
    """检查各列缺失比例是否低于阈值。"""

    name: str = "missing_ratio_below_5pct"
    level: str = "warning"

    def check(self, df: pd.DataFrame, context: QualityContext) -> QualityResult:
        if df.empty:
            return QualityResult(
                rule_name=self.name, level=self.level, passed=True,
                message="空 DataFrame，跳过",
            )

        n = len(df)
        high_missing: dict[str, float] = {}
        for col in df.columns:
            ratio = float(df[col].isna().sum()) / n
            if ratio > _THRESHOLD:
                high_missing[col] = round(ratio, 4)

        passed = len(high_missing) == 0
        return QualityResult(
            rule_name=self.name,
            level=self.level,
            passed=passed,
            message="" if passed else f"缺失率超过 {_THRESHOLD:.0%}: {high_missing}",
            detail={"high_missing_columns": high_missing},
        )
