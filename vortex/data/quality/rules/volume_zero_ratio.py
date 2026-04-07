"""质量规则：volume_zero_ratio — 零成交量比例低于 10%（warning）。

过高的零成交量比例可能意味着大量停牌或数据源异常。
"""
from __future__ import annotations

import pandas as pd

from vortex.data.quality.report import QualityContext, QualityResult

_THRESHOLD = 0.10  # 10%


class VolumeZeroRatioRule:
    """检查零成交量比例是否低于阈值。"""

    name: str = "volume_zero_ratio_below_10pct"
    level: str = "warning"

    def check(self, df: pd.DataFrame, context: QualityContext) -> QualityResult:
        if df.empty or "volume" not in df.columns:
            return QualityResult(
                rule_name=self.name, level=self.level, passed=True,
                message="无 volume 列或数据为空，跳过",
            )

        n = len(df)
        zero_count = int((df["volume"] == 0).sum())
        ratio = zero_count / n

        passed = ratio <= _THRESHOLD
        return QualityResult(
            rule_name=self.name,
            level=self.level,
            passed=passed,
            message="" if passed else f"零成交量比例 {ratio:.2%} 超过 {_THRESHOLD:.0%}",
            detail={"zero_volume_count": zero_count, "ratio": round(ratio, 4)},
        )
