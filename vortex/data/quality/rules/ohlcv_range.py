"""质量规则：ohlcv_range — OHLCV 值在合理范围内（critical）。

价格必须 > 0，成交量 >= 0，成交额 >= 0。
检测到异常值时报告具体列和异常行数。
"""
from __future__ import annotations

import pandas as pd

from vortex.data.quality.report import QualityContext, QualityResult


class OhlcvRangeRule:
    """检查 OHLCV 字段值是否在合理范围。"""

    name: str = "ohlcv_range"
    level: str = "critical"

    # 价格列（必须 > 0）和量额列（必须 >= 0）
    _PRICE_COLS = ("open", "high", "low", "close")
    _VOLUME_COLS = ("volume", "amount")

    def check(self, df: pd.DataFrame, context: QualityContext) -> QualityResult:
        if df.empty:
            return QualityResult(
                rule_name=self.name, level=self.level, passed=True,
                message="空 DataFrame，跳过",
            )

        violations: dict[str, int] = {}

        for col in self._PRICE_COLS:
            if col in df.columns:
                bad = (df[col].notna()) & (df[col] <= 0)
                cnt = int(bad.sum())
                if cnt > 0:
                    violations[f"{col}<=0"] = cnt

        for col in self._VOLUME_COLS:
            if col in df.columns:
                bad = (df[col].notna()) & (df[col] < 0)
                cnt = int(bad.sum())
                if cnt > 0:
                    violations[f"{col}<0"] = cnt

        passed = len(violations) == 0
        return QualityResult(
            rule_name=self.name,
            level=self.level,
            passed=passed,
            message="" if passed else f"OHLCV 范围异常: {violations}",
            detail={"violations": violations},
        )
