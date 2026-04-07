"""质量规则：date_continuity — 交易日连续无缺失（critical）。

检查逻辑：将 DataFrame 中出现的交易日与 context.trading_days 对比，
找出遗漏的交易日。仅在 context 提供了 trading_days 时生效。
"""
from __future__ import annotations

from datetime import date as date_type

import pandas as pd

from vortex.data.quality.report import QualityContext, QualityResult


class DateContinuityRule:
    """检查行情数据的交易日是否连续、无缺失。"""

    name: str = "date_continuity"
    level: str = "critical"

    def check(self, df: pd.DataFrame, context: QualityContext) -> QualityResult:
        if not context.trading_days:
            return QualityResult(
                rule_name=self.name, level=self.level, passed=True,
                message="未提供 trading_days，跳过连续性检查",
            )
        if df.empty or "date" not in df.columns:
            return QualityResult(
                rule_name=self.name, level=self.level, passed=True,
                message="无 date 列或数据为空，跳过",
            )

        # 将 date 列转为 date 对象集合
        date_values = set()
        for v in df["date"].unique():
            if isinstance(v, date_type):
                date_values.add(v)
            else:
                s = str(v).replace("-", "")[:8]
                date_values.add(date_type(int(s[:4]), int(s[4:6]), int(s[6:8])))

        expected = set(context.trading_days)
        missing = sorted(expected - date_values)
        passed = len(missing) == 0

        return QualityResult(
            rule_name=self.name,
            level=self.level,
            passed=passed,
            message="" if passed else f"缺失 {len(missing)} 个交易日",
            detail={"missing_dates": [d.isoformat() for d in missing[:20]]},
        )
