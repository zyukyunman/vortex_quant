"""QualityEngine — 质量门禁引擎（06 §3.5）。

Fail-Closed 原则：任一 critical 规则失败即阻断发布。
"""
from __future__ import annotations

from typing import Protocol

import pandas as pd

from vortex.data.quality.report import QualityContext, QualityReport, QualityResult
from vortex.shared.logging import get_logger

logger = get_logger(__name__)


class QualityRule(Protocol):
    """质量规则接口。"""

    name: str
    level: str  # "critical" | "warning"

    def check(self, df: pd.DataFrame, context: QualityContext) -> QualityResult:
        """执行检查。返回通过/失败 + 详情。"""
        ...


class QualityEngine:
    """质量门禁引擎。

    若任一 critical 规则失败 → 整体 FAILED（阻断发布）。
    warning 规则失败仅记录。
    """

    def __init__(self, rules: list[QualityRule]) -> None:
        self._rules = rules

    def run(
        self, dataset: str, df: pd.DataFrame, context: QualityContext,
    ) -> QualityReport:
        """运行所有规则，返回质量报告。"""
        results: list[QualityResult] = []
        has_critical_failure = False
        has_warning_failure = False

        for rule in self._rules:
            try:
                result = rule.check(df, context)
            except Exception as exc:
                # 规则执行异常视为 critical 失败
                logger.error("规则 %s 执行异常: %s", rule.name, exc)
                result = QualityResult(
                    rule_name=rule.name,
                    level=rule.level,
                    passed=False,
                    message=f"规则执行异常: {exc}",
                )

            results.append(result)

            if not result.passed:
                if result.level == "critical":
                    has_critical_failure = True
                    logger.warning(
                        "质量门禁 CRITICAL 失败: %s — %s",
                        rule.name, result.message,
                    )
                else:
                    has_warning_failure = True
                    logger.info(
                        "质量门禁 WARNING: %s — %s",
                        rule.name, result.message,
                    )

        if has_critical_failure:
            overall_status = "FAILED"
        elif has_warning_failure:
            overall_status = "WARNING"
        else:
            overall_status = "PASSED"

        report = QualityReport(
            dataset=dataset,
            overall_status=overall_status,
            results=results,
            row_count=len(df),
        )
        logger.info(
            "质量检查完成: dataset=%s, status=%s, rows=%d",
            dataset, overall_status, len(df),
        )
        return report
