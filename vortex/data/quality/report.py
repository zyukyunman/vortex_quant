"""QualityReport / QualityResult / QualityContext 数据结构（06 §3.5）。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date


@dataclass
class QualityContext:
    """质量检查上下文，传递给每条规则的额外信息。"""

    dataset: str
    profile: str = ""
    market: str = "cn_stock"
    trading_days: list[date] = field(default_factory=list)
    extra: dict = field(default_factory=dict)


@dataclass
class QualityResult:
    """单条规则的检查结果。"""

    rule_name: str
    level: str  # "critical" | "warning"
    passed: bool
    message: str = ""
    detail: dict = field(default_factory=dict)


@dataclass
class QualityReport:
    """质量门禁整体报告。

    overall_status:
      - "PASSED"  — 所有 critical 规则通过
      - "FAILED"  — 至少一条 critical 规则失败（阻断发布）
      - "WARNING" — critical 全通过但有 warning 失败
    """

    dataset: str
    overall_status: str  # "PASSED" | "FAILED" | "WARNING"
    results: list[QualityResult] = field(default_factory=list)
    row_count: int = 0
    detail: dict = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return self.overall_status != "FAILED"

    @property
    def critical_failures(self) -> list[QualityResult]:
        return [r for r in self.results if r.level == "critical" and not r.passed]

    @property
    def warnings(self) -> list[QualityResult]:
        return [r for r in self.results if r.level == "warning" and not r.passed]
