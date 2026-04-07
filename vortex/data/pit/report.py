"""PitReport 数据结构 — PIT 对齐结果报告。"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class PitRecord:
    """单条 PIT 对齐记录。"""

    symbol: str
    report_date: str
    ann_date: str
    effective_from: str  # ISO datetime，对齐后的生效时间
    status: str  # "aligned" | "blocked" | "overridden"
    reason: str = ""


@dataclass
class PitReport:
    """PIT 对齐整体报告。

    overall_status:
      - "OK"      — 全部对齐成功
      - "BLOCKED" — 存在无法对齐的记录（ann_date 缺失或异常）
    """

    overall_status: str  # "OK" | "BLOCKED"
    total_records: int = 0
    aligned_count: int = 0
    blocked_count: int = 0
    overridden_count: int = 0
    records: list[PitRecord] = field(default_factory=list)
    blocked_details: list[dict] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.overall_status == "OK"
