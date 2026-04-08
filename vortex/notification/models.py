"""通知消息模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

NotificationSeverity = Literal["critical", "warning", "info"]


def _severity_icon(severity: NotificationSeverity) -> str:
    return {
        "critical": "🔴",
        "warning": "🟡",
        "info": "🟢",
    }[severity]


@dataclass(frozen=True)
class NotificationMessage:
    """统一的通知消息结构。"""

    event_type: str
    notification_type: str
    severity: NotificationSeverity
    title: str
    summary: str
    impact: str = ""
    suggested_actions: tuple[str, ...] = ()
    run_id: str | None = None
    task_id: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)

    def to_text(self) -> str:
        """渲染为适合 Feishu 文本消息的正文。"""
        lines = [f"{_severity_icon(self.severity)} {self.title}", "", self.summary]
        if self.impact:
            lines.extend(["", f"影响范围：{self.impact}"])
        if self.suggested_actions:
            lines.extend(["", "建议操作："])
            lines.extend(
                f"{index}. {action}"
                for index, action in enumerate(self.suggested_actions, start=1)
            )
        trace = []
        if self.run_id:
            trace.append(f"run_id={self.run_id}")
        if self.task_id:
            trace.append(f"task_id={self.task_id}")
        if trace:
            lines.extend(["", f"追溯入口：{' | '.join(trace)}"])
        return "\n".join(lines).strip()
