"""Data 域自检查 / 自恢复辅助。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from vortex.data.pipeline import RunReport
from vortex.notification.models import NotificationMessage

DEFAULT_DATA_AUTO_RECOVERY_DELAYS_SECONDS: tuple[float, ...] = (
    300.0,
    900.0,
    1800.0,
)
_RETRYABLE_ERROR_CODES = {
    "DATA_PROVIDER_FETCH_FAILED",
}
_NON_RETRYABLE_ERROR_CODES = {
    "DATA_PROVIDER_PERMISSION_DENIED",
    "DATA_PROVIDER_PERMISSION_REQUIRED",
    "DATA_PROVIDER_API_NOT_FOUND",
    "DATA_PROVIDER_UNSUPPORTED_FETCH_MODE",
    "DATA_PROVIDER_UNSUPPORTED_REFERENCE",
    "DATA_PUBLISH_QUALITY_FAILED",
}
_RETRYABLE_MESSAGE_TOKENS = (
    "connection reset",
    "connection aborted",
    "read timed out",
    "timed out",
    "timeout",
    "temporarily unavailable",
    "reset by peer",
    "rate limit",
    "too many requests",
    "超频",
    "频率限制",
    "429",
)
_NON_RETRYABLE_MESSAGE_TOKENS = (
    "权限",
    "permission",
    "必填参数",
    "unsupported",
    "not supported",
    "not unique",
    "质量门禁",
    "quality gate",
    "api not found",
)


@dataclass(frozen=True)
class DataFailureReason:
    """一次失败原因。"""

    dataset: str | None
    reason: str
    retryable: bool


@dataclass(frozen=True)
class DataRecoveryPlan:
    """本轮 run 的恢复决策。"""

    current_attempt: int
    max_attempts: int
    should_retry: bool
    next_delay_seconds: float | None
    retryable_failures: tuple[DataFailureReason, ...]
    terminal_failures: tuple[DataFailureReason, ...]
    event_type: str | None = None
    notification_type: str | None = None
    severity: str | None = None
    summary: str | None = None


def evaluate_run_report(
    report: RunReport,
    *,
    attempt: int,
    retry_delays: tuple[float, ...] = DEFAULT_DATA_AUTO_RECOVERY_DELAYS_SECONDS,
) -> DataRecoveryPlan:
    """根据 RunReport 判断是否自动恢复。"""
    max_attempts = 1 + len(retry_delays)
    if report.status == "success":
        return DataRecoveryPlan(
            current_attempt=attempt,
            max_attempts=max_attempts,
            should_retry=False,
            next_delay_seconds=None,
            retryable_failures=(),
            terminal_failures=(),
        )

    if report.status == "partial_success":
        failures = tuple(_extract_failures(report))
        retryable_failures = tuple(item for item in failures if item.retryable)
        terminal_failures = tuple(item for item in failures if not item.retryable)
        should_retry = bool(retryable_failures) and not terminal_failures and attempt < max_attempts
        next_delay_seconds = retry_delays[attempt - 1] if should_retry else None
        summary = f"{len(failures)} 个 dataset 未成功完成"
        return DataRecoveryPlan(
            current_attempt=attempt,
            max_attempts=max_attempts,
            should_retry=should_retry,
            next_delay_seconds=next_delay_seconds,
            retryable_failures=retryable_failures,
            terminal_failures=terminal_failures,
            event_type=None if should_retry else "data.sync.partial_failed",
            notification_type=None if should_retry else "data_anomaly",
            severity=None if should_retry else "warning",
            summary=summary,
        )

    if report.status == "cancelled":
        return DataRecoveryPlan(
            current_attempt=attempt,
            max_attempts=max_attempts,
            should_retry=False,
            next_delay_seconds=None,
            retryable_failures=(),
            terminal_failures=(),
        )

    failure = DataFailureReason(
        dataset=None,
        reason=report.error or report.status,
        retryable=_is_retryable_reason(report.error or report.status),
    )
    should_retry = failure.retryable and attempt < max_attempts
    next_delay_seconds = retry_delays[attempt - 1] if should_retry else None
    is_quality_blocked = "质量门禁" in (report.error or "")
    return DataRecoveryPlan(
        current_attempt=attempt,
        max_attempts=max_attempts,
        should_retry=should_retry,
        next_delay_seconds=next_delay_seconds,
        retryable_failures=(failure,) if failure.retryable else (),
        terminal_failures=() if failure.retryable else (failure,),
        event_type=None if should_retry else ("data.quality.blocked" if is_quality_blocked else "data.sync.failed"),
        notification_type=None if should_retry else "data_anomaly",
        severity=None if should_retry else "critical",
        summary=report.error or report.status,
    )


def build_run_notification_message(
    *,
    report: RunReport,
    plan: DataRecoveryPlan,
    action: str,
    root: Path,
    task_id: str | None,
) -> NotificationMessage:
    """把 Data 运行结果映射成统一通知消息。"""
    failures = plan.terminal_failures or plan.retryable_failures
    if report.status == "partial_success":
        title = "Vortex Data 通知"
        summary = f"{action} 部分完成，{plan.summary or '存在未完成 dataset'}"
        impact = _format_failure_list(failures)
    else:
        title = "Vortex Data 告警"
        summary = f"{action} 失败：{plan.summary or report.error or report.status}"
        impact = _format_failure_list(failures) or "本次运行未能收敛到可用结果"

    suggested_actions = [
        f"vortex data status --root {root}",
    ]
    if task_id:
        suggested_actions.append(
            f"vortex data logs --root {root} --task-id {task_id} --follow"
        )
    else:
        suggested_actions.append(f"vortex data logs --root {root} --follow")
    return NotificationMessage(
        event_type=plan.event_type or "data.sync.failed",
        notification_type=plan.notification_type or "data_anomaly",
        severity=(plan.severity or "critical"),
        title=title,
        summary=summary,
        impact=impact,
        suggested_actions=tuple(suggested_actions),
        run_id=report.run_id,
        task_id=task_id,
        detail={
            "action": action,
            "status": report.status,
            "total_rows": report.total_rows,
            "skipped_datasets": report.detail.get("skipped_datasets", []),
        },
    )


def _extract_failures(report: RunReport) -> list[DataFailureReason]:
    skipped = report.detail.get("skipped_datasets", [])
    failures: list[DataFailureReason] = []
    if not isinstance(skipped, list):
        return failures
    for item in skipped:
        if not isinstance(item, dict):
            continue
        dataset = str(item.get("dataset")) if item.get("dataset") is not None else None
        reason = str(item.get("reason") or "dataset 执行失败")
        failures.append(
            DataFailureReason(
                dataset=dataset,
                reason=reason,
                retryable=_is_retryable_reason(reason),
            )
        )
    return failures


def _is_retryable_reason(reason: str) -> bool:
    error_code = _extract_error_code(reason)
    if error_code in _NON_RETRYABLE_ERROR_CODES:
        return False
    if error_code in _RETRYABLE_ERROR_CODES:
        return True
    normalized = reason.lower()
    if any(token in normalized for token in _NON_RETRYABLE_MESSAGE_TOKENS):
        return False
    if any(token in normalized for token in _RETRYABLE_MESSAGE_TOKENS):
        return True
    return False


def _extract_error_code(reason: str) -> str | None:
    if not reason.startswith("["):
        return None
    closing = reason.find("]")
    if closing <= 1:
        return None
    return reason[1:closing]


def _format_failure_list(failures: tuple[DataFailureReason, ...]) -> str:
    if not failures:
        return ""
    parts: list[str] = []
    for failure in failures[:5]:
        if failure.dataset:
            parts.append(f"{failure.dataset}: {failure.reason}")
        else:
            parts.append(failure.reason)
    if len(failures) > 5:
        parts.append(f"其余 {len(failures) - 5} 项见日志")
    return "; ".join(parts)
