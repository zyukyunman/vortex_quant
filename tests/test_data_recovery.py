"""Data 自恢复判定测试。"""

from __future__ import annotations

from pathlib import Path

from vortex.data.pipeline import RunReport
from vortex.data.recovery import build_run_notification_message, evaluate_run_report


class TestDataRecovery:
    def test_retryable_partial_success_schedules_auto_recovery(self):
        report = RunReport(
            run_id="data_20260409_000001_abcd",
            action="bootstrap",
            status="partial_success",
            total_rows=100,
            error="1 个 dataset 被跳过",
            detail={
                "skipped_datasets": [
                    {
                        "dataset": "events",
                        "reason": "[DATA_PROVIDER_FETCH_FAILED] timeout",
                    }
                ]
            },
        )

        plan = evaluate_run_report(report, attempt=1)

        assert plan.should_retry is True
        assert plan.next_delay_seconds == 300.0
        assert plan.retryable_failures[0].dataset == "events"
        assert plan.event_type is None

    def test_non_retryable_partial_success_emits_warning(self):
        report = RunReport(
            run_id="data_20260409_000002_abcd",
            action="bootstrap",
            status="partial_success",
            total_rows=100,
            error="1 个 dataset 被跳过",
            detail={
                "skipped_datasets": [
                    {
                        "dataset": "limit_step",
                        "reason": "[DATA_PROVIDER_PERMISSION_DENIED] no access",
                    }
                ]
            },
        )

        plan = evaluate_run_report(report, attempt=1)

        assert plan.should_retry is False
        assert plan.event_type == "data.sync.partial_failed"
        assert plan.severity == "warning"
        assert plan.terminal_failures[0].dataset == "limit_step"

    def test_quality_gate_failure_emits_critical(self):
        report = RunReport(
            run_id="data_20260409_000003_abcd",
            action="bootstrap",
            status="failed",
            error="质量门禁未通过，阻断发布",
        )

        plan = evaluate_run_report(report, attempt=1)

        assert plan.should_retry is False
        assert plan.event_type == "data.quality.blocked"
        assert plan.severity == "critical"

    def test_build_notification_message_contains_trace_and_commands(self):
        report = RunReport(
            run_id="data_20260409_000004_abcd",
            action="bootstrap",
            status="partial_success",
            total_rows=42,
            error="1 个 dataset 被跳过",
            detail={
                "skipped_datasets": [
                    {
                        "dataset": "limit_step",
                        "reason": "[DATA_PROVIDER_PERMISSION_DENIED] no access",
                    }
                ]
            },
        )
        plan = evaluate_run_report(report, attempt=1)

        message = build_run_notification_message(
            report=report,
            plan=plan,
            action="bootstrap",
            root=Path("/tmp/vortex_workspace"),
            task_id="task-1",
        )

        text = message.to_text()
        assert message.event_type == "data.sync.partial_failed"
        assert "run_id=data_20260409_000004_abcd" in text
        assert "task_id=task-1" in text
        assert "vortex data status --root /tmp/vortex_workspace" in text
