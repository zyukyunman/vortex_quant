"""CLI 后台任务 / server 测试。"""
from __future__ import annotations

import argparse
import json
import logging
import signal
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

import vortex.cli as cli
from vortex.config.profile.models import DataProfile
from vortex.data.manifest import SyncManifest
from vortex.data.pipeline import RunReport
from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend
from vortex.runtime.database import Database
from vortex.runtime.task_queue import TaskProgress, TaskQueue, TaskStatus
from vortex.runtime.workspace import Workspace
from vortex.shared.errors import DataError
from vortex.cli import (
    _collect_data_status,
    _submit_data_background_task,
)


class TestServerStart:
    def test_cmd_server_start_launches_background_process(
        self, monkeypatch, tmp_path, capsys
    ):
        calls: list[dict] = []

        class _Proc:
            pid = 56789

            def poll(self):
                return None

        def _fake_popen(command, **kwargs):
            calls.append({"command": command, "kwargs": kwargs})
            return _Proc()

        monkeypatch.setattr(cli.subprocess, "Popen", _fake_popen)

        cli.cmd_server(
            argparse.Namespace(
                server_action="start",
                root=str(tmp_path / "workspace"),
                foreground=False,
            )
        )

        assert len(calls) == 1
        command = calls[0]["command"]
        assert command[:4] == [cli.sys.executable, "-m", "vortex", "server"]
        assert "--foreground" in command
        assert calls[0]["kwargs"]["start_new_session"] is True
        assert "已在后台启动" in capsys.readouterr().out


class TestUpdateFrequencyScope:
    def test_update_uses_workday_defaults_when_user_did_not_override(self):
        resolved = cli._resolve_update_frequency_scope(
            "update",
            datasets=None,
            update_frequencies=[],
            now=datetime(2026, 4, 17, 9, 30),
        )

        assert resolved == ["daily", "intraday"]

    def test_update_uses_weekend_defaults_when_user_did_not_override(self):
        resolved = cli._resolve_update_frequency_scope(
            "update",
            datasets=None,
            update_frequencies=[],
            now=datetime(2026, 4, 18, 9, 30),
        )

        assert resolved == ["weekly", "monthly", "quarterly", "other"]

    def test_explicit_dataset_or_frequency_override_disables_default_bucket(self):
        assert cli._resolve_update_frequency_scope(
            "update",
            datasets=["events"],
            update_frequencies=[],
            now=datetime(2026, 4, 18, 9, 30),
        ) == []
        assert cli._resolve_update_frequency_scope(
            "update",
            datasets=None,
            update_frequencies=["daily"],
            now=datetime(2026, 4, 18, 9, 30),
        ) == ["daily"]


class TestLatestLogLinks:
    def test_refresh_latest_log_links_updates_generic_and_prefixed_aliases(self, tmp_path):
        logs_dir = tmp_path / "logs"
        first = logs_dir / "data-bootstrap-20260409_100117.log"
        second = logs_dir / "data-bootstrap-20260409_120305.log"

        cli._refresh_latest_log_links(first)
        assert (logs_dir / "latest.log").is_symlink()
        assert (logs_dir / "latest.log").readlink() == Path(first.name)
        assert (logs_dir / "data-bootstrap-latest.log").is_symlink()
        assert (logs_dir / "data-bootstrap-latest.log").readlink() == Path(first.name)

        cli._refresh_latest_log_links(second)
        assert (logs_dir / "latest.log").readlink() == Path(second.name)
        assert (logs_dir / "data-bootstrap-latest.log").readlink() == Path(second.name)


class TestDataBackgroundTasks:
    def test_submit_data_background_task_records_pending_task(
        self, monkeypatch, tmp_path
    ):
        class _Proc:
            pid = 24680

        monkeypatch.setattr(
            cli,
            "_launch_background_process",
            lambda command, log_path: _Proc(),
        )

        root = tmp_path / "workspace"
        Workspace(root).initialize()
        result = _submit_data_background_task(
            root=root,
            profile_name="default",
            action="bootstrap",
            fmt="json",
            datasets=["bars", "valuation"],
        )

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task = TaskQueue(db).get_task(result["task_id"])
        assert task is not None
        assert task["status"] == "pending"
        progress = json.loads(task["progress_json"])
        assert progress["pid"] == 24680
        assert progress["log_path"].endswith(".log")
        db.close()

    def test_submit_data_background_task_reuses_existing_active_task(
        self, monkeypatch, tmp_path
    ):
        calls = []

        class _Proc:
            pid = 13579

        def _fake_launch(command, log_path):
            calls.append((command, log_path))
            return _Proc()

        monkeypatch.setattr(cli, "_launch_background_process", _fake_launch)

        root = tmp_path / "workspace"
        Workspace(root).initialize()
        first = _submit_data_background_task(
            root=root,
            profile_name="default",
            action="bootstrap",
            fmt="json",
        )
        second = _submit_data_background_task(
            root=root,
            profile_name="default",
            action="bootstrap",
            fmt="json",
        )

        assert first["task_id"] == second["task_id"]
        assert second["status"] == "deduplicated"
        assert len(calls) == 1

    def test_collect_data_status_includes_active_tasks(self, monkeypatch, tmp_path):
        class _Proc:
            pid = 97531

        monkeypatch.setattr(
            cli,
            "_launch_background_process",
            lambda command, log_path: _Proc(),
        )

        root = tmp_path / "workspace"
        Workspace(root).initialize()
        result = _submit_data_background_task(
            root=root,
            profile_name="default",
            action="update",
            fmt="json",
        )

        status = _collect_data_status(root, "default")
        assert status["profile"] == "default"
        assert len(status["active_tasks"]) == 1
        assert status["active_tasks"][0]["task_id"] == result["task_id"]
        assert status["latest_task"]["task_id"] == result["task_id"]

    def test_collect_data_status_includes_progress_fields(self, monkeypatch, tmp_path):
        class _Proc:
            pid = 86420

        monkeypatch.setattr(
            cli,
            "_launch_background_process",
            lambda command, log_path: _Proc(),
        )

        root = tmp_path / "workspace"
        Workspace(root).initialize()
        result = _submit_data_background_task(
            root=root,
            profile_name="default",
            action="bootstrap",
            fmt="json",
        )

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task_queue = TaskQueue(db)
        task_queue.update_progress(
            result["task_id"],
            TaskProgress(
                run_id=result["run_id"],
                current_stage="fetch",
                total_stages=5,
                completed_stages=1,
                current_dataset="bars",
                total_datasets=61,
                completed_datasets=2,
                current_chunk=345,
                total_chunks=3460,
                written_rows=123456,
                message="bars trade_date=20260407",
                log_path=result["log_path"],
                pid=_Proc.pid,
            ),
        )
        db.close()

        status = _collect_data_status(root, "default")
        task = status["active_tasks"][0]
        assert task["current_stage"] == "fetch"
        assert task["current_dataset"] == "bars"
        assert task["total_datasets"] == 61
        assert task["current_chunk"] == 345
        assert task["total_chunks"] == 3460
        assert task["written_rows"] == 123456
        assert task["log_path"] == result["log_path"]

    def test_collect_data_status_treats_alive_orphan_task_as_active(
        self, monkeypatch, tmp_path
    ):
        root = tmp_path / "workspace"
        Workspace(root).initialize()

        manifest = SyncManifest(root / "state" / "manifests" / "default" / "sync_manifest.db")
        manifest.create_run("run_orphan", "default", "bootstrap")
        manifest.update_status("run_orphan", "running")
        manifest.close()

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task_queue = TaskQueue(db)
        task_id = task_queue.submit("data", "bootstrap", "default", "run_orphan")
        task_queue.update_progress(
            task_id,
            TaskProgress(
                run_id="run_orphan",
                current_stage="fetch",
                current_dataset="events",
                log_path=str(root / "state" / "logs" / "bootstrap.log"),
                pid=86420,
            ),
        )
        task_queue.update_status(task_id, TaskStatus.FAILED, error="interrupted: server crashed")
        db.close()

        monkeypatch.setattr(cli, "_is_pid_alive", lambda pid: int(pid) == 86420)

        status = _collect_data_status(root, "default")
        assert len(status["active_tasks"]) == 1
        task = status["active_tasks"][0]
        assert task["task_id"] == task_id
        assert task["status"] == "running"
        assert "task_queue 状态已过期" in str(task["message"])

    def test_print_data_logs_returns_tail_as_json(self, monkeypatch, tmp_path, capsys):
        class _Proc:
            pid = 54321

        monkeypatch.setattr(
            cli,
            "_launch_background_process",
            lambda command, log_path: _Proc(),
        )

        root = tmp_path / "workspace"
        Workspace(root).initialize()
        result = _submit_data_background_task(
            root=root,
            profile_name="default",
            action="update",
            fmt="json",
        )
        capsys.readouterr()
        log_path = tmp_path / "task.log"
        log_path.write_text("line-1\nline-2\nline-3\n", encoding="utf-8")

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task_queue = TaskQueue(db)
        task_queue.update_progress(
            result["task_id"],
            TaskProgress(
                run_id=result["run_id"],
                current_stage="fetch",
                total_stages=5,
                completed_stages=1,
                message="running update",
                log_path=str(log_path),
                pid=_Proc.pid,
            ),
        )
        db.close()

        cli._print_data_logs(
            root,
            "default",
            task_id=result["task_id"],
            lines=2,
            follow=False,
            fmt="json",
        )
        payload = json.loads(capsys.readouterr().out)
        assert payload["task_id"] == result["task_id"]
        assert payload["tail"] == "line-2\nline-3"

    def test_cancel_data_task_marks_task_cancelled(self, monkeypatch, tmp_path, capsys):
        class _Proc:
            pid = 45678

        signals: list[tuple[int, int]] = []

        def _fake_kill(pid, sig):
            signals.append((int(pid), int(sig)))

        monkeypatch.setattr(
            cli,
            "_launch_background_process",
            lambda command, log_path: _Proc(),
        )
        monkeypatch.setattr(cli.os, "kill", _fake_kill)
        monkeypatch.setattr(cli.time, "sleep", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(cli, "_is_pid_alive", lambda pid: False)

        root = tmp_path / "workspace"
        Workspace(root).initialize()
        result = _submit_data_background_task(
            root=root,
            profile_name="default",
            action="bootstrap",
            fmt="json",
        )
        capsys.readouterr()
        manifest = SyncManifest(root / "state" / "manifests" / "default" / "sync_manifest.db")
        manifest.create_run(str(result["run_id"]), "default", "bootstrap")
        manifest.update_status(str(result["run_id"]), "running")
        manifest.close()

        cli._cancel_data_task(
            root,
            "default",
            task_id=result["task_id"],
            fmt="json",
        )
        payload = json.loads(capsys.readouterr().out)
        assert payload["task_id"] == result["task_id"]
        assert payload["signal_sent"] is True
        assert (int(_Proc.pid), int(signal.SIGTERM)) in signals

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task_queue = TaskQueue(db)
        task = task_queue.get_task(result["task_id"])
        db.close()
        assert task is not None
        assert task["status"] == "cancelled"

        manifest = SyncManifest(root / "state" / "manifests" / "default" / "sync_manifest.db")
        run = manifest.get_run(str(result["run_id"]))
        manifest.close()
        assert run is not None
        assert run["status"] == "cancelled"

    def test_cancel_data_task_can_cancel_alive_orphan_worker(
        self, monkeypatch, tmp_path, capsys
    ):
        signals: list[tuple[int, int]] = []

        def _fake_kill(pid, sig):
            signals.append((int(pid), int(sig)))

        monkeypatch.setattr(cli.os, "kill", _fake_kill)
        monkeypatch.setattr(cli.time, "sleep", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(cli, "_is_pid_alive", lambda pid: int(pid) == 45678)

        root = tmp_path / "workspace"
        Workspace(root).initialize()

        manifest = SyncManifest(root / "state" / "manifests" / "default" / "sync_manifest.db")
        manifest.create_run("run_orphan", "default", "bootstrap")
        manifest.update_status("run_orphan", "running")
        manifest.close()

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task_queue = TaskQueue(db)
        task_id = task_queue.submit("data", "bootstrap", "default", "run_orphan")
        task_queue.update_progress(
            task_id,
            TaskProgress(
                run_id="run_orphan",
                current_stage="fetch",
                current_dataset="events",
                log_path=str(root / "state" / "logs" / "bootstrap.log"),
                pid=45678,
            ),
        )
        task_queue.update_status(task_id, TaskStatus.FAILED, error="interrupted: server crashed")
        db.close()

        cli._cancel_data_task(root, "default", task_id=None, fmt="json")
        payload = json.loads(capsys.readouterr().out)
        assert payload["task_id"] == task_id
        assert payload["signal_sent"] is True
        assert (45678, int(signal.SIGTERM)) in signals

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task = TaskQueue(db).get_task(task_id)
        db.close()
        assert task is not None
        assert task["status"] == "cancelled"

        manifest = SyncManifest(root / "state" / "manifests" / "default" / "sync_manifest.db")
        run = manifest.get_run("run_orphan")
        manifest.close()
        assert run is not None
        assert run["status"] == "cancelled"

    def test_cmd_data_status_watch_routes_to_helper(self, monkeypatch, tmp_path):
        captured: dict[str, object] = {}

        def _fake_watch(root, profile_name, fmt, interval):
            captured["root"] = root
            captured["profile_name"] = profile_name
            captured["fmt"] = fmt
            captured["interval"] = interval

        monkeypatch.setattr(cli, "_watch_data_status", _fake_watch)

        cli.cmd_data(
            argparse.Namespace(
                data_action="status",
                root=str(tmp_path / "workspace"),
                profile=None,
                datasets=None,
                dry_run=False,
                format="text",
                watch=True,
                interval=2.5,
            )
        )

        assert captured["profile_name"] == "default"
        assert captured["fmt"] == "text"
        assert captured["interval"] == 2.5

    def test_cmd_data_bootstrap_defaults_to_background_submission(
        self, monkeypatch, tmp_path
    ):
        captured: dict[str, object] = {}

        def _fake_submit(**kwargs):
            captured.update(kwargs)
            return {"status": "submitted"}

        monkeypatch.setattr(cli, "_submit_data_background_task", _fake_submit)
        monkeypatch.setattr(cli, "_build_data_pipeline", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("should not build pipeline")))

        cli.cmd_data(
            argparse.Namespace(
                data_action="bootstrap",
                root=str(tmp_path / "workspace"),
                profile=None,
                datasets=None,
                verbose=False,
                dry_run=False,
                format="text",
                foreground=False,
            )
        )

        assert captured["action"] == "bootstrap"
        assert captured["update_frequencies"] == []

    def test_cmd_data_update_defaults_to_workday_frequency_bucket(
        self, monkeypatch, tmp_path
    ):
        captured: dict[str, object] = {}

        def _fake_submit(**kwargs):
            captured.update(kwargs)
            return {"status": "submitted"}

        monkeypatch.setattr(cli, "_submit_data_background_task", _fake_submit)
        monkeypatch.setattr(
            cli,
            "_resolve_update_frequency_scope",
            lambda action, datasets, update_frequencies, now=None: ["daily", "intraday"],
        )

        cli.cmd_data(
            argparse.Namespace(
                data_action="update",
                root=str(tmp_path / "workspace"),
                profile=None,
                datasets=None,
                frequencies=None,
                verbose=False,
                dry_run=False,
                format="text",
                foreground=False,
            )
        )

        assert captured["action"] == "update"
        assert captured["update_frequencies"] == ["daily", "intraday"]

    def test_cmd_data_foreground_worker_accepts_keyword_progress_callback(
        self, monkeypatch, tmp_path
    ):
        root = tmp_path / "workspace"
        Workspace(root).initialize()

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task_queue = TaskQueue(db)
        run_id = "data_test_run"
        task_id = task_queue.submit("data", "bootstrap", "default", run_id)
        task_queue.update_progress(
            task_id,
            TaskProgress(
                run_id=run_id,
                log_path=str(root / "state" / "logs" / "bootstrap.log"),
                pid=12345,
            ),
        )
        db.close()

        class _Manifest:
            def close(self) -> None:
                return

        class _Pipeline:
            def __init__(self, progress_callback):
                self._progress_callback = progress_callback

            def bootstrap(self, profile, dry_run=False, run_id=None):
                self._progress_callback(
                    force=True,
                    current_stage="fetch",
                    total_stages=5,
                    completed_stages=1,
                    current_dataset="bars",
                    total_datasets=3,
                    completed_datasets=0,
                    current_chunk=12,
                    total_chunks=34,
                    written_rows=5678,
                    message="bars chunk 12/34",
                )
                return RunReport(
                    run_id=run_id or "data_test_run",
                    action="bootstrap",
                    status="success",
                    total_rows=5678,
                )

        def _fake_build_data_pipeline(*_args, **kwargs):
            return None, _Manifest(), _Pipeline(kwargs["progress_callback"]), DataProfile(name="default")

        monkeypatch.setattr(cli, "_build_data_pipeline", _fake_build_data_pipeline)

        cli.cmd_data(
            argparse.Namespace(
                data_action="bootstrap",
                root=str(root),
                profile=None,
                datasets=None,
                verbose=False,
                dry_run=False,
                format="json",
                foreground=True,
                task_id=task_id,
                run_id=run_id,
            )
        )

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task = TaskQueue(db).get_task(task_id)
        db.close()

        assert task is not None
        assert task["status"] == "success"
        progress = TaskProgress.from_dict(json.loads(task["progress_json"]))
        assert progress.run_id == run_id
        assert progress.current_stage == "finished"
        assert progress.completed_stages == 5
        assert progress.current_dataset == "bars"
        assert progress.current_chunk == 12
        assert progress.total_chunks == 34
        assert progress.written_rows == 5678
        assert progress.log_path and progress.log_path.endswith("bootstrap.log")
        assert progress.pid == 12345

    def test_cmd_data_foreground_worker_marks_cancelled_without_failed_log(
        self, monkeypatch, tmp_path, capsys, caplog
    ):
        root = tmp_path / "workspace"
        Workspace(root).initialize()

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task_queue = TaskQueue(db)
        run_id = "data_cancel_run"
        task_id = task_queue.submit("data", "bootstrap", "default", run_id)
        task_queue.update_progress(
            task_id,
            TaskProgress(
                run_id=run_id,
                log_path=str(root / "state" / "logs" / "bootstrap.log"),
                pid=12345,
            ),
        )
        db.close()

        class _Manifest:
            def close(self) -> None:
                return

        class _Pipeline:
            def __init__(self, progress_callback):
                self._progress_callback = progress_callback

            def bootstrap(self, profile, dry_run=False, run_id=None):
                self._progress_callback(
                    force=True,
                    current_stage="fetch",
                    total_stages=5,
                    completed_stages=1,
                    current_dataset="dc_hot",
                    total_datasets=52,
                    completed_datasets=31,
                    current_chunk=486,
                    total_chunks=545,
                    written_rows=123456,
                    message="dc_hot trade_date=20260105",
                )
                raise DataError(
                    code="DATA_TASK_CANCELLED",
                    message="数据任务已取消",
                )

        def _fake_build_data_pipeline(*_args, **kwargs):
            return None, _Manifest(), _Pipeline(kwargs["progress_callback"]), DataProfile(name="default")

        monkeypatch.setattr(cli, "_build_data_pipeline", _fake_build_data_pipeline)
        caplog.set_level(logging.INFO)

        with pytest.raises(SystemExit) as exc_info:
            cli.cmd_data(
                argparse.Namespace(
                    data_action="bootstrap",
                    root=str(root),
                    profile=None,
                    datasets=None,
                    verbose=False,
                    dry_run=False,
                    format="text",
                    foreground=True,
                    task_id=task_id,
                    run_id=run_id,
                )
            )

        assert exc_info.value.code == 130
        err = capsys.readouterr().err
        assert "🛑 [DATA_TASK_CANCELLED] 数据任务已取消" in err
        assert "data bootstrap 已取消" in caplog.text
        assert "data bootstrap 失败" not in caplog.text

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task = TaskQueue(db).get_task(task_id)
        db.close()

        assert task is not None
        assert task["status"] == "cancelled"
        assert task["error"] == "[DATA_TASK_CANCELLED] 数据任务已取消"
        progress = TaskProgress.from_dict(json.loads(task["progress_json"]))
        assert progress.current_stage == "cancelled"
        assert progress.current_dataset == "dc_hot"

    def test_cmd_data_foreground_worker_auto_recovers_retryable_partial_success(
        self,
        monkeypatch,
        tmp_path,
    ):
        root = tmp_path / "workspace"
        Workspace(root).initialize()

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task_queue = TaskQueue(db)
        task_id = task_queue.submit("data", "bootstrap", "default", "data_initial_run")
        db.close()

        class _Manifest:
            def close(self) -> None:
                return

        class _Pipeline:
            def __init__(self):
                self.calls = 0

            def bootstrap(self, profile, dry_run=False, run_id=None):
                self.calls += 1
                if self.calls == 1:
                    return RunReport(
                        run_id=run_id or "data_first_attempt",
                        action="bootstrap",
                        status="partial_success",
                        total_rows=12,
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
                return RunReport(
                    run_id=run_id or "data_second_attempt",
                    action="bootstrap",
                    status="success",
                    total_rows=24,
                )

        pipeline = _Pipeline()
        notifications: list[object] = []

        class _NotificationService:
            def __init__(self, _db):
                return

            def notify(self, message, profile_notification=None):
                notifications.append((message, profile_notification))
                return [{"status": "sent"}]

        def _fake_build_data_pipeline(*_args, **_kwargs):
            return None, _Manifest(), pipeline, DataProfile(
                name="default",
                notification={"enabled": True, "level": "warning", "channel": "feishu"},
            )

        monkeypatch.setattr(cli, "_build_data_pipeline", _fake_build_data_pipeline)
        monkeypatch.setattr(cli.time, "sleep", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(
            "vortex.notification.service.NotificationService",
            _NotificationService,
        )

        cli.cmd_data(
            argparse.Namespace(
                data_action="bootstrap",
                root=str(root),
                profile=None,
                datasets=None,
                verbose=False,
                dry_run=False,
                format="json",
                foreground=True,
                task_id=task_id,
                run_id="data_initial_run",
            )
        )

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task = TaskQueue(db).get_task(task_id)
        db.close()

        assert pipeline.calls == 2
        assert not notifications
        assert task is not None
        assert task["status"] == "success"
        progress = TaskProgress.from_dict(json.loads(task["progress_json"]))
        assert progress.retry_attempt == 2
        assert progress.max_retry_attempts == 4
        assert progress.next_retry_at is None

    def test_cmd_data_foreground_worker_notifies_non_retryable_partial_success(
        self,
        monkeypatch,
        tmp_path,
    ):
        root = tmp_path / "workspace"
        Workspace(root).initialize()

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task_queue = TaskQueue(db)
        task_id = task_queue.submit("data", "bootstrap", "default", "data_warning_run")
        db.close()

        class _Manifest:
            def close(self) -> None:
                return

        class _Pipeline:
            calls = 0

            def bootstrap(self, profile, dry_run=False, run_id=None):
                self.calls += 1
                return RunReport(
                    run_id=run_id or "data_warning_run",
                    action="bootstrap",
                    status="partial_success",
                    total_rows=10,
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

        notifications: list[object] = []

        class _NotificationService:
            def __init__(self, _db):
                return

            def notify(self, message, profile_notification=None):
                notifications.append((message, profile_notification))
                return [{"status": "sent"}]

        def _fake_build_data_pipeline(*_args, **_kwargs):
            return None, _Manifest(), _Pipeline(), DataProfile(
                name="default",
                notification={"enabled": True, "level": "warning", "channel": "feishu"},
            )

        monkeypatch.setattr(cli, "_build_data_pipeline", _fake_build_data_pipeline)
        monkeypatch.setattr(
            "vortex.notification.service.NotificationService",
            _NotificationService,
        )

        cli.cmd_data(
            argparse.Namespace(
                data_action="bootstrap",
                root=str(root),
                profile=None,
                datasets=None,
                verbose=False,
                dry_run=False,
                format="json",
                foreground=True,
                task_id=task_id,
                run_id="data_warning_run",
            )
        )

        db = Database(root / "state" / "control.db")
        db.initialize_tables()
        task = TaskQueue(db).get_task(task_id)
        db.close()

        assert task is not None
        assert task["status"] == "partial_success"
        assert len(notifications) == 1
        message, profile_notification = notifications[0]
        assert message.event_type == "data.sync.partial_failed"
        assert message.severity == "warning"
        assert "部分完成" in message.summary
        assert profile_notification == {
            "enabled": True,
            "level": "warning",
            "channel": "feishu",
        }

    def test_cmd_data_inspect_returns_dataset_summary_and_preview(
        self, tmp_path, capsys
    ):
        root = tmp_path / "workspace"
        Workspace(root).initialize()
        storage = ParquetDuckDBBackend(root / "data")
        storage.initialize()
        storage.upsert(
            "bars",
            pd.DataFrame(
                {
                    "symbol": ["000001.SZ", "600519.SH"],
                    "date": ["20260407", "20260407"],
                    "close": [12.3, 1800.0],
                }
            ),
            {"date": "20260407"},
        )

        cli.cmd_data(
            argparse.Namespace(
                data_action="inspect",
                root=str(root),
                profile=None,
                datasets=None,
                dry_run=False,
                format="json",
                dataset="bars",
                columns="symbol,date,close",
                filters=["symbol=000001.SZ"],
                limit=5,
            )
        )

        payload = json.loads(capsys.readouterr().out)
        assert payload["dataset"] == "bars"
        assert payload["api"] == "daily"
        assert payload["api_doc_url"] == "https://tushare.pro/document/2?doc_id=27"
        assert "不复权日线" in payload["note"]
        assert payload["materialized"] is True
        assert payload["total_rows"] == 2
        assert payload["matching_rows"] == 1
        assert payload["preview_rows"][0]["symbol"] == "000001.SZ"
        assert any(item["name"] == "close" for item in payload["columns"])
        assert any(
            item["name"] == "close" and "收盘价" in item["description"]
            for item in payload["columns"]
        )

    def test_cmd_data_inspect_without_dataset_lists_materialized_tables(
        self, tmp_path, capsys
    ):
        root = tmp_path / "workspace"
        Workspace(root).initialize()
        storage = ParquetDuckDBBackend(root / "data")
        storage.initialize()
        storage.upsert(
            "bars",
            pd.DataFrame({"symbol": ["000001.SZ"], "date": ["20260407"], "close": [12.3]}),
            {"date": "20260407"},
        )
        storage.upsert(
            "instruments",
            pd.DataFrame({"symbol": ["000001.SZ"], "name": ["平安银行"]}),
            {},
        )

        cli.cmd_data(
            argparse.Namespace(
                data_action="inspect",
                root=str(root),
                profile=None,
                datasets=None,
                dry_run=False,
                format="json",
                dataset=None,
                columns=None,
                filters=[],
                limit=10,
            )
        )

        payload = json.loads(capsys.readouterr().out)
        assert payload["mode"] == "catalog"
        assert {item["dataset"] for item in payload["datasets"]} == {"bars", "instruments"}
        assert any(
            item["dataset"] == "bars"
            and item["api"] == "daily"
            and item["api_doc_url"] == "https://tushare.pro/document/2?doc_id=27"
            and item["note"]
            for item in payload["datasets"]
        )
