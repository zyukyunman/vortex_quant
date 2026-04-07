"""CLI 交互辅助测试。"""
from __future__ import annotations

import argparse
import io
import json
import signal

import pytest

import vortex.cli as cli
from vortex.config.profile.models import DataProfile
from vortex.data.pipeline import RunReport
from vortex.runtime.database import Database
from vortex.runtime.task_queue import TaskProgress, TaskQueue
from vortex.runtime.workspace import Workspace
from vortex.cli import (
    InitCancelled,
    _apply_multi_select_command,
    _apply_dataset_override,
    _apply_multi_select_key,
    _build_data_task_action,
    _build_default_data_config,
    _collect_data_status,
    _format_selection_summary,
    _multi_select_window,
    _parse_dataset_override,
    _prompt,
    _redraw_multi_select,
    _run_initial_bootstrap,
    _resolve_data_profile_name,
    _submit_data_background_task,
    _truncate_terminal_line,
)


class TestApplyMultiSelectCommand:
    def test_toggle_with_spaces_and_commas(self):
        options = ["instruments", "calendar", "bars", "fundamental", "events"]
        current = ["instruments", "calendar", "bars"]

        updated, error = _apply_multi_select_command(
            current=current,
            options=options,
            defaults=current,
            answer="2, 4 5",
        )

        assert error is None
        assert updated == ["instruments", "bars", "fundamental", "events"]

    def test_select_all(self):
        options = ["a1", "a2", "a3"]
        updated, error = _apply_multi_select_command(
            current=["a1"],
            options=options,
            defaults=["a1"],
            answer="a",
        )

        assert error is None
        assert updated == options

    def test_clear_all(self):
        options = ["a1", "a2", "a3"]
        updated, error = _apply_multi_select_command(
            current=["a1", "a2"],
            options=options,
            defaults=["a1"],
            answer="n",
        )

        assert error is None
        assert updated == []

    def test_restore_defaults(self):
        options = ["a1", "a2", "a3"]
        defaults = ["a1", "a3"]
        updated, error = _apply_multi_select_command(
            current=["a2"],
            options=options,
            defaults=defaults,
            answer="d",
        )

        assert error is None
        assert updated == defaults

    def test_reject_invalid_token(self):
        options = ["a1", "a2", "a3"]
        current = ["a1"]
        updated, error = _apply_multi_select_command(
            current=current,
            options=options,
            defaults=current,
            answer="1 foo",
        )

        assert updated == current
        assert error == "无法识别的输入: foo"

    def test_reject_out_of_range_index(self):
        options = ["a1", "a2", "a3"]
        current = ["a1"]
        updated, error = _apply_multi_select_command(
            current=current,
            options=options,
            defaults=current,
            answer="4",
        )

        assert updated == current
        assert error == "编号超出范围: 4"


class TestApplyMultiSelectKey:
    def test_space_toggles_current_option(self):
        cursor, selected, done = _apply_multi_select_key(
            cursor=1,
            current=["instruments"],
            options=["instruments", "calendar", "bars"],
            defaults=["instruments"],
            key="space",
        )

        assert cursor == 1
        assert selected == ["instruments", "calendar"]
        assert done is False

    def test_up_down_move_cursor(self):
        cursor, selected, done = _apply_multi_select_key(
            cursor=0,
            current=["instruments"],
            options=["instruments", "calendar", "bars"],
            defaults=["instruments"],
            key="down",
        )
        assert cursor == 1
        assert selected == ["instruments"]
        assert done is False

        cursor, selected, done = _apply_multi_select_key(
            cursor=cursor,
            current=selected,
            options=["instruments", "calendar", "bars"],
            defaults=["instruments"],
            key="up",
        )
        assert cursor == 0
        assert selected == ["instruments"]
        assert done is False

    def test_a_n_d_and_enter(self):
        options = ["instruments", "calendar", "bars"]

        cursor, selected, done = _apply_multi_select_key(
            cursor=0,
            current=["instruments"],
            options=options,
            defaults=["instruments", "bars"],
            key="a",
        )
        assert selected == options
        assert done is False

        cursor, selected, done = _apply_multi_select_key(
            cursor=cursor,
            current=selected,
            options=options,
            defaults=["instruments", "bars"],
            key="n",
        )
        assert selected == []
        assert done is False

        cursor, selected, done = _apply_multi_select_key(
            cursor=cursor,
            current=selected,
            options=options,
            defaults=["instruments", "bars"],
            key="d",
        )
        assert selected == ["instruments", "bars"]
        assert done is False

        cursor, selected, done = _apply_multi_select_key(
            cursor=cursor,
            current=selected,
            options=options,
            defaults=["instruments", "bars"],
            key="enter",
        )
        assert selected == ["instruments", "bars"]
        assert done is True


class TestDataCliHelpers:
    def test_build_default_data_config_is_minimal(self):
        config = _build_default_data_config(history_start="20120101")

        assert config == {
            "name": "default",
            "type": "data",
            "provider": "tushare",
            "history_start": "20120101",
        }

    def test_build_default_data_config_keeps_schedule_only_when_set(self):
        config = _build_default_data_config(
            history_start="20120101",
            schedule="0 18 * * 1-5",
        )

        assert config["schedule"] == "0 18 * * 1-5"
        assert "quality_pack" not in config
        assert "pit_pack" not in config
        assert "publish_pack" not in config
        assert "storage_pack" not in config

    def test_resolve_data_profile_name_defaults_to_default(self):
        assert _resolve_data_profile_name(None) == "default"
        assert _resolve_data_profile_name("") == "default"

    def test_resolve_data_profile_name_accepts_yaml_path(self):
        assert (
            _resolve_data_profile_name(
                "/Users/demo/vortex_workspace/profiles/default.yaml"
            )
            == "default"
        )

    def test_parse_dataset_override(self):
        assert _parse_dataset_override("bars, valuation ,calendar") == [
            "bars",
            "valuation",
            "calendar",
        ]

    def test_apply_dataset_override(self):
        profile = DataProfile(name="default")
        updated = _apply_dataset_override(profile, ["bars", "valuation"])
        assert updated.datasets == ["bars", "valuation"]
        assert updated.exclude_datasets == []
        assert updated.priority_datasets == ["bars", "valuation"]

    def test_multi_select_window_pages_around_cursor(self):
        start, end = _multi_select_window(total_options=61, cursor=30, visible_count=10)
        assert end - start == 10
        assert start <= 30 < end

    def test_format_selection_summary_truncates_preview(self):
        text = _format_selection_summary(
            ["a", "b", "c", "d", "e", "f"],
            max_items=3,
        )
        assert text == "已选择 6 项：a, b, c ..."

    def test_truncate_terminal_line_respects_display_width(self):
        assert _truncate_terminal_line("操作说明：↑↓ 移动；空格 勾选/取消；回车 确认", 20).endswith(
            "..."
        )
        assert _truncate_terminal_line("top_list", 20) == "top_list"

    def test_redraw_multi_select_uses_crlf_in_raw_terminal(self, monkeypatch):
        buffer = io.StringIO()
        monkeypatch.setattr(cli.sys, "stdout", buffer)
        _redraw_multi_select(["line1", "line2"])
        assert "\r\n" in buffer.getvalue()

    def test_build_data_task_action_with_ranges(self):
        assert _build_data_task_action("bootstrap") == "bootstrap"
        assert _build_data_task_action(
            "backfill",
            start="20250101",
            end="20250131",
        ) == "backfill:20250101-20250131"
        assert _build_data_task_action("publish", as_of="20250407") == "publish:20250407"


class TestInitCancellation:
    def test_prompt_keyboard_interrupt_raises_init_cancelled(self, monkeypatch):
        def _raise(_: str) -> str:
            raise KeyboardInterrupt

        monkeypatch.setattr("builtins.input", _raise)
        with pytest.raises(InitCancelled):
            _prompt("请输入", "x")

    def test_cmd_init_cancelled_before_workspace_write(self, monkeypatch, tmp_path, capsys):
        root = tmp_path / "workspace"

        monkeypatch.setattr(cli, "_is_interactive", lambda: True)
        monkeypatch.setattr(cli, "_check_tushare_token", lambda: None)
        monkeypatch.setattr(cli, "_prompt", lambda *_args, **_kwargs: "20170101")

        def _cancel(*_args, **_kwargs):
            raise InitCancelled

        monkeypatch.setattr(cli, "_prompt_yes_no", _cancel)

        cli.cmd_init(argparse.Namespace(root=str(root), non_interactive=False))

        assert not root.exists()
        assert "已取消初始化" in capsys.readouterr().out

    def test_cmd_init_bootstrap_launch_failure_keeps_workspace(
        self, monkeypatch, tmp_path, capsys
    ):
        root = tmp_path / "workspace"

        monkeypatch.setattr(cli, "_is_interactive", lambda: True)
        monkeypatch.setattr(cli, "_check_tushare_token", lambda: "token")
        monkeypatch.setattr(cli, "_smoke_test_tushare", lambda _token: True)
        monkeypatch.setattr(cli, "_prompt", lambda *_args, **_kwargs: "20170101")

        answers = iter([True, False])
        monkeypatch.setattr(
            cli,
            "_prompt_yes_no",
            lambda *_args, **_kwargs: next(answers),
        )
        monkeypatch.setattr(cli, "_prompt_multi_select", lambda *_args, **_kwargs: ["bars"])

        def _launch_fail(*_args, **_kwargs):
            raise RuntimeError("launch failed")

        monkeypatch.setattr(cli, "_run_initial_bootstrap", _launch_fail)

        cli.cmd_init(argparse.Namespace(root=str(root), non_interactive=False))

        assert root.exists()
        assert "default.yaml" in {p.name for p in (root / "profiles").iterdir()}
        assert "首次数据更新启动失败" in capsys.readouterr().out


class TestInitialBootstrapLaunch:
    def test_run_initial_bootstrap_uses_shared_submit_helper(
        self, monkeypatch, tmp_path, capsys
    ):
        captured: dict[str, object] = {}

        def _fake_submit(**kwargs):
            captured.update(kwargs)
            return {"status": "submitted"}

        monkeypatch.setattr(cli, "_submit_data_background_task", _fake_submit)

        root = tmp_path / "workspace"
        _run_initial_bootstrap(root, "default", ["bars", "valuation"])

        assert captured["root"] == root
        assert captured["profile_name"] == "default"
        assert captured["action"] == "bootstrap"
        assert captured["datasets"] == ["bars", "valuation"]
        assert "数据集: 已选择 2 项" in capsys.readouterr().out


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
