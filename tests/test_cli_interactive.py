"""CLI 交互 / init 流程测试。"""
from __future__ import annotations

import argparse
import io

import pytest

import vortex.cli as cli
from vortex.config.profile.models import DataProfile
from vortex.cli import (
    InitCancelled,
    _apply_dataset_override,
    _apply_multi_select_command,
    _apply_multi_select_key,
    _build_data_task_action,
    _build_default_data_config,
    _format_selection_summary,
    _multi_select_window,
    _parse_dataset_override,
    _prompt,
    _redraw_multi_select,
    _resolve_data_profile_name,
    _run_initial_bootstrap,
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

        # answers: step3 run_bootstrap=True, step4 schedule=False, step5 feishu=False, step6 agent=False
        answers = iter([True, False, False, False])
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
