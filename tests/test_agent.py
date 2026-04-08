"""Agent 基础设施测试。

覆盖：
- AgentConfig 从 dict / env 构建
- CopilotBackend 可用性检测与调用
- AgentChannel 协议实现与 prompt 构建
- NotificationService 对 AgentChannel 的自动发现与投递
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from vortex.agent.backend import AgentConfig, AgentResult, create_backend
from vortex.agent.copilot import CopilotBackend
from vortex.notification.channel.agent import AgentChannel
from vortex.notification.models import NotificationMessage
from vortex.notification.service import NotificationService
from vortex.runtime.database import Database
from vortex.shared.errors import NotificationError


# ------------------------------------------------------------------
# AgentConfig
# ------------------------------------------------------------------


class TestAgentConfig:
    def test_defaults(self):
        cfg = AgentConfig()
        assert cfg.enabled is False
        assert cfg.backend == "copilot"
        assert cfg.scope == ""
        assert cfg.effort == "high"
        assert cfg.max_attempts == 2

    def test_from_dict_enabled(self):
        cfg = AgentConfig.from_dict({
            "enabled": True,
            "backend": "copilot",
            "scope": "/tmp/test",
            "effort": "medium",
            "max_attempts": 3,
        })
        assert cfg.enabled is True
        assert cfg.scope == "/tmp/test"
        assert cfg.effort == "medium"
        assert cfg.max_attempts == 3

    def test_from_dict_empty(self):
        cfg = AgentConfig.from_dict({})
        assert cfg.enabled is False
        assert cfg.backend == "copilot"

    def test_from_dict_non_dict(self):
        cfg = AgentConfig.from_dict(None)  # type: ignore[arg-type]
        assert cfg.enabled is False

    def test_from_env(self):
        env = {
            "VORTEX_AGENT_ENABLED": "true",
            "VORTEX_AGENT_BACKEND": "copilot",
            "VORTEX_AGENT_SCOPE": "/my/repo",
            "VORTEX_AGENT_EFFORT": "low",
            "VORTEX_AGENT_MAX_ATTEMPTS": "5",
        }
        with patch.dict(os.environ, env, clear=False):
            cfg = AgentConfig.from_env()
        assert cfg.enabled is True
        assert cfg.scope == "/my/repo"
        assert cfg.effort == "low"
        assert cfg.max_attempts == 5

    def test_from_env_disabled_by_default(self):
        with patch.dict(os.environ, {}, clear=True):
            cfg = AgentConfig.from_env()
        assert cfg.enabled is False

    def test_from_env_bad_max_attempts(self):
        with patch.dict(os.environ, {"VORTEX_AGENT_MAX_ATTEMPTS": "abc"}, clear=False):
            cfg = AgentConfig.from_env()
        assert cfg.max_attempts == 2


# ------------------------------------------------------------------
# CopilotBackend
# ------------------------------------------------------------------


class TestCopilotBackend:
    def test_name(self):
        backend = CopilotBackend()
        assert backend.name == "copilot"

    def test_is_available_depends_on_binary(self):
        backend = CopilotBackend()
        # 不确定测试环境是否有 copilot，只验证返回 bool
        assert isinstance(backend.is_available(), bool)

    def test_invoke_without_binary_returns_failure(self):
        backend = CopilotBackend()
        backend._binary = None  # 模拟未安装
        result = backend.invoke("test prompt")
        assert result.success is False
        assert result.exit_code == -1
        assert "未安装" in result.output

    def test_install_hint_is_nonempty(self):
        backend = CopilotBackend()
        hint = backend.install_hint()
        assert "copilot" in hint.lower() or "Copilot" in hint


# ------------------------------------------------------------------
# create_backend
# ------------------------------------------------------------------


class TestCreateBackend:
    def test_copilot(self):
        backend = create_backend("copilot")
        assert backend.name == "copilot"

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="不支持"):
            create_backend("openai")


# ------------------------------------------------------------------
# AgentChannel
# ------------------------------------------------------------------


class _FakeBackend:
    """测试用的 Agent 后端桩。"""

    def __init__(self, available: bool = True, success: bool = True):
        self._available = available
        self._success = success
        self.last_prompt: str | None = None
        self.last_scope: str | None = None
        self.last_effort: str | None = None

    @property
    def name(self) -> str:
        return "fake"

    def is_available(self) -> bool:
        return self._available

    def invoke(self, prompt, *, scope="", effort="high") -> AgentResult:
        self.last_prompt = prompt
        self.last_scope = scope
        self.last_effort = effort
        return AgentResult(
            success=self._success,
            output="fake output",
            exit_code=0 if self._success else 1,
            backend="fake",
        )

    def install_hint(self) -> str:
        return "请安装 fake backend"


def _make_message(**overrides) -> NotificationMessage:
    defaults = {
        "event_type": "data.sync.completed",
        "notification_type": "data_sync_complete",
        "severity": "info",
        "title": "数据同步完成",
        "summary": "bootstrap 全量完成",
    }
    defaults.update(overrides)
    return NotificationMessage(**defaults)


class TestAgentChannel:
    def test_name(self):
        cfg = AgentConfig(enabled=True)
        ch = AgentChannel(cfg, backend=_FakeBackend())
        assert ch.name == "agent"

    def test_is_available_when_enabled_and_backend_ok(self):
        cfg = AgentConfig(enabled=True)
        ch = AgentChannel(cfg, backend=_FakeBackend(available=True))
        assert ch.is_available() is True

    def test_not_available_when_disabled(self):
        cfg = AgentConfig(enabled=False)
        ch = AgentChannel(cfg, backend=_FakeBackend(available=True))
        assert ch.is_available() is False

    def test_not_available_when_backend_missing(self):
        cfg = AgentConfig(enabled=True)
        ch = AgentChannel(cfg, backend=_FakeBackend(available=False))
        assert ch.is_available() is False

    def test_send_success(self):
        backend = _FakeBackend(available=True, success=True)
        cfg = AgentConfig(enabled=True, scope="/test", effort="medium")
        ch = AgentChannel(cfg, backend=backend)
        msg = _make_message()
        result = ch.send(msg)
        assert result["status"] == "sent"
        assert result["backend"] == "fake"
        assert backend.last_prompt is not None
        assert "数据同步完成" in backend.last_prompt
        assert backend.last_scope == "/test"
        assert backend.last_effort == "medium"

    def test_send_failure(self):
        backend = _FakeBackend(available=True, success=False)
        cfg = AgentConfig(enabled=True)
        ch = AgentChannel(cfg, backend=backend)
        msg = _make_message()
        result = ch.send(msg)
        assert result["status"] == "failed"

    def test_send_raises_when_unavailable(self):
        backend = _FakeBackend(available=False)
        cfg = AgentConfig(enabled=True)
        ch = AgentChannel(cfg, backend=backend)
        msg = _make_message()
        with pytest.raises(NotificationError, match="AGENT_UNAVAILABLE"):
            ch.send(msg)

    def test_build_prompt_includes_key_fields(self):
        msg = _make_message(
            impact="bars 表受影响",
            suggested_actions=("检查日志", "重跑 repair"),
            run_id="run-123",
            task_id="task-456",
        )
        prompt = AgentChannel._build_prompt(msg)
        assert "数据同步完成" in prompt
        assert "data.sync.completed" in prompt
        assert "bars 表受影响" in prompt
        assert "检查日志" in prompt
        assert "重跑 repair" in prompt
        assert "run-123" in prompt
        assert "task-456" in prompt
        assert "请分析上述事件" in prompt

    def test_from_env_if_available_returns_none_when_disabled(self):
        with patch.dict(os.environ, {"VORTEX_AGENT_ENABLED": "false"}, clear=True):
            ch = AgentChannel.from_env_if_available()
        assert ch is None

    def test_from_env_if_available_returns_channel_when_enabled(self):
        env = {
            "VORTEX_AGENT_ENABLED": "true",
            "VORTEX_AGENT_BACKEND": "copilot",
        }
        with patch.dict(os.environ, env, clear=False):
            ch = AgentChannel.from_env_if_available()
        # 可能返回 AgentChannel（即使 copilot 不在 PATH 上，channel 仍会创建）
        assert ch is not None
        assert ch.name == "agent"


# ------------------------------------------------------------------
# NotificationService Agent 集成
# ------------------------------------------------------------------


class TestNotificationServiceAgentIntegration:
    def test_agent_channel_auto_discovery(self, tmp_path):
        """验证 NotificationService 在环境变量设置时自动发现 AgentChannel。"""
        env = {"VORTEX_AGENT_ENABLED": "true", "VORTEX_AGENT_BACKEND": "copilot"}
        with patch.dict(os.environ, env, clear=False):
            db = Database(tmp_path / "control.db")
            db.initialize_tables()
            service = NotificationService(db)
            assert "agent" in service._channels
            db.close()

    def test_agent_channel_not_discovered_when_disabled(self, tmp_path):
        """验证 Agent 未启用时不会被自动发现。"""
        with patch.dict(os.environ, {"VORTEX_AGENT_ENABLED": "false"}, clear=True):
            db = Database(tmp_path / "control.db")
            db.initialize_tables()
            service = NotificationService(db)
            assert "agent" not in service._channels
            db.close()

    def test_notify_routes_to_agent(self, tmp_path):
        """验证通知消息可以路由到 agent 渠道并成功投递。"""
        backend = _FakeBackend(available=True, success=True)
        cfg = AgentConfig(enabled=True)
        agent_ch = AgentChannel(cfg, backend=backend)

        db = Database(tmp_path / "control.db")
        db.initialize_tables()
        service = NotificationService(db, channels={"agent": agent_ch})

        msg = _make_message(severity="warning")
        deliveries = service.notify(msg, {"channel": "agent"})
        db.close()

        assert len(deliveries) == 1
        assert deliveries[0]["status"] == "sent"
        assert deliveries[0]["channel"] == "agent"

    def test_notify_agent_unavailable_records_failed(self, tmp_path):
        """验证 Agent 不可用时，投递结果为 failed 而非异常。"""
        backend = _FakeBackend(available=False)
        cfg = AgentConfig(enabled=True)
        agent_ch = AgentChannel(cfg, backend=backend)

        db = Database(tmp_path / "control.db")
        db.initialize_tables()
        service = NotificationService(db, channels={"agent": agent_ch})

        msg = _make_message(severity="critical")
        deliveries = service.notify(msg, {"channel": "agent"})
        db.close()

        assert len(deliveries) == 1
        assert deliveries[0]["status"] == "failed"


# ------------------------------------------------------------------
# _merge_env_file / _init_step_feishu / _init_step_agent
# ------------------------------------------------------------------


class TestMergeEnvFile:
    def test_create_new_file(self, tmp_path):
        from vortex.cli import _merge_env_file

        env_file = tmp_path / ".env"
        _merge_env_file(env_file, {"A": "1", "B": "2"})
        content = env_file.read_text()
        assert "A=1" in content
        assert "B=2" in content

    def test_update_existing_vars(self, tmp_path):
        from vortex.cli import _merge_env_file

        env_file = tmp_path / ".env"
        env_file.write_text("A=old\nB=keep\n")
        _merge_env_file(env_file, {"A": "new"})
        content = env_file.read_text()
        assert "A=new" in content
        assert "B=keep" in content
        assert "A=old" not in content

    def test_preserves_comments(self, tmp_path):
        from vortex.cli import _merge_env_file

        env_file = tmp_path / ".env"
        env_file.write_text("# this is a comment\nX=1\n")
        _merge_env_file(env_file, {"Y": "2"})
        content = env_file.read_text()
        assert "# this is a comment" in content
        assert "X=1" in content
        assert "Y=2" in content

    def test_append_new_vars(self, tmp_path):
        from vortex.cli import _merge_env_file

        env_file = tmp_path / ".env"
        env_file.write_text("OLD=value\n")
        _merge_env_file(env_file, {"NEW": "val"})
        content = env_file.read_text()
        assert "OLD=value" in content
        assert "NEW=val" in content
