"""Agent 后端协议与配置模型。

这一层定义了两件事：

1. AgentConfig —— Agent 调用的配置数据结构，描述"用哪个后端、对准哪个目录、
   推理强度多高"等参数。配置来源可以是环境变量（`VORTEX_AGENT_*`）
   或 profile YAML 中的 `agent:` 段。

2. AgentBackend —— 所有 Agent 后端必须实现的统一接口（Protocol）。
   目前唯一的实现是 CopilotBackend（见 copilot.py），但设计上
   可以扩展为 OpenAI / Claude / 本地模型等。

3. AgentResult —— 一次 Agent 调用的结果数据结构。

4. create_backend() —— 根据后端名称字符串构造对应的 Backend 实例。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class AgentConfig:
    """Agent 调用配置。

    字段说明：
    - enabled: 是否启用 Agent 通知渠道（默认关闭）
    - backend: 使用哪个 Agent 后端，当前仅支持 "copilot"
    - scope: Agent 执行时的工作目录范围（Copilot 的 --add-dir 参数）
    - effort: 推理强度，对应 Copilot 的 --effort 参数（high / medium / low）
    - max_attempts: 同一事件最多调用 Agent 的次数，防止死循环
    """

    enabled: bool = False
    backend: str = "copilot"
    scope: str = ""
    effort: str = "high"
    max_attempts: int = 2

    @classmethod
    def from_env(cls) -> AgentConfig:
        """从 VORTEX_AGENT_* 环境变量构建配置。

        环境变量映射：
        - VORTEX_AGENT_ENABLED  → enabled (true/1/yes 为启用)
        - VORTEX_AGENT_BACKEND  → backend (默认 copilot)
        - VORTEX_AGENT_SCOPE    → scope (空字符串表示不限制)
        - VORTEX_AGENT_EFFORT   → effort (默认 high)
        - VORTEX_AGENT_MAX_ATTEMPTS → max_attempts (默认 2)
        """
        enabled_raw = os.environ.get("VORTEX_AGENT_ENABLED", "").strip().lower()
        max_attempts_raw = os.environ.get("VORTEX_AGENT_MAX_ATTEMPTS", "2").strip()
        try:
            max_attempts = int(max_attempts_raw)
        except ValueError:
            max_attempts = 2

        return cls(
            enabled=enabled_raw in ("true", "1", "yes"),
            backend=(os.environ.get("VORTEX_AGENT_BACKEND", "").strip() or "copilot"),
            scope=os.environ.get("VORTEX_AGENT_SCOPE", "").strip(),
            effort=(os.environ.get("VORTEX_AGENT_EFFORT", "").strip() or "high"),
            max_attempts=max_attempts,
        )

    @classmethod
    def from_dict(cls, data: dict) -> AgentConfig:
        """从字典构建配置（通常来自 profile YAML 的 agent 段）。"""
        if not isinstance(data, dict):
            return cls()
        enabled_raw = data.get("enabled")
        if isinstance(enabled_raw, bool):
            enabled = enabled_raw
        else:
            enabled = str(enabled_raw).strip().lower() in ("true", "1", "yes")
        max_attempts_raw = data.get("max_attempts", 2)
        try:
            max_attempts = int(max_attempts_raw)
        except (ValueError, TypeError):
            max_attempts = 2

        return cls(
            enabled=enabled,
            backend=str(data.get("backend", "copilot")).strip() or "copilot",
            scope=str(data.get("scope", "")).strip(),
            effort=str(data.get("effort", "high")).strip() or "high",
            max_attempts=max_attempts,
        )


@dataclass(frozen=True)
class AgentResult:
    """一次 Agent 调用的结果。

    字段说明：
    - success: 调用是否成功（exit_code == 0）
    - output: Agent 的标准输出 + 标准错误的合并文本
    - exit_code: 进程退出码，-1 表示未能启动
    - backend: 使用的后端名称（如 "copilot"）
    """

    success: bool
    output: str
    exit_code: int
    backend: str


class AgentBackend(Protocol):
    """Agent 后端统一接口。

    所有 Agent 后端（Copilot / OpenAI / 本地模型等）都必须实现此协议。
    NotificationChannel 的 AgentChannel 会调用这个接口来执行实际的 Agent 任务。
    """

    @property
    def name(self) -> str:
        """后端名称，如 "copilot"。"""
        ...

    def is_available(self) -> bool:
        """检查后端是否可用（二进制存在、API key 配置等）。"""
        ...

    def invoke(
        self,
        prompt: str,
        *,
        scope: str = "",
        effort: str = "high",
    ) -> AgentResult:
        """执行一次 Agent 调用。

        Args:
            prompt: 发送给 Agent 的提示词
            scope: 工作目录范围（后端自行解释）
            effort: 推理强度（high / medium / low）

        Returns:
            AgentResult 包含调用结果
        """
        ...

    def install_hint(self) -> str:
        """返回安装提示信息，在检测到后端不可用时展示给用户。"""
        ...


def create_backend(backend_name: str) -> AgentBackend:
    """根据后端名称构造对应的 Backend 实例。

    当前仅支持 "copilot"，未来可扩展更多后端。

    Raises:
        ValueError: 不支持的后端名称
    """
    if backend_name == "copilot":
        from vortex.agent.copilot import CopilotBackend

        return CopilotBackend()
    raise ValueError(f"不支持的 Agent 后端: {backend_name!r}，当前仅支持: copilot")
