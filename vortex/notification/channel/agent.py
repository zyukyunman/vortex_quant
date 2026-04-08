"""AI Agent 通知渠道。

将 Vortex 的通知消息（NotificationMessage）转化为 Agent 可理解的 prompt，
然后通过 AgentBackend 执行。这使得 Agent 调用复用了现有的
NotificationService → Router → Channel 管线。

调用链路：
    NotificationService.notify()
    → NotificationRouter.route()  → targets 包含 channel="agent"
    → AgentChannel.send()
    → CopilotBackend.invoke()

配置来源：
    环境变量 VORTEX_AGENT_* → AgentConfig.from_env()

可用性检查：
    1. agent.enabled 必须为 true
    2. 对应后端的二进制/API 必须可用
    两个条件都满足时 is_available() 才返回 True。
"""

from __future__ import annotations

from vortex.agent.backend import AgentConfig, AgentResult, create_backend
from vortex.notification.models import NotificationMessage
from vortex.shared.errors import NotificationError


class AgentChannel:
    """AI Agent 通知渠道。

    实现 NotificationChannel 协议，使 Agent 可作为通知目标接入路由体系。
    当路由结果中包含 channel="agent" 的目标时，NotificationService 会调用此渠道。
    """

    def __init__(
        self,
        config: AgentConfig | None = None,
        *,
        backend: object | None = None,
    ) -> None:
        """构造 AgentChannel。

        Args:
            config: Agent 配置，为 None 时从环境变量读取
            backend: 可选的自定义后端（测试用），为 None 时根据 config.backend 自动创建
        """
        self._config = config or AgentConfig.from_env()
        if backend is not None:
            self._backend = backend
        else:
            self._backend = create_backend(self._config.backend)

    @property
    def name(self) -> str:
        return "agent"

    @classmethod
    def from_env_if_available(cls) -> AgentChannel | None:
        """尝试从环境变量构建 AgentChannel。

        如果 Agent 未启用或配置缺失，返回 None 而非抛异常。
        这和 FeishuChannel.from_env_if_available() 的模式一致。
        """
        try:
            config = AgentConfig.from_env()
            if not config.enabled:
                return None
            channel = cls(config)
            return channel
        except Exception:  # noqa: BLE001
            return None

    def is_available(self) -> bool:
        """检查 Agent 渠道是否可用。

        需要同时满足：
        1. 配置中 enabled=True
        2. 后端二进制/API 可用
        """
        return self._config.enabled and self._backend.is_available()

    def send(
        self,
        message: NotificationMessage,
        *,
        receive_id: str | None = None,
        receive_id_type: str | None = None,
    ) -> dict[str, object]:
        """将通知消息转为 prompt 并调用 Agent 后端。

        在调用前会检查后端可用性，不可用时抛出 NotificationError。

        Args:
            message: 统一通知消息
            receive_id: 忽略（Agent 渠道不使用接收人概念）
            receive_id_type: 忽略

        Returns:
            包含 status / backend / exit_code 等信息的字典

        Raises:
            NotificationError: 后端不可用时
        """
        if not self.is_available():
            hint = ""
            if hasattr(self._backend, "install_hint"):
                hint = f"\n{self._backend.install_hint()}"
            raise NotificationError(
                code="NOTIFICATION_AGENT_UNAVAILABLE",
                message=(
                    f"Agent 后端 '{self._config.backend}' 不可用。"
                    f"请确认已安装并完成认证。{hint}"
                ),
            )

        prompt = self._build_prompt(message)
        result: AgentResult = self._backend.invoke(
            prompt,
            scope=self._config.scope,
            effort=self._config.effort,
        )
        return {
            "status": "sent" if result.success else "failed",
            "backend": result.backend,
            "exit_code": result.exit_code,
            "output_length": len(result.output),
        }

    @staticmethod
    def _build_prompt(message: NotificationMessage) -> str:
        """把 NotificationMessage 转为 Agent 可理解的 prompt。

        prompt 结构：
        1. 事件标题与元信息（类型、严重程度）
        2. 摘要与影响范围
        3. 建议操作（如果有）
        4. 追溯入口（run_id / task_id）
        5. 指令：请求 Agent 分析并给出处理建议
        """
        lines = [
            f"[Vortex 事件通知] {message.title}",
            "",
            f"事件类型: {message.event_type}",
            f"严重程度: {message.severity}",
            f"摘要: {message.summary}",
        ]
        if message.impact:
            lines.append(f"影响范围: {message.impact}")
        if message.suggested_actions:
            lines.append("")
            lines.append("建议操作：")
            for i, action in enumerate(message.suggested_actions, 1):
                lines.append(f"  {i}. {action}")
        trace_parts = []
        if message.run_id:
            trace_parts.append(f"run_id={message.run_id}")
        if message.task_id:
            trace_parts.append(f"task_id={message.task_id}")
        if trace_parts:
            lines.append(f"\n追溯入口: {' | '.join(trace_parts)}")
        lines.append("")
        lines.append("请分析上述事件并给出处理建议。")
        return "\n".join(lines)
