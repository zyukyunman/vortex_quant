"""Agent 域：AI Agent 后端抽象与实现。

本包提供统一的 Agent 调用接口，使 Vortex 能够在特定事件发生时
（如数据同步完成、质量校验失败）自动调用外部 AI Agent 进行分析或修复。

当前支持的后端：
- copilot：GitHub Copilot CLI（通过 `copilot -p ... --yolo` 非交互调用）

使用方式：
    from vortex.agent import AgentConfig, create_backend

    config = AgentConfig.from_env()
    backend = create_backend(config.backend)
    if backend.is_available():
        result = backend.invoke("请分析最近一次数据同步的失败原因")
"""

from vortex.agent.backend import AgentConfig, AgentResult, create_backend

__all__ = ["AgentConfig", "AgentResult", "create_backend"]
