"""通知渠道实现。

当前支持的渠道：
- feishu: 飞书文本通知（通过飞书开放平台 API）
- agent: AI Agent 通知（通过 Copilot CLI 等后端）
"""

from vortex.notification.channel.agent import AgentChannel
from vortex.notification.channel.feishu import FeishuChannel

__all__ = ["AgentChannel", "FeishuChannel"]
