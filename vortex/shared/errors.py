"""Vortex 错误码体系。

错误码格式：{DOMAIN}_{CATEGORY}_{DETAIL}
例如 DATA_PROVIDER_FETCH_FAILED 表示数据域-提供方-拉取失败。
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class VortexError(Exception):
    """所有 Vortex 业务异常的基类。"""

    code: str
    message: str
    detail: dict = field(default_factory=dict)

    def __str__(self) -> str:
        base = f"[{self.code}] {self.message}"
        if self.detail:
            base += f" | detail={self.detail}"
        return base


class DataError(VortexError):
    """数据域异常（拉取、清洗、发布等）。"""


class ResearchError(VortexError):
    """研究域异常（因子计算、回测等）。"""


class StrategyError(VortexError):
    """策略域异常（组合构建、风控等）。"""


class TradeError(VortexError):
    """交易域异常（下单、成交、对账等）。"""


class NotificationError(VortexError):
    """通知域异常（消息推送、格式化等）。"""


class ConfigError(VortexError):
    """配置域异常（Profile 加载、校验等）。"""


class RuntimeError_(VortexError):
    """运行时域异常（workspace、任务队列等）。

    名称加下划线以避免与内建 RuntimeError 冲突。
    """
