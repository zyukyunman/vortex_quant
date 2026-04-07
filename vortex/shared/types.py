"""全局共享类型定义。"""
from __future__ import annotations

from typing import Literal, NewType

# RunId 标识一次完整的运行（数据拉取、因子计算等）
RunId = NewType("RunId", str)

# 域标识
Domain = Literal["data", "research", "strategy", "trade", "notification"]

# 事件级别
EventLevel = Literal["critical", "warning", "info"]
