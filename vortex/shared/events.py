"""进程内事件总线（Event Bus）。

采用同步发布/订阅模式，所有 handler 在 publish 线程内依次执行。
适用于单进程场景，不跨进程或网络。
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable

from vortex.shared.types import Domain, EventLevel


@dataclass
class Event:
    """事件对象。"""

    event_type: str  # e.g. "data.publish.completed"
    domain: Domain
    level: EventLevel
    payload: dict
    timestamp: str  # ISO datetime
    run_id: str | None = None


class EventBus:
    """进程内事件发布/订阅。"""

    def __init__(self) -> None:
        self._handlers: dict[str, list[Callable[[Event], None]]] = defaultdict(list)

    def subscribe(self, event_type: str, handler: Callable[[Event], None]) -> None:
        """订阅指定类型的事件。"""
        self._handlers[event_type].append(handler)

    def publish(self, event: Event) -> None:
        """发布事件，依次调用所有已注册的 handler。"""
        for handler in self._handlers.get(event_type := event.event_type, []):
            handler(event)
        # 通配符 "*" 接收所有事件
        for handler in self._handlers.get("*", []):
            handler(event)

    def clear(self) -> None:
        """清除所有订阅。"""
        self._handlers.clear()


# 模块级单例
_bus = EventBus()


def get_event_bus() -> EventBus:
    """获取全局 EventBus 单例。"""
    return _bus
