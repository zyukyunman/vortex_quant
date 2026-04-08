"""通知路由。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time

from vortex.notification.models import NotificationMessage, NotificationSeverity

_SEVERITY_RANK = {
    "info": 1,
    "warning": 2,
    "critical": 3,
}
_EVENT_TYPE_TO_NOTIFICATION_TYPE = {
    "data.sync.failed": "data_anomaly",
    "data.sync.partial_failed": "data_anomaly",
    "data.quality.blocked": "data_anomaly",
    "data.sync.completed": "data_sync_complete",
}


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_severity(value: object) -> NotificationSeverity:
    normalized = str(value or "warning").strip().lower()
    if normalized in {"warn", "warning"}:
        return "warning"
    if normalized in {"critical", "error"}:
        return "critical"
    return "info"


@dataclass(frozen=True)
class NotificationTarget:
    """一次通知投递目标。"""

    channel: str
    receive_id: str | None = None
    receive_id_type: str | None = None


class NotificationRouter:
    """事件 -> 渠道目标。"""

    def route(
        self,
        message: NotificationMessage,
        profile_notification: dict | None,
    ) -> list[NotificationTarget]:
        config = profile_notification if isinstance(profile_notification, dict) else {}
        if not self._should_deliver(message, config):
            return []
        targets = self._resolve_route_targets(message, config)
        if targets:
            return targets
        return [
            NotificationTarget(
                channel=str(config.get("channel") or "feishu"),
                receive_id=_optional_text(config.get("receive_id")),
                receive_id_type=_optional_text(config.get("receive_id_type")),
            )
        ]

    def _should_deliver(
        self,
        message: NotificationMessage,
        config: dict,
    ) -> bool:
        if message.severity == "critical":
            return True
        if not bool(config.get("enabled", True)):
            return False
        if self._in_quiet_hours(_optional_text(config.get("quiet_hours"))):
            return False
        min_level = _normalize_severity(config.get("level"))
        return _SEVERITY_RANK[message.severity] >= _SEVERITY_RANK[min_level]

    def _resolve_route_targets(
        self,
        message: NotificationMessage,
        config: dict,
    ) -> list[NotificationTarget]:
        routes = config.get("routes")
        if not isinstance(routes, list):
            return []
        matched: list[NotificationTarget] = []
        for route in routes:
            if not isinstance(route, dict):
                continue
            route_type = _optional_text(route.get("type"))
            notification_type = (
                _optional_text(message.notification_type)
                or _EVENT_TYPE_TO_NOTIFICATION_TYPE.get(message.event_type)
            )
            if route_type not in {message.event_type, notification_type}:
                continue
            min_level = _normalize_severity(route.get("level") or config.get("level"))
            if _SEVERITY_RANK[message.severity] < _SEVERITY_RANK[min_level]:
                continue
            matched.append(
                NotificationTarget(
                    channel=str(route.get("channel") or config.get("channel") or "feishu"),
                    receive_id=_optional_text(route.get("receive_id")),
                    receive_id_type=_optional_text(route.get("receive_id_type")),
                )
            )
        return self._deduplicate_targets(matched)

    @staticmethod
    def _deduplicate_targets(targets: list[NotificationTarget]) -> list[NotificationTarget]:
        seen: set[tuple[str, str | None, str | None]] = set()
        ordered: list[NotificationTarget] = []
        for target in targets:
            key = (target.channel, target.receive_id, target.receive_id_type)
            if key in seen:
                continue
            seen.add(key)
            ordered.append(target)
        return ordered

    @staticmethod
    def _in_quiet_hours(window: str | None) -> bool:
        if not window or "-" not in window:
            return False
        start_text, end_text = [segment.strip() for segment in window.split("-", 1)]
        try:
            start = NotificationRouter._parse_clock(start_text)
            end = NotificationRouter._parse_clock(end_text)
        except ValueError:
            return False
        now = datetime.now().time()
        if start <= end:
            return start <= now <= end
        return now >= start or now <= end

    @staticmethod
    def _parse_clock(value: str) -> time:
        hour_text, minute_text = value.split(":", 1)
        return time(int(hour_text), int(minute_text))
