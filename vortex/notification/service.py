"""通知服务入口。"""

from __future__ import annotations

import json
import uuid
from datetime import datetime

from vortex.notification.channel.base import NotificationChannel
from vortex.notification.channel.feishu import FeishuChannel
from vortex.notification.models import NotificationMessage
from vortex.notification.router import NotificationRouter, NotificationTarget
from vortex.runtime.database import Database


class NotificationService:
    """通知服务。

    自动发现已配置的通知渠道（飞书、Agent），并在 notify() 时
    根据路由规则将消息投递到对应渠道。

    渠道发现逻辑：
    1. 检查是否已通过构造函数显式传入 channels
    2. 对未传入的渠道，尝试从环境变量自动发现
    3. 发现失败（配置缺失）不报错，只是该渠道不可用
    """

    def __init__(
        self,
        db: Database,
        *,
        channels: dict[str, NotificationChannel] | None = None,
        router: NotificationRouter | None = None,
    ) -> None:
        self._db = db
        self._router = router or NotificationRouter()
        self._channels: dict[str, NotificationChannel] = dict(channels or {})

        # 自动发现飞书渠道
        if "feishu" not in self._channels:
            feishu = FeishuChannel.from_env_if_available()
            if feishu is not None:
                self._channels["feishu"] = feishu

        # 自动发现 Agent 渠道
        if "agent" not in self._channels:
            from vortex.notification.channel.agent import AgentChannel

            agent = AgentChannel.from_env_if_available()
            if agent is not None:
                self._channels["agent"] = agent

    def notify(
        self,
        message: NotificationMessage,
        profile_notification: dict | None = None,
    ) -> list[dict[str, object]]:
        targets = self._router.route(message, profile_notification)
        if not targets:
            return [
                self._record_delivery(
                    event_type=message.event_type,
                    severity=message.severity,
                    channel="none",
                    status="skipped",
                    message_summary=message.summary,
                    detail={"reason": "router returned no targets"},
                )
            ]
        return [
            self._deliver_to_target(message, target)
            for target in targets
        ]

    def _deliver_to_target(
        self,
        message: NotificationMessage,
        target: NotificationTarget,
    ) -> dict[str, object]:
        channel = self._channels.get(target.channel)
        if channel is None:
            return self._record_delivery(
                event_type=message.event_type,
                severity=message.severity,
                channel=target.channel,
                status="skipped",
                message_summary=message.summary,
                detail={"reason": "channel unavailable"},
            )
        try:
            payload = channel.send(
                message,
                receive_id=target.receive_id,
                receive_id_type=target.receive_id_type,
            )
            return self._record_delivery(
                event_type=message.event_type,
                severity=message.severity,
                channel=target.channel,
                status="sent",
                message_summary=message.summary,
                detail=payload,
            )
        except Exception as exc:  # noqa: BLE001
            return self._record_delivery(
                event_type=message.event_type,
                severity=message.severity,
                channel=target.channel,
                status="failed",
                message_summary=message.summary,
                detail={"error": str(exc)},
            )

    def _record_delivery(
        self,
        *,
        event_type: str,
        severity: str,
        channel: str,
        status: str,
        message_summary: str,
        detail: dict[str, object],
    ) -> dict[str, object]:
        notification_id = str(uuid.uuid4())
        detail_json = json.dumps(detail, ensure_ascii=False)
        sent_at = datetime.now().isoformat()
        self._db.execute(
            """INSERT INTO notification_log
               (notification_id, event_type, severity, channel, status, sent_at, message_summary, detail)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                notification_id,
                event_type,
                severity,
                channel,
                status,
                sent_at,
                message_summary,
                detail_json,
            ),
        )
        return {
            "notification_id": notification_id,
            "event_type": event_type,
            "severity": severity,
            "channel": channel,
            "status": status,
            "sent_at": sent_at,
            "message_summary": message_summary,
            "detail": detail,
        }
