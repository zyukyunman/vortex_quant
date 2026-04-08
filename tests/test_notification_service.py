"""NotificationService 测试。"""

from __future__ import annotations

from vortex.notification.models import NotificationMessage
from vortex.notification.service import NotificationService
from vortex.runtime.database import Database


class _DummyChannel:
    @property
    def name(self) -> str:
        return "dummy"

    def is_available(self) -> bool:
        return True

    def send(self, message, *, receive_id=None, receive_id_type=None):
        return {
            "status": "sent",
            "message": message.summary,
            "receive_id": receive_id,
            "receive_id_type": receive_id_type,
        }


class TestNotificationService:
    def test_notify_records_delivery_log(self, tmp_path):
        db = Database(tmp_path / "control.db")
        db.initialize_tables()
        service = NotificationService(db, channels={"dummy": _DummyChannel()})
        message = NotificationMessage(
            event_type="data.sync.partial_failed",
            notification_type="data_anomaly",
            severity="warning",
            title="Vortex Data 通知",
            summary="bootstrap 部分完成",
            impact="events 未更新",
        )

        deliveries = service.notify(
            message,
            {
                "enabled": True,
                "level": "warning",
                "channel": "dummy",
            },
        )
        rows = db.fetchall("SELECT * FROM notification_log")
        db.close()

        assert len(deliveries) == 1
        assert deliveries[0]["status"] == "sent"
        assert len(rows) == 1
        assert rows[0]["event_type"] == "data.sync.partial_failed"
        assert rows[0]["severity"] == "warning"
        assert rows[0]["channel"] == "dummy"
