"""通知渠道协议。"""

from __future__ import annotations

from typing import Protocol

from vortex.notification.models import NotificationMessage


class NotificationChannel(Protocol):
    """通知渠道统一接口。"""

    @property
    def name(self) -> str:
        ...

    def send(
        self,
        message: NotificationMessage,
        *,
        receive_id: str | None = None,
        receive_id_type: str | None = None,
    ) -> dict[str, object]:
        ...

    def is_available(self) -> bool:
        ...
