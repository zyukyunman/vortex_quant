"""Feishu 通道。"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from vortex.notification.models import NotificationMessage
from vortex.shared.errors import NotificationError

DEFAULT_API_BASE = "https://open.feishu.cn"
_VALID_RECEIVE_ID_TYPES = {"open_id", "user_id", "union_id", "email", "chat_id"}


@dataclass(frozen=True)
class FeishuConfig:
    """Feishu Bot 配置。"""

    app_id: str
    app_secret: str
    default_receive_id: str
    default_receive_id_type: str = "open_id"
    api_base: str = DEFAULT_API_BASE

    @classmethod
    def from_env(cls) -> "FeishuConfig":
        app_id = os.environ.get("FEISHU_APP_ID", "").strip()
        app_secret = os.environ.get("FEISHU_APP_SECRET", "").strip()
        default_receive_id = os.environ.get("FEISHU_DEFAULT_RECEIVE_ID", "").strip()
        default_receive_id_type = (
            os.environ.get("FEISHU_DEFAULT_RECEIVE_ID_TYPE", "").strip() or "open_id"
        )
        default_open_id = os.environ.get("FEISHU_DEFAULT_OPEN_ID", "").strip()
        api_base = os.environ.get("FEISHU_API_BASE", DEFAULT_API_BASE).strip() or DEFAULT_API_BASE

        if not default_receive_id and default_open_id:
            default_receive_id = default_open_id
            default_receive_id_type = "open_id"

        missing = [
            name
            for name, value in (
                ("FEISHU_APP_ID", app_id),
                ("FEISHU_APP_SECRET", app_secret),
                ("FEISHU_DEFAULT_RECEIVE_ID", default_receive_id),
            )
            if not value
        ]
        if missing:
            raise NotificationError(
                code="NOTIFICATION_FEISHU_CONFIG_MISSING",
                message=f"缺少飞书配置: {', '.join(missing)}",
            )
        if default_receive_id_type not in _VALID_RECEIVE_ID_TYPES:
            raise NotificationError(
                code="NOTIFICATION_FEISHU_CONFIG_INVALID",
                message="FEISHU_DEFAULT_RECEIVE_ID_TYPE 不合法",
            )
        return cls(
            app_id=app_id,
            app_secret=app_secret,
            default_receive_id=default_receive_id,
            default_receive_id_type=default_receive_id_type,
            api_base=api_base.rstrip("/"),
        )


class _FeishuClient:
    """飞书开放平台最小客户端。"""

    def __init__(self, config: FeishuConfig) -> None:
        self._config = config
        self._tenant_access_token: str | None = None
        self._token_expire_at: float = 0.0

    def send_text_message(
        self,
        message: str,
        *,
        receive_id: str | None = None,
        receive_id_type: str | None = None,
    ) -> dict[str, Any]:
        text = message.strip()
        if not text:
            raise NotificationError(
                code="NOTIFICATION_MESSAGE_EMPTY",
                message="通知正文不能为空",
            )
        target_receive_id = (receive_id or self._config.default_receive_id).strip()
        target_receive_id_type = (
            receive_id_type or self._config.default_receive_id_type
        ).strip() or "open_id"
        if not target_receive_id:
            raise NotificationError(
                code="NOTIFICATION_TARGET_MISSING",
                message="未配置默认飞书接收人",
            )
        if target_receive_id_type not in _VALID_RECEIVE_ID_TYPES:
            raise NotificationError(
                code="NOTIFICATION_TARGET_INVALID",
                message="receive_id_type 不合法",
            )

        payload = {
            "receive_id": target_receive_id,
            "msg_type": "text",
            "content": json.dumps({"text": text}, ensure_ascii=False),
        }
        response = self._request(
            method="POST",
            path="/open-apis/im/v1/messages",
            query={"receive_id_type": target_receive_id_type},
            payload=payload,
            headers={"Authorization": f"Bearer {self._get_tenant_access_token()}"},
        )
        data = response.get("data", {})
        return {
            "status": "sent",
            "receive_id": target_receive_id,
            "receive_id_type": target_receive_id_type,
            "message_id": data.get("message_id"),
            "root_id": data.get("root_id"),
        }

    def _get_tenant_access_token(self) -> str:
        now = time.time()
        if self._tenant_access_token and now < self._token_expire_at:
            return self._tenant_access_token

        response = self._request(
            method="POST",
            path="/open-apis/auth/v3/tenant_access_token/internal",
            payload={
                "app_id": self._config.app_id,
                "app_secret": self._config.app_secret,
            },
        )
        token = str(response.get("tenant_access_token", "")).strip()
        expires_in = int(response.get("expire", response.get("expires_in", 0)) or 0)
        if not token or expires_in <= 0:
            raise NotificationError(
                code="NOTIFICATION_FEISHU_TOKEN_INVALID",
                message="飞书返回了空 token 或非法过期时间",
            )
        self._tenant_access_token = token
        self._token_expire_at = now + max(expires_in - 60, 60)
        return token

    def _request(
        self,
        *,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        query: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        url = f"{self._config.api_base}{path}"
        if query:
            url = f"{url}?{urllib.parse.urlencode(query)}"
        body = None
        request_headers = {"Content-Type": "application/json; charset=utf-8"}
        if headers:
            request_headers.update(headers)
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        request = urllib.request.Request(
            url=url,
            method=method,
            data=body,
            headers=request_headers,
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise NotificationError(
                code="NOTIFICATION_FEISHU_HTTP_FAILED",
                message=f"飞书 HTTP 错误: status={exc.code}, body={error_body}",
            ) from exc
        except urllib.error.URLError as exc:
            raise NotificationError(
                code="NOTIFICATION_FEISHU_CONNECT_FAILED",
                message=f"连接飞书失败: {exc.reason}",
            ) from exc

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise NotificationError(
                code="NOTIFICATION_FEISHU_RESPONSE_INVALID",
                message=f"飞书返回了非法 JSON: {raw}",
            ) from exc
        if parsed.get("code", 0) != 0:
            raise NotificationError(
                code="NOTIFICATION_FEISHU_API_FAILED",
                message=(
                    "飞书 API 返回失败: "
                    f"code={parsed.get('code')}, msg={parsed.get('msg')}, "
                    f"log_id={parsed.get('log_id')}"
                ),
            )
        return parsed


class FeishuChannel:
    """Feishu 文本通知渠道。"""

    def __init__(self, config: FeishuConfig | None = None) -> None:
        self._config = config or FeishuConfig.from_env()
        self._client = _FeishuClient(self._config)

    @property
    def name(self) -> str:
        return "feishu"

    @classmethod
    def from_env_if_available(cls) -> "FeishuChannel | None":
        try:
            return cls()
        except NotificationError:
            return None

    def is_available(self) -> bool:
        return True

    def send(
        self,
        message: NotificationMessage,
        *,
        receive_id: str | None = None,
        receive_id_type: str | None = None,
    ) -> dict[str, object]:
        return self._client.send_text_message(
            message.to_text(),
            receive_id=receive_id,
            receive_id_type=receive_id_type,
        )
