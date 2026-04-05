"""本地 Feishu MCP server。

用途：

1. 让 Copilot 直接通过 MCP（Model Context Protocol，模型上下文协议）调用飞书开放平台。
2. 默认向配置好的 `open_id` 发送消息，不再依赖 OpenClaw。
3. 同时提供最小的诊断入口，便于在终端里直接验证 bot 配置与投递链路。

密钥口径：

- `FEISHU_APP_ID`
- `FEISHU_APP_SECRET`
- `FEISHU_DEFAULT_OPEN_ID`

这些值应通过 VS Code `mcp.json` 的 `envFile` 或外部环境变量注入，
不要写进 Git 版本库。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


MCP_PROTOCOL_VERSION = "2025-03-26"
SERVER_NAME = "feishuDirect"
SERVER_VERSION = "0.1.0"
DEFAULT_API_BASE = "https://open.feishu.cn"


class FeishuConfigError(RuntimeError):
    """表示本地飞书配置缺失或不合法。"""


class FeishuApiError(RuntimeError):
    """表示调用飞书开放平台失败。"""


@dataclass
class FeishuConfig:
    """飞书 bot 本地配置。"""

    app_id: str
    app_secret: str
    default_receive_id: str
    default_receive_id_type: str
    api_base: str = DEFAULT_API_BASE

    @classmethod
    def from_env(cls) -> "FeishuConfig":
        app_id = os.environ.get("FEISHU_APP_ID", "").strip()
        app_secret = os.environ.get("FEISHU_APP_SECRET", "").strip()
        default_receive_id = os.environ.get("FEISHU_DEFAULT_RECEIVE_ID", "").strip()
        default_receive_id_type = os.environ.get("FEISHU_DEFAULT_RECEIVE_ID_TYPE", "").strip() or "open_id"
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
            raise FeishuConfigError(f"缺少必要环境变量: {', '.join(missing)}")

        if default_receive_id_type not in {"open_id", "user_id", "union_id", "email", "chat_id"}:
            raise FeishuConfigError(
                "FEISHU_DEFAULT_RECEIVE_ID_TYPE 仅支持 open_id、user_id、union_id、email、chat_id"
            )

        return cls(
            app_id=app_id,
            app_secret=app_secret,
            default_receive_id=default_receive_id,
            default_receive_id_type=default_receive_id_type,
            api_base=api_base.rstrip("/"),
        )


class FeishuClient:
    """飞书开放平台最小客户端。"""

    def __init__(self, config: FeishuConfig) -> None:
        self._config = config
        self._tenant_access_token: str | None = None
        self._token_expire_at: float = 0.0

    def validate_config(self) -> dict[str, Any]:
        """校验 app_id/app_secret 是否可换取 token。"""
        token = self._get_tenant_access_token()
        return {
            "status": "ok",
            "app_id": self._config.app_id,
            "default_receive_id": self._config.default_receive_id,
            "default_receive_id_type": self._config.default_receive_id_type,
            "token_preview": _mask_value(token),
        }

    def get_delivery_profile(self) -> dict[str, Any]:
        """返回当前投递配置摘要。"""
        return {
            "server": SERVER_NAME,
            "app_id": self._config.app_id,
            "default_receive_id": self._config.default_receive_id,
            "default_receive_id_type": self._config.default_receive_id_type,
            "api_base": self._config.api_base,
        }

    def send_text_message(
        self,
        message: str,
        receive_id: str | None = None,
        receive_id_type: str | None = None,
    ) -> dict[str, Any]:
        """发送文本消息到飞书直聊。"""
        text = message.strip()
        if not text:
            raise FeishuApiError("message 不能为空")

        target_receive_id = (receive_id or self._config.default_receive_id).strip()
        if not target_receive_id:
            raise FeishuApiError("receive_id 不能为空，且未配置默认 FEISHU_DEFAULT_RECEIVE_ID")

        target_receive_id_type = (receive_id_type or self._config.default_receive_id_type).strip() or "open_id"
        if target_receive_id_type not in {"open_id", "user_id", "union_id", "email", "chat_id"}:
            raise FeishuApiError("receive_id_type 不合法")

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
            raise FeishuApiError("飞书返回了空 token 或非法过期时间")

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
            raise FeishuApiError(
                f"飞书 HTTP 错误: status={exc.code}, body={error_body}"
            ) from exc
        except urllib.error.URLError as exc:
            raise FeishuApiError(f"连接飞书失败: {exc.reason}") from exc

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise FeishuApiError(f"飞书返回了非法 JSON: {raw}") from exc

        if parsed.get("code", 0) != 0:
            raise FeishuApiError(
                "飞书 API 返回失败: "
                f"code={parsed.get('code')}, msg={parsed.get('msg')}, "
                f"log_id={parsed.get('log_id')}"
            )
        return parsed


TOOLS: list[dict[str, Any]] = [
    {
        "name": "send_feishu_message",
        "description": "向默认飞书接收人发送文本消息；如传入 receive_id，则覆盖默认 open_id。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "message": {
                    "type": "string",
                    "description": "要发送到飞书的正文文本。",
                },
                "receive_id": {
                    "type": "string",
                    "description": "可选。目标收件人 ID；默认使用 FEISHU_DEFAULT_RECEIVE_ID。",
                },
                "receive_id_type": {
                    "type": "string",
                    "description": "可选。收件人 ID 类型，例如 open_id 或 user_id；默认使用 FEISHU_DEFAULT_RECEIVE_ID_TYPE。",
                },
            },
            "required": ["message"],
            "additionalProperties": False,
        },
    },
    {
        "name": "validate_feishu_config",
        "description": "校验当前飞书 bot 配置是否可用，并返回默认接收人摘要。",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
    {
        "name": "get_feishu_delivery_profile",
        "description": "返回当前 Feishu MCP 的投递配置摘要，不触发真实发信。",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
]


def _mask_value(value: str) -> str:
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def _tool_result(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "content": [
            {
                "type": "text",
                "text": json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            }
        ]
    }


def _tool_error(message: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": message}], "isError": True}


def _handle_tool_call(client: FeishuClient, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    try:
        if name == "send_feishu_message":
            return _tool_result(
                client.send_text_message(
                    message=str(arguments.get("message", "")),
                    receive_id=_optional_string(arguments.get("receive_id")),
                    receive_id_type=_optional_string(arguments.get("receive_id_type")),
                )
            )
        if name == "validate_feishu_config":
            return _tool_result(client.validate_config())
        if name == "get_feishu_delivery_profile":
            return _tool_result(client.get_delivery_profile())
    except (FeishuConfigError, FeishuApiError, ValueError, TypeError) as exc:
        return _tool_error(str(exc))

    return _tool_error(f"未知工具: {name}")


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _read_message(wire_mode: str | None) -> tuple[dict[str, Any] | None, str | None]:
    line = sys.stdin.buffer.readline()
    if not line:
        return None, wire_mode

    if line in (b"\r\n", b"\n"):
        return _read_message(wire_mode)

    stripped = line.strip()
    if stripped.startswith(b"{"):
        try:
            return json.loads(stripped.decode("utf-8")), "json_line"
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"非法 JSON 行消息: {stripped!r}") from exc

    headers: dict[str, str] = {}
    current_line = line
    while True:
        if current_line in (b"\r\n", b"\n"):
            break
        try:
            header_name, header_value = current_line.decode("utf-8").split(":", 1)
        except ValueError as exc:
            raise RuntimeError(f"非法 MCP 头部: {current_line!r}") from exc
        headers[header_name.strip().lower()] = header_value.strip()
        current_line = sys.stdin.buffer.readline()
        if not current_line:
            raise RuntimeError("MCP 消息头未完整结束")

    content_length = int(headers.get("content-length", "0"))
    if content_length <= 0:
        raise RuntimeError("MCP 消息缺少有效的 Content-Length")

    body = sys.stdin.buffer.read(content_length)
    if len(body) != content_length:
        raise RuntimeError("MCP 消息体长度不足")
    return json.loads(body.decode("utf-8")), "content_length"


def _write_message(payload: dict[str, Any], wire_mode: str | None) -> None:
    raw = json.dumps(payload, ensure_ascii=False)
    if wire_mode == "content_length":
        encoded = raw.encode("utf-8")
        sys.stdout.buffer.write(f"Content-Length: {len(encoded)}\r\n\r\n".encode("utf-8"))
        sys.stdout.buffer.write(encoded)
    else:
        sys.stdout.buffer.write(raw.encode("utf-8"))
        sys.stdout.buffer.write(b"\n")
    sys.stdout.buffer.flush()


def _write_error(request_id: Any, code: int, message: str, wire_mode: str | None) -> None:
    _write_message(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": code, "message": message},
        },
        wire_mode,
    )


def run_mcp_server() -> int:
    # 启动时尝试加载配置，失败时记录错误但不退出。
    # 这样即使 envFile 未正确加载，服务器也能响应 initialize 握手，
    # 工具调用时再返回详细错误，而不是让 VS Code 一直等待超时。
    _client: FeishuClient | None = None
    _config_error: str | None = None
    try:
        _client = FeishuClient(FeishuConfig.from_env())
    except FeishuConfigError as exc:
        _config_error = str(exc)
        print(f"[feishuDirect] 配置加载失败（工具调用将返回错误）: {exc}", file=sys.stderr)

    wire_mode: str | None = None
    while True:
        try:
            message, wire_mode = _read_message(wire_mode)
        except Exception as exc:  # noqa: BLE001
            print(f"[feishuDirect] 读取消息失败: {exc}", file=sys.stderr)
            return 1

        if message is None:
            return 0

        method = message.get("method")
        request_id = message.get("id")
        params = message.get("params", {})

        if method == "notifications/initialized":
            continue

        if method == "initialize":
            _write_message(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "protocolVersion": MCP_PROTOCOL_VERSION,
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
                    },
                }
                ,
                wire_mode,
            )
            continue

        if method == "ping":
            _write_message({"jsonrpc": "2.0", "id": request_id, "result": {}}, wire_mode)
            continue

        if method == "tools/list":
            _write_message({"jsonrpc": "2.0", "id": request_id, "result": {"tools": TOOLS}}, wire_mode)
            continue

        if method == "tools/call":
            # 配置未加载时返回可读错误，而非崩溃
            if _client is None:
                _write_message(
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": _tool_error(f"Feishu 配置未加载: {_config_error}"),
                    },
                    wire_mode,
                )
                continue
            tool_name = str(params.get("name", ""))
            arguments = params.get("arguments", {})
            if not isinstance(arguments, dict):
                _write_error(request_id, -32602, "tools/call.arguments 必须是对象", wire_mode)
                continue
            _write_message(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": _handle_tool_call(_client, tool_name, arguments),
                },
                wire_mode,
            )
            continue

        if request_id is None:
            continue
        _write_error(request_id, -32601, f"不支持的方法: {method}", wire_mode)


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="本地 Feishu MCP server 与诊断入口")
    parser.add_argument(
        "--validate",
        action="store_true",
        help="只校验本地飞书配置，不启动 MCP server。",
    )
    parser.add_argument(
        "--send-text",
        help="直接发送一条文本消息到默认 open_id（或 --receive-id 指定目标），不启动 MCP server。",
    )
    parser.add_argument(
        "--receive-id",
        help="配合 --send-text 使用，覆盖默认 open_id。",
    )
    return parser


def _run_cli_mode(args: argparse.Namespace) -> int:
    # CLI 诊断模式（--validate / --send-text）才立即加载配置；
    # 纯 MCP server 模式不在此处创建 client，避免配置缺失时污染 stdout
    # 导致 VS Code 无法收到 initialize 响应。
    if args.validate or args.send_text:
        client = FeishuClient(FeishuConfig.from_env())
        if args.validate:
            print(json.dumps(client.validate_config(), ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        if args.send_text:
            print(
                json.dumps(
                    client.send_text_message(message=args.send_text, receive_id=args.receive_id),
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
    return run_mcp_server()


def main(argv: list[str] | None = None) -> int:
    parser = _build_argument_parser()
    args = parser.parse_args(argv)
    try:
        return _run_cli_mode(args)
    except (FeishuConfigError, FeishuApiError) as exc:
        print(json.dumps({"status": "failed", "reason": str(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
