"""HTTP adapter for qmt-bridge style services.

Default endpoint mapping targets atompilot/qmt-bridge:
- /api/meta/health
- /api/meta/connection_status
- /api/trading/asset
- /api/trading/positions
- /api/trading/orders
- /api/trading/trades
- /api/market/full_tick
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from vortex.trade.broker import BrokerHealth, CashSnapshot, FillRecord, OrderIntent, OrderRecord, Position, Quote

Transport = Callable[[str, str, dict[str, Any] | None, dict[str, str]], dict[str, Any]]


@dataclass(frozen=True)
class QmtBridgeConfig:
    base_url: str
    token: str | None = None
    account_id: str | None = None
    allow_trading: bool = False
    endpoints: dict[str, str] = field(
        default_factory=lambda: {
            "health": "/api/meta/health",
            "connection_status": "/api/meta/connection_status",
            "cash": "/api/trading/asset",
            "positions": "/api/trading/positions",
            "orders": "/api/trading/orders",
            "fills": "/api/trading/trades",
            "quotes": "/api/market/full_tick",
            "submit_order": "/api/trading/order",
            "cancel_order": "/api/trading/cancel",
        }
    )


class QmtBridgeAdapter:
    """Thin fail-closed adapter; trading is disabled unless explicitly enabled."""

    def __init__(self, config: QmtBridgeConfig, transport: Transport | None = None) -> None:
        if not config.base_url:
            raise ValueError("base_url is required")
        self.config = config
        self._transport = transport or self._default_transport

    def health(self) -> BrokerHealth:
        try:
            data = self._get("health")
        except Exception as exc:  # noqa: BLE001 - health should surface bridge failure as data.
            return BrokerHealth(ok=False, mode="qmt_bridge", message=str(exc))
        ok = bool(data.get("ok", data.get("success", data.get("status") == "ok")))
        message = str(data.get("message", data.get("status", "ok" if ok else "failed")))
        return BrokerHealth(ok=ok, mode="qmt_bridge", message=message)

    def connection_status(self) -> dict[str, Any]:
        data = self._get("connection_status")
        payload = _unwrap_payload(data)
        if isinstance(payload, dict):
            return payload
        raise ValueError("unexpected connection_status response shape")

    def get_cash(self) -> CashSnapshot:
        data = self._get("cash", params=self._account_params())
        row = _cash_row(data)
        return CashSnapshot(
            available_cash=float(_first(row, "available_cash", "cash", "enable_balance", "m_dCash")),
            frozen_cash=float(_first_optional(row, "frozen_cash", "frozen_balance", "m_dFrozenCash", default=0.0)),
            total_asset=float(_first(row, "total_asset", "asset", "total_balance", "m_dBalance")),
            market_value=float(_first_optional(row, "market_value", "stock_value", "m_dMarketValue", default=0.0)),
        )

    def get_positions(self) -> list[Position]:
        data = self._get("positions", params=self._account_params())
        rows = _rows(data, "positions")
        return [
            Position(
                symbol=str(_first(row, "symbol", "stock_code", "code")),
                shares=int(_first(row, "shares", "volume", "total_volume", "m_nVolume")),
                available_shares=int(
                    _first(
                        row,
                        "available_shares",
                        "available_volume",
                        "enable_amount",
                        "can_use_volume",
                        "m_nCanUseVolume",
                    )
                ),
                cost_price=float(_first(row, "cost_price", "avg_price", "open_price", "m_dOpenPrice")),
                last_price=float(_first_optional(row, "last_price", "price", "market_price", "m_dLastPrice", default=0.0)),
            )
            for row in rows
        ]

    def get_orders(self) -> list[OrderRecord]:
        data = self._get("orders", params=self._account_params())
        rows = _rows(data, "orders")
        return [_order_record_from_bridge(row) for row in rows]

    def get_fills(self) -> list[FillRecord]:
        data = self._get("fills", params=self._account_params())
        rows = _rows(data, "fills")
        return [_fill_record_from_bridge(row) for row in rows]

    def get_quotes(self, symbols: list[str]) -> dict[str, Quote]:
        if not symbols:
            return {}
        endpoint = self.config.endpoints["quotes"]
        if endpoint.endswith("/full_tick"):
            data = self._request(
                "GET",
                _append_query(endpoint, {"stocks": ",".join(symbols)}),
                None,
            )
        else:
            data = self._post("quotes", {"symbols": symbols})
        rows = _quote_rows(data)
        quotes = {
            str(_first(row, "symbol", "stock_code", "code")): Quote(
                symbol=str(_first(row, "symbol", "stock_code", "code")),
                open_price=float(_first_optional(row, "open_price", "open", "price", "lastClose", default=0.0)),
                last_price=float(_first_optional(row, "last_price", "lastPrice", "price", default=0.0)),
                volume=int(row["volume"]) if "volume" in row and row["volume"] is not None else None,
                amount=float(row["amount"]) if "amount" in row and row["amount"] is not None else None,
                is_suspended=bool(row.get("is_suspended", False)),
                is_limit_up=bool(row.get("is_limit_up", False)),
                is_limit_down=bool(row.get("is_limit_down", False)),
            )
            for row in rows
        }
        missing = [symbol for symbol in symbols if symbol not in quotes]
        if missing:
            raise KeyError(f"missing quotes: {missing}")
        return quotes

    def submit_order(self, intent: OrderIntent) -> OrderRecord:
        if not self.config.allow_trading:
            return OrderRecord(
                order_id="",
                intent=intent,
                status="rejected",
                filled_shares=0,
                remaining_shares=intent.shares,
                avg_fill_price=None,
                message="qmt bridge trading disabled",
                created_at="",
            )
        data = self._post("submit_order", _intent_payload(intent, account_id=self.config.account_id))
        payload = _unwrap_payload(data)
        if isinstance(payload, dict) and "order_id" in payload and "stock_code" not in payload and "symbol" not in payload:
            return OrderRecord(
                order_id=str(payload["order_id"]),
                intent=intent,
                status=_normalize_status(payload.get("status", "submitted")),
                filled_shares=0,
                remaining_shares=intent.shares,
                avg_fill_price=None,
                message=str(payload.get("message", "")),
                created_at=str(payload.get("created_at", "")),
            )
        row = dict(payload) if isinstance(payload, dict) else {}
        row.setdefault("symbol", intent.symbol)
        row.setdefault("side", intent.side)
        row.setdefault("shares", intent.shares)
        row.setdefault("limit_price", intent.limit_price)
        return _order_record_from_bridge(row)

    def cancel_order(self, order_id: str) -> OrderRecord:
        if not self.config.allow_trading:
            raise PermissionError("qmt bridge trading disabled")
        endpoint = self.config.endpoints["cancel_order"]
        if "{order_id}" in endpoint:
            data = self._request("POST", endpoint.format(order_id=order_id), None)
        else:
            data = self._request(
                "POST",
                endpoint,
                {"order_id": _to_int_if_possible(order_id), "account_id": self.config.account_id or ""},
            )
        payload = _unwrap_payload(data)
        if isinstance(payload, dict) and ("symbol" in payload or "stock_code" in payload):
            return _order_record_from_bridge(payload)
        return OrderRecord(
            order_id=str(order_id),
            intent=OrderIntent(symbol="", side="buy", shares=0),
            status=_normalize_status(payload.get("status", "cancelled") if isinstance(payload, dict) else "cancelled"),
            filled_shares=0,
            remaining_shares=0,
            avg_fill_price=None,
            message=str(payload.get("message", "") if isinstance(payload, dict) else ""),
            created_at=str(payload.get("created_at", "") if isinstance(payload, dict) else ""),
        )

    def _account_params(self) -> dict[str, Any] | None:
        if self.config.account_id:
            return {"account_id": self.config.account_id}
        return None

    def _get(self, name: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        endpoint = self.config.endpoints[name]
        if params:
            endpoint = _append_query(endpoint, params)
        return self._request("GET", endpoint, None)

    def _post(self, name: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", self.config.endpoints[name], payload)

    def _request(self, method: str, endpoint: str, payload: dict[str, Any] | None) -> dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        if self.config.token:
            headers["Authorization"] = f"Bearer {self.config.token}"
            headers["X-API-Key"] = self.config.token
        return self._transport(method, endpoint, payload, headers)

    def _default_transport(
        self,
        method: str,
        endpoint: str,
        payload: dict[str, Any] | None,
        headers: dict[str, str],
    ) -> dict[str, Any]:
        url = self.config.base_url.rstrip("/") + endpoint
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = Request(url, data=body, headers=headers, method=method)
        try:
            with urlopen(request, timeout=10) as response:  # noqa: S310 - URL is user-configured local bridge.
                return json.loads(response.read().decode("utf-8"))
        except URLError as exc:
            raise ConnectionError(f"qmt bridge request failed: {exc}") from exc


def is_known_connection_status_bug(connection: dict[str, object]) -> bool:
    if connection.get("connected") is not False:
        return False
    error_text = str(connection.get("error", ""))
    return "get_connect_status" in error_text and "IPythonApiClient" in error_text


def _first(data: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in data and data[name] is not None:
            return data[name]
    raise KeyError(f"missing fields: {names}")


def _first_optional(data: dict[str, Any], *names: str, default: Any) -> Any:
    for name in names:
        if name in data and data[name] is not None:
            return data[name]
    return default


def _unwrap_payload(data: Any) -> Any:
    if isinstance(data, dict):
        if "data" in data and data["data"] is not None:
            return _unwrap_payload(data["data"])
        if "result" in data and data["result"] is not None:
            return _unwrap_payload(data["result"])
    return data


def _cash_row(data: dict[str, Any]) -> dict[str, Any]:
    payload = _unwrap_payload(data)
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        return dict(payload[0])
    raise ValueError("unexpected cash response shape")


def _rows(data: dict[str, Any], key: str) -> list[dict[str, Any]]:
    value = _unwrap_payload(data)
    if isinstance(value, dict) and key in value and isinstance(value[key], list):
        return [dict(item) for item in value[key] if isinstance(item, dict)]
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        # single-row fallback for bridges returning one object instead of a list
        if any(name in value for name in {"stock_code", "symbol", "order_id", "trade_id", "traded_id"}):
            return [dict(value)]
    raise ValueError(f"unexpected {key} response shape")


def _quote_rows(data: dict[str, Any]) -> list[dict[str, Any]]:
    value = _unwrap_payload(data)
    if isinstance(value, dict) and all(isinstance(item, dict) for item in value.values()):
        rows: list[dict[str, Any]] = []
        for symbol, row in value.items():
            row_payload = dict(row)
            row_payload.setdefault("symbol", symbol)
            rows.append(row_payload)
        return rows
    if isinstance(value, dict) and "quotes" in value and isinstance(value["quotes"], list):
        return [dict(item) for item in value["quotes"] if isinstance(item, dict)]
    if isinstance(value, list):
        return [dict(item) for item in value if isinstance(item, dict)]
    raise ValueError("unexpected quotes response shape")


def _order_record_from_bridge(row: dict[str, Any]) -> OrderRecord:
    shares = int(_first(row, "shares", "volume", "order_volume", "m_nOrderVolume"))
    filled = int(_first_optional(row, "filled_shares", "filled_volume", "traded_volume", "m_nTradedVolume", default=0))
    intent = OrderIntent(
        symbol=str(_first(row, "symbol", "stock_code", "code")),
        side=_normalize_side(_first_optional(row, "side", "direction", "order_type", "offset_flag", default="buy")),
        shares=shares,
        price_type=str(_first_optional(row, "price_type", default="limit")),
        limit_price=float(_first_optional(row, "limit_price", "price", "price_value", default=0.0))
        if _first_optional(row, "limit_price", "price", "price_value", default=None) is not None
        else None,
        reason=str(row.get("reason", "")),
        strategy_version=str(row.get("strategy_version", "")),
        run_id=str(row.get("run_id", "")),
    )
    return OrderRecord(
        order_id=str(_first(row, "order_id", "entrust_no", "id")),
        intent=intent,
        status=_normalize_status(_first_optional(row, "status", "order_status", default="unknown")),
        filled_shares=filled,
        remaining_shares=int(_first_optional(row, "remaining_shares", "remaining_volume", default=max(0, shares - filled))),
        avg_fill_price=float(_first_optional(row, "avg_fill_price", "avg_price", default=0.0))
        if _first_optional(row, "avg_fill_price", "avg_price", default=None) is not None
        else None,
        message=str(_first_optional(row, "message", "order_status_msg", "error_msg", default="")),
        created_at=str(_first_optional(row, "created_at", "order_time", default="")),
    )


def _fill_record_from_bridge(row: dict[str, Any]) -> FillRecord:
    shares = int(_first(row, "shares", "volume", "trade_volume", "traded_volume", "m_nTradedVolume"))
    price = float(_first(row, "price", "trade_price", "traded_price", "m_dTradedPrice"))
    gross = float(_first_optional(row, "gross_value", "amount", "traded_amount", default=shares * price))
    return FillRecord(
        fill_id=str(_first(row, "fill_id", "trade_id", "traded_id", "id")),
        order_id=str(_first_optional(row, "order_id", "entrust_no", default="")),
        symbol=str(_first(row, "symbol", "stock_code", "code")),
        side=_normalize_side(_first_optional(row, "side", "direction", "offset_flag", default="buy")),
        shares=shares,
        price=price,
        gross_value=gross,
        fee=float(_first_optional(row, "fee", "commission", default=0.0)),
        created_at=str(_first_optional(row, "created_at", "trade_time", "traded_time", default="")),
    )


def _intent_payload(intent: OrderIntent, *, account_id: str | None) -> dict[str, Any]:
    return {
        "account_id": account_id or "",
        "stock_code": intent.symbol,
        "order_type": 23 if intent.side == "buy" else 24,
        "order_volume": intent.shares,
        "price_type": 11 if intent.price_type == "limit" else 5,
        "price": float(intent.limit_price or 0.0),
        "strategy_name": intent.strategy_version,
        "order_remark": intent.reason or intent.run_id,
    }


def _append_query(endpoint: str, params: dict[str, Any]) -> str:
    filtered = {key: value for key, value in params.items() if value is not None and value != ""}
    if not filtered:
        return endpoint
    separator = "&" if "?" in endpoint else "?"
    return f"{endpoint}{separator}{urlencode(filtered)}"


def _normalize_status(value: Any) -> str:
    text = str(value).strip().lower()
    if text in {"submitted", "open", "pending", "已报"}:
        return "open"
    if "part" in text or "部分" in text:
        return "partial"
    if "fill" in text or "成交" in text:
        return "filled"
    if "cancel" in text or "撤" in text:
        return "cancelled"
    if "reject" in text or "废单" in text or "失败" in text:
        return "rejected"
    return text or "unknown"


def _normalize_side(value: Any) -> str:
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"buy", "b", "long", "多", "买", "23", "48"}:
            return "buy"
        if text in {"sell", "s", "short", "空", "卖", "24", "49"}:
            return "sell"
    if isinstance(value, (int, float)):
        numeric = int(value)
        if numeric in {23, 48}:
            return "buy"
        if numeric in {24, 49}:
            return "sell"
    return "buy"


def _to_int_if_possible(value: Any) -> Any:
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return value
