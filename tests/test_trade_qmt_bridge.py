from __future__ import annotations

import pytest

from vortex.trade import OrderIntent
from vortex.trade.qmt_bridge import QmtBridgeAdapter, QmtBridgeConfig, is_known_connection_status_bug


def test_qmt_bridge_readonly_methods_with_fake_transport() -> None:
    calls = []

    def transport(method, endpoint, payload, headers):
        calls.append((method, endpoint, payload, headers))
        if endpoint == "/api/meta/health":
            return {"status": "ok"}
        if endpoint == "/api/trading/asset?account_id=99034443":
            return {"data": {"cash": 100_000, "frozen_cash": 0, "total_asset": 120_000, "market_value": 20_000}}
        if endpoint == "/api/trading/positions?account_id=99034443":
            return {
                "data": [
                    {
                        "stock_code": "000001.SZ",
                        "volume": 1_000,
                        "can_use_volume": 1_000,
                        "open_price": 10.0,
                        "market_price": 11.0,
                    }
                ]
            }
        if endpoint == "/api/trading/orders?account_id=99034443":
            return {"data": []}
        if endpoint == "/api/trading/trades?account_id=99034443":
            return {"data": []}
        if endpoint == "/api/market/full_tick?stocks=000001.SZ":
            return {
                "data": {
                    "000001.SZ": {
                        "open": 10.0,
                        "lastPrice": 10.1,
                        "askPrice": [10.11, 0, 0, 0, 0],
                        "bidPrice": [10.09, 0, 0, 0, 0],
                        "volume": 10_000,
                    }
                }
            }
        raise AssertionError(endpoint)

    adapter = QmtBridgeAdapter(
        QmtBridgeConfig(
            base_url="http://127.0.0.1:8000",
            token="secret",
            account_id="99034443",
        ),
        transport,
    )

    assert adapter.health().ok is True
    assert adapter.get_cash().available_cash == 100_000
    assert adapter.get_positions()[0].symbol == "000001.SZ"
    assert adapter.get_orders() == []
    assert adapter.get_fills() == []
    assert adapter.get_quotes(["000001.SZ"])["000001.SZ"].last_price == 10.1
    assert adapter.get_quotes(["000001.SZ"])["000001.SZ"].ask_price_1 == 10.11
    assert adapter.get_quotes(["000001.SZ"])["000001.SZ"].bid_price_1 == 10.09
    assert adapter.get_quotes(["000001.SZ"])["000001.SZ"].is_suspended is None
    assert adapter.get_quotes(["000001.SZ"])["000001.SZ"].is_limit_up is None
    assert adapter.get_quotes(["000001.SZ"])["000001.SZ"].is_limit_down is None
    assert calls[0][3]["Authorization"] == "Bearer secret"
    assert calls[0][3]["X-API-Key"] == "secret"


def test_qmt_bridge_rejects_submit_when_trading_disabled() -> None:
    adapter = QmtBridgeAdapter(QmtBridgeConfig(base_url="http://127.0.0.1:8000", allow_trading=False))

    order = adapter.submit_order(OrderIntent(symbol="000001.SZ", side="buy", shares=100, limit_price=10.0))

    assert order.status == "rejected"
    assert order.message == "qmt bridge trading disabled"

    with pytest.raises(PermissionError):
        adapter.cancel_order("abc")


def test_qmt_bridge_derives_limit_status_from_limit_prices() -> None:
    def transport(method, endpoint, payload, headers):  # noqa: ARG001
        if endpoint == "/api/market/full_tick?stocks=000001.SZ":
            return {
                "data": {
                    "000001.SZ": {
                        "open": 10.0,
                        "lastPrice": 10.1,
                        "up_limit": 10.1,
                        "down_limit": 9.1,
                        "volume": 10_000,
                    }
                }
            }
        raise AssertionError(endpoint)

    adapter = QmtBridgeAdapter(
        QmtBridgeConfig(base_url="http://127.0.0.1:8000", account_id="99034443"),
        transport,
    )

    quote = adapter.get_quotes(["000001.SZ"])["000001.SZ"]
    assert quote.is_limit_up is True
    assert quote.is_limit_down is False
    assert quote.is_suspended is None


def test_qmt_bridge_filters_zero_share_shell_positions() -> None:
    def transport(method, endpoint, payload, headers):  # noqa: ARG001
        if endpoint == "/api/trading/positions?account_id=99034443":
            return {
                "data": [
                    {
                        "stock_code": "000001.SZ",
                        "volume": 0,
                        "can_use_volume": 0,
                        "open_price": 10.0,
                        "market_price": 10.0,
                    },
                    {
                        "stock_code": "000002.SZ",
                        "volume": 100,
                        "can_use_volume": 100,
                        "open_price": 10.0,
                        "market_price": 10.0,
                    },
                ]
            }
        raise AssertionError(endpoint)

    adapter = QmtBridgeAdapter(
        QmtBridgeConfig(
            base_url="http://127.0.0.1:8000",
            account_id="99034443",
        ),
        transport,
    )

    positions = adapter.get_positions()
    assert [(item.symbol, item.shares) for item in positions] == [("000002.SZ", 100)]


def test_is_known_connection_status_bug_matches_expected_payload() -> None:
    assert is_known_connection_status_bug(
        {
            "connected": False,
            "error": "'xtquant.datacenter.IPythonApiClient' object has no attribute 'get_connect_status'",
        }
    )
    assert not is_known_connection_status_bug({"connected": False, "error": "socket closed"})


def test_qmt_bridge_normalizes_numeric_order_status_codes() -> None:
    def transport(method, endpoint, payload, headers):  # noqa: ARG001
        if endpoint == "/api/trading/orders?account_id=99034443":
            return {
                "data": [
                    {
                        "stock_code": "000001.SZ",
                        "order_id": 1,
                        "order_volume": 100,
                        "order_status": 54,
                    },
                    {
                        "stock_code": "000002.SZ",
                        "order_id": 2,
                        "order_volume": 100,
                        "order_status": 56,
                        "traded_volume": 100,
                    },
                    {
                        "stock_code": "000003.SZ",
                        "order_id": 3,
                        "order_volume": 100,
                        "order_status": 57,
                    },
                ]
            }
        raise AssertionError(endpoint)

    adapter = QmtBridgeAdapter(
        QmtBridgeConfig(
            base_url="http://127.0.0.1:8000",
            account_id="99034443",
        ),
        transport,
    )

    statuses = [item.status for item in adapter.get_orders()]
    assert statuses == ["cancelled", "filled", "rejected"]


def test_qmt_bridge_prefers_order_type_for_side_over_direction() -> None:
    def transport(method, endpoint, payload, headers):  # noqa: ARG001
        if endpoint == "/api/trading/orders?account_id=99034443":
            return {
                "data": [
                    {
                        "stock_code": "000001.SZ",
                        "order_id": 1,
                        "order_volume": 100,
                        "order_status": 57,
                        "order_type": 24,
                        "direction": 48,
                        "offset_flag": 49,
                    }
                ]
            }
        raise AssertionError(endpoint)

    adapter = QmtBridgeAdapter(
        QmtBridgeConfig(
            base_url="http://127.0.0.1:8000",
            account_id="99034443",
        ),
        transport,
    )

    orders = adapter.get_orders()
    assert len(orders) == 1
    assert orders[0].intent.side == "sell"
