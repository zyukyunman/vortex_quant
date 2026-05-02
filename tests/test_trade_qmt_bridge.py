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
            return {"data": {"000001.SZ": {"open": 10.0, "lastPrice": 10.1, "volume": 10_000}}}
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
    assert calls[0][3]["Authorization"] == "Bearer secret"
    assert calls[0][3]["X-API-Key"] == "secret"


def test_qmt_bridge_rejects_submit_when_trading_disabled() -> None:
    adapter = QmtBridgeAdapter(QmtBridgeConfig(base_url="http://127.0.0.1:8000", allow_trading=False))

    order = adapter.submit_order(OrderIntent(symbol="000001.SZ", side="buy", shares=100, limit_price=10.0))

    assert order.status == "rejected"
    assert order.message == "qmt bridge trading disabled"

    with pytest.raises(PermissionError):
        adapter.cancel_order("abc")


def test_is_known_connection_status_bug_matches_expected_payload() -> None:
    assert is_known_connection_status_bug(
        {
            "connected": False,
            "error": "'xtquant.datacenter.IPythonApiClient' object has no attribute 'get_connect_status'",
        }
    )
    assert not is_known_connection_status_bug({"connected": False, "error": "socket closed"})
