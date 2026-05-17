from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from vortex.trade.target_portfolio import TargetPortfolioBuildConfig, build_target_portfolio
from vortex.trade.xueqiu import (
    XueqiuConfig,
    check_xueqiu_auth,
    classify_xueqiu_exception,
    run_xueqiu_rebalance,
)


def _portfolio():
    return build_target_portfolio(
        pd.DataFrame(
            [
                {"symbol": "600519.SH", "target_weight": 0.30, "reference_price": 1500.0},
                {"symbol": "000001.SZ", "target_weight": 0.20, "reference_price": 10.0},
            ]
        ),
        trade_date="20260518",
        strategy_version="baseline_top110_large",
        run_id="run_1",
        snapshot_id="snap_1",
        config=TargetPortfolioBuildConfig(notional=1_000_000),
    )


def _stock(symbol: str) -> dict[str, object]:
    if symbol == "600519":
        return {
            "stock_id": 1001,
            "code": "SH600519",
            "name": "贵州茅台",
            "flag": 1,
            "current": 1500.0,
            "chg": 0.0,
            "percent": 0.0,
            "ind_id": 101,
            "ind_name": "白酒",
            "ind_color": "#d9633b",
        }
    return {
        "stock_id": 1002,
        "code": "SZ000001",
        "name": "平安银行",
        "flag": 1,
        "current": 10.0,
        "chg": 0.0,
        "percent": 0.0,
        "ind_id": 102,
        "ind_name": "银行",
        "ind_color": "#2b7bbb",
    }


def test_run_xueqiu_rebalance_dry_run_writes_payload_without_submit(tmp_path: Path) -> None:
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def fake_transport(method, url, payload, headers):  # noqa: ANN001
        assert headers["Cookie"] == "u=1; xq_a_token=token"
        calls.append((method, url, payload))
        if url.endswith("/current.json"):
            return {"last_rb": {"cash": 100.0, "holdings": []}}
        if url.endswith("/search.json"):
            return {"stocks": [_stock(str(payload["code"]))]}
        raise AssertionError(f"unexpected call: {method} {url}")

    artifacts = run_xueqiu_rebalance(
        _portfolio(),
        config=XueqiuConfig(cube_symbol="ZH3625640", cookie="u=1; xq_a_token=token"),
        output_root=tmp_path,
        transport=fake_transport,
    )

    assert artifacts.summary["status"] == "dry_run"
    assert artifacts.summary["submitted"] is False
    assert all(call[0] != "POST" for call in calls)
    payload = json.loads(artifacts.payload_path.read_text(encoding="utf-8"))
    holdings = json.loads(payload["holdings"])
    assert payload["cash"] == 50.0
    assert [item["stock_symbol"] for item in holdings] == ["SH600519", "SZ000001"]
    assert [item["weight"] for item in holdings] == [30.0, 20.0]


def test_run_xueqiu_rebalance_submit_posts_form_payload(tmp_path: Path) -> None:
    posted: list[dict[str, object]] = []

    def fake_transport(method, url, payload, headers):  # noqa: ANN001, ARG001
        if url.endswith("/current.json"):
            return {"last_rb": {"cash": 100.0, "holdings": []}}
        if url.endswith("/search.json"):
            return {"stocks": [_stock(str(payload["code"]))]}
        if url.endswith("/create.json"):
            posted.append(dict(payload))
            return {"id": 224, "created_at": 1778994667053, "updated_at": 1778994667053}
        raise AssertionError(f"unexpected call: {method} {url}")

    artifacts = run_xueqiu_rebalance(
        _portfolio(),
        config=XueqiuConfig(cube_symbol="ZH3625640", cookie="u=1", allow_submit=True),
        output_root=tmp_path,
        comment="test rebalance",
        transport=fake_transport,
    )

    assert artifacts.summary["status"] == "submitted"
    assert artifacts.summary["submitted"] is True
    assert len(posted) == 1
    assert posted[0]["cube_symbol"] == "ZH3625640"
    assert posted[0]["comment"] == "test rebalance"
    assert json.loads(str(posted[0]["holdings"]))[0]["stock_symbol"] == "SH600519"


def test_run_xueqiu_rebalance_is_idempotent_for_same_target(tmp_path: Path) -> None:
    portfolio = _portfolio()

    def fake_transport(method, url, payload, headers):  # noqa: ANN001, ARG001
        if url.endswith("/current.json"):
            return {"last_rb": {"cash": 100.0, "holdings": []}}
        if url.endswith("/search.json"):
            return {"stocks": [_stock(str(payload["code"]))]}
        raise AssertionError(f"unexpected call: {method} {url}")

    run_xueqiu_rebalance(
        portfolio,
        config=XueqiuConfig(cube_symbol="ZH3625640", cookie="u=1", allow_submit=True),
        output_root=tmp_path,
        transport=lambda method, url, payload, headers: (  # noqa: ARG005
            {"id": 224, "created_at": 1778994667053, "updated_at": 1778994667053}
            if url.endswith("/create.json")
            else fake_transport(method, url, payload, headers)
        ),
    )

    def fail_transport(*args, **kwargs):  # noqa: ANN001
        raise AssertionError("transport should not be called for existing report")

    second = run_xueqiu_rebalance(
        portfolio,
        config=XueqiuConfig(cube_symbol="ZH3625640", cookie="u=1", allow_submit=True),
        output_root=tmp_path,
        transport=fail_transport,
    )

    assert second.summary["status"] == "skipped_existing"
    assert second.summary["submitted"] is False


def test_check_xueqiu_auth_returns_ok_for_current_endpoint() -> None:
    def fake_transport(method, url, payload, headers):  # noqa: ANN001, ARG001
        assert method == "GET"
        if url.endswith("/current.json"):
            return {
                "last_rb": {
                    "cash": 50.0,
                    "holdings": [
                        {
                            "stock_symbol": "SZ000001",
                            "stock_name": "平安银行",
                            "weight": 12.5,
                            "price": "11.2",
                            "avg_cost": "10.8",
                        }
                    ],
                }
            }
        if url.endswith("/quote.json"):
            return {"ZH3625640": {"name": "伺机而动", "net_value": "1.0000"}}
        raise AssertionError(url)

    result = check_xueqiu_auth(
        config=XueqiuConfig(cube_symbol="ZH3625640", cookie="u=1"),
        transport=fake_transport,
    )

    assert result["status"] == "ok"
    assert result["authenticated"] is True
    assert result["login_required"] is False
    assert result["holding_count"] == 1
    assert result["current_holdings"][0]["name"] == "平安银行"
    assert result["current_holdings"][0]["weight"] == 12.5
    assert result["current_holdings"][0]["cost_price"] == 10.8
    assert result["current_cash_pct"] == 50.0
    assert result["net_value"] == "1.0000"


def test_check_xueqiu_auth_classifies_login_required_error() -> None:
    def fake_transport(method, url, payload, headers):  # noqa: ANN001, ARG001
        return {
            "error_code": "400016",
            "error_description": "遇到错误，请刷新页面或者重新登录帐号后再试",
            "error_uri": "/cubes/rebalancing/current.json",
        }

    result = check_xueqiu_auth(
        config=XueqiuConfig(cube_symbol="ZH3625640", cookie="expired"),
        transport=fake_transport,
    )

    assert result["status"] == "login_required"
    assert result["authenticated"] is False
    assert result["login_required"] is True
    assert result["error_code"] == "400016"


def test_check_xueqiu_auth_treats_quote_ok_as_accessible_when_current_is_unavailable() -> None:
    calls: list[dict[str, object]] = []

    def fake_transport(method, url, payload, headers):  # noqa: ANN001, ARG001
        calls.append({"method": method, "url": url, "payload": payload})
        if url.endswith("/current.json"):
            return {
                "error_code": "10022",
                "error_description": "",
                "error_uri": "/cubes/rebalancing/current.json",
            }
        if url.endswith("/quote.json"):
            return {
                "ZH3625640": {
                    "symbol": "ZH3625640",
                    "name": "伺机而动",
                    "net_value": "1.0000",
                }
            }
        raise AssertionError(url)

    result = check_xueqiu_auth(
        config=XueqiuConfig(cube_symbol="ZH3625640", cookie="u=1; xq_a_token=token"),
        transport=fake_transport,
    )

    assert result["status"] == "quote_ok_current_unavailable"
    assert result["authenticated"] is True
    assert result["login_required"] is False
    assert result["holding_count"] == 0
    assert result["cube_name"] == "伺机而动"
    assert [call["url"].rsplit("/", 1)[-1] for call in calls] == ["current.json", "quote.json"]


def test_classify_xueqiu_exception_recognizes_anti_automation_login_state() -> None:
    result = classify_xueqiu_exception(ValueError("xueqiu error 20842: login expired"))

    assert result["status"] == "login_required"
    assert result["login_required"] is True
    assert result["error_code"] == "20842"
