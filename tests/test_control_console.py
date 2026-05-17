from __future__ import annotations

import json
import threading
import time
from urllib.request import Request, urlopen

import pytest
import vortex.runtime.control_console as console
from vortex.research.cogalpha import RUN_MANIFEST_SCHEMA


def _serve(root):
    server = console.ControlConsoleServer(("127.0.0.1", 0), root)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, f"http://127.0.0.1:{server.server_port}"


def _get_json(url: str):
    with urlopen(url, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _post_json(url: str, payload: dict):
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _wait_for_job(base_url: str, job_id: str, *, timeout: float = 5.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        payload = _get_json(f"{base_url}/api/jobs/{job_id}")
        job = payload["job"]
        if job["status"] in {"success", "failed"}:
            return job
        time.sleep(0.05)
    raise AssertionError(f"job did not finish: {job_id}")


def test_control_console_saves_lark_config_and_masks_status(monkeypatch, tmp_path):
    for key in console.LARK_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    server, base_url = _serve(tmp_path / "workspace")
    try:
        result = _post_json(
            f"{base_url}/api/config/lark",
            {
                "app_id": "cli_test_lark",
                "app_secret": "secret_123456789",
                "default_receive_id": "ou_receiver",
                "default_receive_id_type": "open_id",
            },
        )
        status = _get_json(f"{base_url}/api/status")
    finally:
        server.shutdown()
        server.server_close()

    env_text = (tmp_path / "workspace" / ".env").read_text(encoding="utf-8")
    assert result["status"] == "saved"
    assert "LARK_APP_ID=cli_test_lark" in env_text
    assert "LARK_APP_SECRET=secret_123456789" in env_text
    assert status["lark"]["configured"] is True
    assert status["lark"]["env"]["LARK_APP_SECRET"] == "secr...6789"
    assert status["notification_provider"] == "lark"


def test_control_console_switches_notification_provider(monkeypatch, tmp_path):
    monkeypatch.delenv("VORTEX_NOTIFICATION_PROVIDER", raising=False)
    server, base_url = _serve(tmp_path / "workspace")
    try:
        result = _post_json(
            f"{base_url}/api/config/notification-provider",
            {"provider": "feishu"},
        )
        status = _get_json(f"{base_url}/api/status")
    finally:
        server.shutdown()
        server.server_close()

    env_text = (tmp_path / "workspace" / ".env").read_text(encoding="utf-8")
    assert result["status"] == "saved"
    assert result["provider"] == "feishu"
    assert "VORTEX_NOTIFICATION_PROVIDER=feishu" in env_text
    assert status["notification_provider"] == "feishu"


def test_control_console_saves_qmt_config_and_masks_status(monkeypatch, tmp_path):
    for key in console.QMT_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    server, base_url = _serve(tmp_path / "workspace")
    try:
        result = _post_json(
            f"{base_url}/api/config/qmt",
            {
                "qmt_bridge_url": "http://127.0.0.1:8000",
                "qmt_bridge_token": "qmt_secret_123456",
                "qmt_account_id": "paper_account",
            },
        )
        status = _get_json(f"{base_url}/api/status")
    finally:
        server.shutdown()
        server.server_close()

    env_text = (tmp_path / "workspace" / ".env").read_text(encoding="utf-8")
    assert result["status"] == "saved"
    assert "QMT_BRIDGE_URL=http://127.0.0.1:8000" in env_text
    assert "QMT_BRIDGE_BASE_URL=http://127.0.0.1:8000" in env_text
    assert "QMT_BRIDGE_TOKEN=qmt_secret_123456" in env_text
    assert status["qmt"]["configured"] is True
    assert status["qmt"]["bridge_url"] == "http://127.0.0.1:8000"
    assert status["qmt"]["token_configured"] is True
    assert status["qmt"]["env"]["QMT_BRIDGE_TOKEN"] == "qmt_...3456"
    assert "QMT_BRIDGE_API_KEY=qmt_secret_123456" in env_text
    assert "QMT_BRIDGE_TRADING_ACCOUNT_ID=paper_account" in env_text


def test_control_console_saves_qmt_api_key_alias(monkeypatch, tmp_path):
    for key in console.QMT_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    server, base_url = _serve(tmp_path / "workspace")
    try:
        result = _post_json(
            f"{base_url}/api/config/qmt",
            {
                "qmt_bridge_url": "http://127.0.0.1:8000",
                "qmt_bridge_api_key": "api_key_abcdef",
                "qmt_account_id": "paper_account",
            },
        )
        status = _get_json(f"{base_url}/api/status")
    finally:
        server.shutdown()
        server.server_close()

    env_text = (tmp_path / "workspace" / ".env").read_text(encoding="utf-8")
    assert result["status"] == "saved"
    assert "QMT_BRIDGE_TOKEN=api_key_abcdef" in env_text
    assert "QMT_BRIDGE_API_KEY=api_key_abcdef" in env_text
    assert status["qmt"]["token_configured"] is True


def test_control_console_status_reports_tushare_and_model_env(monkeypatch, tmp_path):
    monkeypatch.setenv("TUSHARE_TOKEN", "tushare_secret_abcdef")
    monkeypatch.setenv("TUSHARE_EXTRA_PERMISSIONS", "stock_minutes")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "deepseek_secret_abcdef")
    server, base_url = _serve(tmp_path / "workspace")
    try:
        status = _get_json(f"{base_url}/api/status")
    finally:
        server.shutdown()
        server.server_close()

    assert status["tushare"]["configured"] is True
    assert status["tushare"]["permissions"] == "stock_minutes"
    assert status["tushare"]["env"]["TUSHARE_TOKEN"] == "tush...cdef"
    assert status["models"]["configured"] is True
    assert status["models"]["env"]["DEEPSEEK_API_KEY"] == "deep...cdef"


def test_control_console_discovers_qmt_config_from_strategy_artifact(monkeypatch, tmp_path):
    for key in console.QMT_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    root = tmp_path / "workspace"
    strategy_dir = root / "strategy"
    strategy_dir.mkdir(parents=True)
    (strategy_dir / "auto.json").write_text(
        json.dumps(
            {
                "qmt_bridge_url": "http://192.168.64.4:8000",
                "qmt_account_id": "99034443",
            }
        ),
        encoding="utf-8",
    )
    server, base_url = _serve(root)
    try:
        status = _get_json(f"{base_url}/api/status")
    finally:
        server.shutdown()
        server.server_close()

    assert status["qmt"]["configured"] is True
    assert status["qmt"]["persisted"] is False
    assert status["qmt"]["bridge_url"] == "http://192.168.64.4:8000"
    assert status["qmt"]["account_id"] == "99034443"
    assert status["qmt"]["source"].endswith("auto.json")


def test_control_console_qmt_health_endpoint_records_runtime_state(monkeypatch, tmp_path):
    def _fake_probe(qmt_config):
        return {
            "checked_at": "2026-05-16T09:30:00",
            "status": "success",
            "ok": True,
            "bridge_url": qmt_config["bridge_url"],
            "account_id": qmt_config["account_id"],
            "token_configured": qmt_config["token_configured"],
            "blocking_reason": "-",
            "cash": {
                "available_cash": 100.0,
                "total_asset": 1000.0,
                "market_value": 900.0,
                "frozen_cash": 0.0,
            },
            "position_count": 2,
            "order_count": 1,
            "fill_count": 1,
        }

    monkeypatch.setattr(console, "_probe_qmt_bridge", _fake_probe)
    monkeypatch.setenv("QMT_BRIDGE_URL", "http://127.0.0.1:8000")
    monkeypatch.setenv("QMT_ACCOUNT_ID", "paper_account")
    monkeypatch.setenv("QMT_BRIDGE_TOKEN", "secret")
    server, base_url = _serve(tmp_path / "workspace")
    try:
        result = _post_json(f"{base_url}/api/qmt/health", {})
        job = _wait_for_job(base_url, result["job"]["job_id"])
        status = _get_json(f"{base_url}/api/status")
    finally:
        server.shutdown()
        server.server_close()

    assert result["status"] == "accepted"
    assert job["status"] == "success"
    assert status["qmt"]["health"]["ok"] is True
    assert status["daily_trade_review"]["state"] == "ready"


def test_control_console_saves_xueqiu_config_and_masks_status(monkeypatch, tmp_path):
    for key in console.XUEQIU_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    server, base_url = _serve(tmp_path / "workspace")
    try:
        result = _post_json(
            f"{base_url}/api/config/xueqiu",
            {
                "cube_symbol": "ZH3625640",
                "market": "cn",
                "cookie": "u=1; xq_a_token=secret_abcdef",
                "cookie_file": "",
                "submit_enabled": False,
                "notification_profile": "default",
            },
        )
        status = _get_json(f"{base_url}/api/status")
    finally:
        server.shutdown()
        server.server_close()

    env_text = (tmp_path / "workspace" / ".env").read_text(encoding="utf-8")
    assert result["status"] == "saved"
    assert "XUEQIU_CUBE_SYMBOL=ZH3625640" in env_text
    assert "XUEQIU_MARKET=cn" in env_text
    assert "XUEQIU_COOKIE=u=1; xq_a_token=secret_abcdef" in env_text
    assert "XUEQIU_SUBMIT=0" in env_text
    assert status["xueqiu"]["configured"] is True
    assert status["xueqiu"]["cube_symbol"] == "ZH3625640"
    assert status["xueqiu"]["cookie_configured"] is True
    assert status["xueqiu"]["submit_enabled"] is False
    assert status["xueqiu"]["env"]["XUEQIU_COOKIE"] != "u=1; xq_a_token=secret_abcdef"
    assert status["xueqiu"]["env"]["XUEQIU_COOKIE"].endswith("cdef")


def test_control_console_xueqiu_auth_endpoint_records_runtime_state(monkeypatch, tmp_path):
    def _fake_probe(xueqiu_config):
        return {
            "status": "ok",
            "authenticated": True,
            "login_required": False,
            "cube_symbol": xueqiu_config["cube_symbol"],
            "checked_at": "2026-05-17T10:00:00",
            "holding_count": 3,
        }

    monkeypatch.setattr(console, "_probe_xueqiu_auth", _fake_probe)
    monkeypatch.setenv("XUEQIU_CUBE_SYMBOL", "ZH3625640")
    monkeypatch.setenv("XUEQIU_MARKET", "cn")
    monkeypatch.setenv("XUEQIU_COOKIE", "u=1")
    server, base_url = _serve(tmp_path / "workspace")
    try:
        result = _post_json(f"{base_url}/api/xueqiu/auth-check", {})
        job = _wait_for_job(base_url, result["job"]["job_id"])
        status = _get_json(f"{base_url}/api/status")
    finally:
        server.shutdown()
        server.server_close()

    assert result["status"] == "accepted"
    assert job["status"] == "success"
    assert job["result"]["status"] == "ok"
    assert status["xueqiu"]["auth"]["authenticated"] is True
    assert status["xueqiu"]["auth"]["holding_count"] == 3


def test_control_console_xueqiu_auth_failure_notifies_current_channel(monkeypatch, tmp_path):
    notifications: list[dict[str, object]] = []

    def _fake_probe(xueqiu_config):
        return {
            "status": "login_required",
            "authenticated": False,
            "login_required": True,
            "cube_symbol": xueqiu_config["cube_symbol"],
            "checked_at": "2026-05-17T10:00:00",
            "error_code": "400016",
            "error": "login expired",
        }

    def _fake_notify(root, result):
        notifications.append({"root": str(root), "result": result})
        return {"status": "sent", "channel": "lark"}

    monkeypatch.setattr(console, "_probe_xueqiu_auth", _fake_probe)
    monkeypatch.setattr(console, "_notify_xueqiu_auth_check_failure", _fake_notify)
    monkeypatch.setenv("XUEQIU_CUBE_SYMBOL", "ZH3625640")
    monkeypatch.setenv("XUEQIU_COOKIE", "expired")
    server, base_url = _serve(tmp_path / "workspace")
    try:
        result = _post_json(f"{base_url}/api/xueqiu/auth-check", {})
        job = _wait_for_job(base_url, result["job"]["job_id"])
    finally:
        server.shutdown()
        server.server_close()

    assert job["status"] == "success"
    assert job["result"]["notification"]["status"] == "sent"
    assert notifications[0]["result"]["status"] == "login_required"


def test_control_console_xueqiu_auth_accessible_warning_does_not_notify_failure(monkeypatch, tmp_path):
    def _fake_probe(xueqiu_config):
        return {
            "status": "quote_ok_current_unavailable",
            "authenticated": True,
            "login_required": False,
            "cube_symbol": xueqiu_config["cube_symbol"],
            "checked_at": "2026-05-17T10:00:00",
            "warning": "组合可访问，但 current 调仓接口不可读。",
        }

    def _fake_notify(root, result):  # noqa: ARG001
        raise AssertionError("accessible warning should not send failure notification")

    monkeypatch.setattr(console, "_probe_xueqiu_auth", _fake_probe)
    monkeypatch.setattr(console, "_notify_xueqiu_auth_check_failure", _fake_notify)
    monkeypatch.setenv("XUEQIU_CUBE_SYMBOL", "ZH3625640")
    monkeypatch.setenv("XUEQIU_COOKIE", "u=1")
    server, base_url = _serve(tmp_path / "workspace")
    try:
        result = _post_json(f"{base_url}/api/xueqiu/auth-check", {})
        job = _wait_for_job(base_url, result["job"]["job_id"])
    finally:
        server.shutdown()
        server.server_close()

    assert job["status"] == "success"
    assert job["result"]["authenticated"] is True
    assert "notification" not in job["result"]


def test_control_console_imports_xueqiu_cookie_from_browser(monkeypatch, tmp_path):
    def _fake_extract():
        return {
            "status": "found",
            "cookie": "u=1; xq_a_token=token_from_chrome",
            "source": "/Users/demo/Chrome/Default/Cookies",
            "cookie_count": 2,
        }

    monkeypatch.setattr(console, "_extract_xueqiu_cookie_from_browsers", _fake_extract)
    server, base_url = _serve(tmp_path / "workspace")
    try:
        result = _post_json(
            f"{base_url}/api/xueqiu/import-cookie",
            {"cube_symbol": "ZH3625640", "market": "cn"},
        )
        status = _get_json(f"{base_url}/api/status")
    finally:
        server.shutdown()
        server.server_close()

    env_text = (tmp_path / "workspace" / ".env").read_text(encoding="utf-8")
    assert result["status"] == "imported"
    assert result["cookie"]["masked"].endswith("rome")
    assert "XUEQIU_COOKIE=u=1; xq_a_token=token_from_chrome" in env_text
    assert "XUEQIU_CUBE_SYMBOL=ZH3625640" in env_text
    assert status["xueqiu"]["configured"] is True


def test_control_console_reads_xueqiu_cookie_from_active_chrome_tab(monkeypatch):
    monkeypatch.setattr(
        console,
        "_extract_xueqiu_cookie_from_browser_tabs",
        lambda: {
            "status": "found",
            "cookie": "u=1; xq_a_token=token_from_tab",
            "source": "chrome_tab:https://xueqiu.com/",
            "cookie_count": 2,
            "names": ["u", "xq_a_token"],
        },
    )
    monkeypatch.setattr(
        console,
        "_browser_cookie_paths",
        lambda: (_ for _ in ()).throw(AssertionError("database scan should not run")),
    )

    result = console._extract_xueqiu_cookie_from_browsers()

    assert result["status"] == "found"
    assert result["cookie"] == "u=1; xq_a_token=token_from_tab"
    assert result["source"] == "chrome_tab:https://xueqiu.com/"


def test_control_console_reports_chrome_javascript_disabled_for_xueqiu_cookie(monkeypatch):
    monkeypatch.setattr(
        console,
        "_extract_xueqiu_cookie_from_browser_tabs",
        lambda: {
            "status": "javascript_disabled",
            "message": "Chrome 已打开雪球标签，但禁止通过 AppleScript 读取当前页面 Cookie。",
            "checked_sources": ["chrome_tab:https://xueqiu.com/"],
        },
    )
    monkeypatch.setattr(console, "_browser_cookie_paths", lambda: [])

    result = console._extract_xueqiu_cookie_from_browsers()

    assert result["status"] == "javascript_disabled"
    assert "AppleScript" in result["message"]
    assert result["checked_sources"] == ["chrome_tab:https://xueqiu.com/"]


def test_control_console_reports_xueqiu_sync_in_active_strategy(monkeypatch, tmp_path):
    monkeypatch.setenv("XUEQIU_CUBE_SYMBOL", "ZH3625640")
    monkeypatch.setenv("XUEQIU_COOKIE", "u=1")
    root = tmp_path / "workspace"
    target_dir = root / "trade" / "targets" / "20260518"
    target_dir.mkdir(parents=True)
    target_path = target_dir / "tp_20260518_demo.json"
    target_path.write_text(
        json.dumps(
            {
                "portfolio_id": "tp_20260518_demo",
                "trade_date": "20260518",
                "strategy_version": "baseline_top110_large",
                "run_id": "handoff_20260517",
                "cash_target": 500000.0,
                "positions": [
                    {"symbol": "600519.SH", "target_weight": 0.30, "target_value": 300000.0, "target_shares": 200},
                    {"symbol": "000001.SZ", "target_weight": 0.20, "target_value": 200000.0, "target_shares": 20000},
                ],
            }
        ),
        encoding="utf-8",
    )
    xueqiu_dir = root / "trade" / "xueqiu" / "xq_20260518_demo"
    xueqiu_dir.mkdir(parents=True)
    report_path = xueqiu_dir / "xueqiu_report.json"
    payload_path = xueqiu_dir / "rebalance_payload.json"
    payload_path.write_text(
        json.dumps(
            {
                "cash": 50.0,
                "cube_symbol": "ZH3625640",
                "holdings": json.dumps(
                    [
                        {
                            "stock_symbol": "SH600519",
                            "stock_name": "贵州茅台",
                            "weight": 30.0,
                            "price": "1500.0",
                            "proactive": True,
                            "segment_name": "食品饮料",
                        },
                        {
                            "stock_symbol": "SZ000001",
                            "stock_name": "平安银行",
                            "weight": 20.0,
                            "price": "11.2",
                            "proactive": True,
                            "segment_name": "银行",
                        },
                    ],
                    ensure_ascii=False,
                ),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    report_path.write_text(
        json.dumps(
            {
                "sync_id": "xq_20260518_demo",
                "mode": "xueqiu",
                "status": "dry_run",
                "submitted": False,
                "cube_symbol": "ZH3625640",
                "trade_date": "20260518",
                "portfolio_id": "tp_20260518_demo",
                "strategy_version": "baseline_top110_large",
                "target_position_count": 2,
                "xueqiu_holding_count": 3,
                "cash_pct": 50.0,
                "weight_sum_pct": 50.0,
                "changed_symbols": ["SH600519", "SZ000001"],
                "payload_path": str(payload_path),
                "report_path": str(report_path),
                "created_at": "2026-05-17T10:01:00",
            }
        ),
        encoding="utf-8",
    )
    task_dir = root / "state" / "trade" / "pending_qmt"
    task_dir.mkdir(parents=True)
    (task_dir / "20260518-demo.json").write_text(
        json.dumps(
            {
                "task_type": "earnings_forecast_qmt_rebalance",
                "status": "pending",
                "trade_date": "20260518",
                "as_of": "20260517",
                "strategy_version": "baseline_top110_large",
                "target_portfolio_path": str(target_path),
                "xueqiu_sync_status": "dry_run",
                "xueqiu_sync_report_path": str(report_path),
                "xueqiu_cube_symbol": "ZH3625640",
            }
        ),
        encoding="utf-8",
    )

    server, base_url = _serve(root)
    try:
        status = _get_json(f"{base_url}/api/status")
    finally:
        server.shutdown()
        server.server_close()

    xueqiu = status["active_strategy"]["xueqiu"]
    assert xueqiu["status"] == "dry_run"
    assert xueqiu["cube_symbol"] == "ZH3625640"
    assert xueqiu["changed_count"] == 2
    assert xueqiu["weight_sum_pct"] == 50.0
    assert xueqiu["planned_holding_count"] == 2
    assert xueqiu["changed_holdings"][0]["name"] == "贵州茅台"
    assert xueqiu["changed_holdings"][1]["weight"] == 20.0
    assert any(step["name"] == "雪球组合同步" for step in status["active_strategy"]["workflow"])


def test_control_console_reports_xueqiu_browser_submit_summary(tmp_path):
    root = tmp_path / "workspace"
    xueqiu_dir = root / "trade" / "xueqiu" / "browser-submit-20260517-zh3625640"
    xueqiu_dir.mkdir(parents=True)
    report_path = xueqiu_dir / "xueqiu_browser_submit_report.json"
    report_path.write_text(
        json.dumps(
            {
                "started_at": "2026-05-17T06:42:11",
                "finished_at": "2026-05-17T06:42:14",
                "cube_symbol": "ZH3625640",
                "portfolio_id": "tp_20260515_demo",
                "trade_date": "20260515",
                "strategy_version": "baseline_top110_large",
                "target_position_count": 13,
                "target_weight_sum_pct": 65,
                "target_cash_pct": 35,
                "status": "error",
                "submitted": False,
                "error": "xueqiu 20842",
            }
        ),
        encoding="utf-8",
    )

    xueqiu = console._latest_xueqiu_sync_summary(root)

    assert xueqiu["status"] == "error"
    assert xueqiu["source"] == "browser_submit"
    assert xueqiu["report_path"] == str(report_path)
    assert xueqiu["cash_pct"] == 35
    assert xueqiu["weight_sum_pct"] == 65
    assert xueqiu["target_position_count"] == 13
    assert xueqiu["error"] == "xueqiu 20842"


def test_control_console_auto_once_passes_xueqiu_config(monkeypatch, tmp_path):
    import vortex.strategy.earnings_forecast_live as live

    captured: dict[str, object] = {}

    def _fake_auto(root, **kwargs):  # noqa: ANN001
        captured.update(kwargs)
        return {"status": "ok", "summary": {"root": str(root)}}

    monkeypatch.setattr(live, "run_earnings_forecast_auto_once", _fake_auto)
    monkeypatch.setenv("QMT_BRIDGE_URL", "http://127.0.0.1:8000")
    monkeypatch.setenv("XUEQIU_CUBE_SYMBOL", "ZH3625640")
    monkeypatch.setenv("XUEQIU_COOKIE_FILE", "/tmp/xueqiu.cookie")
    monkeypatch.setenv("XUEQIU_SUBMIT", "0")

    result = console._run_earnings_forecast_auto_once_from_payload(
        tmp_path / "workspace",
        {
            "start": "20170101",
            "enable_xueqiu": True,
            "xueqiu_market": "cn",
            "profile": "default",
        },
    )

    assert result["status"] == "ok"
    assert captured["xueqiu_enabled"] is True
    assert captured["xueqiu_cube_symbol"] == "ZH3625640"
    assert captured["xueqiu_cookie_file"] == "/tmp/xueqiu.cookie"
    assert captured["xueqiu_market"] == "cn"
    assert captured["xueqiu_submit"] is False


def test_control_console_reports_active_strategy_daily_state(monkeypatch, tmp_path):
    monkeypatch.setattr(console, "_stock_name_lookup", lambda root, symbols: {"000001.SZ": "平安银行"})
    root = tmp_path / "workspace"
    target_dir = root / "trade" / "targets" / "20260515"
    target_dir.mkdir(parents=True)
    target_path = target_dir / "tp_20260515_demo.json"
    target_path.write_text(
        json.dumps(
            {
                "portfolio_id": "tp_20260515_demo",
                "trade_date": "20260515",
                "strategy_version": "baseline_top110_large",
                "run_id": "handoff_20260514",
                "cash_target": 1000.0,
                "positions": [
                    {
                        "symbol": "000001.SZ",
                        "target_weight": 0.05,
                        "target_value": 50000.0,
                        "target_shares": 1000,
                        "reference_price": 50.0,
                        "reason": "buy_or_increase",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    task_dir = root / "state" / "trade" / "pending_qmt"
    task_dir.mkdir(parents=True)
    (task_dir / "20260515-demo.json").write_text(
        json.dumps(
            {
                "task_type": "earnings_forecast_qmt_rebalance",
                "status": "pending",
                "trade_date": "20260515",
                "as_of": "20260514",
                "strategy_version": "baseline_top110_large",
                "target_portfolio_path": str(target_path),
                "target_diagnostics": {
                    "final_position_count": 1,
                    "selection_funnel": {
                        "raw_signal_count": 25,
                        "positive_signal_count": 20,
                        "after_liquidity_count": 18,
                        "after_st_filter_count": 10,
                        "selected_position_count": 1,
                    },
                    "data_freshness": {
                        "required_as_of": "20260514",
                        "status": "ok",
                        "datasets": {"bars": {"max_date": "20260514", "required_as_of": "20260514", "ok": True}},
                    },
                },
                "quality_summary": {"holding_count": 1, "label_counts": {"pass": 1}, "review_symbols": []},
            }
        ),
        encoding="utf-8",
    )
    status_dir = root / "state" / "strategy" / "earnings_forecast_auto"
    status_dir.mkdir(parents=True)
    (status_dir / "status.json").write_text(
        json.dumps(
            {
                "service_status": "running",
                "loop_mode": "loop",
                "last_tick_status": "success",
                "last_tick_at": "2026-05-15T09:30:00",
                "config": {"preset_name": "baseline_top110_large", "prepare_time": "08:10", "execute_time": "09:25"},
                "last_tick": {"skipped": []},
            }
        ),
        encoding="utf-8",
    )
    nav_state = root / "state" / "nav"
    nav_state.mkdir(parents=True)
    run_id = "earnings_forecast_auto-baseline_top110_large-paper"
    (nav_state / f"{run_id}.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "strategy_name": "earnings_forecast_auto",
                "strategy_version": "baseline_top110_large",
                "account_id": "paper",
                "initial_equity": 1000000.0,
                "start_date": "20260515",
                "benchmark": "000852.SH",
            }
        ),
        encoding="utf-8",
    )
    nav_trade = root / "trade" / "nav"
    nav_trade.mkdir(parents=True)
    (nav_trade / f"{run_id}.csv").write_text(
        "trade_date,net_value,total_asset,daily_return,benchmark_return,excess_return\n"
        "20260515,1.01,1010000,0.01,0.002,0.008\n",
        encoding="utf-8",
    )
    (root / "state").mkdir(exist_ok=True)
    (root / "state" / "live-service-health-latest.json").write_text(
        json.dumps(
            {
                "checked_at": "2026-05-15T18:10:00",
                "status": "ok",
                "scheduled_profiles": [{"name": "default", "schedule": "0 18 * * *"}],
                "data": {
                    "latest_success_update": {"run_id": "data_ok", "status": "success", "snapshot_id": "snap_1"},
                    "latest_snapshot": {"snapshot_id": "snap_1", "as_of": "20260515"},
                    "active_tasks": [{"stage": "fetch", "dataset": "bars", "message": "bars 20260515"}],
                },
            }
        ),
        encoding="utf-8",
    )

    server, base_url = _serve(root)
    try:
        status = _get_json(f"{base_url}/api/status")
        runs = _get_json(f"{base_url}/api/runs")
    finally:
        server.shutdown()
        server.server_close()

    assert len(status["strategies"]) == 1
    assert len(runs["strategies"]) == 1
    assert status["active_strategy"]["current"]["strategy_version"] == "baseline_top110_large"
    assert status["active_strategy"]["service"]["preset_name"] == "baseline_top110_large"
    assert status["active_strategy"]["target"]["position_count"] == 1
    assert status["active_strategy"]["target"]["positions"][0]["name"] == "平安银行"
    assert status["active_strategy"]["selection_funnel"][0]["count"] == 25
    assert status["active_strategy"]["nav"]["latest_net_value"] == 1.01
    assert status["active_strategy"]["nav"]["snapshot_count"] == 1
    assert status["active_strategy"]["nav"]["has_return_window"] is False
    assert status["active_strategy"]["nav"]["daily_return"] is None
    assert status["active_strategy"]["nav"]["max_drawdown"] is None
    assert "少于 2 条" in status["active_strategy"]["nav"]["metric_notes"]["max_drawdown"]
    assert status["data_service"]["active_task_count"] == 1


def test_control_console_data_progress_log_overrides_stale_running_task(tmp_path):
    root = tmp_path / "workspace"
    log_dir = root / "state" / "logs"
    log_dir.mkdir(parents=True)
    log_path = log_dir / "data-update-20260516_180000.log"
    log_path.write_text(
        "2026-05-16 18:00:01,000 [INFO] vortex.data.pipeline: 开始同步 dataset=calendar (1/2)\n"
        "2026-05-16 18:00:02,000 [INFO] vortex.data.pipeline: dataset=calendar 完成: fetch_elapsed=1.0s, pit_elapsed=0.0s, write_elapsed=0.0s, total_elapsed=1.0s, rows=10\n"
        "2026-05-16 18:00:03,000 [INFO] vortex.data.pipeline: 开始同步 dataset=stock_company (2/2)\n"
        "2026-05-16 18:00:04,000 [INFO] vortex.data.provider.tushare: stock_company exchange=SSE: 1/3\n"
        "2026-05-16 18:00:05,000 [INFO] vortex.data.pipeline: dataset=stock_company 完成: fetch_elapsed=1.0s, pit_elapsed=0.0s, write_elapsed=0.0s, total_elapsed=1.0s, rows=20\n"
        '{\n'
        '  "run_id": "data_20260516_180000_37c2",\n'
        '  "action": "update",\n'
        '  "status": "success",\n'
        '  "total_rows": 30,\n'
        '  "error": null\n'
        '}\n',
        encoding="utf-8",
    )
    state_dir = root / "state"
    state_dir.mkdir(exist_ok=True)
    (state_dir / "live-service-health-latest.json").write_text(
        json.dumps(
            {
                "checked_at": "2026-05-16T18:00:04",
                "status": "ok",
                "data": {
                    "latest_run": {
                        "run_id": "data_20260516_180000_37c2",
                        "status": "running",
                        "total_rows": 0,
                    },
                    "active_tasks": [
                        {
                            "run_id": "data_20260516_180000_37c2",
                            "status": "running",
                            "dataset": "stock_company",
                            "log_path": str(log_path),
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )

    summary = console._data_service_summary(root)

    assert summary["latest_run"]["status"] == "success"
    assert summary["latest_run"]["total_rows"] == 30
    assert summary["active_task_count"] == 0
    assert summary["progress"]["dataset_index"] == 2
    assert summary["progress"]["dataset_total"] == 2
    assert summary["progress"]["completed_dataset_count"] == 2


def test_live_rebalance_uses_qmt_positions_and_hides_matched_holdings():
    target = {
        "trade_date": "20260515",
        "all_positions": [
            {
                "symbol": "301345.SZ",
                "name": "涛涛车业",
                "target_shares": 200,
                "reference_price": 247.56,
            },
            {
                "symbol": "000729.SZ",
                "name": "燕京啤酒",
                "target_shares": 4000,
                "reference_price": 12.47,
            },
        ],
    }
    qmt_health = {
        "ok": True,
        "checked_at": "2026-05-16T20:28:45",
        "positions": [
            {"symbol": "301345.SZ", "name": "涛涛车业", "shares": 200, "last_price": 239.79},
            {"symbol": "000729.SZ", "name": "燕京啤酒", "shares": 3800, "last_price": 12.27},
            {"symbol": "002082.SZ", "name": "ST万邦", "shares": 900, "last_price": 18.91},
        ],
    }

    summary = console._live_rebalance_summary(target, qmt_health)

    assert summary["status"] == "ready"
    assert summary["order_count"] == 2
    by_symbol = {item["symbol"]: item for item in summary["orders"]}
    assert "301345.SZ" not in by_symbol
    assert by_symbol["000729.SZ"]["side"] == "buy"
    assert by_symbol["000729.SZ"]["shares"] == 200
    assert by_symbol["000729.SZ"]["current_shares"] == 3800
    assert by_symbol["000729.SZ"]["target_shares"] == 4000
    assert by_symbol["002082.SZ"]["side"] == "sell"
    assert by_symbol["002082.SZ"]["shares"] == 900
    assert by_symbol["002082.SZ"]["target_shares"] == 0


def test_control_console_home_renders_workbench_navigation(tmp_path):
    html = console._render_console_html(tmp_path / "workspace")

    sidebar = html.split("<aside>", 1)[1].split("</aside>", 1)[0]
    assert "运营驾驶舱" not in sidebar
    assert "运行中心" not in sidebar
    assert "因子研究实验室" not in sidebar
    assert "策略启动向导" not in sidebar
    assert "交易监控" in sidebar
    assert "策略配置" in sidebar
    assert "雪球账户" in html
    assert 'id="page-xueqiu"' in html
    assert "当前雪球持仓" in html
    assert "最近生成的雪球调仓计划" in html
    assert "成本价" in html
    assert "同步节奏" in html
    assert html.count('class="xueqiu-table"') == 2
    xueqiu_html = html.split('id="page-xueqiu"', 1)[1].split('id="page-data"', 1)[0]
    assert "xueqiu-holdings-grid" in xueqiu_html
    assert "行业/分组" not in xueqiu_html
    assert "生成策略任务" not in xueqiu_html
    assert "goPage('strategy')" in xueqiu_html
    assert "QMT成本" not in xueqiu_html
    assert "策略参考成本" not in xueqiu_html
    assert "未成交/未获取" in html
    assert "基础设置" in html
    assert "/api/config/qmt" in html
    assert "雪球组合" in html
    assert "/api/config/xueqiu" in html
    assert "/api/xueqiu/auth-check" in html
    assert "/api/xueqiu/import-cookie" in html
    assert "数据服务" in html
    assert "启动服务" in html
    assert "立即更新" in html
    assert "立即执行一轮" in html
    assert "应用配置并重启" in html
    assert "暂停自动服务" in html
    assert "允许 QMT 真实下单" in html
    assert "/api/data/server-start" in html
    assert "/api/data/update-now" in html
    assert "/api/strategy/earnings-forecast/auto-loop-start" in html
    assert "/api/strategy/earnings-forecast/auto-loop-stop" in html
    assert "交易门禁与流程" in html
    trading_html = html.split('id="page-trading"', 1)[1].split('id="page-xueqiu"', 1)[0]
    assert "雪球组合账户" not in trading_html
    assert "/api/qmt/health" in html
    assert "高级：手工 Cookie 覆盖" in html
    wait_fn = html.split("async function waitForJobResult", 1)[1].split("function renderResearchRuns", 1)[0]
    assert "payload.job || payload" in wait_fn
    check_fn = html.split("async function checkXueqiuAuth()", 1)[1].split("async function testLark()", 1)[0]
    assert "goPage('trading')" not in check_fn


def test_control_console_start_strategy_auto_loop_uses_default_label(monkeypatch, tmp_path):
    commands: list[list[str]] = []
    captured_env: list[dict[str, str]] = []

    class _FakeProcess:
        pid = 24680

        def poll(self):
            return None

    def _fake_launch(command, log_path, *, env_overrides=None):
        commands.append(command)
        captured_env.append(dict(env_overrides or {}))
        return _FakeProcess()

    monkeypatch.setattr(console, "_launch_console_background_process", _fake_launch)
    monkeypatch.setattr(console.time, "sleep", lambda *_args, **_kwargs: None)
    result = console._start_earnings_forecast_auto_loop_from_payload(
        tmp_path / "workspace",
        {
            "start": "20260101",
            "qmt_bridge_url": "http://127.0.0.1:8000",
            "qmt_bridge_token": "token",
            "qmt_account_id": "99034443",
            "enable_xueqiu": True,
            "xueqiu_cube_symbol": "ZH3625640",
            "xueqiu_cookie": "cookie-secret",
        },
    )

    assert result["status"] == "started"
    assert result["pid"] == 24680
    command = commands[0]
    assert "--label" in command
    assert command[command.index("--label") + 1] == console.DEFAULT_AUTO_LABEL
    assert "--disable-trading" in command
    assert "--qmt-bridge-token" not in command
    assert "token" not in command
    assert "--xueqiu-cookie" not in command
    assert "cookie-secret" not in command
    assert captured_env[0]["QMT_BRIDGE_TOKEN"] == "token"
    assert captured_env[0]["XUEQIU_COOKIE"] == "cookie-secret"


def test_control_console_start_strategy_auto_loop_requires_live_gate(monkeypatch, tmp_path):
    with pytest.raises(ValueError, match="matching allowed account"):
        console._start_earnings_forecast_auto_loop_from_payload(
            tmp_path / "workspace",
            {
                "start": "20260101",
                "qmt_bridge_url": "http://127.0.0.1:8000",
                "qmt_bridge_token": "token",
                "qmt_account_id": "99034443",
                "allow_trading": True,
                "confirm_live_trading": "我确认允许执行交易",
            },
        )


def test_control_console_start_strategy_auto_loop_enables_live_gate(monkeypatch, tmp_path):
    commands: list[list[str]] = []

    class _FakeProcess:
        pid = 24681

        def poll(self):
            return None

    def _fake_launch(command, log_path, *, env_overrides=None):  # noqa: ARG001
        commands.append(command)
        return _FakeProcess()

    monkeypatch.setattr(console, "_launch_console_background_process", _fake_launch)
    monkeypatch.setattr(console.time, "sleep", lambda *_args, **_kwargs: None)

    result = console._start_earnings_forecast_auto_loop_from_payload(
        tmp_path / "workspace",
        {
            "start": "20260101",
            "qmt_bridge_url": "http://127.0.0.1:8000",
            "qmt_account_id": "99034443",
            "allow_trading": True,
            "allowed_account_id": "99034443",
            "confirm_trading": "CONFIRM_AUTO_TRADING",
        },
    )

    command = commands[0]
    assert result["status"] == "started"
    assert result["allow_trading"] is True
    assert "--enable-trading" in command
    assert "--disable-trading" not in command
    assert command[command.index("--allowed-account-id") + 1] == "99034443"
    assert command[command.index("--confirm-trading") + 1] == "CONFIRM_AUTO_TRADING"


def test_control_console_stop_strategy_auto_loop_updates_status(monkeypatch, tmp_path):
    root = tmp_path / "workspace"
    status_dir = root / "state" / "strategy" / "earnings_forecast_auto"
    status_dir.mkdir(parents=True)
    status_path = status_dir / "status.json"
    status_path.write_text(
        json.dumps({"service_status": "running", "pid": 12345, "last_tick_status": "success"}),
        encoding="utf-8",
    )
    killed: list[tuple[int, int]] = []

    monkeypatch.setattr(console, "_is_pid_alive", lambda pid: int(pid) == 12345)
    monkeypatch.setattr(console.os, "kill", lambda pid, sig: killed.append((pid, sig)))
    monkeypatch.setattr(console.time, "sleep", lambda *_args, **_kwargs: None)

    result = console._stop_earnings_forecast_auto_loop(root)

    saved = json.loads(status_path.read_text(encoding="utf-8"))
    assert result["status"] == "stopped"
    assert killed
    assert saved["service_status"] == "stopped"
    assert saved["pid"] == 12345


def test_control_console_research_endpoint_uses_company_run(monkeypatch, tmp_path):
    def _fake_run(root, **kwargs):
        return {
            "schema": RUN_MANIFEST_SCHEMA,
            "run": {"run_id": kwargs.get("run_id") or "fake-run", "status": "success"},
            "input": {"days": kwargs["days"], "symbols": kwargs["symbols"]},
        }

    monkeypatch.setattr(console, "run_cogalpha_company_demo_cycle", _fake_run)
    server, base_url = _serve(tmp_path / "workspace")
    try:
        result = _post_json(
            f"{base_url}/api/research/cogalpha-cycle",
            {"days": 120, "symbols": 30, "run_id": "console-test"},
        )
        job = _wait_for_job(base_url, result["job"]["job_id"])
        jobs = _get_json(f"{base_url}/api/jobs")
    finally:
        server.shutdown()
        server.server_close()

    assert result["status"] == "accepted"
    assert job["status"] == "success"
    assert job["result"]["schema"] == RUN_MANIFEST_SCHEMA
    assert job["result"]["run"]["run_id"] == "console-test"
    assert job["result"]["input"] == {"days": 120, "symbols": 30}
    assert jobs["jobs"][0]["job_id"] == result["job"]["job_id"]


def test_control_console_lists_research_run_manifests(tmp_path):
    root = tmp_path / "workspace"
    run_dir = root / "research" / "cogalpha" / "company_runs" / "run-001"
    run_dir.mkdir(parents=True)
    (run_dir / "run_manifest.json").write_text(
        json.dumps(
            {
                "schema": RUN_MANIFEST_SCHEMA,
                "run": {
                    "run_id": "run-001",
                    "kind": "research.cogalpha_cycle",
                    "status": "success",
                    "started_at": "2026-05-16T09:00:00",
                    "output_dir": str(run_dir),
                },
                "quality_gate": {
                    "status": "passed_with_candidates",
                    "promoted_candidate_count": 2,
                },
                "decision": {"approval_required": True},
                "artifacts": {
                    "run_manifest": {"path": str(run_dir / "run_manifest.json")}
                },
            }
        ),
        encoding="utf-8",
    )
    server, base_url = _serve(root)
    try:
        payload = _get_json(f"{base_url}/api/runs")
    finally:
        server.shutdown()
        server.server_close()

    assert payload["research_runs"][0]["run_id"] == "run-001"
    assert payload["research_runs"][0]["quality_gate"] == "passed_with_candidates"
    assert payload["research_runs"][0]["decision_required"] is True
    assert payload["strategies"][0]["strategy_id"] == "earnings_forecast_drift"
