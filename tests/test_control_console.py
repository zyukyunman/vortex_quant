from __future__ import annotations

import json
import threading
import time
from urllib.request import Request, urlopen

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

    assert "运营驾驶舱" in html
    assert "运行中心" in html
    assert "因子研究实验室" in html
    assert "策略启动向导" in html
    assert "设置 / 集成管理" in html
    assert "/api/config/qmt" in html
    assert "数据服务" in html
    assert "交易门禁与流程" in html
    assert "/api/qmt/health" in html


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
