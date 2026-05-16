"""Local HTTP control console for Vortex operations."""
from __future__ import annotations

import html
import csv
import json
import os
import re
import threading
import time
import uuid
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from vortex.notification.channel.feishu import LARK_API_BASE, FeishuChannel, FeishuConfig
from vortex.notification.models import NotificationMessage
from vortex.research.cogalpha import run_cogalpha_company_demo_cycle
from vortex.runtime.workspace import Workspace

LARK_ENV_KEYS = (
    "LARK_APP_ID",
    "LARK_APP_SECRET",
    "LARK_DEFAULT_RECEIVE_ID",
    "LARK_DEFAULT_RECEIVE_ID_TYPE",
    "LARK_API_BASE",
    "VORTEX_NOTIFICATION_PROVIDER",
)

FEISHU_ENV_KEYS = (
    "FEISHU_APP_ID",
    "FEISHU_APP_SECRET",
    "FEISHU_DEFAULT_RECEIVE_ID",
    "FEISHU_DEFAULT_RECEIVE_ID_TYPE",
    "FEISHU_API_BASE",
)

QMT_ENV_KEYS = (
    "QMT_BRIDGE_URL",
    "QMT_BRIDGE_BASE_URL",
    "QMT_BRIDGE_TOKEN",
    "QMT_BRIDGE_API_KEY",
    "QMT_ACCOUNT_ID",
    "QMT_BRIDGE_TRADING_ACCOUNT_ID",
)

TUSHARE_ENV_KEYS = (
    "TUSHARE_TOKEN",
    "TUSHARE_POINTS",
    "TUSHARE_EXTRA_PERMISSIONS",
)

MODEL_ENV_KEYS = (
    "OPENAI_API_KEY",
    "DEEPSEEK_API_KEY",
    "VORTEX_AGENT_BACKEND",
)

AUTO_RUN_STATUS_FILE = "status.json"
DEFAULT_AUTO_PREPARE_TIME = "08:10"
DEFAULT_AUTO_EXECUTE_TIME = "09:25"
DEFAULT_AUTO_PRESET = "baseline_top110_large"
_STOCK_NAME_CACHE: dict[str, dict[str, str]] = {}


class ControlConsoleServer(ThreadingHTTPServer):
    """HTTP server carrying workspace context."""

    def __init__(self, server_address: tuple[str, int], root: Path) -> None:
        super().__init__(server_address, ControlConsoleHandler)
        self.workspace = Workspace(root.expanduser().resolve())
        self.workspace.initialize()
        self.jobs: dict[str, dict[str, Any]] = {}
        self.jobs_lock = threading.Lock()
        self.runtime_state: dict[str, Any] = {"qmt_health": None}
        self.runtime_state_lock = threading.Lock()
        _load_workspace_env(self.workspace.root)


class ControlConsoleHandler(BaseHTTPRequestHandler):
    """Small JSON API plus one HTML page for local operations."""

    server: ControlConsoleServer

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
        path = urlparse(self.path).path
        if path == "/":
            self._send_html(_render_console_html(self.server.workspace.root))
            return
        if path == "/api/status":
            self._send_json(_status_payload(self.server))
            return
        if path == "/api/jobs":
            self._send_json(_jobs_payload(self.server))
            return
        if path.startswith("/api/jobs/"):
            self._send_json(_job_payload(self.server, path.rsplit("/", 1)[-1]))
            return
        if path == "/api/runs":
            self._send_json(_runs_payload(self.server.workspace.root))
            return
        self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)

    def do_HEAD(self) -> None:  # noqa: N802 - stdlib handler API
        path = urlparse(self.path).path
        if path == "/":
            self.send_response(HTTPStatus.OK.value)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        if path in {"/api/status", "/api/jobs", "/api/runs"}:
            self.send_response(HTTPStatus.OK.value)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        self.send_response(HTTPStatus.NOT_FOUND.value)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
        try:
            path = urlparse(self.path).path
            payload = self._read_json()
            if path == "/api/config/lark":
                response = _save_lark_config(self.server.workspace.root, payload)
            elif path == "/api/config/notification-provider":
                response = _save_notification_provider(self.server.workspace.root, payload)
            elif path == "/api/config/qmt":
                response = _save_qmt_config(self.server.workspace.root, payload)
            elif path == "/api/config/trading":
                response = _save_trading_config(self.server.workspace.root, payload)
            elif path == "/api/lark/test":
                response = _send_lark_test_message(self.server.workspace.root, payload)
            elif path == "/api/qmt/health":
                response = _submit_console_job(
                    self.server,
                    kind="qmt.health_check",
                    name="QMT 只读健康检查",
                    action=lambda: _run_qmt_health_check(self.server),
                )
            elif path == "/api/research/cogalpha-cycle":
                response = _submit_console_job(
                    self.server,
                    kind="research.cogalpha_cycle",
                    name="CogAlpha 因子研究闭环",
                    action=lambda: _run_cogalpha_cycle_from_payload(
                        self.server.workspace.root,
                        payload,
                    ),
                )
            elif path == "/api/strategy/earnings-forecast/prepare":
                response = _submit_console_job(
                    self.server,
                    kind="strategy.earnings_forecast.prepare",
                    name="业绩预告策略任务生成",
                    action=lambda: _prepare_earnings_forecast_from_payload(
                        self.server.workspace.root,
                        payload,
                    ),
                )
            elif path == "/api/strategy/earnings-forecast/auto-once":
                response = _submit_console_job(
                    self.server,
                    kind="strategy.earnings_forecast.auto_once",
                    name="业绩预告自动编排一次",
                    action=lambda: _run_earnings_forecast_auto_once_from_payload(
                        self.server.workspace.root,
                        payload,
                    ),
                )
            else:
                self._send_json({"error": "not_found"}, status=HTTPStatus.NOT_FOUND)
                return
            self._send_json(response)
        except ValueError as exc:
            self._send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # noqa: BLE001 - returned to local operator UI.
            self._send_json(
                {"error": type(exc).__name__, "message": str(exc)},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"[vortex-console] {self.address_string()} - {fmt % args}")

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("请求体必须是 JSON") from exc
        if not isinstance(payload, dict):
            raise ValueError("请求体必须是 JSON object")
        return payload

    def _send_json(self, payload: dict[str, Any], *, status: HTTPStatus = HTTPStatus.OK) -> None:
        raw = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def _send_html(self, body: str) -> None:
        raw = body.encode("utf-8")
        self.send_response(HTTPStatus.OK.value)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)


def run_control_console(root: str | Path, *, host: str = "127.0.0.1", port: int = 8765) -> None:
    """Run the local control console until interrupted."""

    server = ControlConsoleServer((host, port), Path(root))
    url = f"http://{host}:{server.server_port}"
    print(f"Vortex 控制台已启动: {url}")
    print("按 Ctrl+C 退出")
    try:
        server.serve_forever()
    finally:
        server.server_close()


def _submit_console_job(
    server: ControlConsoleServer,
    *,
    kind: str,
    name: str,
    action: Any,
) -> dict[str, Any]:
    job_id = f"{kind}-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    job = {
        "job_id": job_id,
        "kind": kind,
        "name": name,
        "status": "queued",
        "stage": "等待执行",
        "created_at": _now(),
        "started_at": None,
        "finished_at": None,
        "updated_at": _now(),
        "duration_seconds": None,
        "result": None,
        "error": None,
    }
    with server.jobs_lock:
        server.jobs[job_id] = job

    def _worker() -> None:
        started = time.time()
        _update_job(
            server,
            job_id,
            status="running",
            stage="执行中",
            started_at=_now(),
        )
        try:
            result = action()
        except Exception as exc:  # noqa: BLE001 - surfaced to local console.
            _update_job(
                server,
                job_id,
                status="failed",
                stage="失败",
                finished_at=_now(),
                duration_seconds=round(time.time() - started, 3),
                error={"type": type(exc).__name__, "message": str(exc)},
            )
            return
        _update_job(
            server,
            job_id,
            status="success",
            stage="完成",
            finished_at=_now(),
            duration_seconds=round(time.time() - started, 3),
            result=result,
        )

    threading.Thread(target=_worker, name=f"vortex-console-{job_id}", daemon=True).start()
    return {"status": "accepted", "job": _public_job(job, include_result=False)}


def _update_job(server: ControlConsoleServer, job_id: str, **updates: Any) -> None:
    with server.jobs_lock:
        job = server.jobs.get(job_id)
        if job is None:
            return
        job.update(updates)
        job["updated_at"] = _now()


def _jobs_payload(server: ControlConsoleServer) -> dict[str, Any]:
    with server.jobs_lock:
        jobs = [_public_job(job, include_result=False) for job in server.jobs.values()]
    jobs.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    return {"jobs": jobs[:50]}


def _job_payload(server: ControlConsoleServer, job_id: str) -> dict[str, Any]:
    with server.jobs_lock:
        job = server.jobs.get(job_id)
        if job is None:
            return {"error": "not_found", "job_id": job_id}
        return {"job": _public_job(job, include_result=True)}


def _public_job(job: dict[str, Any], *, include_result: bool) -> dict[str, Any]:
    payload = dict(job)
    result = payload.get("result")
    if isinstance(result, dict):
        payload["result_summary"] = _summarize_result(result)
        if not include_result:
            payload.pop("result", None)
    return payload


def _summarize_result(result: dict[str, Any]) -> dict[str, Any]:
    if isinstance(result.get("run"), dict):
        run = dict(result["run"])
        quality = dict(result.get("quality_gate") or {})
        artifacts = dict(result.get("artifacts") or {})
        manifest = artifacts.get("run_manifest") if isinstance(artifacts, dict) else None
        return {
            "run_id": run.get("run_id"),
            "status": run.get("status"),
            "output_dir": run.get("output_dir"),
            "quality_gate": quality.get("status"),
            "promoted_candidate_count": quality.get("promoted_candidate_count"),
            "run_manifest": manifest.get("path") if isinstance(manifest, dict) else None,
        }
    if isinstance(result.get("summary"), dict):
        summary = dict(result["summary"])
        return {
            "status": result.get("status") or summary.get("status"),
            "trade_date": summary.get("trade_date"),
            "strategy_version": summary.get("strategy_version"),
            "holding_count": summary.get("holding_count"),
            "target_portfolio_path": summary.get("target_portfolio_path"),
            "task_path": summary.get("task_path"),
        }
    return {"status": result.get("status", "ok")}


def _status_payload(server: ControlConsoleServer) -> dict[str, Any]:
    root = server.workspace.root
    _load_workspace_env(root)
    provider = _notification_provider_from_env()
    lark_env = {key: _masked(os.environ.get(key, "")) for key in LARK_ENV_KEYS}
    feishu_env = {key: _masked(os.environ.get(key, "")) for key in FEISHU_ENV_KEYS}
    tushare_env = {key: _masked(os.environ.get(key, "")) for key in TUSHARE_ENV_KEYS}
    model_env = {key: _masked(os.environ.get(key, "")) for key in MODEL_ENV_KEYS}
    lark_missing = [
        key
        for key in ("LARK_APP_ID", "LARK_APP_SECRET", "LARK_DEFAULT_RECEIVE_ID")
        if not os.environ.get(key, "").strip()
    ]
    feishu_missing = [
        key
        for key in ("FEISHU_APP_ID", "FEISHU_APP_SECRET", "FEISHU_DEFAULT_RECEIVE_ID")
        if not os.environ.get(key, "").strip()
    ]
    qmt_config = _qmt_config_from_sources(root)
    with server.runtime_state_lock:
        qmt_config["health"] = server.runtime_state.get("qmt_health")
    recent_runs = _list_run_manifests(root)
    strategy_tasks = _list_strategy_tasks(root)
    strategies = _strategy_catalog(root)
    trading_config = _trading_config_summary(root, strategies, qmt_config)
    active_strategy = _active_strategy_summary(root, strategy_tasks, qmt_config.get("health"))
    data_service = _data_service_summary(root)
    active_jobs = [
        job
        for job in _jobs_payload(server)["jobs"]
        if job.get("status") in {"queued", "running"}
    ]
    return {
        "workspace": str(root),
        "env_file": str(root / ".env"),
        "notification_provider": provider,
        "lark": {
            "configured": not lark_missing,
            "missing": lark_missing,
            "api_base": os.environ.get("LARK_API_BASE", LARK_API_BASE),
            "env": lark_env,
        },
        "feishu_legacy": {
            "configured": not feishu_missing,
            "missing": feishu_missing,
            "env": feishu_env,
        },
        "qmt": qmt_config,
        "tushare": {
            "configured": bool(os.environ.get("TUSHARE_TOKEN", "").strip()),
            "permissions": os.environ.get("TUSHARE_EXTRA_PERMISSIONS", "").strip(),
            "points": os.environ.get("TUSHARE_POINTS", "").strip(),
            "env": tushare_env,
        },
        "models": {
            "configured": any(os.environ.get(key, "").strip() for key in MODEL_ENV_KEYS),
            "env": model_env,
        },
        "actions": {
            "can_test_lark": not lark_missing,
            "can_run_demo_research": True,
            "strategy_prepare_requires": [
                "start",
                "as_of",
                "qmt_bridge_url",
            ],
        },
        "trading_config": trading_config,
        "overview": {
            "active_job_count": len(active_jobs),
            "recent_research_run_count": len(recent_runs),
            "pending_strategy_task_count": len(strategy_tasks),
            "strategy_count": len(strategies),
        },
        "active_jobs": active_jobs,
        "recent_runs": recent_runs[:5],
        "recent_strategy_tasks": strategy_tasks[:5],
        "strategies": strategies,
        "active_strategy": active_strategy,
        "data_service": data_service,
        "daily_trade_review": _daily_trade_review_summary(strategy_tasks, qmt_config, strategies, trading_config),
    }


def _save_lark_config(root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    app_id = _required_text(payload, "app_id")
    app_secret = _required_text(payload, "app_secret")
    receive_id = _required_text(payload, "default_receive_id")
    receive_id_type = str(payload.get("default_receive_id_type") or "open_id").strip()
    api_base = str(payload.get("api_base") or LARK_API_BASE).strip()
    if receive_id_type not in {"open_id", "user_id", "union_id", "email", "chat_id"}:
        raise ValueError("default_receive_id_type 必须是 open_id/user_id/union_id/email/chat_id")
    values = {
        "VORTEX_NOTIFICATION_PROVIDER": "lark",
        "LARK_APP_ID": app_id,
        "LARK_APP_SECRET": app_secret,
        "LARK_DEFAULT_RECEIVE_ID": receive_id,
        "LARK_DEFAULT_RECEIVE_ID_TYPE": receive_id_type,
        "LARK_API_BASE": api_base,
    }
    _merge_env_file(root / ".env", values)
    os.environ.update(values)
    return {
        "status": "saved",
        "env_file": str(root / ".env"),
        "lark": {
            "configured": True,
            "missing": [],
            "api_base": api_base,
            "env": {key: _masked(os.environ.get(key, "")) for key in LARK_ENV_KEYS},
        },
    }


def _save_notification_provider(root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    provider = str(payload.get("provider") or "lark").strip().lower()
    if provider not in {"lark", "feishu"}:
        raise ValueError("provider 必须是 lark 或 feishu")
    values = {"VORTEX_NOTIFICATION_PROVIDER": provider}
    _merge_env_file(root / ".env", values)
    os.environ.update(values)
    return {"status": "saved", "provider": provider, "env_file": str(root / ".env")}


def _save_qmt_config(root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    bridge_url = _required_text(payload, "qmt_bridge_url")
    values = {
        "QMT_BRIDGE_URL": bridge_url,
        "QMT_BRIDGE_BASE_URL": bridge_url,
    }
    token = _optional_text(payload.get("qmt_bridge_token")) or _optional_text(payload.get("qmt_bridge_api_key"))
    account_id = _optional_text(payload.get("qmt_account_id"))
    if token is not None:
        values["QMT_BRIDGE_TOKEN"] = token
        values["QMT_BRIDGE_API_KEY"] = token
    if account_id is not None:
        values["QMT_ACCOUNT_ID"] = account_id
        values["QMT_BRIDGE_TRADING_ACCOUNT_ID"] = account_id
    _merge_env_file(root / ".env", values)
    os.environ.update(values)
    return {
        "status": "saved",
        "env_file": str(root / ".env"),
        "qmt": {
            "configured": True,
            "missing": [],
            "bridge_url": bridge_url,
            "account_id": os.environ.get("QMT_ACCOUNT_ID", "").strip(),
            "token_configured": bool(_qmt_token_from_env()),
            "source": "workspace_env",
            "persisted": True,
            "env": {key: _masked(os.environ.get(key, "")) for key in QMT_ENV_KEYS},
        },
    }


def _qmt_config_from_sources(root: Path) -> dict[str, Any]:
    env_url = _qmt_bridge_url_from_env()
    env_account = _qmt_account_from_env()
    bridge_url = env_url
    account_id = env_account
    source = "workspace_env" if env_url or env_account else ""

    if not bridge_url or not account_id:
        discovered = _discover_qmt_config_from_workspace(root)
        if not bridge_url and discovered.get("bridge_url"):
            bridge_url = str(discovered["bridge_url"])
            source = str(discovered.get("source") or source)
        if not account_id and discovered.get("account_id"):
            account_id = str(discovered["account_id"])
            source = str(discovered.get("source") or source)

    missing = []
    if not bridge_url:
        missing.append("QMT_BRIDGE_URL")
    if not account_id:
        missing.append("QMT_ACCOUNT_ID")
    return {
        "configured": not missing,
        "missing": missing,
        "bridge_url": bridge_url,
        "account_id": account_id,
        "token_configured": bool(_qmt_token_from_env()),
        "source": source or "not_found",
        "persisted": bool(env_url and env_account),
        "env": {key: _masked(os.environ.get(key, "")) for key in QMT_ENV_KEYS},
    }


def _active_strategy_id(strategies: list[dict[str, Any]]) -> str:
    configured = os.environ.get("VORTEX_ACTIVE_STRATEGY_ID", "").strip()
    valid_ids = {str(strategy.get("strategy_id") or "") for strategy in strategies}
    if configured and configured in valid_ids:
        return configured
    return str(strategies[0].get("strategy_id") or "") if strategies else ""


def _trading_config_summary(
    root: Path,
    strategies: list[dict[str, Any]],
    qmt_config: dict[str, Any],
) -> dict[str, Any]:
    active_strategy_id = _active_strategy_id(strategies)
    active_strategy = next(
        (strategy for strategy in strategies if strategy.get("strategy_id") == active_strategy_id),
        {},
    )
    return {
        "single_account_mode": True,
        "active_strategy_id": active_strategy_id,
        "active_strategy_name": active_strategy.get("name") or "-",
        "available_strategy_count": len(strategies),
        "account_id": qmt_config.get("account_id") or "",
        "source": "VORTEX_ACTIVE_STRATEGY_ID" if os.environ.get("VORTEX_ACTIVE_STRATEGY_ID", "").strip() else "default_first_strategy",
        "env": {"VORTEX_ACTIVE_STRATEGY_ID": _masked(os.environ.get("VORTEX_ACTIVE_STRATEGY_ID", ""))},
    }


def _save_trading_config(root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    strategies = _strategy_catalog(root)
    strategy_id = _required_text(payload, "active_strategy_id")
    valid_ids = {str(strategy.get("strategy_id") or "") for strategy in strategies}
    if strategy_id not in valid_ids:
        raise ValueError(f"未知策略：{strategy_id}")
    values = {"VORTEX_ACTIVE_STRATEGY_ID": strategy_id}
    _merge_env_file(root / ".env", values)
    os.environ.update(values)
    qmt_config = _qmt_config_from_sources(root)
    return {
        "status": "saved",
        "env_file": str(root / ".env"),
        "trading_config": _trading_config_summary(root, strategies, qmt_config),
    }


def _discover_qmt_config_from_workspace(root: Path) -> dict[str, Any]:
    candidates: list[Path] = []
    candidates.extend((root / "strategy").glob("*.json"))
    candidates.extend((root / "state" / "trade" / "pending_qmt").glob("*.json"))
    candidates = [path for path in candidates if path.is_file()]
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)

    bridge_url: str | None = None
    account_id: str | None = None
    source: str | None = None
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        if bridge_url is None:
            bridge_url = _optional_text(payload.get("qmt_bridge_url")) or _optional_text(
                payload.get("bridge_url")
            )
            if bridge_url:
                source = str(path)
        if account_id is None:
            account_id = _optional_text(payload.get("qmt_account_id")) or _optional_text(
                payload.get("account_id")
            )
            if account_id and source is None:
                source = str(path)
        if bridge_url and account_id:
            break
    return {
        "bridge_url": bridge_url,
        "account_id": account_id,
        "source": source,
    }


def _run_qmt_health_check(server: ControlConsoleServer) -> dict[str, Any]:
    root = server.workspace.root
    _load_workspace_env(root)
    qmt_config = _qmt_config_from_sources(root)
    qmt_config["_workspace_root"] = str(root)
    result = _probe_qmt_bridge(qmt_config)
    with server.runtime_state_lock:
        server.runtime_state["qmt_health"] = result
    return result


def _probe_qmt_bridge(qmt_config: dict[str, Any]) -> dict[str, Any]:
    from vortex.trade import QmtBridgeAdapter, QmtBridgeConfig, is_known_connection_status_bug

    checked_at = _now()
    bridge_url = _optional_text(qmt_config.get("bridge_url"))
    account_id = _optional_text(qmt_config.get("account_id"))
    token = _qmt_token_from_env()
    result: dict[str, Any] = {
        "checked_at": checked_at,
        "status": "failed",
        "ok": False,
        "bridge_url": bridge_url or "",
        "account_id": account_id or "",
        "token_configured": bool(token),
        "blocking_reason": "",
        "health": None,
        "connection_status": None,
        "cash": None,
        "positions": [],
        "position_count": None,
        "order_count": None,
        "fill_count": None,
    }
    if not bridge_url:
        result["blocking_reason"] = "缺少 QMT Bridge URL"
        return result

    adapter = QmtBridgeAdapter(
        QmtBridgeConfig(
            base_url=bridge_url,
            token=token,
            account_id=account_id,
            allow_trading=False,
        )
    )
    health = adapter.health()
    result["health"] = {"ok": health.ok, "message": health.message}
    if not health.ok:
        result["blocking_reason"] = f"bridge health failed: {health.message}"
        return result
    try:
        connection = adapter.connection_status()
        cash = adapter.get_cash()
        positions = adapter.get_positions()
        orders = adapter.get_orders()
        fills = adapter.get_fills()
    except Exception as exc:  # noqa: BLE001 - surfaced to local control console.
        result["blocking_reason"] = f"bridge read failed: {exc}"
        return result

    root_text = _optional_text(qmt_config.get("_workspace_root"))
    name_map = _stock_name_lookup(Path(root_text), [item.symbol for item in positions]) if root_text else {}
    position_rows = sorted(
        [
            {
                "symbol": item.symbol,
                "name": name_map.get(item.symbol) or "",
                "shares": item.shares,
                "available_shares": item.available_shares,
                "cost_price": item.cost_price,
                "last_price": item.last_price,
                "market_value": item.shares * (item.last_price or item.cost_price),
            }
            for item in positions
        ],
        key=lambda item: float(item.get("market_value") or 0.0),
        reverse=True,
    )
    result.update(
        {
            "status": "success",
            "ok": True,
            "blocking_reason": "-",
            "connection_status": connection,
            "cash": {
                "available_cash": cash.available_cash,
                "total_asset": cash.total_asset,
                "market_value": cash.market_value,
                "frozen_cash": cash.frozen_cash,
            },
            "positions": position_rows[:50],
            "position_count": len(positions),
            "order_count": len(orders),
            "fill_count": len(fills),
        }
    )
    if isinstance(connection, dict) and connection.get("connected") is False:
        if is_known_connection_status_bug(connection):
            result["connection_warning"] = (
                "connection_status 使用了已知不兼容接口；资产/持仓/委托/成交可读时不阻断。"
            )
        else:
            result["status"] = "failed"
            result["ok"] = False
            result["blocking_reason"] = f"bridge connected=false: {connection}"
    return result


def _strategy_catalog(root: Path) -> list[dict[str, Any]]:
    tasks = _list_strategy_tasks(root, limit=200)
    earnings_task_count = sum(
        1
        for task in tasks
        if str(task.get("strategy_version") or "").startswith("baseline")
        or str(task.get("task_type") or "") == "earnings_forecast_qmt_rebalance"
    )
    return [
        {
            "strategy_id": "earnings_forecast_drift",
            "name": "业绩预告漂移策略",
            "status": "paper_ready",
            "mode": "shadow / paper / gated_live",
            "skill": "earnings-forecast-live-handoff",
            "live_entry": "vortex strategy earnings-forecast auto-once",
            "task_count": earnings_task_count,
            "review_skill": "live-trading-review",
            "description": "当前主实盘候选；通过 QMT Bridge 生成交接包和 pending task，默认禁用交易。",
        },
    ]


def _daily_trade_review_summary(
    strategy_tasks: list[dict[str, Any]],
    qmt_config: dict[str, Any],
    strategies: list[dict[str, Any]],
    trading_config: dict[str, Any],
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    for task in strategy_tasks:
        status = str(task.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
    latest_task = strategy_tasks[0] if strategy_tasks else None
    qmt_health = qmt_config.get("health") if isinstance(qmt_config.get("health"), dict) else None
    qmt_ok = bool(qmt_health.get("ok")) if qmt_health else None
    if qmt_ok is False:
        state = "blocked"
    elif qmt_ok is True:
        state = "ready"
    elif qmt_config.get("configured"):
        state = "needs_probe"
    else:
        state = "missing_qmt"
    return {
        "state": state,
        "strategy_count": len(strategies),
        "selected_strategy_id": trading_config.get("active_strategy_id") or _active_strategy_id(strategies),
        "latest_task": latest_task,
        "status_counts": status_counts,
        "qmt_ok": qmt_ok,
        "qmt_checked_at": qmt_health.get("checked_at") if qmt_health else None,
        "attention": _daily_trade_attention(state, latest_task, qmt_health),
    }


def _daily_trade_attention(
    state: str,
    latest_task: dict[str, Any] | None,
    qmt_health: dict[str, Any] | None,
) -> list[str]:
    items: list[str] = []
    if state == "missing_qmt":
        items.append("QMT Bridge 未配置，无法进行交易链路探测。")
    elif state == "needs_probe":
        items.append("QMT 已检测到配置，但本轮控制台启动后还没有做只读健康检查。")
    elif state == "blocked":
        items.append(str(qmt_health.get("blocking_reason") or "QMT 健康检查失败。") if qmt_health else "QMT 健康检查失败。")
    if latest_task and latest_task.get("status") == "pending":
        items.append(f"存在待处理策略任务：{latest_task.get('trade_date') or latest_task.get('as_of') or '-'}。")
    if not items:
        items.append("没有阻断项；实盘动作仍需要人工确认。")
    return items


def _active_strategy_summary(
    root: Path,
    strategy_tasks: list[dict[str, Any]],
    qmt_health: dict[str, Any] | None = None,
) -> dict[str, Any]:
    status_path = root / "state" / "strategy" / "earnings_forecast_auto" / AUTO_RUN_STATUS_FILE
    service_status = _read_json_file(status_path)
    latest_task = _latest_strategy_task_payload(strategy_tasks)
    target_summary = _target_portfolio_summary(root, latest_task)
    diagnostics = latest_task.get("target_diagnostics") if isinstance(latest_task.get("target_diagnostics"), dict) else {}
    quality = latest_task.get("quality_summary") if isinstance(latest_task.get("quality_summary"), dict) else {}
    execution = _latest_execution_summary(root, latest_task)
    nav = _strategy_nav_summary(root)
    return {
        "strategy_id": "earnings_forecast_drift",
        "name": "业绩预告漂移策略",
        "active": True,
        "live_entry": "vortex strategy earnings-forecast auto-once",
        "review_skill": "live-trading-review",
        "description": "当前主实盘候选；通过 QMT Bridge 生成交接包和 pending task，默认禁用交易。",
        "current": {
            "strategy_name": "earnings_forecast_auto",
            "strategy_version": latest_task.get("strategy_version")
            or dict(service_status.get("config") or {}).get("preset_name")
            or DEFAULT_AUTO_PRESET,
            "mode": "shadow / paper / gated_live",
            "status": latest_task.get("status") or "-",
            "trade_date": latest_task.get("trade_date") or "-",
            "as_of": latest_task.get("as_of") or "-",
            "task_path": latest_task.get("task_path") or "",
            "updated_at": latest_task.get("updated_at")
            or _mtime_iso(_optional_text(latest_task.get("task_path"))),
        },
        "service": _auto_service_summary(service_status, status_path),
        "selection_funnel": _selection_funnel_steps(diagnostics),
        "data_freshness": _data_freshness_summary(diagnostics),
        "target": target_summary,
        "rebalance": _planned_rebalance_summary(root, target_summary),
        "live_rebalance": _live_rebalance_summary(target_summary, qmt_health),
        "execution": execution,
        "nav": nav,
        "quality": {
            "holding_count": quality.get("holding_count"),
            "label_counts": quality.get("label_counts") or {},
            "blocked_symbols": quality.get("blocked_symbols") or [],
            "review_symbols": quality.get("review_symbols") or [],
            "watch_symbols": quality.get("watch_symbols") or [],
        },
        "workflow": _strategy_workflow_steps(latest_task, diagnostics, execution, service_status),
    }


def _latest_strategy_task_payload(strategy_tasks: list[dict[str, Any]]) -> dict[str, Any]:
    if not strategy_tasks:
        return {}
    path = _optional_text(strategy_tasks[0].get("task_path"))
    if not path:
        return dict(strategy_tasks[0])
    payload = _read_json_file(Path(path))
    payload.setdefault("task_path", path)
    payload.setdefault("_task_path", path)
    return payload


def _auto_service_summary(payload: dict[str, Any], status_path: Path) -> dict[str, Any]:
    config = payload.get("config") if isinstance(payload.get("config"), dict) else {}
    last_tick = payload.get("last_tick") if isinstance(payload.get("last_tick"), dict) else {}
    return {
        "status_path": str(status_path) if status_path.exists() else "",
        "service_status": payload.get("service_status") or "unknown",
        "loop_mode": payload.get("loop_mode") or "",
        "pid": payload.get("pid"),
        "started_at": payload.get("started_at"),
        "updated_at": payload.get("updated_at"),
        "last_tick_status": payload.get("last_tick_status"),
        "last_tick_at": payload.get("last_tick_at"),
        "last_error": payload.get("last_error"),
        "prepare_time": config.get("prepare_time") or DEFAULT_AUTO_PREPARE_TIME,
        "execute_time": config.get("execute_time") or DEFAULT_AUTO_EXECUTE_TIME,
        "allow_trading": bool(config.get("allow_trading")),
        "last_skipped": last_tick.get("skipped") or [],
    }


def _selection_funnel_steps(diagnostics: dict[str, Any]) -> list[dict[str, Any]]:
    funnel = diagnostics.get("selection_funnel") if isinstance(diagnostics.get("selection_funnel"), dict) else {}
    labels = [
        ("raw_signal_count", "原始信号"),
        ("positive_signal_count", "正向信号"),
        ("after_liquidity_count", "流动性过滤后"),
        ("after_st_filter_count", "ST/风险过滤后"),
        ("after_market_cap_top50_count", "市值门控后"),
        ("after_open_block_count", "开盘可交易过滤后"),
        ("after_quality_block_count", "持仓质量审查后"),
        ("after_permission_count", "市场权限过滤后"),
        ("executable_candidate_count", "可执行候选"),
        ("selected_position_count", "最终目标持仓"),
    ]
    rows: list[dict[str, Any]] = []
    previous: int | None = None
    for key, label in labels:
        value = funnel.get(key)
        count = int(value) if isinstance(value, (int, float)) else None
        removed = previous - count if previous is not None and count is not None else None
        rows.append({"key": key, "name": label, "count": count, "removed": removed})
        if count is not None:
            previous = count
    return rows


def _data_freshness_summary(diagnostics: dict[str, Any]) -> dict[str, Any]:
    freshness = diagnostics.get("data_freshness") if isinstance(diagnostics.get("data_freshness"), dict) else {}
    datasets = freshness.get("datasets") if isinstance(freshness.get("datasets"), dict) else {}
    return {
        "status": freshness.get("status") or "unknown",
        "required_as_of": freshness.get("required_as_of"),
        "datasets": [
            {
                "name": name,
                "max_date": info.get("max_date") if isinstance(info, dict) else None,
                "required_as_of": info.get("required_as_of") if isinstance(info, dict) else None,
                "ok": bool(info.get("ok")) if isinstance(info, dict) else False,
            }
            for name, info in datasets.items()
        ],
        "skipped_counts": diagnostics.get("skipped_counts") if isinstance(diagnostics.get("skipped_counts"), dict) else {},
        "market_gate": diagnostics.get("market_gate") if isinstance(diagnostics.get("market_gate"), dict) else {},
    }


def _stock_name_lookup(root: Path, symbols: list[str]) -> dict[str, str]:
    requested = {str(symbol) for symbol in symbols if symbol}
    if not requested:
        return {}
    cache_key = str((root / "data").resolve())
    if cache_key not in _STOCK_NAME_CACHE:
        _STOCK_NAME_CACHE[cache_key] = _load_stock_name_cache(root)
    stock_names = _STOCK_NAME_CACHE.get(cache_key, {})
    return {symbol: stock_names[symbol] for symbol in requested if symbol in stock_names}


def _load_stock_name_cache(root: Path) -> dict[str, str]:
    try:
        from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend

        backend = ParquetDuckDBBackend(root / "data")
        names: dict[str, str] = {}
        for dataset in ("instruments", "stock_basic", "stock_company"):
            frame = backend.read(dataset)
            if frame.empty:
                continue
            symbol_col = _first_existing_column(frame.columns, ["symbol", "ts_code", "code"])
            name_col = _first_existing_column(frame.columns, ["name", "stock_name", "short_name", "com_name"])
            if not symbol_col or not name_col:
                continue
            for row in frame[[symbol_col, name_col]].dropna().itertuples(index=False):
                symbol = str(row[0]).strip()
                name = _short_stock_name(str(row[1]).strip())
                if symbol and name:
                    names[symbol] = name
            if names:
                return names
    except Exception:  # noqa: BLE001 - stock names are presentation enrichment only.
        return {}
    return {}


def _first_existing_column(columns: Any, candidates: list[str]) -> str | None:
    available = {str(column) for column in columns}
    for candidate in candidates:
        if candidate in available:
            return candidate
    return None


def _short_stock_name(name: str) -> str:
    cleaned = re.sub(r"(股份有限公司|有限责任公司|有限公司)$", "", name)
    return cleaned or name


def _target_portfolio_summary(root: Path, task_payload: dict[str, Any]) -> dict[str, Any]:
    path = _optional_text(task_payload.get("target_portfolio_path"))
    if not path:
        return {"path": "", "position_count": 0, "positions": []}
    payload = _read_json_file(Path(path))
    positions = [item for item in payload.get("positions") or [] if isinstance(item, dict)]
    gross_value = sum(float(item.get("target_value") or 0.0) for item in positions)
    name_map = _stock_name_lookup(root, [str(item.get("symbol")) for item in positions if item.get("symbol")])
    position_rows = sorted(
        [
            {
                "symbol": item.get("symbol"),
                "name": name_map.get(str(item.get("symbol"))) or "",
                "target_weight": item.get("target_weight"),
                "target_value": item.get("target_value"),
                "target_shares": item.get("target_shares"),
                "reference_price": item.get("reference_price"),
                "reason": item.get("reason"),
            }
            for item in positions
        ],
        key=lambda item: float(item.get("target_value") or 0.0),
        reverse=True,
    )
    return {
        "path": path,
        "portfolio_id": payload.get("portfolio_id"),
        "trade_date": payload.get("trade_date"),
        "strategy_version": payload.get("strategy_version"),
        "run_id": payload.get("run_id"),
        "updated_at": _mtime_iso(path),
        "cash_target": payload.get("cash_target"),
        "gross_target_value": gross_value,
        "position_count": len(positions),
        "positions": position_rows[:20],
        "all_positions": position_rows,
    }


def _planned_rebalance_summary(root: Path, target: dict[str, Any]) -> dict[str, Any]:
    current_path = _optional_text(target.get("path"))
    current_trade_date = _optional_text(target.get("trade_date"))
    if not current_path or not current_trade_date:
        return {"status": "missing_target", "orders": []}
    previous = _previous_target_payload(root, current_trade_date)
    current_positions = {
        str(item.get("symbol")): item
        for item in target.get("all_positions") or target.get("positions") or []
        if isinstance(item, dict) and item.get("symbol")
    }
    previous_positions = {
        str(item.get("symbol")): item
        for item in previous.get("positions") or []
        if isinstance(item, dict) and item.get("symbol")
    }
    name_map = _stock_name_lookup(root, sorted(set(current_positions) | set(previous_positions)))
    orders: list[dict[str, Any]] = []
    for symbol in sorted(set(current_positions) | set(previous_positions)):
        current = current_positions.get(symbol, {})
        prior = previous_positions.get(symbol, {})
        current_shares = int(current.get("target_shares") or 0)
        prior_shares = int(prior.get("target_shares") or 0)
        diff = current_shares - prior_shares
        if diff == 0:
            continue
        side = "buy" if diff > 0 else "sell"
        price = float((current or prior).get("reference_price") or 0.0)
        orders.append(
            {
                "symbol": symbol,
                "name": name_map.get(symbol) or str((current or prior).get("name") or ""),
                "side": side,
                "shares": abs(diff),
                "prior_shares": prior_shares,
                "target_shares": current_shares,
                "reference_price": price,
                "estimated_value": abs(diff) * price,
            }
        )
    return {
        "status": "ready" if previous else "no_previous_target",
        "previous_trade_date": previous.get("trade_date"),
        "order_count": len(orders),
        "buy_count": sum(1 for item in orders if item["side"] == "buy"),
        "sell_count": sum(1 for item in orders if item["side"] == "sell"),
        "estimated_buy_value": sum(float(item["estimated_value"]) for item in orders if item["side"] == "buy"),
        "estimated_sell_value": sum(float(item["estimated_value"]) for item in orders if item["side"] == "sell"),
        "orders": sorted(orders, key=lambda item: float(item["estimated_value"]), reverse=True)[:20],
    }


def _live_rebalance_summary(target: dict[str, Any], qmt_health: dict[str, Any] | None) -> dict[str, Any]:
    target_rows = [
        item
        for item in target.get("all_positions") or target.get("positions") or []
        if isinstance(item, dict) and item.get("symbol")
    ]
    if not target_rows:
        return {"status": "missing_target", "orders": []}
    if not isinstance(qmt_health, dict) or not qmt_health.get("ok"):
        return {
            "status": "needs_qmt_health",
            "orders": [],
            "explain": "需要先做 QMT 只读检查，才能按真实持仓计算待执行差异。",
        }
    qmt_rows = [
        item
        for item in qmt_health.get("positions") or []
        if isinstance(item, dict) and item.get("symbol")
    ]
    target_by_symbol = {str(item["symbol"]): item for item in target_rows}
    current_by_symbol = {str(item["symbol"]): item for item in qmt_rows}
    orders: list[dict[str, Any]] = []
    for symbol in sorted(set(target_by_symbol) | set(current_by_symbol)):
        target_item = target_by_symbol.get(symbol, {})
        current_item = current_by_symbol.get(symbol, {})
        target_shares = int(target_item.get("target_shares") or 0)
        current_shares = int(current_item.get("shares") or 0)
        diff = target_shares - current_shares
        if diff == 0:
            continue
        side = "buy" if diff > 0 else "sell"
        price = float(
            target_item.get("reference_price")
            or current_item.get("last_price")
            or current_item.get("cost_price")
            or 0.0
        )
        orders.append(
            {
                "symbol": symbol,
                "name": str(target_item.get("name") or current_item.get("name") or ""),
                "side": side,
                "shares": abs(diff),
                "current_shares": current_shares,
                "target_shares": target_shares,
                "reference_price": price,
                "estimated_value": abs(diff) * price,
                "source": "qmt_realtime_vs_target",
            }
        )
    return {
        "status": "ready",
        "source": "qmt_realtime_vs_target",
        "target_trade_date": target.get("trade_date"),
        "checked_at": qmt_health.get("checked_at"),
        "order_count": len(orders),
        "buy_count": sum(1 for item in orders if item["side"] == "buy"),
        "sell_count": sum(1 for item in orders if item["side"] == "sell"),
        "estimated_buy_value": sum(float(item["estimated_value"]) for item in orders if item["side"] == "buy"),
        "estimated_sell_value": sum(float(item["estimated_value"]) for item in orders if item["side"] == "sell"),
        "orders": sorted(orders, key=lambda item: float(item["estimated_value"]), reverse=True)[:50],
    }


def _previous_target_payload(root: Path, current_trade_date: str) -> dict[str, Any]:
    candidates: list[tuple[str, Path]] = []
    for path in (root / "trade" / "targets").glob("*/*.json"):
        payload = _read_json_file(path)
        trade_date = _optional_text(payload.get("trade_date"))
        if trade_date and trade_date < current_trade_date:
            candidates.append((trade_date, path))
    if not candidates:
        return {}
    candidates.sort(key=lambda item: item[0], reverse=True)
    return _read_json_file(candidates[0][1])


def _latest_execution_summary(root: Path, task_payload: dict[str, Any]) -> dict[str, Any]:
    path = _optional_text(task_payload.get("execution_report_path"))
    report_path = Path(path) if path else _latest_file(root / "trade" / "executions", "execution_report.json")
    if not report_path or not report_path.exists():
        return {"status": "missing", "path": ""}
    report = _read_json_file(report_path)
    order_plan = report.get("order_plan") if isinstance(report.get("order_plan"), dict) else {}
    orders = order_plan.get("orders") if isinstance(order_plan.get("orders"), list) else []
    fills = report.get("fills") if isinstance(report.get("fills"), list) else []
    risk = report.get("risk_result") if isinstance(report.get("risk_result"), dict) else {}
    name_map = _stock_name_lookup(
        root,
        [str(item.get("symbol")) for item in orders if isinstance(item, dict) and item.get("symbol")],
    )
    order_rows = [
        {
            "symbol": item.get("symbol"),
            "name": name_map.get(str(item.get("symbol"))) or "",
            "side": item.get("side"),
            "shares": item.get("shares"),
            "limit_price": item.get("limit_price"),
            "estimated_value": float(item.get("shares") or 0) * float(item.get("limit_price") or 0.0),
            "reason": item.get("reason"),
        }
        for item in orders
        if isinstance(item, dict)
    ]
    return {
        "status": "available",
        "path": str(report_path),
        "exec_id": report.get("exec_id"),
        "trade_date": report.get("trade_date"),
        "mode": report.get("mode"),
        "risk_passed": risk.get("passed"),
        "blocking_reasons": risk.get("blocking_reasons") or [],
        "order_count": len(orders),
        "buy_count": sum(1 for item in orders if isinstance(item, dict) and item.get("side") == "buy"),
        "sell_count": sum(1 for item in orders if isinstance(item, dict) and item.get("side") == "sell"),
        "fill_count": len(fills),
        "unfilled_summary": report.get("unfilled_summary") or {},
        "cash": report.get("cash") or {},
        "orders": sorted(order_rows, key=lambda item: float(item.get("estimated_value") or 0.0), reverse=True)[:20],
    }


def _strategy_nav_summary(root: Path) -> dict[str, Any]:
    binding_path = _latest_file(root / "state" / "nav", "*.json")
    if not binding_path:
        return {"status": "missing"}
    binding = _read_json_file(binding_path)
    run_id = _optional_text(binding.get("run_id"))
    ledger_path = root / "trade" / "nav" / f"{run_id}.csv" if run_id else None
    rows = _read_csv_rows(ledger_path) if ledger_path else []
    if not rows:
        return {"status": "missing_ledger", "binding": binding, "ledger_path": str(ledger_path or "")}
    latest = rows[-1]
    net_values = [float(row.get("net_value") or 0.0) for row in rows if row.get("net_value")]
    has_return_window = len(net_values) >= 2
    max_drawdown = _max_drawdown(net_values) if has_return_window else None
    return {
        "status": "available",
        "binding": binding,
        "ledger_path": str(ledger_path),
        "snapshot_count": len(rows),
        "has_return_window": has_return_window,
        "metric_notes": {
            "daily_return": "净值快照少于 2 条，日收益只是初始化值。" if not has_return_window else "",
            "max_drawdown": "净值快照少于 2 条，最大回撤不可判定。" if not has_return_window else "",
        },
        "latest": latest,
        "latest_net_value": _float_or_none(latest.get("net_value")),
        "latest_total_asset": _float_or_none(latest.get("total_asset")),
        "daily_return": _float_or_none(latest.get("daily_return")) if has_return_window else None,
        "benchmark_return": _float_or_none(latest.get("benchmark_return")),
        "excess_return": _float_or_none(latest.get("excess_return")),
        "max_drawdown": max_drawdown,
    }


def _strategy_workflow_steps(
    latest_task: dict[str, Any],
    diagnostics: dict[str, Any],
    execution: dict[str, Any],
    service_status: dict[str, Any],
) -> list[dict[str, Any]]:
    freshness = _data_freshness_summary(diagnostics)
    return [
        {
            "name": "数据新鲜度门禁",
            "status": freshness.get("status") or "unknown",
            "detail": f"required_as_of={freshness.get('required_as_of') or '-'}",
        },
        {
            "name": "信号与候选筛选",
            "status": "ok" if latest_task.get("target_diagnostics") else "missing",
            "detail": f"最终持仓 {diagnostics.get('final_position_count') or '-'} 只",
        },
        {
            "name": "持仓质量审查",
            "status": "ok" if latest_task.get("quality_summary") else "missing",
            "detail": f"待复核 {len((latest_task.get('quality_summary') or {}).get('review_symbols') or [])} 只",
        },
        {
            "name": "目标组合冻结",
            "status": latest_task.get("status") or "unknown",
            "detail": latest_task.get("target_portfolio_path") or "-",
        },
        {
            "name": "QMT 交接 / 执行",
            "status": execution.get("status") or "missing",
            "detail": f"订单 {execution.get('order_count') or 0}；成交 {execution.get('fill_count') or 0}",
        },
        {
            "name": "净值快照",
            "status": "ok" if execution.get("risk_passed") else "waiting",
            "detail": str((service_status.get("last_tick") or {}).get("nav_snapshot") or "等待执行后记录"),
        },
    ]


def _data_service_summary(root: Path) -> dict[str, Any]:
    payload = _read_json_file(root / "state" / "live-service-health-latest.json")
    data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
    active_tasks = data.get("active_tasks") if isinstance(data.get("active_tasks"), list) else []
    scheduled_profiles = payload.get("scheduled_profiles") if isinstance(payload.get("scheduled_profiles"), list) else []
    log_path = _data_update_log_path(root, active_tasks)
    progress = _parse_data_update_log(log_path) if log_path else {}
    latest_run = dict(data.get("latest_run") or {})
    if progress.get("status") in {"success", "failed"} and (
        not latest_run.get("run_id") or latest_run.get("run_id") == progress.get("run_id")
    ):
        latest_run.update(
            {
                "run_id": progress.get("run_id") or latest_run.get("run_id"),
                "status": progress.get("status"),
                "finished_at": progress.get("finished_at") or latest_run.get("finished_at"),
                "total_rows": progress.get("total_rows") or latest_run.get("total_rows"),
            }
        )
        active_tasks = [
            task
            for task in active_tasks
            if not (isinstance(task, dict) and task.get("run_id") == progress.get("run_id"))
        ]
    return {
        "checked_at": payload.get("checked_at"),
        "status": payload.get("status") or "unknown",
        "alerts": payload.get("alerts") or [],
        "scheduled_profiles": [
            {
                **profile,
                "label": _cron_label(str(profile.get("schedule") or "")),
            }
            for profile in scheduled_profiles
            if isinstance(profile, dict)
        ],
        "latest_run": latest_run,
        "latest_success_update": data.get("latest_success_update") or {},
        "latest_snapshot": data.get("latest_snapshot") or {},
        "progress": progress,
        "active_tasks": active_tasks,
        "active_task_count": len(active_tasks),
    }


def _data_update_log_path(root: Path, active_tasks: list[Any]) -> Path | None:
    for task in active_tasks:
        if not isinstance(task, dict):
            continue
        log_path = _optional_text(task.get("log_path"))
        if log_path and Path(log_path).exists():
            return Path(log_path)
    latest = _latest_file(root / "state" / "logs", "data-update-*.log")
    return latest if latest and latest.exists() else None


def _parse_data_update_log(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return {}
    dataset_index: int | None = None
    dataset_total: int | None = None
    current_dataset: str | None = None
    sub_current: int | None = None
    sub_total: int | None = None
    sub_message = ""
    completed: list[dict[str, Any]] = []
    last_timestamp = ""
    start_re = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*开始同步 dataset=([A-Za-z0-9_]+) \((\d+)/(\d+)\)")
    progress_re = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*: ([A-Za-z0-9_]+)(?: [^:]+)?: (\d+)/(\d+)")
    done_re = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*dataset=([A-Za-z0-9_]+) 完成: .*rows=(\d+)")
    for line in text.splitlines():
        start_match = start_re.search(line)
        if start_match:
            last_timestamp = start_match.group(1)
            current_dataset = start_match.group(2)
            dataset_index = int(start_match.group(3))
            dataset_total = int(start_match.group(4))
            sub_current = None
            sub_total = None
            sub_message = ""
            continue
        progress_match = progress_re.search(line)
        if progress_match:
            last_timestamp = progress_match.group(1)
            dataset = progress_match.group(2)
            if current_dataset is None or dataset == current_dataset:
                current_dataset = dataset
                sub_current = int(progress_match.group(3))
                sub_total = int(progress_match.group(4))
                sub_message = line.split(": ", 1)[-1]
            continue
        done_match = done_re.search(line)
        if done_match:
            last_timestamp = done_match.group(1)
            dataset = done_match.group(2)
            rows = int(done_match.group(3))
            completed.append({"dataset": dataset, "rows": rows, "finished_at": last_timestamp})
            current_dataset = dataset
            continue
    final_payload = _final_json_from_log(text)
    status = str(final_payload.get("status") or ("running" if current_dataset else "unknown"))
    if status in {"success", "failed"}:
        current_dataset = None
        if dataset_total is not None:
            dataset_index = dataset_total
        sub_current = sub_total
    return {
        "log_path": str(path),
        "run_id": final_payload.get("run_id") or _run_id_from_data_log_name(path),
        "status": status,
        "dataset_index": dataset_index,
        "dataset_total": dataset_total,
        "current_dataset": current_dataset,
        "sub_current": sub_current,
        "sub_total": sub_total,
        "sub_message": sub_message,
        "completed_dataset_count": len(completed),
        "completed_datasets": completed[-8:],
        "total_rows": final_payload.get("total_rows"),
        "finished_at": last_timestamp,
        "error": final_payload.get("error"),
    }


def _final_json_from_log(text: str) -> dict[str, Any]:
    marker = text.rfind("\n{")
    json_text = text[marker + 1 :].strip() if marker >= 0 else text.strip()
    try:
        payload = json.loads(json_text)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _run_id_from_data_log_name(path: Path) -> str:
    match = re.search(r"data-update-(\d{8}_\d{6})", path.name)
    return f"data_{match.group(1)}" if match else ""


def _send_lark_test_message(root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    _load_workspace_env(root)
    provider = _notification_provider_from_env()
    provider_label = "Lark 国际版" if provider == "lark" else "飞书国内版"
    config = FeishuConfig.from_env(provider)
    message = NotificationMessage(
        event_type=f"console.{provider}.test",
        notification_type="ops_test",
        severity="info",
        title=f"Vortex {provider_label} 通知测试",
        summary=str(payload.get("text") or f"本地控制台已成功调用 {provider_label} 出站消息接口。"),
        impact="只验证通知链路，不触发研究、策略或交易。",
        suggested_actions=("确认手机端收到消息", "回到控制台继续启动研究闭环"),
    )
    result = FeishuChannel(config).send(message)
    return {"status": "sent", "provider": provider, "delivery": result}


def _run_cogalpha_cycle_from_payload(root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    notify = bool(payload.get("notify", False))
    notification_config = None
    if notify:
        notification_config = {
            "enabled": True,
            "level": "info",
            "channel": _notification_provider_from_env(),
        }
    return run_cogalpha_company_demo_cycle(
        root,
        run_id=_optional_text(payload.get("run_id")),
        days=_int_payload(payload, "days", 220),
        symbols=_int_payload(payload, "symbols", 60),
        min_periods=_int_payload(payload, "min_periods", 30),
        groups=_int_payload(payload, "groups", 5),
        top_n=_int_payload(payload, "top_n", 10),
        notify=notify,
        notification_config=notification_config,
    )


def _prepare_earnings_forecast_from_payload(root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    from vortex.strategy.earnings_forecast_live import prepare_earnings_forecast_next_session

    result = prepare_earnings_forecast_next_session(
        root,
        start=_required_text(payload, "start"),
        as_of=_required_text(payload, "as_of"),
        qmt_bridge_url=_payload_text_or_env(payload, "qmt_bridge_url", "QMT_BRIDGE_URL", "QMT_BRIDGE_BASE_URL"),
        qmt_bridge_token=_payload_text_or_env_optional(payload, "qmt_bridge_token", "QMT_BRIDGE_TOKEN", "QMT_BRIDGE_API_KEY"),
        qmt_account_id=_payload_text_or_env_optional(payload, "qmt_account_id", "QMT_ACCOUNT_ID", "QMT_BRIDGE_TRADING_ACCOUNT_ID"),
        preset_name=str(payload.get("preset") or "baseline_top110_large"),
        label=_optional_text(payload.get("label")),
        portfolio_notional=float(payload.get("portfolio_notional") or 1_000_000.0),
        min_position_value=float(payload.get("min_position_value") or 3_000.0),
        require_precise_data=not bool(payload.get("allow_missing_precise_data", False)),
    )
    return {"status": "prepared", "summary": result.summary}


def _run_earnings_forecast_auto_once_from_payload(root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    from vortex.strategy.earnings_forecast_live import run_earnings_forecast_auto_once

    allow_trading = bool(payload.get("allow_trading", False))
    if allow_trading and payload.get("confirm_live_trading") != "我确认允许执行交易":
        raise ValueError("允许交易必须填写确认语：我确认允许执行交易")
    return run_earnings_forecast_auto_once(
        root,
        start=_required_text(payload, "start"),
        profile_name=str(payload.get("profile") or "default"),
        qmt_bridge_url=_payload_text_or_env(payload, "qmt_bridge_url", "QMT_BRIDGE_URL", "QMT_BRIDGE_BASE_URL"),
        qmt_bridge_token=_payload_text_or_env_optional(payload, "qmt_bridge_token", "QMT_BRIDGE_TOKEN", "QMT_BRIDGE_API_KEY"),
        qmt_account_id=_payload_text_or_env_optional(payload, "qmt_account_id", "QMT_ACCOUNT_ID", "QMT_BRIDGE_TRADING_ACCOUNT_ID"),
        preset_name=str(payload.get("preset") or "baseline_top110_large"),
        label=str(payload.get("label") or "auto_run"),
        prepare_time=str(payload.get("prepare_time") or "08:10"),
        execute_time=str(payload.get("execute_time") or "09:25"),
        allow_trading=allow_trading,
        nav_initial_equity=float(payload.get("nav_initial_equity") or 1_000_000.0),
        nav_benchmark=str(payload.get("nav_benchmark") or "000852.SH"),
    )


def _runs_payload(root: Path) -> dict[str, Any]:
    return {
        "research_runs": _list_run_manifests(root),
        "strategy_tasks": _list_strategy_tasks(root),
        "strategies": _strategy_catalog(root),
    }


def _list_run_manifests(root: Path, *, limit: int = 20) -> list[dict[str, Any]]:
    run_root = root / "research" / "cogalpha" / "company_runs"
    if not run_root.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in run_root.glob("*/run_manifest.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        run = dict(payload.get("run") or {})
        gate = dict(payload.get("quality_gate") or {})
        artifacts = dict(payload.get("artifacts") or {})
        candidates = [
            candidate
            for candidate in gate.get("top_candidates") or []
            if isinstance(candidate, dict)
        ]
        agents = sorted(
            {
                str(candidate.get("agent"))
                for candidate in candidates
                if candidate.get("agent")
            }
        )
        rows.append(
            {
                "run_id": run.get("run_id"),
                "kind": run.get("kind"),
                "status": run.get("status"),
                "started_at": run.get("started_at"),
                "finished_at": run.get("finished_at"),
                "duration_seconds": run.get("duration_seconds"),
                "output_dir": run.get("output_dir"),
                "quality_gate": gate.get("status"),
                "decision_counts": gate.get("decision_counts") or {},
                "promoted_candidate_count": gate.get("promoted_candidate_count"),
                "agent_count": len(agents),
                "agents": agents[:8],
                "top_candidate_summaries": _summarize_factor_candidates(candidates),
                "next_stage": _research_next_stage(payload),
                "decision_required": bool(
                    dict(payload.get("decision") or {}).get("approval_required")
                ),
                "run_manifest": str(path),
                "artifact_count": len(artifacts),
                "task_count": len(payload.get("tasks") or []),
                "mtime": path.stat().st_mtime,
            }
        )
    rows.sort(key=lambda item: float(item.get("mtime") or 0), reverse=True)
    return rows[:limit]


def _summarize_factor_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in candidates[:8]:
        fitness = candidate.get("fitness") if isinstance(candidate.get("fitness"), dict) else {}
        rows.append(
            {
                "agent": candidate.get("agent"),
                "alpha_id": candidate.get("alpha_id"),
                "name": candidate.get("name"),
                "decision": candidate.get("decision"),
                "score": candidate.get("score"),
                "semantic_status": candidate.get("semantic_status"),
                "primary_horizon": fitness.get("primary_horizon"),
                "rank_ic_mean": fitness.get("rank_ic_mean"),
                "long_short_sharpe": fitness.get("long_short_sharpe"),
                "rejection_reasons": candidate.get("rejection_reasons") or [],
            }
        )
    return rows


def _research_next_stage(payload: dict[str, Any]) -> dict[str, Any]:
    gate = dict(payload.get("quality_gate") or {})
    decision = dict(payload.get("decision") or {})
    promoted = int(gate.get("promoted_candidate_count") or 0)
    if promoted <= 0:
        return {
            "stage": "研究归档",
            "owner": "Research Director",
            "reason": "本轮没有可晋升候选。",
        }
    if decision.get("approval_required"):
        return {
            "stage": "因子质量与代码审查",
            "owner": "Factor Quality Reviewer / Risk Officer",
            "reason": "候选因子只能先进入审查队列，不能直接进入策略。",
        }
    return {
        "stage": "策略晋升评估",
        "owner": "Strategy Promotion Officer",
        "reason": "等待 signal snapshot 与策略候选评估。",
    }


def _list_strategy_tasks(root: Path, *, limit: int = 20) -> list[dict[str, Any]]:
    task_dir = root / "state" / "trade" / "pending_qmt"
    if not task_dir.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in task_dir.glob("*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        rows.append(
            {
                "task_path": str(path),
                "task_type": payload.get("task_type"),
                "status": payload.get("status"),
                "trade_date": payload.get("trade_date"),
                "as_of": payload.get("as_of"),
                "preset": payload.get("preset"),
                "strategy_version": payload.get("strategy_version"),
                "holding_count": payload.get("target_diagnostics", {}).get("holding_count")
                if isinstance(payload.get("target_diagnostics"), dict)
                else None,
                "target_portfolio_path": payload.get("target_portfolio_path"),
                "updated_at": payload.get("updated_at"),
                "error": payload.get("error"),
                "mtime": path.stat().st_mtime,
            }
        )
    rows.sort(key=lambda item: float(item.get("mtime") or 0), reverse=True)
    return rows[:limit]


def _render_console_html_legacy(root: Path) -> str:
    root_text = html.escape(str(root))
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Vortex 控制台</title>
  <link rel="icon" href="data:," />
  <style>
    :root {{
      color-scheme: light;
      --bg: #f6f7f9;
      --surface: #ffffff;
      --line: #d9dee7;
      --text: #182230;
      --muted: #667085;
      --blue: #1d4ed8;
      --green: #087443;
      --red: #b42318;
      --amber: #b54708;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    header {{
      border-bottom: 1px solid var(--line);
      background: var(--surface);
      padding: 16px 24px;
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: center;
    }}
    h1 {{ margin: 0; font-size: 20px; }}
    main {{
      width: min(1320px, 100%);
      margin: 0 auto;
      padding: 20px;
      display: grid;
      grid-template-columns: 380px minmax(0, 1fr);
      gap: 16px;
    }}
    section {{
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
    }}
    h2 {{ margin: 0 0 12px; font-size: 16px; }}
    label {{ display: block; margin: 10px 0 4px; color: var(--muted); }}
    input, select, textarea {{
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 9px 10px;
      font: inherit;
      background: #fff;
    }}
    textarea {{ min-height: 96px; resize: vertical; }}
    button {{
      border: 1px solid var(--blue);
      background: var(--blue);
      color: #fff;
      border-radius: 6px;
      padding: 9px 12px;
      font-weight: 600;
      cursor: pointer;
    }}
    button.secondary {{ background: #fff; color: var(--blue); }}
    button.danger {{ background: var(--red); border-color: var(--red); }}
    .row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }}
    .stack {{ display: grid; gap: 16px; }}
    .wide {{ grid-column: 1 / -1; }}
    .toolbar {{ display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }}
    .status {{ color: var(--muted); font-size: 13px; }}
    .ok {{ color: var(--green); }}
    .warn {{ color: var(--amber); }}
    .bad {{ color: var(--red); }}
    .status-grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }}
    .metric {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      background: #fbfcfe;
    }}
    .metric span {{ color: var(--muted); font-size: 12px; }}
    .metric strong {{ display: block; margin-top: 3px; font-size: 22px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 9px 8px;
      text-align: left;
      vertical-align: top;
      font-size: 13px;
    }}
    th {{ color: var(--muted); font-weight: 600; }}
    .pill {{
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      padding: 2px 8px;
      border-radius: 999px;
      background: #eef4ff;
      color: var(--blue);
      font-size: 12px;
      font-weight: 600;
    }}
    .pill.success {{ background: #ecfdf3; color: var(--green); }}
    .pill.failed {{ background: #fef3f2; color: var(--red); }}
    .pill.running, .pill.queued {{ background: #eef4ff; color: var(--blue); }}
    pre {{
      margin: 0;
      padding: 14px;
      background: #111827;
      color: #e5e7eb;
      border-radius: 8px;
      overflow: auto;
      min-height: 320px;
      max-height: 760px;
      white-space: pre-wrap;
      word-break: break-word;
    }}
    @media (max-width: 920px) {{
      main {{ grid-template-columns: 1fr; padding: 12px; }}
      header {{ align-items: flex-start; flex-direction: column; }}
    }}
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Vortex 控制台</h1>
      <div class="status">工作区：{root_text}</div>
    </div>
    <button class="secondary" onclick="refreshStatus()">刷新状态</button>
  </header>
  <main>
    <section class="wide">
      <h2>运行中心</h2>
      <div class="status-grid" id="overview_cards"></div>
      <div class="row">
        <div>
          <h2>当前任务</h2>
          <table>
            <thead><tr><th>任务</th><th>状态</th><th>阶段</th><th>更新时间</th></tr></thead>
            <tbody id="job_rows"><tr><td colspan="4">暂无任务</td></tr></tbody>
          </table>
        </div>
        <div>
          <h2>最近研究运行</h2>
          <table>
            <thead><tr><th>运行</th><th>状态</th><th>候选</th><th>产物</th></tr></thead>
            <tbody id="run_rows"><tr><td colspan="4">暂无运行</td></tr></tbody>
          </table>
        </div>
      </div>
    </section>
    <div class="stack">
      <section>
        <h2>Lark 国际版配置</h2>
        <label>App ID</label>
        <input id="lark_app_id" placeholder="cli_xxx" />
        <label>App Secret</label>
        <input id="lark_app_secret" type="password" />
        <label>接收人 ID</label>
        <input id="lark_receive_id" placeholder="ou_xxx 或 oc_xxx" />
        <label>接收人类型</label>
        <select id="lark_receive_type">
          <option value="open_id">open_id</option>
          <option value="chat_id">chat_id</option>
          <option value="email">email</option>
          <option value="user_id">user_id</option>
          <option value="union_id">union_id</option>
        </select>
        <div class="toolbar">
          <button onclick="saveLark()">保存变量</button>
          <button class="secondary" onclick="testLark()">发送测试</button>
        </div>
      </section>

      <section>
        <h2>因子研究闭环</h2>
        <div class="status">先跑确定性演示数据，验证研究流程、质量门禁、产物和审批入口。</div>
        <div class="row">
          <div><label>交易日数量</label><input id="research_days" value="220" /></div>
          <div><label>股票数量</label><input id="research_symbols" value="60" /></div>
        </div>
        <div class="row">
          <div><label>最少期数</label><input id="research_min_periods" value="30" /></div>
          <div><label>Top N</label><input id="research_top_n" value="10" /></div>
        </div>
        <label><input id="research_notify" type="checkbox" style="width:auto" /> 完成后通知 Lark</label>
        <div class="toolbar">
          <button onclick="runResearch()">启动 CogAlpha 研究</button>
        </div>
      </section>
    </div>

    <div class="stack">
      <section>
        <h2>策略控制</h2>
        <div class="status">当前主策略为业绩预告漂移。先生成任务和交接包，默认不下单。</div>
        <div class="row">
          <div><label>回看起始日</label><input id="strategy_start" placeholder="20170101" /></div>
          <div><label>基准日期</label><input id="strategy_as_of" placeholder="20260516" /></div>
        </div>
        <div class="row">
          <div><label>Preset</label><select id="strategy_preset">
            <option>baseline_top110_large</option>
            <option>stable_100w</option>
            <option>aggressive_100w</option>
            <option>liquidity_top80</option>
            <option>liquidity_top90_1000w</option>
          </select></div>
          <div><label>组合本金</label><input id="strategy_notional" value="1000000" /></div>
        </div>
        <label>QMT Bridge URL</label>
        <input id="qmt_bridge_url" placeholder="http://127.0.0.1:8000" />
        <label>QMT Token</label>
        <input id="qmt_bridge_token" type="password" />
        <label>账户 ID</label>
        <input id="qmt_account_id" />
        <div class="toolbar">
          <button onclick="prepareStrategy()">生成策略任务（不下单）</button>
          <button class="secondary" onclick="autoOnce(false)">自动编排一次（禁用交易）</button>
        </div>
        <div class="status">要跑起来：先确认 data 已更新，再填 start/as-of/QMT Bridge。模拟盘也先保持禁用交易，检查目标组合和 pending task 后再执行。</div>
      </section>

      <section>
        <h2>运行输出</h2>
        <pre id="output">等待操作...</pre>
      </section>
    </div>
  </main>
  <script>
    const out = document.getElementById('output');
    function show(data) {{ out.textContent = typeof data === 'string' ? data : JSON.stringify(data, null, 2); }}
    function esc(value) {{
      return String(value ?? '').replace(/[&<>"']/g, ch => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}}[ch]));
    }}
    function pill(status) {{
      const value = String(status || '-');
      return `<span class="pill ${{esc(value)}}">${{esc(value)}}</span>`;
    }}
    async function api(path, body) {{
      const res = await fetch(path, {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify(body || {{}})
      }});
      const data = await res.json();
      if (!res.ok) throw data;
      return data;
    }}
    async function refreshStatus() {{
      const res = await fetch('/api/status');
      const data = await res.json();
      renderOverview(data);
      return data;
    }}
    async function refreshRuns() {{
      const [jobsRes, runsRes] = await Promise.all([
        fetch('/api/jobs'),
        fetch('/api/runs')
      ]);
      renderJobs((await jobsRes.json()).jobs || []);
      renderRuns((await runsRes.json()).research_runs || []);
    }}
    function renderOverview(data) {{
      const overview = data.overview || {{}};
      const lark = data.lark || {{}};
      document.getElementById('overview_cards').innerHTML = [
        ['活动任务', overview.active_job_count ?? 0],
        ['研究运行', overview.recent_research_run_count ?? 0],
        ['策略任务', overview.pending_strategy_task_count ?? 0],
        ['Lark', lark.configured ? '已配置' : '未配置']
      ].map(([label, value]) => `<div class="metric"><span>${{esc(label)}}</span><strong>${{esc(value)}}</strong></div>`).join('');
      renderJobs(data.active_jobs || []);
      renderRuns(data.recent_runs || []);
    }}
    function renderJobs(jobs) {{
      const body = document.getElementById('job_rows');
      if (!jobs.length) {{
        body.innerHTML = '<tr><td colspan="4">暂无任务</td></tr>';
        return;
      }}
      body.innerHTML = jobs.map(job => `
        <tr onclick="showJob('${{esc(job.job_id)}}')" style="cursor:pointer">
          <td>${{esc(job.name)}}<br><span class="status">${{esc(job.job_id)}}</span></td>
          <td>${{pill(job.status)}}</td>
          <td>${{esc(job.stage)}}</td>
          <td>${{esc(job.updated_at || '')}}</td>
        </tr>
      `).join('');
    }}
    async function showJob(jobId) {{
      const res = await fetch('/api/jobs/' + encodeURIComponent(jobId));
      show(await res.json());
    }}
    function renderRuns(runs) {{
      const body = document.getElementById('run_rows');
      if (!runs.length) {{
        body.innerHTML = '<tr><td colspan="4">暂无运行</td></tr>';
        return;
      }}
      body.innerHTML = runs.map(run => `
        <tr onclick='show(${{JSON.stringify(JSON.stringify(run))}})' style="cursor:pointer">
          <td>${{esc(run.run_id)}}<br><span class="status">${{esc(run.started_at || '')}}</span></td>
          <td>${{pill(run.status)}}<br><span class="status">${{esc(run.quality_gate || '')}}</span></td>
          <td>${{esc(run.promoted_candidate_count ?? '-')}}</td>
          <td>${{esc(run.artifact_count ?? 0)}}<br><span class="status">${{esc(run.run_manifest || '')}}</span></td>
        </tr>
      `).join('');
    }}
    async function saveLark() {{
      try {{
        show(await api('/api/config/lark', {{
          app_id: document.getElementById('lark_app_id').value,
          app_secret: document.getElementById('lark_app_secret').value,
          default_receive_id: document.getElementById('lark_receive_id').value,
          default_receive_id_type: document.getElementById('lark_receive_type').value
        }}));
      }} catch (e) {{ show(e); }}
    }}
    async function testLark() {{
      try {{ show(await api('/api/lark/test', {{text: 'Vortex 控制台测试消息'}})); }}
      catch (e) {{ show(e); }}
    }}
    async function runResearch() {{
      try {{
        const result = await api('/api/research/cogalpha-cycle', {{
          days: Number(document.getElementById('research_days').value || 220),
          symbols: Number(document.getElementById('research_symbols').value || 60),
          min_periods: Number(document.getElementById('research_min_periods').value || 30),
          top_n: Number(document.getElementById('research_top_n').value || 10),
          notify: document.getElementById('research_notify').checked
        }});
        show(result);
        await refreshStatus();
      }} catch (e) {{ show(e); }}
    }}
    function strategyPayload() {{
      return {{
        start: document.getElementById('strategy_start').value,
        as_of: document.getElementById('strategy_as_of').value,
        preset: document.getElementById('strategy_preset').value,
        portfolio_notional: Number(document.getElementById('strategy_notional').value || 1000000),
        qmt_bridge_url: document.getElementById('qmt_bridge_url').value,
        qmt_bridge_token: document.getElementById('qmt_bridge_token').value,
        qmt_account_id: document.getElementById('qmt_account_id').value
      }};
    }}
    async function prepareStrategy() {{
      try {{
        const result = await api('/api/strategy/earnings-forecast/prepare', strategyPayload());
        show(result);
        await refreshStatus();
      }}
      catch (e) {{ show(e); }}
    }}
    async function autoOnce(allowTrading) {{
      try {{
        const body = strategyPayload();
        body.allow_trading = Boolean(allowTrading);
        const result = await api('/api/strategy/earnings-forecast/auto-once', body);
        show(result);
        await refreshStatus();
      }} catch (e) {{ show(e); }}
    }}
    refreshStatus();
    setInterval(() => {{ refreshStatus().catch(() => {{}}); refreshRuns().catch(() => {{}}); }}, 2000);
  </script>
</body>
</html>"""


def _render_console_html(root: Path) -> str:
    root_text = html.escape(str(root))
    return """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Vortex 工作台</title>
  <link rel="icon" href="data:," />
  <style>
    :root {
      color-scheme: light;
      --bg: #f4f6f8;
      --surface: #ffffff;
      --surface-2: #f9fafb;
      --line: #d9dee7;
      --text: #182230;
      --muted: #667085;
      --blue: #1d4ed8;
      --green: #087443;
      --red: #b42318;
      --amber: #b54708;
      --nav: #111827;
      --nav-muted: #9ca3af;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    .app {
      min-height: 100vh;
      display: grid;
      grid-template-columns: 244px minmax(0, 1fr);
    }
    aside {
      background: var(--nav);
      color: #fff;
      padding: 18px 14px;
    }
    .brand {
      padding: 4px 8px 18px;
      border-bottom: 1px solid rgba(255, 255, 255, .1);
      margin-bottom: 14px;
    }
    .brand h1 {
      margin: 0;
      font-size: 18px;
      line-height: 1.25;
    }
    .brand div {
      margin-top: 6px;
      color: var(--nav-muted);
      font-size: 12px;
      word-break: break-all;
    }
    .nav {
      width: 100%;
      display: flex;
      align-items: center;
      gap: 10px;
      border: 0;
      background: transparent;
      color: var(--nav-muted);
      border-radius: 6px;
      padding: 10px;
      margin: 2px 0;
      text-align: left;
      font: inherit;
      cursor: pointer;
    }
    .nav:hover,
    .nav.active {
      background: rgba(255, 255, 255, .1);
      color: #fff;
    }
    .nav span:first-child {
      width: 22px;
      text-align: center;
      font-weight: 700;
    }
    .nav-group-label {
      margin: 18px 8px 6px;
      color: rgba(255, 255, 255, .45);
      font-size: 11px;
      font-weight: 700;
      letter-spacing: .08em;
    }
    .content {
      min-width: 0;
      display: flex;
      flex-direction: column;
    }
    header {
      border-bottom: 1px solid var(--line);
      background: var(--surface);
      padding: 16px 24px;
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: center;
    }
    header h2 { margin: 0; font-size: 18px; }
    header .subtitle { color: var(--muted); font-size: 13px; }
    main {
      width: min(1480px, 100%);
      padding: 18px;
      display: block;
    }
    .page { display: none; }
    .page.active { display: block; }
    .page-grid {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 360px;
      gap: 14px;
      align-items: start;
    }
    .wide { grid-column: 1 / -1; }
    .two-col {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
    }
    .stack { display: grid; gap: 14px; }
    section, .panel {
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 16px;
    }
    h3 { margin: 0 0 12px; font-size: 16px; }
    h4 { margin: 14px 0 8px; font-size: 13px; color: var(--muted); }
    label {
      display: block;
      margin: 10px 0 4px;
      color: var(--muted);
      font-size: 13px;
    }
    input, select, textarea {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 9px 10px;
      font: inherit;
      background: #fff;
    }
    input:disabled {
      color: var(--muted);
      background: var(--surface-2);
    }
    button {
      border: 1px solid var(--blue);
      background: var(--blue);
      color: #fff;
      border-radius: 6px;
      padding: 9px 12px;
      font-weight: 600;
      cursor: pointer;
    }
    button.secondary { background: #fff; color: var(--blue); }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
    .toolbar { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }
    .status { color: var(--muted); font-size: 13px; }
    .hint { color: var(--muted); font-size: 13px; margin-top: 8px; }
    .status-grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }
    .metric {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      background: var(--surface-2);
    }
    .metric span { color: var(--muted); font-size: 12px; }
    .metric strong { display: block; margin-top: 3px; font-size: 20px; }
    table { width: 100%; border-collapse: collapse; table-layout: auto; }
    th, td {
      border-bottom: 1px solid var(--line);
      padding: 9px 8px;
      text-align: left;
      vertical-align: top;
      font-size: 13px;
      overflow-wrap: anywhere;
    }
    th { color: var(--muted); font-weight: 600; white-space: nowrap; }
    tr.clickable { cursor: pointer; }
    tr.clickable:hover { background: #f8fafc; }
    .pill {
      display: inline-flex;
      align-items: center;
      min-height: 22px;
      padding: 2px 8px;
      border-radius: 999px;
      background: #eef4ff;
      color: var(--blue);
      font-size: 12px;
      font-weight: 600;
    }
    .pill.success, .pill.configured { background: #ecfdf3; color: var(--green); }
    .pill.failed, .pill.unconfigured { background: #fef3f2; color: var(--red); }
    .pill.running, .pill.queued { background: #eef4ff; color: var(--blue); }
    .pill.blocked, .pill.warn { background: #fffaeb; color: var(--amber); }
    .step-list {
      display: grid;
      gap: 8px;
      margin: 0;
      padding: 0;
      list-style: none;
    }
    .step-list li {
      border-left: 3px solid var(--line);
      padding: 8px 0 8px 10px;
    }
    .step-list strong { display: block; }
    pre {
      margin: 0;
      padding: 14px;
      background: #111827;
      color: #e5e7eb;
      border-radius: 8px;
      overflow: auto;
      min-height: 320px;
      max-height: 760px;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .danger-zone {
      border-color: #fecdca;
      background: #fffafa;
    }
    .desk-grid {
      display: grid;
      grid-template-columns: minmax(0, 1.35fr) minmax(360px, .65fr);
      gap: 14px;
      align-items: start;
    }
    .review-strip {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin: 12px 0;
    }
    .ops-strip {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 8px;
      margin-top: 10px;
    }
    .ops-card {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: var(--surface-2);
      min-height: 92px;
    }
    .ops-card h4 {
      margin: 0 0 6px;
      color: var(--text);
      font-size: 13px;
    }
    .ops-card .toolbar {
      margin-top: 8px;
    }
    .ops-card button {
      padding: 5px 8px;
      font-size: 12px;
    }
    .priority-section { margin-bottom: 14px; }
    #page-trading .priority-section {
      padding: 12px 14px;
    }
    #page-trading .priority-section h3 { margin: 0; }
    #page-trading .priority-section .status { font-size: 11px; }
    #page-trading .account-strip { margin: 8px 0 0; gap: 8px; }
    #page-trading .account-strip .review-item { padding: 8px 10px; }
    #page-trading .account-strip .review-item strong { font-size: 16px; }
    .action-priority { margin-bottom: 14px; }
    .action-priority .section-title-row { margin-bottom: 8px; }
    details.details-panel {
      border-top: 1px solid var(--line);
      margin-top: 14px;
      padding-top: 10px;
    }
    details.details-panel summary {
      cursor: pointer;
      color: var(--muted);
      font-weight: 700;
    }
    .metric-note {
      display: block;
      margin-top: 3px;
      color: var(--muted);
      font-size: 11px;
      line-height: 1.35;
    }
    .section-title-row {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
    }
    .section-title-row .toolbar { margin-top: 0; }
    .account-strip {
      grid-template-columns: repeat(4, minmax(0, 1fr));
    }
    .review-item {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: #fff;
    }
    .review-item span { display: block; color: var(--muted); font-size: 12px; }
    .review-item strong {
      display: block;
      margin-top: 4px;
      font-size: 15px;
      overflow-wrap: anywhere;
      word-break: break-word;
    }
    .review-item.is-primary strong { font-size: 18px; }
    #page-data .review-strip {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .progress-track {
      height: 8px;
      margin: 12px 0 4px;
      overflow: hidden;
      border-radius: 999px;
      background: #edf2f7;
    }
    .progress-fill {
      display: block;
      height: 100%;
      width: 0;
      border-radius: inherit;
      background: linear-gradient(90deg, #1d4ed8, #16a34a);
      transition: width .2s ease;
    }
    .diagnostic-details {
      margin-top: 10px;
    }
    .diagnostic-details summary {
      cursor: pointer;
      color: var(--muted);
      font-weight: 700;
    }
    .diagnostic-details table {
      margin-top: 8px;
    }
    .diagnostic-details th {
      min-width: 170px;
    }
    .compact-table { margin-top: 12px; }
    .stock-cell strong { display: block; font-size: 13px; }
    .stock-cell span { display: block; margin-top: 2px; }
    .side-buy { color: var(--green); font-weight: 700; }
    .side-sell { color: var(--red); font-weight: 700; }
    .refresh-feedback {
      display: inline-flex;
      align-items: center;
      min-height: 28px;
      padding: 4px 9px;
      border-radius: 6px;
      background: #ecfdf3;
      color: var(--green);
      font-weight: 600;
    }
    .refresh-feedback:empty { display: none; }
    .strategy-list {
      display: grid;
      gap: 10px;
    }
    .strategy-option {
      width: 100%;
      border: 1px solid var(--line);
      background: #fff;
      color: var(--text);
      border-radius: 8px;
      padding: 12px;
      text-align: left;
      cursor: pointer;
    }
    .strategy-option.active {
      border-color: var(--blue);
      box-shadow: inset 3px 0 0 var(--blue);
    }
    .strategy-option strong { display: block; font-size: 14px; }
    .strategy-option .status { display: block; margin-top: 4px; }
    .attention-list {
      margin: 8px 0 0;
      padding-left: 18px;
    }
    .attention-list li { margin: 5px 0; }
    .mono {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      font-size: 12px;
      word-break: break-all;
    }
    .inline-field {
      display: flex;
      align-items: center;
      gap: 8px;
      margin: 8px 0;
    }
    .inline-field label { margin: 0; color: var(--text); }
    .inline-field input { width: auto; }
    @media (max-width: 1080px) {
      .app { grid-template-columns: 1fr; }
      aside {
        display: flex;
        align-items: center;
        gap: 8px;
        overflow-x: auto;
        padding: 12px;
      }
      .brand {
        flex: 0 0 240px;
        border-bottom: 0;
        border-right: 1px solid rgba(255, 255, 255, .1);
        margin: 0;
        padding: 0 12px 0 0;
      }
      .nav {
        flex: 0 0 auto;
        width: auto;
        white-space: nowrap;
        margin: 0;
      }
      .page-grid, .two-col, .desk-grid { grid-template-columns: 1fr; }
      .status-grid, .review-strip, .ops-strip { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      main { padding: 12px; }
      header { align-items: flex-start; flex-direction: column; }
    }
  </style>
</head>
<body>
  <div class="app">
    <aside>
      <div class="brand">
        <h1>Vortex 工作台</h1>
        <div>工作区：__WORKSPACE_ROOT__</div>
      </div>
      <div class="nav-group-label">日常监督</div>
      <button class="nav active" data-page="trading"><span>05</span><span>交易台</span></button>
      <button class="nav" data-page="data"><span>06</span><span>数据服务</span></button>
      <button class="nav" data-page="settings"><span>07</span><span>设置 / 集成管理</span></button>
      <button class="nav" data-page="output"><span>08</span><span>运行输出</span></button>
      <div class="nav-group-label">研发 / Agent 入口</div>
      <button class="nav" data-page="dashboard"><span>01</span><span>运营驾驶舱</span></button>
      <button class="nav" data-page="runs"><span>02</span><span>运行中心</span></button>
      <button class="nav" data-page="research"><span>03</span><span>因子研究实验室</span></button>
      <button class="nav" data-page="strategy"><span>04</span><span>策略启动向导</span></button>
    </aside>

    <div class="content">
      <header>
        <div>
          <h2 id="page_title">交易台</h2>
          <div class="subtitle" id="page_subtitle">当前绑定策略的账户、换仓、门禁和选股过程。</div>
        </div>
        <div class="toolbar">
          <button class="secondary" onclick="refreshAll('global_refresh_status', this)">刷新状态</button>
          <span id="global_refresh_status" class="status"></span>
        </div>
      </header>

      <main>
        <section class="page" id="page-dashboard">
          <div class="status-grid" id="dashboard_cards"></div>
          <div class="page-grid">
            <div class="stack">
              <section>
                <h3>当前任务</h3>
                <table>
                  <thead><tr><th>任务</th><th>状态</th><th>阶段</th><th>更新时间</th></tr></thead>
                  <tbody id="dashboard_job_rows"><tr><td colspan="4">暂无任务</td></tr></tbody>
                </table>
              </section>
              <section>
                <h3>最近研究运行</h3>
                <table>
                  <thead><tr><th>运行</th><th>Agent</th><th>质量门禁</th><th>候选</th></tr></thead>
                  <tbody id="dashboard_run_rows"><tr><td colspan="4">暂无运行</td></tr></tbody>
                </table>
              </section>
            </div>
            <div class="stack">
              <section>
                <h3>待处理策略任务</h3>
                <table>
                  <thead><tr><th>交易日</th><th>状态</th><th>Preset</th></tr></thead>
                  <tbody id="dashboard_strategy_rows"><tr><td colspan="3">暂无任务</td></tr></tbody>
                </table>
              </section>
              <section>
                <h3>移动通知</h3>
                <div id="dashboard_lark_state" class="status">读取中...</div>
                <div class="toolbar">
                  <button class="secondary" onclick="goPage('settings')">管理 Lark</button>
                  <button class="secondary" onclick="testLark()">发送测试</button>
                </div>
              </section>
            </div>
          </div>
        </section>

        <section class="page" id="page-runs">
          <div class="two-col">
            <section>
              <h3>Job 队列</h3>
              <table>
                <thead><tr><th>任务</th><th>状态</th><th>阶段</th><th>耗时</th><th>错误</th></tr></thead>
                <tbody id="run_center_job_rows"><tr><td colspan="5">暂无任务</td></tr></tbody>
              </table>
            </section>
            <section>
              <h3>策略任务</h3>
              <table>
                <thead><tr><th>交易日</th><th>策略版本</th><th>状态</th><th>目标组合</th></tr></thead>
                <tbody id="run_center_strategy_rows"><tr><td colspan="4">暂无任务</td></tr></tbody>
              </table>
            </section>
          </div>
          <section style="margin-top:14px">
            <h3>研究运行</h3>
            <table>
              <thead><tr><th>Run</th><th>状态</th><th>Agent</th><th>质量门禁</th><th>候选/产物</th></tr></thead>
              <tbody id="run_center_research_rows"><tr><td colspan="5">暂无运行</td></tr></tbody>
            </table>
          </section>
        </section>

        <section class="page" id="page-research">
          <div class="page-grid">
            <section>
              <h3>启动 CogAlpha 因子研究闭环</h3>
              <div class="status">v1 使用确定性演示数据，验证研究流程、质量门禁、产物和审批入口。</div>
              <div class="row">
                <div><label>交易日数量</label><input id="research_days" value="220" /></div>
                <div><label>股票数量</label><input id="research_symbols" value="60" /></div>
              </div>
              <div class="row">
                <div><label>最少期数</label><input id="research_min_periods" value="30" /></div>
                <div><label>分组数</label><input id="research_groups" value="5" /></div>
              </div>
              <div class="row">
                <div><label>Top N</label><input id="research_top_n" value="10" /></div>
                <div><label>运行编号，可留空</label><input id="research_run_id" /></div>
              </div>
              <label><input id="research_notify" type="checkbox" style="width:auto" /> 完成后通知 Lark</label>
              <div class="toolbar">
                <button onclick="runResearch()">启动研究闭环</button>
                <button class="secondary" onclick="goPage('runs')">查看运行中心</button>
              </div>
            </section>
            <div class="stack">
              <section>
                <h3>研究门禁</h3>
                <ul class="step-list">
                  <li><strong>候选生成</strong><span class="status">多视角 recipe 生成候选表达式。</span></li>
                  <li><strong>质量审查</strong><span class="status">过滤无效、重复、不可评测候选。</span></li>
                  <li><strong>适应度评测</strong><span class="status">脚本输出 IC、分组、稳定性等指标。</span></li>
                  <li><strong>晋升判断</strong><span class="status">只进入下一代候选队列，不直接进实盘。</span></li>
                </ul>
              </section>
              <section>
                <h3>最近研究摘要</h3>
                <table>
                  <thead><tr><th>Run</th><th>Agent</th><th>下一步</th></tr></thead>
                  <tbody id="research_lab_rows"><tr><td colspan="3">暂无运行</td></tr></tbody>
                </table>
              </section>
            </div>
          </div>
        </section>

        <section class="page" id="page-strategy">
          <div class="page-grid">
            <section>
              <h3>业绩预告漂移策略启动向导</h3>
              <div id="strategy_selected_state" class="status">读取策略目录中...</div>
              <ol class="step-list">
                <li><strong>1. 选择策略</strong><span class="status">当前支持 baseline_top110_large 等 preset。</span></li>
                <li><strong>2. 检查前置条件</strong><span class="status">确认回看起始日、基准日期和数据约束。</span></li>
                <li><strong>3. 配置执行通道</strong><span class="status">QMT 配置来自设置页，也可临时覆盖。</span></li>
                <li><strong>4. 生成任务</strong><span class="status">写入 pending QMT 任务，不下单。</span></li>
                <li><strong>5. 自动编排</strong><span class="status">第一版只建议禁用交易运行。</span></li>
              </ol>
              <div class="row">
                <div><label>回看起始日</label><input id="strategy_start" placeholder="20170101" /></div>
                <div><label>基准日期</label><input id="strategy_as_of" placeholder="20260516" /></div>
              </div>
              <div class="row">
                <div><label>Preset</label><select id="strategy_preset">
                  <option>baseline_top110_large</option>
                  <option>stable_100w</option>
                  <option>aggressive_100w</option>
                  <option>liquidity_top80</option>
                  <option>liquidity_top90_1000w</option>
                </select></div>
                <div><label>组合本金</label><input id="strategy_notional" value="1000000" /></div>
              </div>
              <label>QMT Bridge URL</label>
              <input id="qmt_bridge_url" placeholder="http://127.0.0.1:8000" />
              <div class="row">
                <div><label>QMT Token，可留空使用设置页</label><input id="qmt_bridge_token" type="password" /></div>
                <div><label>账户 ID</label><input id="qmt_account_id" /></div>
              </div>
              <label><input id="allow_missing_precise_data" type="checkbox" style="width:auto" /> 允许缺少精确停牌/涨跌停数据</label>
              <div class="toolbar">
                <button onclick="prepareStrategy()">生成策略任务（不下单）</button>
                <button class="secondary" onclick="autoOnce(false)">自动编排一次（禁用交易）</button>
              </div>
              <div class="hint">策略跑起来的第一步是生成任务、看目标组合和 pending task。</div>
            </section>
            <div class="stack">
              <section class="danger-zone">
                <h3>交易门禁</h3>
                <div class="status">默认禁用交易。下单动作需要额外确认，不通过普通启动按钮触发。</div>
              </section>
              <section>
                <h3>最近策略任务</h3>
                <table>
                  <thead><tr><th>交易日</th><th>状态</th><th>持仓</th></tr></thead>
                  <tbody id="strategy_wizard_rows"><tr><td colspan="3">暂无任务</td></tr></tbody>
                </table>
              </section>
            </div>
          </div>
        </section>

        <section class="page active" id="page-trading">
          <section class="priority-section">
            <div class="section-title-row">
              <div>
                <h3>净值与账户总览</h3>
                <div id="strategy_nav_source" class="status"></div>
              </div>
              <div class="toolbar">
                <button class="secondary" onclick="checkQmtHealth()">更新 QMT 只读账户</button>
              </div>
            </div>
            <div class="review-strip account-strip" id="strategy_nav_metrics"></div>
            <div class="ops-strip">
              <div class="ops-card">
                <h4>当前绑定策略</h4>
                <div id="trading_binding_state" class="status">读取中...</div>
                <div class="toolbar">
                  <button class="secondary" onclick="goPage('settings')">配置交易台</button>
                </div>
              </div>
              <div class="ops-card">
                <h4>数据服务</h4>
                <div id="trading_data_state" class="status">读取中...</div>
                <div class="toolbar">
                  <button class="secondary" onclick="goPage('data')">查看详情</button>
                </div>
              </div>
              <div class="ops-card">
                <h4>QMT Bridge</h4>
                <div id="trading_qmt_compact" class="status">读取中...</div>
                <div class="toolbar">
                  <button class="secondary" onclick="checkQmtHealth()">只读检查</button>
                </div>
              </div>
            </div>
          </section>
          <section class="action-priority">
            <div class="section-title-row">
              <div>
                <h3>待执行换仓</h3>
                <div id="strategy_rebalance_state" class="status">读取中...</div>
              </div>
              <div class="toolbar">
                <button class="secondary" onclick="checkQmtHealth()">重新核对持仓差异</button>
              </div>
            </div>
            <table>
              <thead><tr><th>动作</th><th>股票</th><th>当前 → 目标</th><th>估算金额</th></tr></thead>
              <tbody id="strategy_rebalance_rows"><tr><td colspan="4">暂无换仓计划</td></tr></tbody>
            </table>
            <details class="details-panel">
              <summary>查看历史执行报告（不是当前待执行）</summary>
              <div id="strategy_execution_state" class="status"></div>
              <table>
                <thead><tr><th>方向</th><th>股票</th><th>委托股数</th><th>限价 / 金额</th></tr></thead>
                <tbody id="strategy_execution_rows"><tr><td colspan="4">暂无执行计划</td></tr></tbody>
              </table>
            </details>
          </section>
          <div class="desk-grid">
            <div class="stack">
              <section>
                <h3>交易门禁与流程</h3>
                <div id="trading_review_state" class="status">读取中...</div>
                <div class="review-strip" id="trading_review_metrics"></div>
                <ul class="attention-list" id="trading_review_attention"></ul>
                <table>
                  <thead><tr><th>步骤</th><th>状态</th><th>说明</th></tr></thead>
                  <tbody id="strategy_workflow_rows"><tr><td colspan="3">暂无流程状态</td></tr></tbody>
                </table>
              </section>
              <section>
                <div class="section-title-row">
                  <div>
                    <h3>今日选股过程</h3>
                    <div id="strategy_funnel_meta" class="status"></div>
                  </div>
                  <div class="toolbar">
                    <button class="secondary" onclick="prepareStrategy('trading', 'strategy_prepare_feedback', this)">重新筛选 / 生成目标组合</button>
                  </div>
                </div>
                <div id="strategy_prepare_feedback" class="status refresh-feedback"></div>
                <table>
                  <thead><tr><th>阶段</th><th>剩余</th><th>剔除</th></tr></thead>
                  <tbody id="strategy_funnel_rows"><tr><td colspan="3">暂无筛选数据</td></tr></tbody>
                </table>
                <h4>最终目标持仓</h4>
                <table>
                  <thead><tr><th>股票</th><th>权重</th><th>股数</th><th>目标市值</th><th>原因</th></tr></thead>
                  <tbody id="strategy_holding_rows"><tr><td colspan="5">暂无持仓</td></tr></tbody>
                </table>
                <details class="details-panel">
                  <summary>查看交接包历史（审计 / 回滚）</summary>
                  <div class="status">交接包用于确认某个交易日的目标组合、任务状态和回滚依据，不作为当前待执行换仓列表。</div>
                  <table>
                    <thead><tr><th>交易日</th><th>状态</th><th>策略版本</th><th>交接包</th></tr></thead>
                    <tbody id="trading_strategy_rows"><tr><td colspan="4">暂无任务</td></tr></tbody>
                  </table>
                </details>
              </section>
            </div>
            <div class="stack">
              <section>
                <h3>QMT 账户补充信息</h3>
                <div id="trading_qmt_state" class="status">读取中...</div>
                <div id="trading_qmt_health" class="hint"></div>
                <div class="review-strip" id="qmt_account_metrics"></div>
                <details class="details-panel">
                  <summary>查看 QMT 持仓明细</summary>
                  <table>
                    <thead><tr><th>股票</th><th>持仓 / 可用</th><th>成本 / 现价</th><th>市值</th></tr></thead>
                    <tbody id="qmt_position_rows"><tr><td colspan="4">暂无 QMT 持仓</td></tr></tbody>
                  </table>
                </details>
              </section>
            </div>
          </div>
        </section>

        <section class="page" id="page-data">
          <div class="page-grid">
            <div class="stack">
              <section>
                <div class="section-title-row">
                  <div>
                    <h3>数据服务总览</h3>
                    <div id="data_page_state" class="status">读取中...</div>
                  </div>
                  <div class="toolbar">
                    <button class="secondary" onclick="refreshAll('data_refresh_feedback', this)">刷新服务与数据状态</button>
                  </div>
                </div>
                <div id="data_refresh_feedback" class="status refresh-feedback"></div>
                <div class="progress-track"><span id="data_progress_fill" class="progress-fill"></span></div>
                <div class="review-strip" id="data_page_metrics"></div>
              </section>
              <section>
                <h3>最近完成数据集</h3>
                <table>
                  <thead><tr><th>数据集</th><th>行数</th><th>完成时间</th></tr></thead>
                  <tbody id="data_progress_rows"><tr><td colspan="3">暂无数据更新进度</td></tr></tbody>
                </table>
              </section>
            </div>
            <section>
              <h3>当前运行与调度</h3>
              <div id="data_service_state" class="hint"></div>
            </section>
          </div>
        </section>

        <section class="page" id="page-settings">
          <div class="two-col">
            <section>
              <h3>交易台绑定策略</h3>
              <div id="settings_trading_state" class="status">读取中...</div>
              <div class="row">
                <div><label>当前执行策略</label><select id="settings_active_strategy"></select></div>
                <div><label>账户模式</label><input disabled value="单账户：同一时间只绑定一个交易策略" /></div>
              </div>
              <div class="toolbar">
                <button onclick="saveTradingConfig()">保存交易台配置</button>
                <button class="secondary" onclick="goPage('trading')">返回交易台</button>
              </div>
              <div class="hint">这里决定交易台展示和自动编排面向哪个策略。当前只有一个账户，因此策略不会在交易台里临时切换。</div>
            </section>
            <section>
              <h3>移动通知通道</h3>
              <div class="row">
                <div><label>当前通道</label><select id="notification_provider">
                  <option value="lark">Lark 国际版</option>
                  <option value="feishu">飞书国内版</option>
                </select></div>
                <div><label>默认说明</label><input disabled value="默认使用 Lark 国际版；飞书国内版仅兼容保留" /></div>
              </div>
              <div id="settings_lark_state" class="status">读取中...</div>
              <div id="settings_lark_current" class="hint"></div>
              <div id="settings_feishu_current" class="hint"></div>
              <label>App ID</label>
              <input id="lark_app_id" placeholder="cli_xxx" />
              <label>App Secret</label>
              <input id="lark_app_secret" type="password" />
              <label>接收人 ID</label>
              <input id="lark_receive_id" placeholder="ou_xxx 或 oc_xxx" />
              <div class="row">
                <div><label>接收人类型</label><select id="lark_receive_type">
                  <option value="open_id">open_id</option>
                  <option value="chat_id">chat_id</option>
                  <option value="email">email</option>
                  <option value="user_id">user_id</option>
                  <option value="union_id">union_id</option>
                </select></div>
                <div><label>API Base</label><input id="lark_api_base" value="https://open.larksuite.com" /></div>
              </div>
              <div class="toolbar">
                <button class="secondary" onclick="saveNotificationProvider()">切换通道</button>
                <button onclick="saveLark()">保存 Lark</button>
                <button class="secondary" onclick="testLark()">发送测试</button>
              </div>
            </section>

            <section>
              <h3>QMT Bridge</h3>
              <div id="settings_qmt_state" class="status">读取中...</div>
              <div id="settings_qmt_current" class="hint"></div>
              <label>Bridge URL</label>
              <input id="qmt_bridge_url_setting" placeholder="http://127.0.0.1:8000" />
              <div class="row">
                <div><label>Token / API Key，可留空保持现值</label><input id="qmt_bridge_token_setting" type="password" /></div>
                <div><label>账户 ID</label><input id="qmt_account_id_setting" /></div>
              </div>
              <div class="toolbar">
                <button onclick="saveQmt()">保存 QMT</button>
                <button class="secondary" onclick="checkQmtHealth()">只读健康检查</button>
              </div>
            </section>

            <section>
              <h3>Tushare 数据源</h3>
              <div id="settings_tushare_state" class="status">读取中...</div>
              <div id="settings_tushare_current" class="hint"></div>
              <label>TUSHARE_TOKEN</label>
              <input disabled placeholder="已从工作区 .env 自动读取；不在页面回显明文" />
            </section>

            <section>
              <h3>模型供应商</h3>
              <div id="settings_model_state" class="status">读取中...</div>
              <div id="settings_model_current" class="hint"></div>
              <label>OpenAI / DeepSeek compatible key</label>
              <input disabled placeholder="已从环境变量自动读取；后续接 agent backend 配置页" />
            </section>
          </div>
        </section>

        <section class="page" id="page-output">
          <section>
            <div class="section-title-row">
              <div>
                <h3>运行输出</h3>
                <div class="status">这里保留最近一次操作、任务或接口返回，用于排查，不作为日常主页面。</div>
              </div>
              <div class="toolbar">
                <button class="secondary" onclick="show('等待操作...')">清空</button>
              </div>
            </div>
            <pre id="output">等待操作...</pre>
          </section>
        </section>
      </main>
    </div>
  </div>

  <script>
    const out = document.getElementById('output');
    const pageMeta = {
      dashboard: ['运营驾驶舱', '后台能力概览：系统健康、当前任务、最近研究和待处理策略任务。'],
      runs: ['运行中心', '面向 Agent 和脚本的 job、研究 run、策略任务状态与产物。'],
      research: ['因子研究实验室', '启动 CogAlpha 因子研究闭环，查看门禁和候选。'],
      strategy: ['策略启动向导', '手动生成策略任务和目标组合；日常监督以交易台为准。'],
      trading: ['交易台', '当前绑定策略的账户、换仓、门禁和选股过程。'],
      data: ['数据服务', '查看自动数据抓取、调度、进度、快照和日志。'],
      settings: ['设置 / 集成管理', '管理本机 Lark、QMT、交易台绑定策略、数据源和模型供应商配置。'],
      output: ['运行输出', '调试最近 API 返回、job 详情和错误消息。']
    };
    const state = {
      status: null,
      jobs: [],
      researchRuns: [],
      strategyTasks: [],
      strategies: [],
      dailyTradeReview: null,
      activeStrategy: null,
      dataService: null,
      tradingConfig: null,
      selectedStrategyId: 'earnings_forecast_drift'
    };

    function show(data) {
      out.textContent = typeof data === 'string' ? data : JSON.stringify(data, null, 2);
    }
    function esc(value) {
      return String(value ?? '').replace(
        /[&<>"']/g,
        ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[ch])
      );
    }
    function statusLabel(value) {
      return ({
        configured: '已配置',
        unconfigured: '未配置',
        success: '通过',
        failed: '失败',
        warn: '待检查',
        blocked: '阻断',
        ready: '就绪',
        running: '运行中',
        queued: '排队中',
        pending: '待执行',
        done: '完成',
        ok: '正常',
        waiting: '等待',
        missing: '缺失',
        available: '可用',
        unknown: '未知',
        no_previous_target: '无前序组合',
        missing_target: '缺目标组合',
        missing_qmt: '缺少 QMT',
        needs_probe: '待检查',
        paper_ready: '模拟盘就绪',
        research_only: '仅研究',
        factor_research: '因子研究'
      })[value] || value;
    }
    function presetName(value) {
      if (!value) return '';
      if (typeof value === 'string') return value;
      if (typeof value === 'object') return value.name || value.description || '';
      return String(value);
    }
    function strategyVersionCell(task) {
      const version = task.strategy_version || '-';
      const preset = presetName(task.preset);
      const presetLine = preset && preset !== version ? `<br><span class="status">${esc(preset)}</span>` : '';
      return `${esc(version)}${presetLine}`;
    }
    function fmtNumber(value, digits = 2) {
      const num = Number(value);
      return Number.isFinite(num) ? num.toLocaleString('zh-CN', {maximumFractionDigits: digits}) : '-';
    }
    function fmtMoney(value, digits = 0) {
      const num = Number(value);
      return Number.isFinite(num)
        ? `¥${num.toLocaleString('zh-CN', {minimumFractionDigits: digits, maximumFractionDigits: digits})}`
        : '-';
    }
    function fmtPercent(value) {
      const num = Number(value);
      return Number.isFinite(num) ? `${(num * 100).toFixed(2)}%` : '-';
    }
    function basename(path) {
      if (!path) return '-';
      return String(path).split('/').filter(Boolean).pop() || String(path);
    }
    function briefDetail(value) {
      if (!value) return '-';
      const text = String(value);
      return text.startsWith('/') ? basename(text) : text;
    }
    function nowLabel() {
      return new Date().toLocaleTimeString('zh-CN', {hour12: false});
    }
    function pill(status) {
      const value = String(status || '-');
      const cssValue = value.replace(/[^a-zA-Z0-9_-]/g, '_');
      return `<span class="pill ${esc(cssValue)}">${esc(statusLabel(value))}</span>`;
    }
    function stockCell(item) {
      const name = item?.name || '';
      const symbol = item?.symbol || '-';
      return `<td class="stock-cell"><strong>${esc(name || symbol)}</strong><span class="mono">${esc(symbol)}</span></td>`;
    }
    function sideLabel(side) {
      const value = String(side || '-');
      const label = value === 'buy' ? '买入' : value === 'sell' ? '卖出' : value;
      const cls = value === 'buy' ? 'side-buy' : value === 'sell' ? 'side-sell' : '';
      return `<span class="${cls}">${esc(label)}</span>`;
    }
    function reasonLabel(reason) {
      const value = String(reason || '-');
      const labels = {
        buy_or_increase: '买入或增持',
        rebalance_buy: '再平衡买入',
        rebalance_sell: '再平衡卖出',
        target_holding: '目标持仓',
        keep: '继续持有',
        unchanged: '保持不变'
      };
      return labels[value] || value;
    }
    function setIfClean(id, value) {
      const el = document.getElementById(id);
      if (!el || el.dataset.dirty || value === undefined || value === null || value === '') return;
      el.value = value;
    }
    function envTable(title, env, extraRows) {
      const rows = Object.entries(env || {}).filter(([, value]) => value !== '');
      const extras = extraRows || [];
      if (!rows.length && !extras.length) return '';
      return `
        <div style="margin-top:10px">
          <h4>${esc(title)}</h4>
          <table>
            <tbody>
              ${extras.map(([key, value]) => `<tr><th>${esc(key)}</th><td>${esc(value || '-')}</td></tr>`).join('')}
              ${rows.map(([key, value]) => `<tr><th>${esc(key)}</th><td>${esc(value)}</td></tr>`).join('')}
            </tbody>
          </table>
        </div>
      `;
    }
    function envDetails(title, env, extraRows) {
      const table = envTable(title, env, extraRows);
      if (!table) return '';
      return `<details class="diagnostic-details"><summary>查看${esc(title)}</summary>${table}</details>`;
    }
    async function api(path, body) {
      const res = await fetch(path, {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(body || {})
      });
      const data = await res.json();
      if (!res.ok) throw data;
      return data;
    }
    function goPage(name) {
      if (!pageMeta[name]) name = 'trading';
      document.querySelectorAll('.nav').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.page === name);
      });
      document.querySelectorAll('.page').forEach(page => {
        page.classList.toggle('active', page.id === `page-${name}`);
      });
      document.getElementById('page_title').textContent = pageMeta[name][0];
      document.getElementById('page_subtitle').textContent = pageMeta[name][1];
      if (location.hash !== `#${name}`) location.hash = name;
    }
    async function refreshAll(feedbackId, button) {
      const feedback = feedbackId ? document.getElementById(feedbackId) : null;
      if (feedback) feedback.textContent = `刷新中... ${nowLabel()}`;
      const oldButtonText = button ? button.textContent : '';
      if (button) {
        button.disabled = true;
        button.textContent = '刷新中...';
      }
      const [statusRes, jobsRes, runsRes] = await Promise.all([
        fetch('/api/status'),
        fetch('/api/jobs'),
        fetch('/api/runs')
      ]);
      state.status = await statusRes.json();
      state.jobs = (await jobsRes.json()).jobs || [];
      const runs = await runsRes.json();
      state.researchRuns = runs.research_runs || [];
      state.strategyTasks = runs.strategy_tasks || [];
      state.strategies = (state.status?.strategies || runs.strategies || []);
      state.dailyTradeReview = state.status?.daily_trade_review || null;
      state.activeStrategy = state.status?.active_strategy || null;
      state.dataService = state.status?.data_service || null;
      state.tradingConfig = state.status?.trading_config || null;
      if (!state.strategies.some(item => item.strategy_id === state.selectedStrategyId)) {
        state.selectedStrategyId = state.tradingConfig?.active_strategy_id || state.strategies[0]?.strategy_id || 'earnings_forecast_drift';
      }
      renderAll();
      if (feedback) {
        feedback.textContent = `已刷新 ${nowLabel()}；状态无变化时表示当前已是最新。`;
      }
      if (button) {
        button.textContent = `已刷新 ${nowLabel()}`;
        setTimeout(() => {
          button.disabled = false;
          button.textContent = oldButtonText || '刷新状态';
        }, 1800);
      }
      return state.status;
    }
    function renderAll() {
      renderOverview(state.status || {});
      renderJobs(
        'dashboard_job_rows',
        state.jobs.filter(job => ['queued', 'running'].includes(job.status)),
        6,
        false
      );
      renderJobs('run_center_job_rows', state.jobs, 50, true);
      renderResearchRuns('dashboard_run_rows', state.researchRuns, 5, false);
      renderResearchRuns('run_center_research_rows', state.researchRuns, 20, true);
      renderResearchLabRows();
      renderStrategyTasks('dashboard_strategy_rows', state.strategyTasks, 5, 'compact');
      renderStrategyTasks('run_center_strategy_rows', state.strategyTasks, 20, 'full');
      renderStrategyTasks('strategy_wizard_rows', state.strategyTasks, 6, 'wizard');
      renderStrategyTasks('trading_strategy_rows', state.strategyTasks, 12, 'trading');
      renderDataPage(state.dataService || {});
      renderTradingDesk(state.status || {});
      renderSettings(state.status || {});
    }
    function renderOverview(data) {
      const overview = data.overview || {};
      const provider = data.notification_provider === 'feishu' ? '飞书' : 'Lark';
      const review = data.daily_trade_review || {};
      const cards = [
        ['活动任务', overview.active_job_count ?? 0],
        ['研究运行', overview.recent_research_run_count ?? 0],
        ['策略任务', overview.pending_strategy_task_count ?? 0],
        ['通知', `${provider} / ${data.lark?.configured || data.feishu_legacy?.configured ? '已配置' : '未配置'}`],
        ['QMT', data.qmt?.health?.ok ? '绿色' : (data.qmt?.configured ? '待检查' : '未配置')],
        ['交易审查', review.state || '-']
      ];
      document.getElementById('dashboard_cards').innerHTML = cards
        .map(([label, value]) => `<div class="metric"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`)
        .join('');
      const activeConfigured = data.notification_provider === 'feishu'
        ? data.feishu_legacy?.configured
        : data.lark?.configured;
      const larkState = activeConfigured ? `${provider} 已配置，可发送移动通知。` : `${provider} 未配置，无法发送移动通知。`;
      document.getElementById('dashboard_lark_state').innerHTML =
        `${pill(activeConfigured ? 'configured' : 'unconfigured')} ${esc(larkState)}`;
    }
    function renderJobs(tbodyId, jobs, limit, verbose) {
      const body = document.getElementById(tbodyId);
      const visible = jobs.slice(0, limit);
      const colSpan = verbose ? 5 : 4;
      if (!visible.length) {
        body.innerHTML = `<tr><td colspan="${colSpan}">暂无任务</td></tr>`;
        return;
      }
      body.innerHTML = visible.map(job => `
        <tr class="clickable" data-job-id="${esc(job.job_id)}">
          <td>${esc(job.name)}<br><span class="status">${esc(job.job_id)}</span></td>
          <td>${pill(job.status)}</td>
          <td>${esc(job.stage)}</td>
          <td>${verbose ? esc(job.duration_seconds ?? '-') : esc(job.updated_at || '')}</td>
          ${verbose ? `<td>${esc(job.error?.message || '')}</td>` : ''}
        </tr>
      `).join('');
    }
    async function showJob(jobId) {
      const res = await fetch('/api/jobs/' + encodeURIComponent(jobId));
      show(await res.json());
      goPage('output');
    }
    function renderResearchRuns(tbodyId, runs, limit, verbose) {
      const body = document.getElementById(tbodyId);
      const visible = runs.slice(0, limit);
      const colSpan = verbose ? 5 : 4;
      if (!visible.length) {
        body.innerHTML = `<tr><td colspan="${colSpan}">暂无运行</td></tr>`;
        return;
      }
      body.innerHTML = visible.map(run => {
        if (verbose) {
          return `<tr class="clickable" data-run-id="${esc(run.run_id)}">
            <td>${esc(run.run_id)}<br><span class="status">${esc(run.started_at || '')}</span></td>
            <td>${pill(run.status)}</td>
            <td>${esc((run.agents || []).slice(0, 3).join(', ') || '-')}<br><span class="status">${esc(run.agent_count ?? 0)} 个 Agent</span></td>
            <td>${esc(run.quality_gate || '')}<br><span class="status">${esc(run.next_stage?.stage || '')}</span></td>
            <td>${esc(run.promoted_candidate_count ?? '-')} / ${esc(run.artifact_count ?? 0)}<br><span class="status">${esc(run.run_manifest || '')}</span></td>
          </tr>`;
        }
        return `<tr class="clickable" data-run-id="${esc(run.run_id)}">
          <td>${esc(run.run_id)}<br><span class="status">${esc(run.started_at || '')}</span></td>
          <td>${esc((run.agents || []).slice(0, 2).join(', ') || '-')}</td>
          <td>${esc(run.quality_gate || '')}<br><span class="status">${esc(run.next_stage?.stage || '')}</span></td>
          <td>${esc(run.promoted_candidate_count ?? '-')}</td>
        </tr>`;
      }).join('');
    }
    function renderResearchLabRows() {
      const body = document.getElementById('research_lab_rows');
      const visible = state.researchRuns.slice(0, 5);
      if (!visible.length) {
        body.innerHTML = '<tr><td colspan="3">暂无运行</td></tr>';
        return;
      }
      body.innerHTML = visible.map(run => `
        <tr class="clickable" data-run-id="${esc(run.run_id)}">
          <td>${esc(run.run_id)}</td>
          <td>${esc((run.agents || []).slice(0, 3).join(', ') || '-')}<br><span class="status">${esc(run.agent_count ?? 0)} 个 Agent</span></td>
          <td>${esc(run.next_stage?.stage || '-')}<br><span class="status">${esc(run.next_stage?.owner || '')}</span></td>
        </tr>
      `).join('');
    }
    function renderStrategyTasks(tbodyId, tasks, limit, mode) {
      const body = document.getElementById(tbodyId);
      const visible = tasks.slice(0, limit);
      const colSpan = (mode === 'full' || mode === 'trading') ? 4 : 3;
      if (!visible.length) {
        body.innerHTML = `<tr><td colspan="${colSpan}">暂无任务</td></tr>`;
        return;
      }
      body.innerHTML = visible.map(task => {
        if (mode === 'full') {
          return `<tr class="clickable" data-task-path="${esc(task.task_path)}">
            <td>${esc(task.trade_date || task.as_of || '-')}</td>
            <td>${strategyVersionCell(task)}</td>
            <td>${pill(task.status)}</td>
            <td>${esc(task.target_portfolio_path || '-')}</td>
          </tr>`;
        }
        if (mode === 'trading') {
          const hasHoldingCount = task.holding_count !== null &&
            task.holding_count !== undefined &&
            task.holding_count !== '' &&
            Number.isFinite(Number(task.holding_count));
          const holdingText = hasHoldingCount
            ? `${fmtNumber(task.holding_count, 0)} 只`
            : '交接包';
          return `<tr class="clickable" data-task-path="${esc(task.task_path)}">
            <td>${esc(task.trade_date || '-')}</td>
            <td>${pill(task.status)}</td>
            <td>${strategyVersionCell(task)}</td>
            <td>${esc(holdingText)}<br><span class="mono">${esc(basename(task.target_portfolio_path))}</span></td>
          </tr>`;
        }
        if (mode === 'wizard') {
          return `<tr class="clickable" data-task-path="${esc(task.task_path)}">
            <td>${esc(task.trade_date || task.as_of || '-')}</td>
            <td>${pill(task.status)}</td>
            <td>${esc(task.holding_count ?? '-')}</td>
          </tr>`;
        }
        return `<tr class="clickable" data-task-path="${esc(task.task_path)}">
          <td>${esc(task.trade_date || task.as_of || '-')}</td>
          <td>${pill(task.status)}</td>
          <td>${esc(presetName(task.preset) || '-')}</td>
        </tr>`;
      }).join('');
    }
    function dataServiceView(dataService) {
      const latestRun = dataService.latest_run || {};
      const latestSuccess = dataService.latest_success_update || {};
      const activeTask = (dataService.active_tasks || [])[0] || {};
      const progress = dataService.progress || {};
      const scheduleText = (dataService.scheduled_profiles || []).map(item => item.label || item.schedule).join('；') || '-';
      const progressText = progress.status === 'success'
        ? `全部完成：${progress.dataset_index || '-'} / ${progress.dataset_total || '-'}；累计 ${fmtNumber(progress.total_rows, 0)} 行`
        : progress.current_dataset
          ? `${progress.current_dataset}：${progress.dataset_index || '-'} / ${progress.dataset_total || '-'}；子进度 ${progress.sub_current || '-'} / ${progress.sub_total || '-'}`
          : '-';
      const activeText = activeTask.dataset
        ? `${activeTask.stage || '-'} ${activeTask.dataset}: ${activeTask.message || ''}`
        : (progress.status === 'success' ? '无，最近更新已结束' : '-');
      const status = progress.status || latestRun.status || dataService.status || 'unknown';
      return {latestRun, latestSuccess, activeTask, progress, scheduleText, progressText, activeText, status};
    }
    function renderDataPage(dataService) {
      const view = dataServiceView(dataService);
      const stateEl = document.getElementById('data_page_state');
      if (!stateEl) return;
      stateEl.innerHTML =
        `${pill(view.status)} 最新运行：${esc(view.latestRun.run_id || '-')}；` +
        `最新成功快照：${esc(view.latestSuccess.snapshot_id || '-')} / ${esc(view.latestSuccess.as_of_end || '-')}`;
      document.getElementById('data_page_metrics').innerHTML = [
        ['调度', view.scheduleText],
        ['更新进度', view.progressText],
        ['当前任务', view.activeText],
        ['日志', basename(view.progress.log_path || view.activeTask.log_path || '-')]
      ].map(([label, value], index) => `<div class="review-item ${index === 1 ? 'is-primary' : ''}"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`).join('');
      document.getElementById('data_service_state').innerHTML =
        envTable('数据服务明细', {}, [
          ['检查时间', dataService.checked_at || '-'],
          ['服务状态', dataService.status || '-'],
          ['最新运行', `${view.latestRun.run_id || '-'} / ${view.latestRun.status || '-'}`],
          ['最新成功快照', `${view.latestSuccess.snapshot_id || '-'} / ${view.latestSuccess.as_of_end || '-'}`],
          ['数据更新进度', view.progressText],
          ['当前任务', view.activeText],
          ['日志', view.progress.log_path || view.activeTask.log_path || '-']
        ]);
      const progressRows = view.progress.completed_datasets || [];
      const fill = document.getElementById('data_progress_fill');
      if (fill) {
        const done = Number(view.progress.dataset_index || view.progress.completed_dataset_count || 0);
        const total = Number(view.progress.dataset_total || done || 0);
        const pct = total > 0 ? Math.max(0, Math.min(100, (done / total) * 100)) : 0;
        fill.style.width = `${pct}%`;
      }
      document.getElementById('data_progress_rows').innerHTML = progressRows.length
        ? progressRows.slice().reverse().map(item => `<tr>
            <td>${esc(item.dataset || '-')}</td>
            <td>${fmtNumber(item.rows, 0)}</td>
            <td>${esc(item.finished_at || '-')}</td>
          </tr>`).join('')
        : '<tr><td colspan="3">暂无数据更新进度</td></tr>';
    }
    function renderTradingDesk(data) {
      const review = data.daily_trade_review || state.dailyTradeReview || {};
      const qmt = data.qmt || {};
      const active = data.active_strategy || state.activeStrategy || {};
      const tradingConfig = data.trading_config || state.tradingConfig || {};
      const selectedStrategyId = tradingConfig.active_strategy_id || state.selectedStrategyId;
      const selected = active.name ? active : (state.strategies.find(item => item.strategy_id === selectedStrategyId) || state.strategies[0] || {});
      const stateLabel = {
        missing_qmt: '缺少 QMT 配置',
        needs_probe: '待做只读检查',
        ready: '交易链路绿色',
        blocked: '交易链路阻断'
      }[review.state] || '暂无审查状态';
      document.getElementById('trading_binding_state').innerHTML =
        `${pill(selected.current?.status || selected.status || 'paper_ready')} ${esc(selected.name || tradingConfig.active_strategy_name || '-')}<br>` +
        `<span class="status">账户：${esc(tradingConfig.account_id || qmt.account_id || '-')}；版本：${esc(active.current?.strategy_version || selected.status || '-')}；单账户绑定。</span>`;
      document.getElementById('trading_review_state').innerHTML =
        `${pill(review.state === 'ready' ? 'success' : review.state === 'blocked' ? 'failed' : 'warn')} ${esc(stateLabel)}`;
      document.getElementById('trading_review_metrics').innerHTML = [
        ['策略任务', Object.values(review.status_counts || {}).reduce((sum, value) => sum + Number(value || 0), 0)],
        ['QMT 检查', review.qmt_ok === true ? '通过' : review.qmt_ok === false ? '失败' : '未检查'],
        ['检查时间', review.qmt_checked_at || '-'],
        ['人工门禁', '实盘/下单仍需确认']
      ].map(([label, value]) => `<div class="review-item"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`).join('');
      document.getElementById('trading_review_attention').innerHTML = (review.attention || [])
        .map(item => `<li>${esc(item)}</li>`)
        .join('');
      document.getElementById('strategy_selected_state').innerHTML =
        `当前选中：${esc(selected.name || '业绩预告漂移策略')}；` +
        `运行版本：${esc(active.current?.strategy_version || selected.status || 'paper_ready')}；` +
        `入口：${esc(selected.live_entry || '当前向导只支持业绩预告策略')}`;
      setIfClean('strategy_start', '20170101');
      setIfClean('strategy_as_of', active.current?.as_of || '');
      setIfClean('strategy_preset', active.current?.strategy_version || '');
      renderActiveStrategyDetails(active, data.data_service || state.dataService || {}, qmt);
      renderQmtHealth(qmt);
    }
    function renderActiveStrategyDetails(active, dataService, qmt) {
      const workflow = active.workflow || [];
      document.getElementById('strategy_workflow_rows').innerHTML = workflow.length
        ? workflow.map(step => `<tr><td>${esc(step.name)}</td><td>${pill(step.status)}</td><td class="mono">${esc(briefDetail(step.detail))}</td></tr>`).join('')
        : '<tr><td colspan="3">暂无流程状态</td></tr>';
      const funnel = active.selection_funnel || [];
      const current = active.current || {};
      const target = active.target || {};
      document.getElementById('strategy_funnel_meta').innerHTML =
        `基准日：${esc(current.as_of || '-')}；交易日：${esc(current.trade_date || target.trade_date || '-')}；` +
        `状态：${esc(current.status || '-')}；更新时间：${esc(target.updated_at || current.updated_at || '-')}`;
      document.getElementById('strategy_funnel_rows').innerHTML = funnel.length
        ? funnel.map(step => `<tr><td>${esc(step.name)}</td><td>${esc(step.count ?? '-')}</td><td>${esc(step.removed ?? '-')}</td></tr>`).join('')
        : '<tr><td colspan="3">暂无筛选数据</td></tr>';
      const positions = active.target?.positions || [];
      document.getElementById('strategy_holding_rows').innerHTML = positions.length
        ? positions.map(pos => `<tr>
            ${stockCell(pos)}
            <td>${fmtPercent(pos.target_weight)}</td>
            <td>${fmtNumber(pos.target_shares, 0)}</td>
            <td>${fmtMoney(pos.target_value)}</td>
            <td>${esc(reasonLabel(pos.reason))}</td>
          </tr>`).join('')
        : '<tr><td colspan="5">暂无持仓</td></tr>';
      const service = active.service || {};
      const dataView = dataServiceView(dataService);
      document.getElementById('trading_data_state').innerHTML =
        `${pill(dataView.status)} ${esc(dataView.progressText)}<br>` +
        `<span class="status">自动服务：${esc(service.service_status || '-')}；最近 tick：${esc(service.last_tick_at || '-')}；准备 ${esc(service.prepare_time || '-')} / 执行 ${esc(service.execute_time || '-')}</span>`;
      const nav = active.nav || {};
      const latest = nav.latest || {};
      const binding = nav.binding || {};
      const cash = qmt?.health?.cash || {};
      const qmtCheckedAt = qmt?.health?.checked_at || '';
      const hasReturnWindow = Boolean(nav.has_return_window);
      const metricNotes = nav.metric_notes || {};
      document.getElementById('strategy_nav_metrics').innerHTML = [
        ['实时总资产', fmtMoney(cash.total_asset || latest.account_total_asset, 2), 'QMT 账户全量资产'],
        ['实时可用现金', fmtMoney(cash.available_cash || latest.account_available_cash || latest.available_cash, 2), 'QMT 只读账户'],
        ['实时持仓市值', fmtMoney(cash.market_value || latest.account_market_value || latest.market_value, 2), 'QMT 只读账户'],
        ['初始资金', fmtMoney(binding.initial_equity || latest.initial_equity, 2), '策略子账本口径'],
        ['策略权益', fmtMoney(nav.latest_total_asset || latest.total_asset, 2), `净值快照 ${esc(nav.snapshot_count ?? 0)} 条`],
        ['净值', fmtNumber(nav.latest_net_value, 4), `ledger: ${esc(basename(nav.ledger_path))}`],
        ['日收益', hasReturnWindow ? fmtPercent(nav.daily_return) : '样本不足', metricNotes.daily_return || ''],
        ['最大回撤', hasReturnWindow ? fmtPercent(nav.max_drawdown) : '样本不足', metricNotes.max_drawdown || '']
      ].map(([label, value, note]) => `<div class="review-item"><span>${esc(label)}</span><strong>${esc(value)}</strong>${note ? `<em class="metric-note">${esc(note)}</em>` : ''}</div>`).join('');
      document.getElementById('strategy_nav_source').innerHTML =
        `实时账户来自 QMT 只读检查：${esc(qmtCheckedAt || '未检查')}；` +
        `策略净值快照日：${esc(latest.trade_date || '-')}；模式：${esc(binding.nav_mode || latest.nav_mode || '-')}；` +
        `外部现金偏移：${fmtMoney(latest.external_cash_offset, 2)}；` +
        `净值快照：${esc(nav.snapshot_count ?? 0)} 条`;
      const rebalance = active.live_rebalance || active.rebalance || {};
      const stalePlan = active.rebalance || {};
      const execution = active.execution || {};
      const rebalanceExplain = rebalance.source === 'qmt_realtime_vs_target'
        ? `按 QMT 实时持仓和目标组合计算；已达标股票已隐藏；QMT 检查时间 ${esc(rebalance.checked_at || '-')}`
        : (rebalance.explain || '未拿到 QMT 实时持仓，展示的是目标组合之间的差分，仅供参考。');
      document.getElementById('strategy_rebalance_state').innerHTML =
        `${pill(rebalance.status || 'unknown')} 待执行差异：${esc(rebalance.order_count ?? 0)} 笔；` +
        `买入 ${esc(rebalance.buy_count ?? 0)} / 卖出 ${esc(rebalance.sell_count ?? 0)}；` +
        `估算买入 ${fmtMoney(rebalance.estimated_buy_value)} / 卖出 ${fmtMoney(rebalance.estimated_sell_value)}<br>` +
        `<span class="status">${rebalanceExplain}</span>` +
        (stalePlan.status && rebalance.source === 'qmt_realtime_vs_target'
          ? `<br><span class="status">旧目标组合差分：${esc(stalePlan.order_count ?? 0)} 笔，只作回溯，不作为当前待执行。</span>`
          : '');
      const rebalanceOrders = rebalance.orders || [];
      document.getElementById('strategy_rebalance_rows').innerHTML = rebalanceOrders.length
        ? rebalanceOrders.map(order => `<tr>
            <td>${sideLabel(order.side)}</td>
            ${stockCell(order)}
            <td>${fmtNumber(order.current_shares ?? order.prior_shares, 0)} → ${fmtNumber(order.target_shares, 0)}<br><span class="status">待${order.side === 'buy' ? '买入' : order.side === 'sell' ? '卖出' : '调整'} ${fmtNumber(order.shares, 0)} 股</span></td>
            <td>${fmtMoney(order.estimated_value)}<br><span class="status">参考价 ${fmtNumber(order.reference_price, 2)}</span></td>
          </tr>`).join('')
        : '<tr><td colspan="4">当前 QMT 持仓与目标组合没有需要执行的股数差异，或尚未完成 QMT 只读检查。</td></tr>';
      const unfilled = execution.unfilled_summary || {};
      document.getElementById('strategy_execution_state').innerHTML =
        `最近执行日：${esc(execution.trade_date || '-')}；订单 ${esc(execution.order_count ?? 0)}；成交 ${esc(execution.fill_count ?? 0)}；` +
        `未成交股数 ${esc(unfilled.remaining_shares ?? 0)}。这块是历史报告，当前要做什么以上面的“待执行换仓”为准。`;
      const executionOrders = execution.orders || [];
      document.getElementById('strategy_execution_rows').innerHTML = executionOrders.length
        ? executionOrders.map(order => `<tr>
            <td>${sideLabel(order.side)}</td>
            ${stockCell(order)}
            <td>${fmtNumber(order.shares, 0)} 股<br><span class="status">${esc(order.reason || '-')}</span></td>
            <td>${fmtNumber(order.limit_price, 2)}<br><span class="status">${fmtMoney(order.estimated_value)}</span></td>
          </tr>`).join('')
        : '<tr><td colspan="4">暂无执行计划</td></tr>';
    }
    function renderQmtHealth(qmt) {
      const qmtText = qmt.configured
        ? `已检测到 Bridge：${qmt.bridge_url || '-'}`
        : `未配置：${(qmt.missing || []).join(', ') || '未知'}`;
      const health = qmt.health || null;
      const healthText = health
        ? `${health.ok ? '只读检查通过' : '只读检查失败'}；时间：${health.checked_at || '-'}；原因：${health.blocking_reason || '-'}`
        : '尚未在本轮控制台启动后做只读健康检查。';
      document.getElementById('trading_qmt_compact').innerHTML =
        `${pill(health?.ok ? 'success' : health ? 'failed' : qmt.configured ? 'warn' : 'unconfigured')} ${esc(healthText)}<br>` +
        `<span class="status">Bridge：${esc(qmt.bridge_url || '-')}；账户：${esc(qmt.account_id || '-')}</span>`;
      document.getElementById('trading_qmt_state').innerHTML =
        `${pill(qmt.configured ? 'configured' : 'unconfigured')} ${esc(qmtText)}<br>` +
        `<span class="status">账户：${esc(qmt.account_id || '-')}；` +
        `Token/API Key：${esc(qmt.token_configured ? '已配置' : '未配置')}（本系统视为同一个 Bridge 密钥）；来源：${esc(qmt.source || '-')}</span>`;
      document.getElementById('trading_qmt_health').innerHTML =
        `${pill(health?.ok ? 'success' : health ? 'failed' : 'warn')} ${esc(healthText)}`;
      document.getElementById('qmt_account_metrics').innerHTML = health?.cash ? [
        ['持仓数量', fmtNumber(health.position_count, 0)],
        ['委托数量', fmtNumber(health.order_count, 0)],
        ['成交数量', fmtNumber(health.fill_count, 0)],
        ['冻结资金', fmtMoney(health.cash.frozen_cash, 2)]
      ].map(([label, value]) => `<div class="review-item"><span>${esc(label)}</span><strong>${esc(value)}</strong></div>`).join('') : '';
      const positions = health?.positions || [];
      document.getElementById('qmt_position_rows').innerHTML = positions.length
        ? positions.map(pos => `<tr>
            ${stockCell(pos)}
            <td>${fmtNumber(pos.shares, 0)} / ${fmtNumber(pos.available_shares, 0)}</td>
            <td>${fmtNumber(pos.cost_price, 2)} / ${fmtNumber(pos.last_price, 2)}</td>
            <td>${fmtMoney(pos.market_value, 2)}</td>
          </tr>`).join('')
        : '<tr><td colspan="4">暂无 QMT 持仓</td></tr>';
    }
    function renderSettings(data) {
      const lark = data.lark || {};
      const feishu = data.feishu_legacy || {};
      const qmt = data.qmt || {};
      const tushare = data.tushare || {};
      const models = data.models || {};
      const tradingConfig = data.trading_config || {};
      const strategies = data.strategies || state.strategies || [];
      const provider = data.notification_provider === 'feishu' ? 'feishu' : 'lark';
      const activeConfigured = provider === 'feishu' ? feishu.configured : lark.configured;
      const providerLabel = provider === 'feishu' ? '飞书国内版' : 'Lark 国际版';
      const larkText = activeConfigured
        ? `当前启用 ${providerLabel} 通知。`
        : `当前启用 ${providerLabel}，但配置不完整。`;
      const qmtText = qmt.configured
        ? `已检测到 Bridge：${qmt.bridge_url || '-'}`
        : `未配置：${(qmt.missing || []).join(', ') || '未知'}`;
      document.getElementById('settings_active_strategy').innerHTML = strategies.length
        ? strategies.map(strategy => `<option value="${esc(strategy.strategy_id)}">${esc(strategy.name || strategy.strategy_id)}</option>`).join('')
        : '<option value="">暂无可绑定策略</option>';
      setIfClean('settings_active_strategy', tradingConfig.active_strategy_id || strategies[0]?.strategy_id || '');
      document.getElementById('settings_trading_state').innerHTML =
        `${pill(tradingConfig.active_strategy_id ? 'configured' : 'unconfigured')} 当前绑定：${esc(tradingConfig.active_strategy_name || '-')}；` +
        `账户：${esc(tradingConfig.account_id || qmt.account_id || '-')}；来源：${esc(tradingConfig.source || '-')}`;
      document.getElementById('settings_lark_state').innerHTML =
        `${pill(activeConfigured ? 'configured' : 'unconfigured')} ${esc(larkText)}`;
      document.getElementById('settings_lark_current').innerHTML =
        envDetails('Lark 国际版变量（脱敏）', lark.env, [['API Base', lark.api_base || '-']]);
      document.getElementById('settings_feishu_current').innerHTML = feishu.configured
        ? envDetails('历史飞书国内版变量（兼容保留，脱敏）', feishu.env, [])
        : '';
      document.getElementById('settings_qmt_state').innerHTML =
        `${pill(qmt.configured ? 'configured' : 'unconfigured')} ${esc(qmtText)}`;
      document.getElementById('settings_qmt_current').innerHTML =
        envDetails('当前 QMT 检测结果（脱敏）', qmt.env, [
          ['Bridge URL', qmt.bridge_url || '-'],
          ['账户 ID', qmt.account_id ? `${String(qmt.account_id).slice(0, 2)}...${String(qmt.account_id).slice(-2)}` : '-'],
          ['Token / API Key', qmt.token_configured ? '已配置，同一个 Bridge 密钥' : '未配置'],
          ['来源', qmt.source || '-'],
          ['已写入 .env', qmt.persisted ? '是' : '否']
        ]);
      document.getElementById('settings_tushare_state').innerHTML =
        `${pill(tushare.configured ? 'configured' : 'unconfigured')} ${esc(tushare.configured ? 'Tushare Token 已检测到。' : '未检测到 Tushare Token。')}`;
      document.getElementById('settings_tushare_current').innerHTML =
        envDetails('当前 Tushare 变量（脱敏）', tushare.env, [
          ['权限', tushare.permissions || '-'],
          ['积分', tushare.points || '-']
        ]);
      document.getElementById('settings_model_state').innerHTML =
        `${pill(models.configured ? 'configured' : 'unconfigured')} ${esc(models.configured ? '模型 Key/后端变量已检测到。' : '未检测到模型供应商变量。')}`;
      document.getElementById('settings_model_current').innerHTML =
        envDetails('当前模型变量（脱敏）', models.env, []);
      setIfClean('notification_provider', provider);
      setIfClean('lark_api_base', lark.api_base || 'https://open.larksuite.com');
      setIfClean('qmt_bridge_url_setting', qmt.bridge_url || '');
      setIfClean('qmt_account_id_setting', qmt.account_id || '');
      setIfClean('qmt_bridge_url', qmt.bridge_url || '');
      setIfClean('qmt_account_id', qmt.account_id || '');
    }
    function showRun(runId) {
      const run = state.researchRuns.find(item => item.run_id === runId);
      if (run) show(run);
      goPage('output');
    }
    function showTask(path) {
      const task = state.strategyTasks.find(item => item.task_path === path);
      if (task) show(task);
      goPage('output');
    }
    async function saveLark() {
      try {
        show(await api('/api/config/lark', {
          app_id: document.getElementById('lark_app_id').value,
          app_secret: document.getElementById('lark_app_secret').value,
          default_receive_id: document.getElementById('lark_receive_id').value,
          default_receive_id_type: document.getElementById('lark_receive_type').value,
          api_base: document.getElementById('lark_api_base').value
        }));
        await refreshAll();
      } catch (e) { show(e); }
    }
    async function saveNotificationProvider() {
      try {
        show(await api('/api/config/notification-provider', {
          provider: document.getElementById('notification_provider').value
        }));
        await refreshAll();
      } catch (e) { show(e); }
    }
    async function saveTradingConfig() {
      try {
        show(await api('/api/config/trading', {
          active_strategy_id: document.getElementById('settings_active_strategy').value
        }));
        await refreshAll();
        goPage('settings');
      } catch (e) { show(e); }
    }
    async function saveQmt() {
      try {
        const token = document.getElementById('qmt_bridge_token_setting').value;
        show(await api('/api/config/qmt', {
          qmt_bridge_url: document.getElementById('qmt_bridge_url_setting').value,
          qmt_bridge_token: token,
          qmt_bridge_api_key: token,
          qmt_account_id: document.getElementById('qmt_account_id_setting').value
        }));
        await refreshAll();
      } catch (e) { show(e); }
    }
    async function checkQmtHealth() {
      try {
        const result = await api('/api/qmt/health', {});
        show(result);
        await refreshAll();
        goPage('trading');
      } catch (e) { show(e); }
    }
    async function testLark() {
      try { show(await api('/api/lark/test', {text: 'Vortex 控制台测试消息'})); }
      catch (e) { show(e); }
    }
    async function runResearch() {
      try {
        const result = await api('/api/research/cogalpha-cycle', {
          run_id: document.getElementById('research_run_id').value,
          days: Number(document.getElementById('research_days').value || 220),
          symbols: Number(document.getElementById('research_symbols').value || 60),
          min_periods: Number(document.getElementById('research_min_periods').value || 30),
          groups: Number(document.getElementById('research_groups').value || 5),
          top_n: Number(document.getElementById('research_top_n').value || 10),
          notify: document.getElementById('research_notify').checked
        });
        show(result);
        await refreshAll();
        goPage('runs');
      } catch (e) { show(e); }
    }
    function strategyPayload() {
      return {
        start: document.getElementById('strategy_start').value,
        as_of: document.getElementById('strategy_as_of').value,
        preset: document.getElementById('strategy_preset').value,
        portfolio_notional: Number(document.getElementById('strategy_notional').value || 1000000),
        qmt_bridge_url: document.getElementById('qmt_bridge_url').value,
        qmt_bridge_token: document.getElementById('qmt_bridge_token').value,
        qmt_account_id: document.getElementById('qmt_account_id').value,
        allow_missing_precise_data: document.getElementById('allow_missing_precise_data').checked
      };
    }
    async function prepareStrategy(nextPage = 'runs', feedbackId = null, button = null) {
      const feedback = feedbackId ? document.getElementById(feedbackId) : null;
      const oldButtonText = button ? button.textContent : '';
      try {
        if (feedback) feedback.textContent = `正在提交生成任务... ${nowLabel()}`;
        if (button) {
          button.disabled = true;
          button.textContent = '提交中...';
        }
        const result = await api('/api/strategy/earnings-forecast/prepare', strategyPayload());
        show(result);
        await refreshAll();
        if (feedback) {
          feedback.textContent = `已提交生成任务 ${result.job?.job_id || ''}；可在运行中心查看执行结果。`;
        }
        goPage(nextPage || 'runs');
      } catch (e) {
        if (feedback) feedback.textContent = `提交失败：${e.message || e.error || '未知错误'}`;
        show(e);
      } finally {
        if (button) {
          button.disabled = false;
          button.textContent = oldButtonText || '重新筛选 / 生成目标组合';
        }
      }
    }
    async function autoOnce(allowTrading) {
      try {
        const body = strategyPayload();
        body.allow_trading = Boolean(allowTrading);
        const result = await api('/api/strategy/earnings-forecast/auto-once', body);
        show(result);
        await refreshAll();
        goPage('runs');
      } catch (e) { show(e); }
    }

    document.querySelectorAll('.nav').forEach(btn => {
      btn.addEventListener('click', () => goPage(btn.dataset.page));
    });
    document.querySelectorAll('input, select, textarea').forEach(el => {
      el.addEventListener('input', () => { el.dataset.dirty = '1'; });
    });
    document.addEventListener('click', event => {
      const strategyOption = event.target.closest('[data-strategy-id]');
      if (strategyOption) {
        state.selectedStrategyId = strategyOption.dataset.strategyId;
        renderTradingDesk(state.status || {});
        return;
      }
      const jobRow = event.target.closest('[data-job-id]');
      if (jobRow) showJob(jobRow.dataset.jobId);
      const runRow = event.target.closest('[data-run-id]');
      if (runRow) showRun(runRow.dataset.runId);
      const taskRow = event.target.closest('[data-task-path]');
      if (taskRow) showTask(taskRow.dataset.taskPath);
    });
    window.addEventListener('hashchange', () => goPage(location.hash.slice(1) || 'trading'));
    goPage(location.hash.slice(1) || 'trading');
    refreshAll().catch(show);
    setInterval(() => { refreshAll().catch(() => {}); }, 2000);
  </script>
</body>
</html>""".replace("__WORKSPACE_ROOT__", root_text)


def _read_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_csv_rows(path: Path | None) -> list[dict[str, str]]:
    if path is None or not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))
    except OSError:
        return []


def _latest_file(root: Path, pattern: str) -> Path | None:
    if not root.exists():
        return None
    paths = [path for path in root.rglob(pattern) if path.is_file()]
    if not paths:
        return None
    paths.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return paths[0]


def _mtime_iso(path_text: str | Path | None) -> str:
    if not path_text:
        return ""
    path = Path(path_text)
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
    except OSError:
        return ""


def _float_or_none(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _max_drawdown(net_values: list[float]) -> float | None:
    if not net_values:
        return None
    peak = net_values[0]
    worst = 0.0
    for value in net_values:
        if value > peak:
            peak = value
        if peak > 0:
            worst = min(worst, value / peak - 1.0)
    return worst


def _cron_label(schedule: str) -> str:
    if schedule == "0 18 * * *":
        return "每天 18:00 自动更新"
    return schedule or "-"


def _required_text(payload: dict[str, Any], key: str) -> str:
    value = _optional_text(payload.get(key))
    if value is None:
        raise ValueError(f"缺少必填字段: {key}")
    return value


def _payload_text_or_env(payload: dict[str, Any], key: str, *env_keys: str) -> str:
    value = _optional_text(payload.get(key))
    if value is not None:
        return value
    for env_key in env_keys:
        value = _optional_text(os.environ.get(env_key))
        if value is not None:
            return value
    preferred_key = env_keys[0] if env_keys else key
    raise ValueError(f"缺少必填字段: {key}，也没有配置 {preferred_key}")


def _payload_text_or_env_optional(payload: dict[str, Any], key: str, *env_keys: str) -> str | None:
    value = _optional_text(payload.get(key))
    if value is not None:
        return value
    for env_key in env_keys:
        value = _optional_text(os.environ.get(env_key))
        if value is not None:
            return value
    return None


def _qmt_bridge_url_from_env() -> str:
    return _optional_text(os.environ.get("QMT_BRIDGE_URL")) or _optional_text(
        os.environ.get("QMT_BRIDGE_BASE_URL")
    ) or ""


def _qmt_token_from_env() -> str | None:
    return _optional_text(os.environ.get("QMT_BRIDGE_TOKEN")) or _optional_text(
        os.environ.get("QMT_BRIDGE_API_KEY")
    )


def _qmt_account_from_env() -> str:
    return _optional_text(os.environ.get("QMT_ACCOUNT_ID")) or _optional_text(
        os.environ.get("QMT_BRIDGE_TRADING_ACCOUNT_ID")
    ) or ""


def _notification_provider_from_env() -> str:
    provider = str(os.environ.get("VORTEX_NOTIFICATION_PROVIDER") or "lark").strip().lower()
    return provider if provider in {"lark", "feishu"} else "lark"


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_payload(payload: dict[str, Any], key: str, default: int) -> int:
    value = payload.get(key, default)
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} 必须是整数") from exc
    if parsed <= 0:
        raise ValueError(f"{key} 必须大于 0")
    return parsed


def _masked(value: str) -> str:
    text = value.strip()
    if not text:
        return ""
    if len(text) <= 8:
        return "*" * len(text)
    return f"{text[:4]}...{text[-4:]}"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_workspace_env(root: Path) -> None:
    env_file = root / ".env"
    if not env_file.exists():
        return
    for raw_line in env_file.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _merge_env_file(env_file: Path, new_vars: dict[str, str]) -> None:
    lines: list[str] = []
    existing_keys: set[str] = set()
    if env_file.exists():
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                lines.append(raw_line)
                continue
            key = stripped.split("=", 1)[0].strip()
            if key in new_vars:
                lines.append(f"{key}={new_vars[key]}")
                existing_keys.add(key)
            else:
                lines.append(raw_line)
    for key, value in new_vars.items():
        if key not in existing_keys:
            lines.append(f"{key}={value}")
    env_file.parent.mkdir(parents=True, exist_ok=True)
    env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
