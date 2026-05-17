#!/usr/bin/env python3
"""持续监控 Vortex 数据调度服务和策略自动执行服务。

这个脚本只做健康检查和日志记录，不主动触发 `data update`。夜间数据更新
应由 `vortex server` 按 data profile 的 schedule 自动提交。
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any


def _pid_alive(pid: int | None) -> bool:
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"error": f"invalid json: {path}"}


def _fetchone(db_path: Path, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    if not db_path.exists():
        return None
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(sql, params).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _fetchall(db_path: Path, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def _parse_progress(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _server_status(root: Path) -> dict[str, Any]:
    pid_path = root / "state" / "vortex.pid"
    pid = None
    if pid_path.exists():
        try:
            pid = int(pid_path.read_text(encoding="utf-8").strip())
        except ValueError:
            pid = None
    return {
        "pid": pid,
        "pid_alive": _pid_alive(pid),
        "pid_path": str(pid_path),
    }


def _scheduled_profiles(root: Path) -> list[dict[str, str]]:
    profiles_dir = root / "profiles"
    results: list[dict[str, str]] = []
    for path in sorted(profiles_dir.glob("*.yaml")):
        text = path.read_text(encoding="utf-8", errors="replace")
        schedule = None
        name = path.stem
        for line in text.splitlines():
            if line.startswith("name:"):
                name = line.split(":", 1)[1].strip().strip("'\"") or name
            if line.startswith("schedule:"):
                schedule = line.split(":", 1)[1].strip().strip("'\"")
        if schedule:
            results.append({"name": name, "schedule": schedule})
    return results


def _data_status(root: Path, profile: str) -> dict[str, Any]:
    manifest_db = root / "state" / "manifests" / profile / "sync_manifest.db"
    control_db = root / "state" / "control.db"
    latest_run = _fetchone(
        manifest_db,
        "SELECT * FROM sync_manifest WHERE profile = ? ORDER BY started_at DESC LIMIT 1",
        (profile,),
    )
    latest_success_update = _fetchone(
        manifest_db,
        """SELECT * FROM sync_manifest
           WHERE profile = ? AND action = 'update' AND status IN ('success', 'partial_success')
           ORDER BY finished_at DESC LIMIT 1""",
        (profile,),
    )
    latest_snapshot = _fetchone(
        manifest_db,
        """SELECT * FROM snapshot_descriptors
           WHERE profile = ? AND status = 'published'
           ORDER BY created_at DESC LIMIT 1""",
        (profile,),
    )
    active_tasks = _fetchall(
        control_db,
        """SELECT * FROM task_queue
           WHERE domain = 'data' AND profile = ? AND status IN ('pending', 'running')
           ORDER BY created_at DESC""",
        (profile,),
    )
    active_payloads = []
    for task in active_tasks:
        progress = _parse_progress(task.get("progress_json"))
        pid = progress.get("pid")
        active_payloads.append(
            {
                "task_id": task.get("task_id"),
                "action": task.get("action"),
                "status": task.get("status"),
                "run_id": task.get("run_id"),
                "updated_at": task.get("updated_at"),
                "pid": pid,
                "pid_alive": _pid_alive(int(pid)) if isinstance(pid, int) else False,
                "stage": progress.get("current_stage"),
                "dataset": progress.get("current_dataset"),
                "message": progress.get("message"),
                "log_path": progress.get("log_path"),
            }
        )
    return {
        "latest_run": latest_run,
        "latest_success_update": latest_success_update,
        "latest_snapshot": latest_snapshot,
        "active_tasks": active_payloads,
    }


def _strategy_status(root: Path) -> dict[str, Any]:
    path = root / "state" / "strategy" / "earnings_forecast_auto" / "status.json"
    payload = _read_json(path) or {}
    pid = payload.get("pid")
    return {
        "path": str(path),
        "service_status": payload.get("service_status"),
        "pid": pid,
        "pid_alive": _pid_alive(int(pid)) if isinstance(pid, int) else False,
        "last_tick_status": payload.get("last_tick_status"),
        "last_error": payload.get("last_error"),
        "last_tick": payload.get("last_tick"),
    }


def _date_prefix(value: str | None) -> str | None:
    if not value:
        return None
    return value[:10]


def collect_health(root: Path, profile: str, data_grace_hours: int) -> dict[str, Any]:
    now = datetime.now()
    server = _server_status(root)
    data = _data_status(root, profile)
    strategy = _strategy_status(root)
    scheduled = _scheduled_profiles(root)
    alerts: list[str] = []

    if not server["pid_alive"]:
        alerts.append("vortex server 未运行，data profile 的 schedule 不会自动提交 data update")
    if not scheduled:
        alerts.append("没有配置 schedule 的 data profile，夜间自动拉数不会触发")

    latest_run = data.get("latest_run") or {}
    if latest_run.get("status") == "running" and not data.get("active_tasks"):
        alerts.append("sync_manifest 存在 running 记录，但 task_queue 没有活跃 data worker")

    for task in data.get("active_tasks") or []:
        if task.get("status") == "running" and not task.get("pid_alive"):
            alerts.append(f"data task {task.get('task_id')} 是 running，但 worker pid 不存在")

    latest_success = data.get("latest_success_update") or {}
    finished_date = _date_prefix(latest_success.get("finished_at"))
    if now.hour >= 18 + data_grace_hours and finished_date != now.strftime("%Y-%m-%d"):
        alerts.append(
            f"已过 {18 + data_grace_hours}:00，但今天还没有成功的 data update"
        )

    if strategy.get("service_status") != "running" or not strategy.get("pid_alive"):
        alerts.append("earnings-forecast auto-run 服务未运行")
    elif strategy.get("last_tick_status") not in {None, "success"}:
        alerts.append(f"earnings-forecast auto-run 最近 tick 异常: {strategy.get('last_tick_status')}")

    status = "ok" if not alerts else "alert"
    return {
        "checked_at": now.isoformat(timespec="seconds"),
        "status": status,
        "alerts": alerts,
        "server": server,
        "scheduled_profiles": scheduled,
        "data": data,
        "strategy": strategy,
    }


def write_health(root: Path, payload: dict[str, Any]) -> None:
    log_dir = root / "state" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = log_dir / "live-service-health.jsonl"
    text_path = log_dir / "live-service-health.log"
    latest_path = root / "state" / "live-service-health-latest.json"

    line = json.dumps(payload, ensure_ascii=False, default=str)
    with jsonl_path.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")
    latest_path.write_text(line + "\n", encoding="utf-8")

    with text_path.open("a", encoding="utf-8") as fh:
        fh.write(f"[{payload['checked_at']}] {payload['status']}\n")
        for alert in payload.get("alerts") or []:
            fh.write(f"  ALERT: {alert}\n")
        data = payload.get("data") or {}
        success = data.get("latest_success_update") or {}
        snapshot = data.get("latest_snapshot") or {}
        fh.write(
            "  server_pid={pid} alive={alive}; latest_success_update={run} "
            "finished_at={finished}; snapshot_as_of={as_of}; strategy={strategy}/{tick}\n".format(
                pid=(payload.get("server") or {}).get("pid"),
                alive=(payload.get("server") or {}).get("pid_alive"),
                run=success.get("run_id"),
                finished=success.get("finished_at"),
                as_of=snapshot.get("as_of"),
                strategy=(payload.get("strategy") or {}).get("service_status"),
                tick=(payload.get("strategy") or {}).get("last_tick_status"),
            )
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="监控 Vortex data server 和策略自动服务")
    parser.add_argument("--root", default=str(Path.home() / "Documents/vortex_workspace"))
    parser.add_argument("--profile", default="default")
    parser.add_argument("--interval", type=int, default=300)
    parser.add_argument("--data-grace-hours", type=int, default=2)
    parser.add_argument("--pid-file", help="写入监控进程自身 PID 的文件路径")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    if args.pid_file:
        pid_file = Path(args.pid_file).expanduser().resolve()
        pid_file.parent.mkdir(parents=True, exist_ok=True)
        pid_file.write_text(f"{os.getpid()}\n", encoding="utf-8")

    stop = False

    def _handle_stop(_signum: int, _frame: Any) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    while not stop:
        payload = collect_health(root, args.profile, max(args.data_grace_hours, 0))
        write_health(root, payload)
        print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)
        if args.once:
            break
        time.sleep(max(args.interval, 30))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
