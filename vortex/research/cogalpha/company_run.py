"""Company-level CogAlpha research run orchestration."""
from __future__ import annotations

import json
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from vortex.research.cogalpha.demo import build_demo_daily_inputs
from vortex.research.cogalpha.fitness import CogAlphaFitnessRule
from vortex.research.cogalpha.quality import QualityGateRule
from vortex.research.cogalpha.research_cycle import (
    CogAlphaResearchCycleConfig,
    run_cogalpha_research_cycle,
)
from vortex.runtime.workspace import Workspace

RUN_MANIFEST_SCHEMA = "vortex.company_research_run_manifest.v1"


def run_cogalpha_company_demo_cycle(
    root: str | Path,
    *,
    output_dir: str | Path | None = None,
    run_id: str | None = None,
    days: int = 220,
    symbols: int = 60,
    min_periods: int = 30,
    groups: int = 5,
    top_n: int = 10,
    notify: bool = False,
    notification_config: dict[str, object] | None = None,
) -> dict[str, object]:
    """Run the deterministic CogAlpha demo as an auditable company research run."""

    workspace = Workspace(Path(root).expanduser())
    workspace.initialize()

    resolved_run_id = run_id or _new_run_id()
    output = (
        Path(output_dir).expanduser()
        if output_dir is not None
        else workspace.research_dir / "cogalpha" / "company_runs" / resolved_run_id
    )
    output.mkdir(parents=True, exist_ok=True)

    started_at = _now()
    started = time.time()
    tasks: list[dict[str, object]] = []
    notification_payload: dict[str, object] = {
        "enabled": notify,
        "channel": _notification_channel(notification_config) if notify else None,
        "mode": "outbound_only",
        "deliveries": [],
    }

    try:
        inputs = build_demo_daily_inputs(days=days, symbols=symbols)
        input_summary = {
            "input_type": "deterministic_synthetic_ohlcv",
            "days": len(inputs.close.index),
            "symbols": len(inputs.close.columns),
            "note": "确定性演示数据只用于验证研发流程，不能作为 A 股收益结论。",
        }
        tasks.append(
            _task(
                "prepare_demo_inputs",
                "准备确定性演示数据",
                "success",
                detail=input_summary,
            )
        )

        config = CogAlphaResearchCycleConfig(
            output_dir=output,
            quality_rule=QualityGateRule(min_valid_dates=40, min_valid_symbols=40),
            fitness_rule=CogAlphaFitnessRule(min_rank_icir=0.0),
            min_periods=min_periods,
            groups=groups,
            top_n=top_n,
            input_type="deterministic_synthetic_ohlcv",
            input_note=input_summary["note"],
        )
        result = run_cogalpha_research_cycle(inputs, config=config)
        tasks.append(
            _task(
                "run_cogalpha_research_cycle",
                "运行 CogAlpha 因子研究循环",
                "success",
                detail={
                    "recipe_count": result["summary"]["recipe_count"],
                    "decision_counts": result["summary"]["decision_counts"],
                },
            )
        )

        manifest = _build_success_manifest(
            workspace=workspace,
            run_id=resolved_run_id,
            output_dir=output,
            started_at=started_at,
            duration_seconds=time.time() - started,
            input_summary=input_summary,
            tasks=tasks,
            result=result,
            notification=notification_payload,
        )
        manifest_path = output / "run_manifest.json"
        _write_json(manifest_path, manifest)

        if notify:
            deliveries = _send_research_notification(
                workspace=workspace,
                manifest=manifest,
                manifest_path=manifest_path,
                notification_config=notification_config,
            )
            manifest["notification"]["deliveries"] = deliveries
            _write_json(manifest_path, manifest)

        return manifest
    except Exception as exc:  # noqa: BLE001
        tasks.append(
            _task(
                "run_cogalpha_company_demo_cycle",
                "公司级 CogAlpha 研究运行",
                "failed",
                error=str(exc),
            )
        )
        manifest = _build_failure_manifest(
            workspace=workspace,
            run_id=resolved_run_id,
            output_dir=output,
            started_at=started_at,
            duration_seconds=time.time() - started,
            input_summary={
                "input_type": "deterministic_synthetic_ohlcv",
                "days": days,
                "symbols": symbols,
                "note": "运行失败，输入摘要可能未完全生成。",
            },
            tasks=tasks,
            error=str(exc),
            notification=notification_payload,
        )
        _write_json(output / "run_manifest.json", manifest)
        return manifest


def _build_success_manifest(
    *,
    workspace: Workspace,
    run_id: str,
    output_dir: Path,
    started_at: str,
    duration_seconds: float,
    input_summary: dict[str, object],
    tasks: list[dict[str, object]],
    result: dict[str, object],
    notification: dict[str, object],
) -> dict[str, object]:
    summary = dict(result["summary"])
    decision_counts = dict(summary.get("decision_counts", {}))
    promoted_count = int(decision_counts.get("qualified", 0)) + int(
        decision_counts.get("elite", 0)
    )
    gate_status = "passed_with_candidates" if promoted_count > 0 else "needs_review"
    artifacts = _artifact_map(
        {
            "generation_report": result["report_path"],
            "generation_summary": result["summary_path"],
            "research_cycle": result["cycle_path"],
        }
    )
    artifacts["run_manifest"] = {
        "path": str(output_dir / "run_manifest.json"),
        "schema": RUN_MANIFEST_SCHEMA,
        "kind": "run_manifest",
    }
    return {
        "schema": RUN_MANIFEST_SCHEMA,
        "run": {
            "run_id": run_id,
            "kind": "research.cogalpha_cycle",
            "status": "success",
            "started_at": started_at,
            "finished_at": _now(),
            "duration_seconds": round(duration_seconds, 3),
            "workspace": str(workspace.root),
            "output_dir": str(output_dir),
        },
        "input": input_summary,
        "tasks": [*tasks, _task("write_run_manifest", "写入公司级运行清单", "success")],
        "artifacts": artifacts,
        "quality_gate": {
            "status": gate_status,
            "decision_counts": decision_counts,
            "promoted_candidate_count": promoted_count,
            "top_candidates": summary.get("top_candidates", []),
        },
        "decision": {
            "approval_required": True,
            "default_action": "review_required",
            "allowed_actions": [
                "批准进入真实数据复验",
                "驳回本轮候选",
                "追问研究智能体",
            ],
            "blocked_actions": [
                "直接进入实盘",
                "直接下单",
                "跳过 signal snapshot 晋升",
            ],
        },
        "next_generation_queue": result["cycle"].get("next_generation_queue", []),
        "notification": notification,
    }


def _build_failure_manifest(
    *,
    workspace: Workspace,
    run_id: str,
    output_dir: Path,
    started_at: str,
    duration_seconds: float,
    input_summary: dict[str, object],
    tasks: list[dict[str, object]],
    error: str,
    notification: dict[str, object],
) -> dict[str, object]:
    return {
        "schema": RUN_MANIFEST_SCHEMA,
        "run": {
            "run_id": run_id,
            "kind": "research.cogalpha_cycle",
            "status": "failed",
            "started_at": started_at,
            "finished_at": _now(),
            "duration_seconds": round(duration_seconds, 3),
            "workspace": str(workspace.root),
            "output_dir": str(output_dir),
        },
        "input": input_summary,
        "tasks": tasks,
        "artifacts": {
            "run_manifest": {
                "path": str(output_dir / "run_manifest.json"),
                "schema": RUN_MANIFEST_SCHEMA,
                "kind": "run_manifest",
            }
        },
        "quality_gate": {
            "status": "failed",
            "decision_counts": {},
            "promoted_candidate_count": 0,
            "top_candidates": [],
        },
        "decision": {
            "approval_required": True,
            "default_action": "incident_review",
            "allowed_actions": ["追问研究智能体", "重试运行", "查看错误日志"],
            "blocked_actions": ["进入真实数据复验", "进入实盘", "直接下单"],
        },
        "next_generation_queue": [],
        "notification": notification,
        "error": error,
    }


def _artifact_map(paths: dict[str, object]) -> dict[str, object]:
    artifacts: dict[str, object] = {}
    for name, raw_path in paths.items():
        path = Path(str(raw_path))
        artifacts[name] = {
            "path": str(path),
            "schema": _read_schema(path),
            "kind": name,
            "exists": path.exists(),
        }
    return artifacts


def _read_schema(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    schema = payload.get("schema")
    return str(schema) if schema else None


def _send_research_notification(
    *,
    workspace: Workspace,
    manifest: dict[str, object],
    manifest_path: Path,
    notification_config: dict[str, object] | None,
) -> list[dict[str, object]]:
    from vortex.notification.models import NotificationMessage
    from vortex.notification.service import NotificationService
    from vortex.runtime.database import Database

    db = Database(workspace.db_path)
    db.initialize_tables()
    try:
        service = NotificationService(db)
        run = dict(manifest["run"])
        quality_gate = dict(manifest["quality_gate"])
        config = notification_config or {
            "enabled": True,
            "level": "info",
            "channel": "lark",
        }
        message = NotificationMessage(
            event_type="research.cogalpha_cycle.completed",
            notification_type="research_result",
            severity="info",
            title="CogAlpha 因子研究运行完成",
            summary=(
                "确定性演示数据闭环已完成，"
                f"候选通过数 {quality_gate.get('promoted_candidate_count', 0)}。"
            ),
            impact="这只证明研发流程可追踪，不代表真实 A 股收益结论。",
            suggested_actions=(
                "查看 run_manifest.json",
                "决定是否进入真实数据复验",
                "不要直接推进实盘",
            ),
            run_id=str(run["run_id"]),
            detail={"manifest_path": str(manifest_path)},
        )
        return service.notify(message, config)
    finally:
        db.close()


def _notification_channel(config: dict[str, object] | None) -> str:
    if isinstance(config, dict) and config.get("channel"):
        return str(config["channel"])
    return "lark"


def _task(
    task_id: str,
    name: str,
    status: str,
    *,
    detail: dict[str, object] | None = None,
    error: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "task_id": task_id,
        "name": name,
        "status": status,
        "updated_at": _now(),
    }
    if detail:
        payload["detail"] = detail
    if error:
        payload["error"] = error
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _new_run_id() -> str:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    suffix = uuid.uuid4().hex[:8]
    return f"research-cogalpha-demo-{stamp}-{suffix}"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")
