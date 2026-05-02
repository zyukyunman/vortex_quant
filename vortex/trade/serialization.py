"""JSON serialization helpers for immutable trade artifacts."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from vortex.trade.broker import CashSnapshot, FillRecord, OrderIntent, OrderRecord, Position
from vortex.trade.models import (
    ExecutionReport,
    Lineage,
    OrderPlan,
    ReconcileReport,
    RiskCheckResult,
    RiskRuleResult,
    TargetPortfolio,
    TargetPosition,
)


def to_plain(value: Any) -> Any:
    if is_dataclass(value):
        return {key: to_plain(item) for key, item in asdict(value).items()}
    if isinstance(value, list):
        return [to_plain(item) for item in value]
    if isinstance(value, dict):
        return {str(key): to_plain(item) for key, item in value.items()}
    if isinstance(value, Path):
        return str(value)
    return value


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_plain(value), ensure_ascii=False, indent=2), encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def lineage_from_dict(data: dict[str, Any] | None) -> Lineage:
    return Lineage(**(data or {}))


def target_position_from_dict(data: dict[str, Any]) -> TargetPosition:
    return TargetPosition(**data)


def target_portfolio_from_dict(data: dict[str, Any]) -> TargetPortfolio:
    return TargetPortfolio(
        portfolio_id=data["portfolio_id"],
        trade_date=data["trade_date"],
        strategy_version=data["strategy_version"],
        run_id=data["run_id"],
        snapshot_id=data["snapshot_id"],
        cash_target=float(data["cash_target"]),
        positions=[target_position_from_dict(item) for item in data["positions"]],
        lineage=lineage_from_dict(data.get("lineage")),
    )


def order_intent_from_dict(data: dict[str, Any]) -> OrderIntent:
    return OrderIntent(**data)


def order_plan_from_dict(data: dict[str, Any]) -> OrderPlan:
    return OrderPlan(
        exec_id=data["exec_id"],
        portfolio_id=data["portfolio_id"],
        trade_date=data["trade_date"],
        orders=[order_intent_from_dict(item) for item in data["orders"]],
        lineage=lineage_from_dict(data.get("lineage")),
    )


def risk_rule_result_from_dict(data: dict[str, Any]) -> RiskRuleResult:
    return RiskRuleResult(**data)


def risk_check_result_from_dict(data: dict[str, Any]) -> RiskCheckResult:
    return RiskCheckResult(
        exec_id=data["exec_id"],
        passed=bool(data["passed"]),
        blocking_reasons=list(data["blocking_reasons"]),
        warnings=list(data["warnings"]),
        rule_results=[risk_rule_result_from_dict(item) for item in data["rule_results"]],
        lineage=lineage_from_dict(data.get("lineage")),
    )


def cash_snapshot_from_dict(data: dict[str, Any]) -> CashSnapshot:
    return CashSnapshot(**data)


def position_from_dict(data: dict[str, Any]) -> Position:
    return Position(**data)


def order_record_from_dict(data: dict[str, Any]) -> OrderRecord:
    return OrderRecord(
        order_id=data["order_id"],
        intent=order_intent_from_dict(data["intent"]),
        status=data["status"],
        filled_shares=int(data["filled_shares"]),
        remaining_shares=int(data["remaining_shares"]),
        avg_fill_price=data.get("avg_fill_price"),
        message=data.get("message", ""),
        created_at=data["created_at"],
    )


def fill_record_from_dict(data: dict[str, Any]) -> FillRecord:
    return FillRecord(**data)


def execution_report_from_dict(data: dict[str, Any]) -> ExecutionReport:
    return ExecutionReport(
        exec_id=data["exec_id"],
        mode=data["mode"],
        portfolio_id=data["portfolio_id"],
        trade_date=data["trade_date"],
        order_plan=order_plan_from_dict(data["order_plan"]),
        risk_result=risk_check_result_from_dict(data["risk_result"]),
        cash=cash_snapshot_from_dict(data["cash"]),
        positions=[position_from_dict(item) for item in data["positions"]],
        orders=[order_record_from_dict(item) for item in data["orders"]],
        fills=[fill_record_from_dict(item) for item in data["fills"]],
        slippage_summary=dict(data.get("slippage_summary", {})),
        unfilled_summary=dict(data.get("unfilled_summary", {})),
        lineage=lineage_from_dict(data.get("lineage")),
    )


def reconcile_report_from_dict(data: dict[str, Any]) -> ReconcileReport:
    return ReconcileReport(
        reconcile_id=data["reconcile_id"],
        exec_id=data["exec_id"],
        trade_date=data["trade_date"],
        abnormal=bool(data["abnormal"]),
        cash_diff=float(data["cash_diff"]),
        position_diffs=list(data["position_diffs"]),
        order_diffs=list(data["order_diffs"]),
        fill_diffs=list(data["fill_diffs"]),
        blocking_reasons=list(data["blocking_reasons"]),
        lineage=lineage_from_dict(data.get("lineage")),
    )
