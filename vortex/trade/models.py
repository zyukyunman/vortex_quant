"""Stable trade-domain artifacts used by paper, simulated and live execution."""

from __future__ import annotations

from dataclasses import dataclass, field

from vortex.trade.broker import CashSnapshot, FillRecord, OrderIntent, OrderRecord, Position


@dataclass(frozen=True)
class Lineage:
    exec_id: str | None = None
    portfolio_id: str | None = None
    strategy_version: str | None = None
    strategy_run_id: str | None = None
    signal_snapshot_id: str | None = None
    snapshot_id: str | None = None
    gateway_type: str | None = None
    profile: str | None = None


@dataclass(frozen=True)
class TargetPosition:
    symbol: str
    target_weight: float
    target_value: float
    target_shares: int
    reference_price: float
    reason: str = ""


@dataclass(frozen=True)
class TargetPortfolio:
    portfolio_id: str
    trade_date: str
    strategy_version: str
    run_id: str
    snapshot_id: str
    cash_target: float
    positions: list[TargetPosition]
    lineage: Lineage = field(default_factory=Lineage)


@dataclass(frozen=True)
class OrderPlan:
    exec_id: str
    portfolio_id: str
    trade_date: str
    orders: list[OrderIntent]
    lineage: Lineage = field(default_factory=Lineage)


@dataclass(frozen=True)
class RiskRuleResult:
    name: str
    passed: bool
    level: str
    message: str = ""
    symbol: str | None = None


@dataclass(frozen=True)
class RiskCheckResult:
    exec_id: str
    passed: bool
    blocking_reasons: list[str]
    warnings: list[str]
    rule_results: list[RiskRuleResult]
    lineage: Lineage = field(default_factory=Lineage)


@dataclass(frozen=True)
class ExecutionReport:
    exec_id: str
    mode: str
    portfolio_id: str
    trade_date: str
    order_plan: OrderPlan
    risk_result: RiskCheckResult
    cash: CashSnapshot
    positions: list[Position]
    orders: list[OrderRecord]
    fills: list[FillRecord]
    slippage_summary: dict[str, float] = field(default_factory=dict)
    unfilled_summary: dict[str, int] = field(default_factory=dict)
    lineage: Lineage = field(default_factory=Lineage)


@dataclass(frozen=True)
class ReconcileReport:
    reconcile_id: str
    exec_id: str
    trade_date: str
    abnormal: bool
    cash_diff: float
    position_diffs: list[dict[str, object]]
    order_diffs: list[dict[str, object]]
    fill_diffs: list[dict[str, object]]
    blocking_reasons: list[str]
    lineage: Lineage = field(default_factory=Lineage)
