"""End-of-day reconciliation for paper and future QMT execution."""

from __future__ import annotations

import hashlib
from pathlib import Path

from vortex.trade.broker import CashSnapshot, FillRecord, OrderRecord, Position
from vortex.trade.models import ExecutionReport, Lineage, ReconcileReport, TargetPortfolio
from vortex.trade.serialization import write_json


def reconcile_execution(
    *,
    exec_id: str,
    portfolio: TargetPortfolio,
    expected_cash: CashSnapshot,
    actual_cash: CashSnapshot,
    expected_positions: list[Position],
    actual_positions: list[Position],
    expected_orders: list[OrderRecord],
    actual_orders: list[OrderRecord],
    expected_fills: list[FillRecord],
    actual_fills: list[FillRecord],
    cash_tolerance: float = 1.0,
    share_tolerance: int = 0,
) -> ReconcileReport:
    cash_diff = actual_cash.available_cash - expected_cash.available_cash
    position_diffs = _position_diffs(expected_positions, actual_positions, share_tolerance)
    order_diffs = _id_diffs("order_id", expected_orders, actual_orders)
    fill_diffs = _id_diffs("fill_id", expected_fills, actual_fills)
    blocking: list[str] = []
    if abs(cash_diff) > cash_tolerance:
        blocking.append("cash mismatch")
    if position_diffs:
        blocking.append("position mismatch")
    if order_diffs:
        blocking.append("order mismatch")
    if fill_diffs:
        blocking.append("fill mismatch")
    reconcile_id = _reconcile_id(exec_id, portfolio.trade_date, cash_diff, position_diffs, order_diffs, fill_diffs)
    return ReconcileReport(
        reconcile_id=reconcile_id,
        exec_id=exec_id,
        trade_date=portfolio.trade_date,
        abnormal=bool(blocking),
        cash_diff=float(cash_diff),
        position_diffs=position_diffs,
        order_diffs=order_diffs,
        fill_diffs=fill_diffs,
        blocking_reasons=blocking,
        lineage=Lineage(exec_id=exec_id, portfolio_id=portfolio.portfolio_id, snapshot_id=portfolio.snapshot_id),
    )


def write_reconcile_report(path: Path, report: ReconcileReport) -> None:
    write_json(path, report)


def reconcile_execution_report(
    report: ExecutionReport,
    *,
    actual_cash: CashSnapshot | None = None,
    actual_positions: list[Position] | None = None,
    actual_orders: list[OrderRecord] | None = None,
    actual_fills: list[FillRecord] | None = None,
    cash_tolerance: float = 1.0,
    share_tolerance: int = 0,
) -> ReconcileReport:
    """Reconcile an execution report against actual snapshots.

    Paper mode can omit actual snapshots, which compares the persisted report
    against itself and proves the artifact is internally consumable. QMT/live
    callers should pass fresh broker snapshots.
    """

    expected_cash = report.cash
    expected_positions = report.positions
    expected_orders = report.orders
    expected_fills = report.fills
    actual_cash = actual_cash or expected_cash
    actual_positions = actual_positions or expected_positions
    actual_orders = actual_orders or expected_orders
    actual_fills = actual_fills or expected_fills

    cash_diff = actual_cash.available_cash - expected_cash.available_cash
    position_diffs = _position_diffs(expected_positions, actual_positions, share_tolerance)
    order_diffs = _id_diffs("order_id", expected_orders, actual_orders)
    fill_diffs = _id_diffs("fill_id", expected_fills, actual_fills)
    blocking: list[str] = []
    if abs(cash_diff) > cash_tolerance:
        blocking.append("cash mismatch")
    if position_diffs:
        blocking.append("position mismatch")
    if order_diffs:
        blocking.append("order mismatch")
    if fill_diffs:
        blocking.append("fill mismatch")
    reconcile_id = _reconcile_id(report.exec_id, report.trade_date, cash_diff, position_diffs, order_diffs, fill_diffs)
    return ReconcileReport(
        reconcile_id=reconcile_id,
        exec_id=report.exec_id,
        trade_date=report.trade_date,
        abnormal=bool(blocking),
        cash_diff=float(cash_diff),
        position_diffs=position_diffs,
        order_diffs=order_diffs,
        fill_diffs=fill_diffs,
        blocking_reasons=blocking,
        lineage=Lineage(
            exec_id=report.exec_id,
            portfolio_id=report.portfolio_id,
            snapshot_id=report.lineage.snapshot_id,
            gateway_type=report.mode,
        ),
    )


def _position_diffs(expected: list[Position], actual: list[Position], tolerance: int) -> list[dict[str, object]]:
    expected_map = {item.symbol: item for item in expected}
    actual_map = {item.symbol: item for item in actual}
    diffs: list[dict[str, object]] = []
    for symbol in sorted(set(expected_map) | set(actual_map)):
        expected_shares = expected_map[symbol].shares if symbol in expected_map else 0
        actual_shares = actual_map[symbol].shares if symbol in actual_map else 0
        if abs(actual_shares - expected_shares) > tolerance:
            diffs.append(
                {
                    "symbol": symbol,
                    "expected_shares": expected_shares,
                    "actual_shares": actual_shares,
                    "diff_shares": actual_shares - expected_shares,
                }
            )
    return diffs


def _id_diffs(id_field: str, expected: list[object], actual: list[object]) -> list[dict[str, object]]:
    expected_ids = {str(getattr(item, id_field)) for item in expected}
    actual_ids = {str(getattr(item, id_field)) for item in actual}
    diffs: list[dict[str, object]] = []
    for missing in sorted(expected_ids - actual_ids):
        diffs.append({id_field: missing, "status": "missing_actual"})
    for extra in sorted(actual_ids - expected_ids):
        diffs.append({id_field: extra, "status": "unexpected_actual"})
    return diffs


def _reconcile_id(
    exec_id: str,
    trade_date: str,
    cash_diff: float,
    position_diffs: list[dict[str, object]],
    order_diffs: list[dict[str, object]],
    fill_diffs: list[dict[str, object]],
) -> str:
    payload = f"{exec_id}|{trade_date}|{cash_diff:.4f}|{position_diffs}|{order_diffs}|{fill_diffs}"
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:10]
    return f"rec_{trade_date}_{digest}"
