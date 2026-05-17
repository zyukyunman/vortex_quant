"""Paper / QMT rebalance orchestration and execution report writing."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from vortex.trade.broker import BrokerHealth, CashSnapshot, OrderRecord, PaperBrokerAdapter, Quote
from vortex.trade.models import ExecutionReport, Lineage, OrderPlan, RiskCheckResult, RiskRuleResult, TargetPortfolio
from vortex.trade.order_plan import OrderPlanConfig, generate_order_plan
from vortex.trade.qmt_bridge import QmtBridgeAdapter, QmtBridgeConfig
from vortex.trade.risk import PreTradeRiskConfig, run_pre_trade_risk_check
from vortex.trade.serialization import write_json


@dataclass(frozen=True)
class PaperRebalanceArtifacts:
    exec_id: str
    root_dir: Path
    order_intent_path: Path
    order_plan_path: Path
    risk_result_path: Path
    execution_report_path: Path
    execution_report_md_path: Path
    report: ExecutionReport


@dataclass(frozen=True)
class QmtRebalanceArtifacts:
    exec_id: str
    root_dir: Path
    order_intent_path: Path
    order_plan_path: Path
    risk_result_path: Path
    execution_report_path: Path
    execution_report_md_path: Path
    report: ExecutionReport


def run_paper_rebalance(
    portfolio: TargetPortfolio,
    *,
    broker: PaperBrokerAdapter,
    quotes: list[Quote],
    output_root: Path,
    st_flags: dict[str, bool] | None,
    order_config: OrderPlanConfig | None = None,
    risk_config: PreTradeRiskConfig | None = None,
) -> PaperRebalanceArtifacts:
    """Run a full local paper rebalance and persist all audit artifacts."""

    broker.update_quotes(quotes)
    quote_map = {quote.symbol: quote for quote in quotes}
    plan = generate_order_plan(
        portfolio,
        cash=broker.get_cash(),
        positions=broker.get_positions(),
        quotes=quote_map,
        config=order_config,
    )
    risk = run_pre_trade_risk_check(
        plan,
        health=broker.health(),
        cash=broker.get_cash(),
        quotes=quote_map,
        st_flags=st_flags,
        config=risk_config,
    )
    exec_dir = output_root / "trade" / "executions" / plan.exec_id
    state_dir = output_root / "state" / "trade" / plan.exec_id
    order_intent_path = state_dir / "order_intent.json"
    order_plan_path = exec_dir / "order_plan.json"
    risk_result_path = exec_dir / "pre_trade_result.json"
    report_path = exec_dir / "execution_report.json"
    report_md_path = exec_dir / "execution_report.md"

    write_json(order_plan_path, plan)
    write_json(risk_result_path, risk)
    write_json(order_intent_path, plan.orders)

    if risk.passed:
        for order in plan.orders:
            broker.submit_order(order)

    report = ExecutionReport(
        exec_id=plan.exec_id,
        mode="paper",
        portfolio_id=portfolio.portfolio_id,
        trade_date=portfolio.trade_date,
        order_plan=plan,
        risk_result=risk,
        cash=broker.get_cash(),
        positions=broker.get_positions(),
        orders=broker.get_orders(),
        fills=broker.get_fills(),
        slippage_summary=_slippage_summary(broker.get_fills(), plan.orders),
        unfilled_summary=_unfilled_summary(broker.get_orders()),
        lineage=plan.lineage,
    )
    write_json(report_path, report)
    _write_markdown_report(report_md_path, report)
    return PaperRebalanceArtifacts(
        exec_id=plan.exec_id,
        root_dir=exec_dir,
        order_intent_path=order_intent_path,
        order_plan_path=order_plan_path,
        risk_result_path=risk_result_path,
        execution_report_path=report_path,
        execution_report_md_path=report_md_path,
        report=report,
    )


def run_qmt_rebalance(
    portfolio: TargetPortfolio,
    *,
    bridge_config: QmtBridgeConfig,
    output_root: Path,
    st_flags: dict[str, bool] | None,
    order_config: OrderPlanConfig | None = None,
    risk_config: PreTradeRiskConfig | None = None,
) -> QmtRebalanceArtifacts:
    """Run a QMT bridge rebalance and persist all audit artifacts.

    第一版保持最小职责：
    1. 从 bridge 读取 cash / positions / quotes；
    2. 生成 sell-first order plan；
    3. 做 fail-closed pre-trade risk；
    4. 风控通过后逐笔 submit_order；
    5. 立即写 execution report，盘后再由 reconcile 补最终核对。
    """

    adapter = QmtBridgeAdapter(bridge_config)
    try:
        health = adapter.health()
    except Exception as exc:  # noqa: BLE001 - bridge failures must still leave an audit artifact.
        return _write_qmt_blocked_report(
            portfolio,
            bridge_config=bridge_config,
            output_root=output_root,
            health=BrokerHealth(ok=False, mode="qmt_bridge", message=str(exc)),
            reason=f"bridge health check failed: {exc}",
        )
    if not health.ok:
        return _write_qmt_blocked_report(
            portfolio,
            bridge_config=bridge_config,
            output_root=output_root,
            health=health,
            reason=f"bridge health failed: {health.message}",
        )

    cash = adapter.get_cash()
    positions = adapter.get_positions()
    symbols = sorted(
        {
            *(item.symbol for item in portfolio.positions),
            *(item.symbol for item in positions),
        }
    )
    quotes = adapter.get_quotes(symbols)
    plan = generate_order_plan(
        portfolio,
        cash=cash,
        positions=positions,
        quotes=quotes,
        config=order_config,
    )
    risk = run_pre_trade_risk_check(
        plan,
        health=health,
        cash=cash,
        quotes=quotes,
        st_flags=st_flags,
        config=risk_config,
    )
    exec_dir = output_root / "trade" / "executions" / plan.exec_id
    state_dir = output_root / "state" / "trade" / plan.exec_id
    order_intent_path = state_dir / "order_intent.json"
    order_plan_path = exec_dir / "order_plan.json"
    risk_result_path = exec_dir / "pre_trade_result.json"
    report_path = exec_dir / "execution_report.json"
    report_md_path = exec_dir / "execution_report.md"

    write_json(order_plan_path, plan)
    write_json(risk_result_path, risk)
    write_json(order_intent_path, plan.orders)

    submitted_orders: list[OrderRecord] = []
    if risk.passed and bridge_config.allow_trading:
        for order in plan.orders:
            submitted_orders.append(adapter.submit_order(order))

    latest_cash = adapter.get_cash()
    latest_positions = adapter.get_positions()
    latest_orders = _merge_order_records(adapter.get_orders(), submitted_orders)
    latest_fills = adapter.get_fills()
    report = ExecutionReport(
        exec_id=plan.exec_id,
        mode="qmt_sim" if bridge_config.allow_trading else "qmt_dry_run",
        portfolio_id=portfolio.portfolio_id,
        trade_date=portfolio.trade_date,
        order_plan=plan,
        risk_result=risk,
        cash=latest_cash,
        positions=latest_positions,
        orders=latest_orders,
        fills=latest_fills,
        slippage_summary=_slippage_summary(latest_fills, plan.orders),
        unfilled_summary=_unfilled_summary(latest_orders),
        lineage=plan.lineage,
    )
    write_json(report_path, report)
    _write_markdown_report(report_md_path, report)
    return QmtRebalanceArtifacts(
        exec_id=plan.exec_id,
        root_dir=exec_dir,
        order_intent_path=order_intent_path,
        order_plan_path=order_plan_path,
        risk_result_path=risk_result_path,
        execution_report_path=report_path,
        execution_report_md_path=report_md_path,
        report=report,
    )


def _write_qmt_blocked_report(
    portfolio: TargetPortfolio,
    *,
    bridge_config: QmtBridgeConfig,
    output_root: Path,
    health: BrokerHealth,
    reason: str,
) -> QmtRebalanceArtifacts:
    exec_id = _blocked_exec_id(portfolio, reason)
    lineage = Lineage(
        exec_id=exec_id,
        portfolio_id=portfolio.portfolio_id,
        strategy_version=portfolio.strategy_version,
        strategy_run_id=portfolio.run_id,
        snapshot_id=portfolio.snapshot_id,
        gateway_type="qmt_bridge",
    )
    plan = OrderPlan(
        exec_id=exec_id,
        portfolio_id=portfolio.portfolio_id,
        trade_date=portfolio.trade_date,
        orders=[],
        lineage=lineage,
    )
    risk = RiskCheckResult(
        exec_id=exec_id,
        passed=False,
        blocking_reasons=[reason],
        warnings=[],
        rule_results=[
            RiskRuleResult(
                name="broker_health",
                passed=False,
                level="critical",
                message=health.message,
            )
        ],
        lineage=lineage,
    )
    exec_dir = output_root / "trade" / "executions" / exec_id
    state_dir = output_root / "state" / "trade" / exec_id
    order_intent_path = state_dir / "order_intent.json"
    order_plan_path = exec_dir / "order_plan.json"
    risk_result_path = exec_dir / "pre_trade_result.json"
    report_path = exec_dir / "execution_report.json"
    report_md_path = exec_dir / "execution_report.md"

    write_json(order_plan_path, plan)
    write_json(risk_result_path, risk)
    write_json(order_intent_path, [])
    report = ExecutionReport(
        exec_id=exec_id,
        mode="qmt_sim" if bridge_config.allow_trading else "qmt_dry_run",
        portfolio_id=portfolio.portfolio_id,
        trade_date=portfolio.trade_date,
        order_plan=plan,
        risk_result=risk,
        cash=CashSnapshot(available_cash=0.0, frozen_cash=0.0, total_asset=0.0, market_value=0.0),
        positions=[],
        orders=[],
        fills=[],
        slippage_summary={},
        unfilled_summary={},
        lineage=lineage,
    )
    write_json(report_path, report)
    _write_markdown_report(report_md_path, report)
    return QmtRebalanceArtifacts(
        exec_id=exec_id,
        root_dir=exec_dir,
        order_intent_path=order_intent_path,
        order_plan_path=order_plan_path,
        risk_result_path=risk_result_path,
        execution_report_path=report_path,
        execution_report_md_path=report_md_path,
        report=report,
    )


def _blocked_exec_id(portfolio: TargetPortfolio, reason: str) -> str:
    digest = hashlib.sha1(f"{portfolio.portfolio_id}|{portfolio.trade_date}|{reason}".encode("utf-8")).hexdigest()[:10]
    return f"exec_{portfolio.trade_date}_blocked_{digest}"


def _slippage_summary(fills, intents) -> dict[str, float]:
    limit_by_symbol_side = {(item.symbol, item.side): item.limit_price for item in intents}
    slips: list[float] = []
    for fill in fills:
        reference = limit_by_symbol_side.get((fill.symbol, fill.side))
        if reference:
            sign = 1.0 if fill.side == "buy" else -1.0
            slips.append(sign * (fill.price / reference - 1.0) * 10_000.0)
    if not slips:
        return {"count": 0.0, "mean_bps": 0.0, "max_bps": 0.0}
    return {"count": float(len(slips)), "mean_bps": float(sum(slips) / len(slips)), "max_bps": float(max(slips))}


def _unfilled_summary(orders) -> dict[str, int]:
    return {
        "order_count": len(orders),
        "rejected": sum(1 for order in orders if order.status == "rejected"),
        "partial": sum(1 for order in orders if order.status == "partial"),
        "remaining_shares": sum(order.remaining_shares for order in orders),
    }


def _merge_order_records(orders: list[OrderRecord], submitted: list[OrderRecord]) -> list[OrderRecord]:
    merged: dict[str, OrderRecord] = {
        order.order_id: order for order in orders if order.order_id
    }
    for order in submitted:
        if order.order_id and order.order_id not in merged:
            merged[order.order_id] = order
    return list(merged.values()) if merged else list(submitted)


def _write_markdown_report(path: Path, report: ExecutionReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# {report.mode} Rebalance Execution Report: {report.exec_id}",
        "",
        "## Summary",
        "",
        f"- Mode: {report.mode}",
        f"- Trade date: {report.trade_date}",
        f"- Portfolio: {report.portfolio_id}",
        f"- Risk passed: {'yes' if report.risk_result.passed else 'no'}",
        f"- Orders: {len(report.orders)}",
        f"- Fills: {len(report.fills)}",
        f"- Available cash: {report.cash.available_cash:.2f}",
        f"- Market value: {report.cash.market_value:.2f}",
        "",
        "## Blocking reasons",
        "",
    ]
    if report.risk_result.blocking_reasons:
        lines.extend([f"- {item}" for item in report.risk_result.blocking_reasons])
    else:
        lines.append("- None")
    lines.extend(
        [
            "",
            "## Orders",
            "",
            "| Order ID | Symbol | Side | Shares | Status | Filled | Remaining | Message |",
            "|---|---|---|---:|---|---:|---:|---|",
        ]
    )
    for order in report.orders:
        lines.append(
            f"| {order.order_id} | {order.intent.symbol} | {order.intent.side} | {order.intent.shares} | "
            f"{order.status} | {order.filled_shares} | {order.remaining_shares} | {order.message} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
