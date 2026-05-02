"""Paper rebalance orchestration and execution report writing."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from vortex.trade.broker import PaperBrokerAdapter, Quote
from vortex.trade.models import ExecutionReport, TargetPortfolio
from vortex.trade.order_plan import OrderPlanConfig, generate_order_plan
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


def _write_markdown_report(path: Path, report: ExecutionReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Paper Rebalance Execution Report: {report.exec_id}",
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
