from __future__ import annotations

from vortex.trade import CashSnapshot, FillRecord, OrderIntent, OrderRecord, Position
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
from vortex.trade.serialization import (
    execution_report_from_dict,
    order_plan_from_dict,
    read_json,
    reconcile_report_from_dict,
    risk_check_result_from_dict,
    target_portfolio_from_dict,
    to_plain,
    write_json,
)


def test_target_portfolio_json_roundtrip(tmp_path):
    portfolio = TargetPortfolio(
        portfolio_id="tp_20260501_a",
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
        cash_target=123.45,
        positions=[
            TargetPosition(
                symbol="000001.SZ",
                target_weight=0.1,
                target_value=100_000,
                target_shares=10_000,
                reference_price=10.0,
                reason="top score",
            )
        ],
        lineage=Lineage(portfolio_id="tp_20260501_a", strategy_run_id="run_1", snapshot_id="snap_1"),
    )

    path = tmp_path / "target_portfolio.json"
    write_json(path, portfolio)
    loaded = target_portfolio_from_dict(read_json(path))

    assert loaded == portfolio
    assert to_plain(loaded)["positions"][0]["symbol"] == "000001.SZ"


def test_order_plan_and_risk_json_roundtrip(tmp_path):
    plan = OrderPlan(
        exec_id="exec_1",
        portfolio_id="tp_1",
        trade_date="20260501",
        orders=[OrderIntent(symbol="000001.SZ", side="buy", shares=100, limit_price=10.1, run_id="run_1")],
        lineage=Lineage(exec_id="exec_1", portfolio_id="tp_1"),
    )
    risk = RiskCheckResult(
        exec_id="exec_1",
        passed=False,
        blocking_reasons=["limit-up buy blocked"],
        warnings=["cash tail"],
        rule_results=[RiskRuleResult(name="limit_up", passed=False, level="critical", symbol="000001.SZ")],
        lineage=Lineage(exec_id="exec_1"),
    )

    plan_path = tmp_path / "order_plan.json"
    risk_path = tmp_path / "pre_trade_result.json"
    write_json(plan_path, plan)
    write_json(risk_path, risk)

    assert order_plan_from_dict(read_json(plan_path)) == plan
    assert risk_check_result_from_dict(read_json(risk_path)) == risk


def test_execution_and_reconcile_json_roundtrip(tmp_path):
    intent = OrderIntent(symbol="000001.SZ", side="buy", shares=100, limit_price=10.1)
    plan = OrderPlan(exec_id="exec_1", portfolio_id="tp_1", trade_date="20260501", orders=[intent])
    risk = RiskCheckResult(exec_id="exec_1", passed=True, blocking_reasons=[], warnings=[], rule_results=[])
    order = OrderRecord(
        order_id="P00000001",
        intent=intent,
        status="filled",
        filled_shares=100,
        remaining_shares=0,
        avg_fill_price=10.0,
        message="",
        created_at="2026-05-01T09:30:00",
    )
    fill = FillRecord(
        fill_id="F00000001",
        order_id="P00000001",
        symbol="000001.SZ",
        side="buy",
        shares=100,
        price=10.0,
        gross_value=1_000.0,
        fee=5.0,
        created_at="2026-05-01T09:30:00",
    )
    report = ExecutionReport(
        exec_id="exec_1",
        mode="paper",
        portfolio_id="tp_1",
        trade_date="20260501",
        order_plan=plan,
        risk_result=risk,
        cash=CashSnapshot(available_cash=99_000, frozen_cash=0, total_asset=100_000, market_value=1_000),
        positions=[Position("000001.SZ", shares=100, available_shares=100, cost_price=10.0, last_price=10.0)],
        orders=[order],
        fills=[fill],
        slippage_summary={"mean_bps": 0.0},
        unfilled_summary={"count": 0},
    )
    reconcile = ReconcileReport(
        reconcile_id="rec_1",
        exec_id="exec_1",
        trade_date="20260501",
        abnormal=False,
        cash_diff=0.0,
        position_diffs=[],
        order_diffs=[],
        fill_diffs=[],
        blocking_reasons=[],
    )

    report_path = tmp_path / "execution_report.json"
    reconcile_path = tmp_path / "reconcile_report.json"
    write_json(report_path, report)
    write_json(reconcile_path, reconcile)

    assert execution_report_from_dict(read_json(report_path)) == report
    assert reconcile_report_from_dict(read_json(reconcile_path)) == reconcile
