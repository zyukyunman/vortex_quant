from __future__ import annotations

import pandas as pd

from vortex.trade import CashSnapshot, PaperBrokerAdapter, PaperBrokerConfig, Position, Quote
from vortex.trade.execution import run_paper_rebalance
from vortex.trade.reconcile import reconcile_execution, reconcile_execution_report, write_reconcile_report
from vortex.trade.target_portfolio import TargetPortfolioBuildConfig, build_target_portfolio
from vortex.trade.serialization import read_json, reconcile_report_from_dict


def test_reconcile_execution_passes_when_snapshots_match(tmp_path) -> None:
    portfolio = build_target_portfolio(
        pd.DataFrame([{"symbol": "000001.SZ", "target_weight": 0.5, "reference_price": 10.0}]),
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
        config=TargetPortfolioBuildConfig(notional=100_000),
    )
    broker = PaperBrokerAdapter(PaperBrokerConfig(initial_cash=100_000, max_participation_rate=1.0))
    artifacts = run_paper_rebalance(
        portfolio,
        broker=broker,
        quotes=[Quote("000001.SZ", open_price=10.0, volume=100_000)],
        output_root=tmp_path,
        st_flags={"000001.SZ": False},
    )
    report = artifacts.report

    reconcile = reconcile_execution(
        exec_id=report.exec_id,
        portfolio=portfolio,
        expected_cash=report.cash,
        actual_cash=report.cash,
        expected_positions=report.positions,
        actual_positions=report.positions,
        expected_orders=report.orders,
        actual_orders=report.orders,
        expected_fills=report.fills,
        actual_fills=report.fills,
    )

    assert reconcile.abnormal is False
    assert reconcile.blocking_reasons == []
    path = tmp_path / "reconcile_report.json"
    write_reconcile_report(path, reconcile)
    assert reconcile_report_from_dict(read_json(path)) == reconcile

    report_reconcile = reconcile_execution_report(report)
    assert report_reconcile.abnormal is False
    assert report_reconcile.exec_id == report.exec_id


def test_reconcile_execution_flags_cash_and_position_mismatch() -> None:
    portfolio = build_target_portfolio(
        pd.DataFrame([{"symbol": "000001.SZ", "target_weight": 0.5, "reference_price": 10.0}]),
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
        config=TargetPortfolioBuildConfig(notional=100_000),
    )

    reconcile = reconcile_execution(
        exec_id="exec_1",
        portfolio=portfolio,
        expected_cash=CashSnapshot(available_cash=50_000, frozen_cash=0, total_asset=100_000, market_value=50_000),
        actual_cash=CashSnapshot(available_cash=49_000, frozen_cash=0, total_asset=99_000, market_value=50_000),
        expected_positions=[Position("000001.SZ", shares=5_000, available_shares=5_000, cost_price=10, last_price=10)],
        actual_positions=[Position("000001.SZ", shares=4_900, available_shares=4_900, cost_price=10, last_price=10)],
        expected_orders=[],
        actual_orders=[],
        expected_fills=[],
        actual_fills=[],
    )

    assert reconcile.abnormal is True
    assert "cash mismatch" in reconcile.blocking_reasons
    assert "position mismatch" in reconcile.blocking_reasons
    assert reconcile.position_diffs[0]["diff_shares"] == -100
