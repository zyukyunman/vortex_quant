from __future__ import annotations

import pandas as pd

from vortex.trade import PaperBrokerAdapter, PaperBrokerConfig, Quote
from vortex.trade.execution import run_paper_rebalance
from vortex.trade.risk import PreTradeRiskConfig
from vortex.trade.target_portfolio import TargetPortfolioBuildConfig, build_target_portfolio


def test_run_paper_rebalance_writes_artifacts_and_submits_when_risk_passes(tmp_path) -> None:
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

    assert artifacts.order_intent_path.exists()
    assert artifacts.order_plan_path.exists()
    assert artifacts.risk_result_path.exists()
    assert artifacts.execution_report_path.exists()
    assert artifacts.execution_report_md_path.exists()
    assert artifacts.report.risk_result.passed is True
    assert artifacts.report.fills[0].symbol == "000001.SZ"
    assert broker.get_positions()[0].shares == 5_000


def test_run_paper_rebalance_does_not_submit_when_risk_blocks(tmp_path) -> None:
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
        quotes=[Quote("000001.SZ", open_price=10.0, is_limit_up=True, volume=100_000)],
        output_root=tmp_path,
        st_flags={"000001.SZ": False},
        risk_config=PreTradeRiskConfig(max_single_order_value=100_000),
    )

    assert artifacts.report.risk_result.passed is False
    assert "limit-up buy blocked" in artifacts.report.risk_result.blocking_reasons
    assert artifacts.report.orders == []
    assert artifacts.report.fills == []
    assert broker.get_positions() == []
