from __future__ import annotations

import pandas as pd

from vortex.trade import PaperBrokerAdapter, PaperBrokerConfig, Quote
from vortex.trade.execution import run_paper_rebalance, run_qmt_rebalance
from vortex.trade.qmt_bridge import QmtBridgeConfig
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


def test_run_qmt_rebalance_writes_artifacts_and_submits_orders(tmp_path, monkeypatch) -> None:
    portfolio = build_target_portfolio(
        pd.DataFrame([{"symbol": "000001.SZ", "target_weight": 0.5, "reference_price": 10.0}]),
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
        config=TargetPortfolioBuildConfig(notional=100_000),
    )

    class _FakeAdapter:
        def __init__(self, config):
            self.config = config
            self._submitted = []

        def health(self):
            from vortex.trade import BrokerHealth

            return BrokerHealth(ok=True, mode="qmt_bridge", message="ok")

        def get_cash(self):
            from vortex.trade import CashSnapshot

            return CashSnapshot(available_cash=100_000.0, frozen_cash=0.0, total_asset=100_000.0, market_value=0.0)

        def get_positions(self):
            return []

        def get_quotes(self, symbols):
            return {symbol: Quote(symbol=symbol, open_price=10.0, volume=100_000) for symbol in symbols}

        def submit_order(self, intent):
            from vortex.trade import OrderRecord

            self._submitted.append(intent)
            return OrderRecord(
                order_id="order_1",
                intent=intent,
                status="open",
                filled_shares=0,
                remaining_shares=intent.shares,
                avg_fill_price=None,
                message="submitted",
                created_at="2026-05-01 09:30:00",
            )

        def get_orders(self):
            return []

        def get_fills(self):
            return []

    monkeypatch.setattr("vortex.trade.execution.QmtBridgeAdapter", _FakeAdapter)

    artifacts = run_qmt_rebalance(
        portfolio,
        bridge_config=QmtBridgeConfig(base_url="http://127.0.0.1:8000", account_id="99034443", allow_trading=True),
        output_root=tmp_path,
        st_flags={"000001.SZ": False},
    )

    assert artifacts.order_intent_path.exists()
    assert artifacts.order_plan_path.exists()
    assert artifacts.risk_result_path.exists()
    assert artifacts.execution_report_path.exists()
    assert artifacts.execution_report_md_path.exists()
    assert artifacts.report.risk_result.passed is True
    assert len(artifacts.report.orders) == 1


def test_run_qmt_rebalance_diffs_against_real_positions_before_submitting(tmp_path, monkeypatch) -> None:
    portfolio = build_target_portfolio(
        pd.DataFrame([{"symbol": "000001.SZ", "target_weight": 1.0, "reference_price": 10.0}]),
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
        config=TargetPortfolioBuildConfig(notional=50_000),
    )

    class _FakeAdapter:
        def __init__(self, config):
            self.config = config
            self._submitted = []

        def health(self):
            from vortex.trade import BrokerHealth

            return BrokerHealth(ok=True, mode="qmt_bridge", message="ok")

        def get_cash(self):
            from vortex.trade import CashSnapshot

            return CashSnapshot(available_cash=30_000.0, frozen_cash=0.0, total_asset=100_000.0, market_value=70_000.0)

        def get_positions(self):
            from vortex.trade import Position

            return [
                Position("000001.SZ", shares=5_000, available_shares=3_000, cost_price=10.0, last_price=10.0),
                Position("000002.SZ", shares=2_000, available_shares=1_000, cost_price=10.0, last_price=10.0),
            ]

        def get_quotes(self, symbols):
            return {symbol: Quote(symbol=symbol, open_price=10.0, volume=100_000) for symbol in symbols}

        def submit_order(self, intent):
            from vortex.trade import OrderRecord

            self._submitted.append(intent)
            return OrderRecord(
                order_id=f"order_{len(self._submitted)}",
                intent=intent,
                status="open",
                filled_shares=0,
                remaining_shares=intent.shares,
                avg_fill_price=None,
                message="submitted",
                created_at="2026-05-01 09:30:00",
            )

        def get_orders(self):
            return []

        def get_fills(self):
            return []

    monkeypatch.setattr("vortex.trade.execution.QmtBridgeAdapter", _FakeAdapter)

    artifacts = run_qmt_rebalance(
        portfolio,
        bridge_config=QmtBridgeConfig(base_url="http://127.0.0.1:8000", account_id="99034443", allow_trading=True),
        output_root=tmp_path,
        st_flags={"000001.SZ": False, "000002.SZ": False},
    )

    assert [(order.intent.side, order.intent.symbol, order.intent.shares) for order in artifacts.report.orders] == [
        ("sell", "000002.SZ", 1_000),
    ]
