from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import vortex.trade as trade_module

from vortex import cli
from vortex.cli import _trade_inspect_summary, _trade_quote_summary, _trade_status_summary
from vortex.trade import Quote
from vortex.trade.serialization import write_json
from vortex.trade.target_portfolio import TargetPortfolioBuildConfig, build_target_portfolio


def test_trade_status_summary_reports_latest_execution(tmp_path: Path) -> None:
    (tmp_path / "trade" / "executions" / "exec_20260501_b").mkdir(parents=True)
    (tmp_path / "trade" / "executions" / "exec_20260430_a").mkdir(parents=True)

    summary = _trade_status_summary(tmp_path)

    assert summary["execution_count"] == 2
    assert summary["latest_exec_id"] == "exec_20260501_b"
    assert summary["paper_ready"] is True
    assert summary["qmt_ready"] is False


def test_trade_status_summary_reports_qmt_bridge_snapshot(monkeypatch, tmp_path: Path) -> None:
    class _FakeAdapter:
        def __init__(self, config):
            self.config = config

        def health(self):
            return trade_module.BrokerHealth(ok=True, mode="qmt_bridge", message="ok")

        def connection_status(self):
            return {"connected": True}

        def get_cash(self):
            return trade_module.CashSnapshot(
                available_cash=1_000_000.0,
                frozen_cash=0.0,
                total_asset=1_200_000.0,
                market_value=200_000.0,
            )

        def get_positions(self):
            return []

        def get_orders(self):
            return []

        def get_fills(self):
            return []

    monkeypatch.setattr(trade_module, "QmtBridgeAdapter", _FakeAdapter)

    summary = _trade_status_summary(
        tmp_path,
        bridge_url="http://127.0.0.1:8000",
        bridge_token="secret",
        bridge_account_id="99034443",
    )

    assert summary["qmt_ready"] is True
    assert summary["qmt_blocking_reason"] == "-"
    assert summary["qmt_cash"] == 1_000_000.0
    assert summary["qmt_position_count"] == 0


def test_trade_quote_summary_reports_realtime_quotes(monkeypatch, tmp_path: Path) -> None:
    class _FakeAdapter:
        def __init__(self, config):
            self.config = config

        def health(self):
            return trade_module.BrokerHealth(ok=True, mode="qmt_bridge", message="ok")

        def get_quotes(self, symbols):
            return {
                symbol: Quote(
                    symbol=symbol,
                    open_price=10.0,
                    last_price=10.2,
                    volume=100_000,
                    amount=1_000_000.0,
                )
                for symbol in symbols
            }

    monkeypatch.setattr(trade_module, "QmtBridgeAdapter", _FakeAdapter)

    payload = _trade_quote_summary(
        tmp_path,
        symbols=("000001.SZ", "600000.SH"),
        bridge_url="http://127.0.0.1:8000",
        bridge_token="secret",
        bridge_account_id="99034443",
    )

    assert payload["qmt_ready"] is True
    assert payload["qmt_blocking_reason"] == "-"
    assert payload["quotes"]["000001.SZ"]["last_price"] == 10.2
    assert payload["quotes"]["600000.SH"]["volume"] == 100_000


def test_trade_status_summary_tolerates_known_connection_status_bug(monkeypatch, tmp_path: Path) -> None:
    class _FakeAdapter:
        def __init__(self, config):
            self.config = config

        def health(self):
            return trade_module.BrokerHealth(ok=True, mode="qmt_bridge", message="ok")

        def connection_status(self):
            return {
                "connected": False,
                "error": "'xtquant.datacenter.IPythonApiClient' object has no attribute 'get_connect_status'",
            }

        def get_cash(self):
            return trade_module.CashSnapshot(
                available_cash=1_000_000.0,
                frozen_cash=0.0,
                total_asset=1_200_000.0,
                market_value=200_000.0,
            )

        def get_positions(self):
            return []

        def get_orders(self):
            return []

        def get_fills(self):
            return []

    monkeypatch.setattr(trade_module, "QmtBridgeAdapter", _FakeAdapter)

    summary = _trade_status_summary(
        tmp_path,
        bridge_url="http://127.0.0.1:8000",
        bridge_token="secret",
        bridge_account_id="99034443",
    )

    assert summary["qmt_ready"] is True
    assert "qmt_connection_status_warning" in summary
    assert summary["qmt_blocking_reason"] == "-"


def test_trade_status_summary_blocks_on_real_connection_status_false(monkeypatch, tmp_path: Path) -> None:
    class _FakeAdapter:
        def __init__(self, config):
            self.config = config

        def health(self):
            return trade_module.BrokerHealth(ok=True, mode="qmt_bridge", message="ok")

        def connection_status(self):
            return {"connected": False, "error": "socket closed"}

        def get_cash(self):
            return trade_module.CashSnapshot(
                available_cash=1_000_000.0,
                frozen_cash=0.0,
                total_asset=1_200_000.0,
                market_value=200_000.0,
            )

        def get_positions(self):
            return []

        def get_orders(self):
            return []

        def get_fills(self):
            return []

    monkeypatch.setattr(trade_module, "QmtBridgeAdapter", _FakeAdapter)

    summary = _trade_status_summary(
        tmp_path,
        bridge_url="http://127.0.0.1:8000",
        bridge_token="secret",
        bridge_account_id="99034443",
    )

    assert summary["qmt_ready"] is False
    assert summary["qmt_blocking_reason"].startswith("bridge connected=false:")


def test_cmd_trade_quote_outputs_json(monkeypatch, tmp_path: Path, capsys) -> None:
    class _FakeAdapter:
        def __init__(self, config):
            self.config = config

        def health(self):
            return trade_module.BrokerHealth(ok=True, mode="qmt_bridge", message="ok")

        def get_quotes(self, symbols):
            return {
                symbol: Quote(symbol=symbol, open_price=10.0, last_price=10.5, volume=88_000, amount=880_000.0)
                for symbol in symbols
            }

    monkeypatch.setattr(trade_module, "QmtBridgeAdapter", _FakeAdapter)

    cli.cmd_trade(
        argparse.Namespace(
            trade_action="quote",
            root=str(tmp_path),
            symbols="000001.SZ,600000.SH",
            qmt_bridge_url="http://127.0.0.1:8000",
            qmt_bridge_token="secret",
            qmt_account_id="99034443",
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["qmt_ready"] is True
    assert payload["quotes"]["000001.SZ"]["last_price"] == 10.5


def test_cmd_trade_paper_rebalance_writes_artifacts_and_json_summary(tmp_path: Path, capsys) -> None:
    portfolio = build_target_portfolio(
        pd.DataFrame([{"symbol": "000001.SZ", "target_weight": 0.5, "reference_price": 10.0}]),
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
        config=TargetPortfolioBuildConfig(notional=100_000),
    )
    target_path = tmp_path / "target_portfolio.json"
    quotes_path = tmp_path / "quotes.json"
    st_flags_path = tmp_path / "st_flags.json"
    write_json(target_path, portfolio)
    write_json(quotes_path, {"quotes": [Quote("000001.SZ", open_price=10.0, volume=100_000)]})
    write_json(st_flags_path, {"000001.SZ": False})

    cli.cmd_trade(
        argparse.Namespace(
            trade_action="paper",
            trade_paper_action="rebalance",
            root=str(tmp_path),
            target_portfolio=str(target_path),
            quotes=str(quotes_path),
            st_flags=str(st_flags_path),
            allow_missing_st_data=False,
            initial_cash=100_000,
            max_participation_rate=1.0,
            commission_bps=2.5,
            min_commission=5.0,
            stamp_duty_sell_bps=5.0,
            buy_limit_bps=30.0,
            sell_limit_bps=30.0,
            min_order_value=3_000.0,
            max_order_count=80,
            max_single_order_value=100_000,
            max_daily_order_value=1_000_000,
            disable_trading=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["risk_passed"] is True
    assert payload["order_count"] == 1
    assert payload["fill_count"] == 1
    assert Path(payload["execution_report_path"]).exists()

    inspect = _trade_inspect_summary(tmp_path, payload["exec_id"])
    assert inspect["risk_passed"] is True
    assert inspect["fill_count"] == 1


def test_cmd_trade_reconcile_writes_report_for_latest_execution(tmp_path: Path, capsys) -> None:
    portfolio = build_target_portfolio(
        pd.DataFrame([{"symbol": "000001.SZ", "target_weight": 0.5, "reference_price": 10.0}]),
        trade_date="20260501",
        strategy_version="earnings_v3",
        run_id="run_1",
        snapshot_id="snap_1",
        config=TargetPortfolioBuildConfig(notional=100_000),
    )
    target_path = tmp_path / "target_portfolio.json"
    quotes_path = tmp_path / "quotes.json"
    write_json(target_path, portfolio)
    write_json(quotes_path, {"quotes": [Quote("000001.SZ", open_price=10.0, volume=100_000)]})
    summary = cli._run_trade_paper_rebalance(
        argparse.Namespace(
            root=str(tmp_path),
            target_portfolio=str(target_path),
            quotes=str(quotes_path),
            st_flags=None,
            allow_missing_st_data=True,
            initial_cash=100_000,
            max_participation_rate=1.0,
            commission_bps=2.5,
            min_commission=5.0,
            stamp_duty_sell_bps=5.0,
            buy_limit_bps=30.0,
            sell_limit_bps=30.0,
            min_order_value=3_000.0,
            max_order_count=80,
            max_single_order_value=100_000,
            max_daily_order_value=1_000_000,
            disable_trading=False,
        )
    )

    cli.cmd_trade(
        argparse.Namespace(
            trade_action="reconcile",
            root=str(tmp_path),
            exec_id=None,
            cash_tolerance=1.0,
            share_tolerance=0,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["exec_id"] == summary["exec_id"]
    assert payload["abnormal"] is False
    assert Path(payload["reconcile_report_path"]).exists()
