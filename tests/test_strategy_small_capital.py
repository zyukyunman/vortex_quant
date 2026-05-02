from __future__ import annotations

import pandas as pd
import pytest

from vortex.strategy.small_capital import SmallCapitalExecutionConfig, run_lot_constrained_backtest


def test_lot_constrained_backtest_respects_lot_size_and_min_trade_value():
    dates = pd.Index(["20240102", "20240103", "20240104"])
    symbols = pd.Index(["A", "B"])
    target = pd.DataFrame(
        {
            "A": [0.0, 0.6, 0.0],
            "B": [0.0, 0.4, 1.0],
        },
        index=dates,
    )
    open_prices = pd.DataFrame({"A": [10.0, 10.0, 10.0], "B": [20.0, 20.0, 20.0]}, index=dates)
    close_prices = pd.DataFrame({"A": [10.0, 11.0, 10.0], "B": [20.0, 20.0, 22.0]}, index=dates)

    result = run_lot_constrained_backtest(
        target,
        open_prices,
        close_prices,
        SmallCapitalExecutionConfig(initial_cash=100_000, lot_size=100, min_trade_value=3_000, min_commission=5),
    )

    assert result.summary["trade_rows"] > 0
    assert set(result.trades["shares"] % 100) == {0}
    assert result.trades["gross_value"].min() >= 3_000
    assert result.equity_curve.iloc[-1] > 100_000
    assert {"cash_ratio", "idle_reason", "target_exposure", "actual_exposure"}.issubset(result.diagnostics.columns)
    assert {"requested_shares", "executed_shares", "status", "reason"}.issubset(result.order_intents.columns)


def test_lot_constrained_backtest_rejects_invalid_cash():
    target = pd.DataFrame({"A": [0.0, 1.0]}, index=pd.Index(["20240102", "20240103"]))
    prices = pd.DataFrame({"A": [10.0, 10.0]}, index=target.index)

    with pytest.raises(ValueError, match="initial_cash"):
        run_lot_constrained_backtest(target, prices, prices, SmallCapitalExecutionConfig(initial_cash=0))


def test_lot_constrained_backtest_reports_idle_reasons():
    dates = pd.Index(["20240102", "20240103", "20240104", "20240105"])
    target = pd.DataFrame({"A": [0.0, 0.0, 1.0, 1.0]}, index=dates)
    prices = pd.DataFrame({"A": [10.0, 10.0, 10.0, 10.0]}, index=dates)
    market_gate = pd.Series([True, False, True, True], index=dates)
    signal = pd.DataFrame({"A": [pd.NA, 1.0, 1.0, 1.0]}, index=dates)

    result = run_lot_constrained_backtest(
        target,
        prices,
        prices,
        SmallCapitalExecutionConfig(initial_cash=100_000, lot_size=100, min_trade_value=3_000),
        market_gate=market_gate,
        signal=signal,
    )

    reasons = set(result.diagnostics["idle_reason"])
    assert "market_risk_off" in reasons
    assert "near_target" in reasons
    assert result.summary["idle_reason_days"]["market_risk_off"] == 1


def test_lot_constrained_backtest_marks_non_tradable_holdings_with_last_price():
    dates = pd.Index(["20240102", "20240103", "20240104"])
    target = pd.DataFrame({"A": [0.0, 1.0, 1.0]}, index=dates)
    open_prices = pd.DataFrame({"A": [10.0, 10.0, 0.0]}, index=dates)
    close_prices = pd.DataFrame({"A": [10.0, 10.2, 0.0]}, index=dates)

    result = run_lot_constrained_backtest(
        target,
        open_prices,
        close_prices,
        SmallCapitalExecutionConfig(initial_cash=100_000, lot_size=100, min_trade_value=3_000),
    )

    # 第三天不可交易也不能把持仓估值成 0；应沿用最近可用价格估值。
    assert result.equity_curve.iloc[-1] > 90_000
    assert result.returns.iloc[-1] > -0.2


def test_lot_constrained_backtest_records_buy_order_intents_with_partial_fill():
    dates = pd.Index(["20240102", "20240103"])
    target = pd.DataFrame({"A": [0.0, 1.0]}, index=dates)
    open_prices = pd.DataFrame({"A": [10.0, 10.0]}, index=dates)
    close_prices = pd.DataFrame({"A": [10.0, 10.0]}, index=dates)

    result = run_lot_constrained_backtest(
        target,
        open_prices,
        close_prices,
        SmallCapitalExecutionConfig(initial_cash=1_000, lot_size=100, min_trade_value=0, min_commission=5),
    )

    intent = result.order_intents.iloc[0]
    assert intent["requested_shares"] == 100
    assert intent["executed_shares"] == 0
    assert intent["status"] == "skipped"
    assert intent["reason"] in {"insufficient_cash", "insufficient_cash_after_fee"}


def test_lot_constrained_backtest_skips_buy_when_opening_volume_cannot_fully_cover():
    dates = pd.Index(["20240102", "20240103"])
    target = pd.DataFrame({"A": [0.0, 1.0]}, index=dates)
    open_prices = pd.DataFrame({"A": [10.0, 10.0]}, index=dates)
    close_prices = pd.DataFrame({"A": [10.0, 10.0]}, index=dates)
    buy_limits = pd.DataFrame({"A": [0, 900]}, index=dates)

    result = run_lot_constrained_backtest(
        target,
        open_prices,
        close_prices,
        SmallCapitalExecutionConfig(
            initial_cash=100_000,
            lot_size=100,
            min_trade_value=0,
            commission_bps=0,
            min_commission=0,
            allow_partial_buy_fills=False,
        ),
        buy_share_limits=buy_limits,
    )

    intent = result.order_intents.iloc[0]
    assert intent["requested_shares"] == 10000
    assert intent["executed_shares"] == 0
    assert intent["status"] == "skipped"
    assert intent["reason"] == "opening_volume_insufficient"
    assert result.summary["trade_rows"] == 0


def test_lot_constrained_backtest_executes_full_buy_when_opening_volume_covers_request():
    dates = pd.Index(["20240102", "20240103"])
    target = pd.DataFrame({"A": [0.0, 1.0]}, index=dates)
    open_prices = pd.DataFrame({"A": [10.0, 10.0]}, index=dates)
    close_prices = pd.DataFrame({"A": [10.0, 10.5]}, index=dates)
    buy_limits = pd.DataFrame({"A": [0, 10000]}, index=dates)

    result = run_lot_constrained_backtest(
        target,
        open_prices,
        close_prices,
        SmallCapitalExecutionConfig(
            initial_cash=100_000,
            lot_size=100,
            min_trade_value=0,
            commission_bps=0,
            min_commission=0,
            allow_partial_buy_fills=False,
        ),
        buy_share_limits=buy_limits,
    )

    intent = result.order_intents.iloc[0]
    assert intent["executed_shares"] == 10000
    assert intent["status"] == "filled"
    assert result.summary["trade_rows"] == 1
    assert result.equity_curve.iloc[-1] > 100_000
