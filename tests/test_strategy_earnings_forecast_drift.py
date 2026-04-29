from __future__ import annotations

import pandas as pd
import pytest

from vortex.research.goal_review import ExperimentQuality
from vortex.research.market_state import MarketStateConfig
from vortex.strategy.earnings_forecast_drift import (
    EarningsForecastDriftConfig,
    SegmentSpec,
    build_limit_event_masks,
    build_open_limit_price_masks,
    build_stock_st_mask,
    build_suspend_trade_mask,
    capacity_report,
    exposure_diagnostics,
    holdings_diagnostics,
    holdings_to_long_frame,
    open_to_close_returns,
    period_returns,
    run_earnings_forecast_drift,
    run_earnings_forecast_grid,
    write_earnings_forecast_report_html,
    write_earnings_forecast_report_json,
    write_holdings_csv,
)


def _fixture_data(days: int = 80):
    dates = pd.Index(pd.date_range("2024-01-02", periods=days, freq="B").strftime("%Y%m%d"))
    symbols = pd.Index(["A", "B", "C"])
    open_prices = pd.DataFrame(100.0, index=dates, columns=symbols)
    close_prices = pd.DataFrame(100.0, index=dates, columns=symbols)
    amount = pd.DataFrame(100000.0, index=dates, columns=symbols)
    for idx in range(2, days):
        close_prices.loc[dates[idx], "A"] = 104.0
        close_prices.loc[dates[idx], "B"] = 98.0
        close_prices.loc[dates[idx], "C"] = 100.0
    forecast = pd.DataFrame(
        {
            "ann_date": [dates[1], dates[1]],
            "symbol": ["A", "B"],
            "type": ["预增", "预减"],
            "p_change_min": [120.0, -80.0],
            "p_change_max": [180.0, -40.0],
        }
    )
    index_close = pd.DataFrame(
        {
            "000300.SH": range(100, 100 + days),
            "000905.SH": range(101, 101 + days),
            "000852.SH": range(102, 102 + days),
        },
        index=dates,
        dtype=float,
    )
    return forecast, open_prices, close_prices, amount, index_close


def _quality():
    return ExperimentQuality(
        pit_safe=True,
        adjusted_prices=True,
        cost_included=True,
        no_future_leakage=True,
        out_of_sample_checked=False,
    )


def test_earnings_forecast_drift_runs_full_alpha_review():
    forecast, open_prices, close_prices, amount, index_close = _fixture_data()

    result = run_earnings_forecast_drift(
        forecast,
        open_prices,
        close_prices,
        amount,
        index_close,
        EarningsForecastDriftConfig(
            hold_days=20,
            top_n=1,
            liquidity_window=1,
            transaction_cost_bps=0,
            market_state=MarketStateConfig(momentum_window=1, support_window=1),
        ),
        quality=_quality(),
        segments=(SegmentSpec("all", "20240101", "20241231"),),
    )

    assert result.backtest.metrics.total_return > 0
    assert result.candidate_review.worth_owning
    assert result.annual_returns.iloc[0] > 0
    assert result.segments[0].name == "all"
    assert result.to_dict()["candidate_review"]["worth_owning"]


def test_earnings_forecast_drift_default_uses_v3_fast_gate():
    config = EarningsForecastDriftConfig()

    assert config.market_state.momentum_window == 5
    assert config.market_state.support_window == 20
    assert config.market_state.min_risk_on_confirmations == 2
    assert config.position_mode == "full_equal_selected"


def test_earnings_forecast_drift_supports_capped_cash_position_mode():
    forecast, open_prices, close_prices, amount, index_close = _fixture_data(days=30)

    result = run_earnings_forecast_drift(
        forecast,
        open_prices,
        close_prices,
        amount,
        index_close,
        EarningsForecastDriftConfig(
            hold_days=20,
            top_n=2,
            max_weight=0.3,
            position_mode="capped_with_cash",
            liquidity_window=1,
            transaction_cost_bps=0,
            market_state=MarketStateConfig(momentum_window=1, support_window=1),
        ),
        quality=_quality(),
        segments=(),
    )

    assert float(result.weights.sum(axis=1).max()) <= 0.6 + 1e-12
    assert float(result.weights.max(axis=1).max()) <= 0.3 + 1e-12


def test_earnings_forecast_grid_reports_cost_pressure_rows():
    forecast, open_prices, close_prices, amount, index_close = _fixture_data()

    grid = run_earnings_forecast_grid(
        forecast,
        open_prices,
        close_prices,
        amount,
        index_close,
        hold_days=[10, 20],
        top_n=[1],
        transaction_cost_bps=[0, 50],
        base_config=EarningsForecastDriftConfig(
            liquidity_window=1,
            market_state=MarketStateConfig(momentum_window=1, support_window=1),
        ),
        quality=_quality(),
    )

    assert set(grid["hold_days"]) == {10, 20}
    assert set(grid["transaction_cost_bps"]) == {0, 50}
    assert {"annual_return", "max_drawdown", "grade", "worth_owning"} <= set(grid.columns)


def test_earnings_forecast_report_json_writer(tmp_path):
    forecast, open_prices, close_prices, amount, index_close = _fixture_data()
    config = EarningsForecastDriftConfig(
        hold_days=20,
        top_n=1,
        liquidity_window=1,
        transaction_cost_bps=0,
        market_state=MarketStateConfig(momentum_window=1, support_window=1),
    )
    result = run_earnings_forecast_drift(
        forecast,
        open_prices,
        close_prices,
        amount,
        index_close,
        config,
        quality=_quality(),
        segments=(SegmentSpec("all", "20240101", "20241231"),),
    )

    output = write_earnings_forecast_report_json(
        result,
        tmp_path / "report.json",
        config=config,
        metadata={"run_id": "unit-test"},
    )

    payload = pd.read_json(output, typ="series")
    assert payload["strategy"] == "earnings_forecast_drift"
    assert payload["metadata"]["run_id"] == "unit-test"
    assert payload["result"]["candidate_review"]["worth_owning"]


def test_earnings_forecast_html_and_holdings_writers(tmp_path):
    forecast, open_prices, close_prices, amount, index_close = _fixture_data()
    config = EarningsForecastDriftConfig(
        hold_days=20,
        top_n=1,
        liquidity_window=1,
        transaction_cost_bps=0,
        market_state=MarketStateConfig(momentum_window=1, support_window=1),
    )
    result = run_earnings_forecast_drift(
        forecast,
        open_prices,
        close_prices,
        amount,
        index_close,
        config,
        quality=_quality(),
        segments=(SegmentSpec("all", "20240101", "20241231"),),
    )
    returns = open_to_close_returns(open_prices, close_prices)
    holdings = holdings_to_long_frame(result, returns=returns, amount=amount)
    diagnostics = holdings_diagnostics(holdings)
    holdings_path = write_holdings_csv(result, tmp_path / "holdings.csv", returns=returns, amount=amount)
    html_path = write_earnings_forecast_report_html(
        result,
        tmp_path / "report.html",
        metadata={"cost_pressure": []},
        holdings_path=holdings_path,
        diagnostics=diagnostics,
    )

    assert not holdings.empty
    assert {"date", "symbol", "weight", "trade_delta", "contribution"} <= set(holdings.columns)
    assert diagnostics["holding_rows"] == len(holdings)
    assert holdings_path.exists()
    assert "业绩预告漂移策略回测报告" in html_path.read_text(encoding="utf-8")


def test_exposure_diagnostics_explains_cash_days():
    forecast, open_prices, close_prices, amount, index_close = _fixture_data()
    config = EarningsForecastDriftConfig(
        hold_days=20,
        top_n=1,
        liquidity_window=1,
        transaction_cost_bps=0,
        market_state=MarketStateConfig(momentum_window=1, support_window=1),
    )
    result = run_earnings_forecast_drift(
        forecast,
        open_prices,
        close_prices,
        amount,
        index_close,
        config,
        quality=_quality(),
        segments=(SegmentSpec("all", "20240101", "20241231"),),
    )
    gate = pd.Series(True, index=result.weights.index)
    gate.iloc[0] = False

    diagnostics = exposure_diagnostics(result, market_gate=gate)

    assert diagnostics["days"] == len(result.weights)
    assert diagnostics["full_cash_days"] >= 0
    assert diagnostics["market_risk_off_days"] == 1


def test_capacity_report_uses_weight_turnover_against_amount():
    dates = pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04"])
    weights = pd.DataFrame(
        {
            "000001.SZ": [0.5, 0.2, 0.0],
            "000002.SZ": [0.0, 0.3, 0.4],
        },
        index=dates,
    )
    amount = pd.DataFrame(100_000_000.0, index=dates, columns=weights.columns)

    report = capacity_report(weights, amount, portfolio_notional=10_000_000)

    assert report["active_days"] == 3
    assert report["max_traded_names"] == 2
    assert report["participation_max"] == 0.05
    assert report["trades_over_100bp"] > 0


def test_capacity_report_validates_positive_notional():
    with pytest.raises(ValueError, match="portfolio_notional"):
        capacity_report(pd.DataFrame({"a": [0.1]}), pd.DataFrame({"a": [1.0]}), portfolio_notional=0)


def test_limit_event_and_st_masks_align_to_trade_matrix():
    dates = pd.Index(["20240102", "20240103"])
    symbols = pd.Index(["000001.SZ", "000002.SZ"])
    limit_events = pd.DataFrame(
        {
            "date": [20240102, 20240103],
            "symbol": ["000001.SZ", "000002.SZ"],
            "limit": ["U", "D"],
        }
    )
    stock_st = pd.DataFrame({"date": [20240103], "symbol": ["000001.SZ"]})

    up, down = build_limit_event_masks(limit_events, dates, symbols)
    st = build_stock_st_mask(stock_st, dates, symbols)

    assert up.loc["20240102", "000001.SZ"]
    assert down.loc["20240103", "000002.SZ"]
    assert st.loc["20240103", "000001.SZ"]


def test_open_limit_price_masks_use_raw_open_prices():
    dates = pd.Index(["20240102", "20240103"])
    symbols = pd.Index(["000001.SZ", "000002.SZ"])
    raw_open = pd.DataFrame(
        {
            "000001.SZ": [11.0, 10.0],
            "000002.SZ": [9.0, 8.0],
        },
        index=dates,
    )
    stk_limit = pd.DataFrame(
        {
            "date": ["20240102", "20240103"],
            "symbol": ["000001.SZ", "000002.SZ"],
            "up_limit": [11.0, 9.0],
            "down_limit": [9.0, 8.0],
        }
    )

    up, down = build_open_limit_price_masks(stk_limit, raw_open)

    assert up.loc["20240102", "000001.SZ"]
    assert not up.loc["20240103", "000002.SZ"]
    assert down.loc["20240103", "000002.SZ"]


def test_suspend_trade_mask_only_blocks_suspend_rows():
    dates = pd.Index(["20240102", "20240103"])
    symbols = pd.Index(["000001.SZ", "000002.SZ"])
    suspend_events = pd.DataFrame(
        {
            "date": ["20240102", "20240103"],
            "symbol": ["000001.SZ", "000002.SZ"],
            "suspend_type": ["S", "R"],
        }
    )

    mask = build_suspend_trade_mask(suspend_events, dates, symbols)

    assert mask.loc["20240102", "000001.SZ"]
    assert not mask.loc["20240103", "000002.SZ"]


def test_earnings_forecast_drift_can_block_limit_up_buy():
    forecast, open_prices, close_prices, amount, index_close = _fixture_data()
    limit_events = pd.DataFrame({"date": [open_prices.index[2]], "symbol": ["A"], "limit": ["U"]})

    result = run_earnings_forecast_drift(
        forecast,
        open_prices,
        close_prices,
        amount,
        index_close,
        EarningsForecastDriftConfig(
            hold_days=20,
            top_n=1,
            liquidity_window=1,
            transaction_cost_bps=0,
            market_state=MarketStateConfig(momentum_window=1, support_window=1),
        ),
        quality=_quality(),
        segments=(SegmentSpec("all", "20240101", "20241231"),),
        limit_events=limit_events,
    )

    assert result.weights.loc[open_prices.index[2], "A"] == pytest.approx(0.0)


def test_earnings_forecast_drift_requires_raw_open_for_stk_limit():
    forecast, open_prices, close_prices, amount, index_close = _fixture_data()
    stk_limit = pd.DataFrame(
        {
            "date": [open_prices.index[2]],
            "symbol": ["A"],
            "up_limit": [100.0],
            "down_limit": [90.0],
        }
    )

    with pytest.raises(ValueError, match="limit_open_prices"):
        run_earnings_forecast_drift(
            forecast,
            open_prices,
            close_prices,
            amount,
            index_close,
            EarningsForecastDriftConfig(
                hold_days=20,
                top_n=1,
                liquidity_window=1,
                transaction_cost_bps=0,
                market_state=MarketStateConfig(momentum_window=1, support_window=1),
            ),
            quality=_quality(),
            segments=(SegmentSpec("all", "20240101", "20241231"),),
            stk_limit=stk_limit,
        )


def test_earnings_forecast_drift_blocks_open_limit_up_buy_with_raw_open():
    forecast, open_prices, close_prices, amount, index_close = _fixture_data()
    raw_open = open_prices.copy()
    stk_limit = pd.DataFrame(
        {
            "date": [open_prices.index[2]],
            "symbol": ["A"],
            "up_limit": [100.0],
            "down_limit": [90.0],
        }
    )

    result = run_earnings_forecast_drift(
        forecast,
        open_prices,
        close_prices,
        amount,
        index_close,
        EarningsForecastDriftConfig(
            hold_days=20,
            top_n=1,
            liquidity_window=1,
            transaction_cost_bps=0,
            market_state=MarketStateConfig(momentum_window=1, support_window=1),
        ),
        quality=_quality(),
        segments=(SegmentSpec("all", "20240101", "20241231"),),
        stk_limit=stk_limit,
        limit_open_prices=raw_open,
    )

    assert result.weights.loc[open_prices.index[2], "A"] == pytest.approx(0.0)


def test_period_returns_and_open_to_close_returns():
    dates = pd.Index(["20240102", "20240103", "20240201"])
    returns = pd.Series([0.1, -0.05, 0.02], index=dates)

    monthly = period_returns(returns, "M")
    oc = open_to_close_returns(
        pd.DataFrame({"A": [100.0]}, index=pd.Index(["20240102"])),
        pd.DataFrame({"A": [105.0]}, index=pd.Index(["20240102"])),
    )

    assert monthly.loc["2024-01"] == pytest.approx(0.045)
    assert oc.loc["20240102", "A"] == pytest.approx(0.05)


def test_earnings_forecast_drift_rejects_invalid_config():
    forecast, open_prices, close_prices, amount, index_close = _fixture_data()

    with pytest.raises(ValueError, match="hold_days"):
        run_earnings_forecast_drift(
            forecast,
            open_prices,
            close_prices,
            amount,
            index_close,
            EarningsForecastDriftConfig(hold_days=0),
        )
