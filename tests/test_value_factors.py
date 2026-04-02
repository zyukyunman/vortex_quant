from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from vortex.factor.value import DividendYield, DividendYield3Y, PayoutRatio3Y


def _mock_ds(
    dividend_df: pd.DataFrame,
    daily_df: pd.DataFrame | None = None,
    income_df: pd.DataFrame | None = None,
):
    ds = MagicMock()
    ds.get_dividend.return_value = dividend_df
    ds.get_daily.return_value = daily_df if daily_df is not None else pd.DataFrame()
    ds.get_income.return_value = income_df if income_df is not None else pd.DataFrame()
    return ds


def test_dividend_yield_aggregates_full_year_implemented_dividends():
    dividend_df = pd.DataFrame([
        {"ts_code": "A.SZ", "end_date": "20241231", "div_proc": "实施", "cash_div": 0.27, "cash_div_tax": 0.30},
        {"ts_code": "A.SZ", "end_date": "20240630", "div_proc": "实施", "cash_div": 0.18, "cash_div_tax": 0.20},
        {"ts_code": "A.SZ", "end_date": "20241231", "div_proc": "预案", "cash_div": 0.00, "cash_div_tax": 0.30},
        {"ts_code": "B.SZ", "end_date": "20241231", "div_proc": "实施", "cash_div": 0.09, "cash_div_tax": 0.10},
        {"ts_code": "B.SZ", "end_date": "20231231", "div_proc": "实施", "cash_div": 0.50, "cash_div_tax": 0.50},
    ])
    daily_df = pd.DataFrame({
        "ts_code": ["A.SZ", "B.SZ"],
        "close": [10.0, 20.0],
    })
    ds = _mock_ds(dividend_df, daily_df=daily_df)

    result = DividendYield().compute(ds, "20260328")

    assert result["A.SZ"] == pytest.approx(0.05)
    assert result["B.SZ"] == pytest.approx(0.005)


def test_dividend_yield_3y_uses_yearly_totals_including_interim_dividends():
    dividend_df = pd.DataFrame([
        {"ts_code": "A.SZ", "end_date": "20241231", "div_proc": "实施", "cash_div_tax": 0.30},
        {"ts_code": "A.SZ", "end_date": "20240630", "div_proc": "实施", "cash_div_tax": 0.20},
        {"ts_code": "A.SZ", "end_date": "20231231", "div_proc": "实施", "cash_div_tax": 0.40},
        {"ts_code": "A.SZ", "end_date": "20221231", "div_proc": "实施", "cash_div_tax": 0.10},
        {"ts_code": "A.SZ", "end_date": "20220630", "div_proc": "实施", "cash_div_tax": 0.20},
    ])
    daily_df = pd.DataFrame({"ts_code": ["A.SZ"], "close": [10.0]})
    ds = _mock_ds(dividend_df, daily_df=daily_df)

    result = DividendYield3Y().compute(ds, "20260328")

    expected_avg_dividend = (0.50 + 0.40 + 0.30) / 3
    assert result["A.SZ"] == pytest.approx(expected_avg_dividend / 10.0)


def test_payout_ratio_3y_computes_from_dividend_totals_and_income():
    dividend_df = pd.DataFrame([
        {"ts_code": "A.SZ", "end_date": "20241231", "div_proc": "实施", "cash_div": 0.18, "cash_div_tax": 0.20, "base_share": 100.0},
        {"ts_code": "A.SZ", "end_date": "20240630", "div_proc": "实施", "cash_div": 0.09, "cash_div_tax": 0.10, "base_share": 100.0},
        {"ts_code": "A.SZ", "end_date": "20231231", "div_proc": "实施", "cash_div": 0.13, "cash_div_tax": 0.15, "base_share": 100.0},
        {"ts_code": "A.SZ", "end_date": "20221231", "div_proc": "实施", "cash_div": 0.08, "cash_div_tax": 0.10, "base_share": 100.0},
        {"ts_code": "A.SZ", "end_date": "20241231", "div_proc": "预案", "cash_div": 0.00, "cash_div_tax": 0.30, "base_share": 100.0},
    ])
    income_df = pd.DataFrame([
        {"ts_code": "A.SZ", "end_date": "20241231", "n_income_attr_p": 10_000_000.0},
        {"ts_code": "A.SZ", "end_date": "20231231", "n_income_attr_p": 10_000_000.0},
        {"ts_code": "A.SZ", "end_date": "20221231", "n_income_attr_p": 10_000_000.0},
    ])
    ds = _mock_ds(dividend_df, income_df=income_df)

    result = PayoutRatio3Y().compute(ds, "20260328")

    expected = (0.03 + 0.015 + 0.01) / 3
    assert result["A.SZ"] == pytest.approx(expected)