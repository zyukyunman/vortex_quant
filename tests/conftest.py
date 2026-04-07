"""Pytest fixtures for Vortex tests."""
from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest
from datetime import date

from vortex.shared.calendar import TradingCalendar


@pytest.fixture
def tmp_path_factory_clean(tmp_path):
    """返回干净的临时目录。"""
    return tmp_path


@pytest.fixture
def sample_bars() -> pd.DataFrame:
    """标准行情 DataFrame（5 日 x 2 标的）。"""
    rows = []
    symbols = ["600519.SH", "000001.SZ"]
    dates = ["20260401", "20260402", "20260403", "20260406", "20260407"]
    for d in dates:
        for s in symbols:
            rows.append({
                "symbol": s, "date": d,
                "open": 100.0, "high": 105.0, "low": 98.0,
                "close": 102.0, "volume": 50000.0, "amount": 5e6,
            })
    return pd.DataFrame(rows)


@pytest.fixture
def sample_fundamental() -> pd.DataFrame:
    """标准基本面 DataFrame（含 ann_date 和 report_date）。"""
    return pd.DataFrame({
        "symbol": ["600519.SH", "600519.SH", "000001.SZ"],
        "ann_date": [date(2026, 3, 28), date(2026, 4, 2), date(2026, 3, 30)],
        "report_date": [date(2025, 12, 31), date(2025, 12, 31), date(2025, 12, 31)],
        "revenue": [100.0, 105.0, 200.0],
        "net_profit": [30.0, 32.0, 50.0],
    })


@pytest.fixture
def trading_calendar() -> TradingCalendar:
    """预加载 2026-03 到 2026-04 交易日历。"""
    cal = TradingCalendar()
    # 模拟一组交易日（排除周末）
    days = []
    for m, ds in [
        (3, list(range(2, 32))),
        (4, list(range(1, 30))),
    ]:
        for d in ds:
            try:
                dt = date(2026, m, d)
            except ValueError:
                continue
            # 跳过周末
            if dt.weekday() < 5:
                days.append(dt)
    cal_df = pd.DataFrame({"cal_date": [d.strftime("%Y%m%d") for d in days]})
    cal.load_from_dataframe(cal_df)
    return cal
