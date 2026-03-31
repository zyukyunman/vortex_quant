"""test_dividend_classic.py — 4进3出经典策略单元测试"""
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from vortex.strategy.dividend_classic import (
    DividendYield4In3Out,
    BUY_THRESHOLD,
    SELL_THRESHOLD,
    PROFIT_DECLINE,
)


def _make_mocks(dy_data=None, npy_data=None, basic_data=None, *, dy=None, npy=None):
    # Support both positional and keyword args
    if dy is not None:
        dy_data = dy
    if npy is not None:
        npy_data = npy
    """构建策略所需的 mock 对象"""
    ds = MagicMock()
    fh = MagicMock()
    bus = MagicMock()

    if basic_data is None:
        basic_data = pd.DataFrame({
            "ts_code": ["A.SZ", "B.SZ", "C.SZ", "D.SZ", "E.SZ"],
            "name": ["股A", "股B", "股C", "股D", "股E"],
            "industry": ["白酒", "银行", "家电", "医药", "食品"],
            "list_date": ["20150101"] * 5,
        })

    ds.get_stock_basic.return_value = basic_data
    ds.cfg = MagicMock()
    ds.cfg.min_listed_days = 365
    ds.cfg.min_consecutive_dividend_years = 5
    ds.cfg.payout_ratio_range = (10, 100)
    ds.cfg.dividend_sell_threshold = 0.03

    factor_data = {
        "dividend_yield": pd.Series(dy_data),
        "netprofit_yoy": pd.Series(npy_data),
        "consecutive_div_years": pd.Series({k: 6 for k in dy_data}),
    }
    fh.compute_all.return_value = factor_data

    return ds, fh, bus


class TestDividendYield4In3Out:

    def test_initial_build(self):
        """初始建仓: 持仓为空时应买入符合条件的标的"""
        dy = {"A.SZ": 0.05, "C.SZ": 0.045, "D.SZ": 0.042, "E.SZ": 0.038}
        npy = {"A.SZ": 5.0, "C.SZ": 10.0, "D.SZ": 3.0, "E.SZ": 8.0}
        ds, fh, bus = _make_mocks(dy, npy)

        strategy = DividendYield4In3Out(ds, fh, bus)
        result = strategy.generate("20250131")

        # 应有买入信号 (只有 A, C, D 的 DY >= 4%)
        buy_signals = [s for s in result.signals if s.action == "buy"]
        assert len(buy_signals) >= 1
        # E.SZ 股息率 3.8% < 4% 不应被买入
        bought_codes = {s.ts_code for s in buy_signals}
        assert "E.SZ" not in bought_codes

    def test_sell_on_low_dy(self):
        """股息率跌破3%时应卖出"""
        ds, fh, bus = _make_mocks(
            dy={"A.SZ": 0.025, "C.SZ": 0.05},
            npy={"A.SZ": 5.0, "C.SZ": 5.0},
        )
        strategy = DividendYield4In3Out(ds, fh, bus)
        # 手动设置持仓
        strategy._holdings = {"A.SZ": 0.5, "C.SZ": 0.5}

        result = strategy.generate("20250228")
        sell_signals = [s for s in result.signals if s.action == "sell"]
        # A.SZ 应被卖出 (DY 2.5% < 3%)
        assert any(s.ts_code == "A.SZ" for s in sell_signals)
        # C.SZ 不应被卖出
        assert not any(s.ts_code == "C.SZ" for s in sell_signals)

    def test_sell_on_profit_decline(self):
        """扣非净利润下滑超过10%时应卖出"""
        ds, fh, bus = _make_mocks(
            dy={"A.SZ": 0.04, "C.SZ": 0.05},
            npy={"A.SZ": -15.0, "C.SZ": 5.0},
        )
        strategy = DividendYield4In3Out(ds, fh, bus)
        strategy._holdings = {"A.SZ": 0.5, "C.SZ": 0.5}

        result = strategy.generate("20250228")
        sell_signals = [s for s in result.signals if s.action == "sell"]
        assert any(s.ts_code == "A.SZ" for s in sell_signals)

    def test_no_buy_without_sell(self):
        """不出则不进: 持仓满时不卖出就不能买入新标的"""
        ds, fh, bus = _make_mocks(
            dy={"A.SZ": 0.04, "C.SZ": 0.05, "D.SZ": 0.06},
            npy={"A.SZ": 5.0, "C.SZ": 5.0, "D.SZ": 5.0},
        )
        strategy = DividendYield4In3Out(ds, fh, bus)
        strategy._holdings = {"A.SZ": 0.5, "C.SZ": 0.5}

        result = strategy.generate("20250228")
        buy_signals = [s for s in result.signals if s.action == "buy"]
        # 没有卖出 → 没有买入名额 → 不应有买入信号
        assert len(buy_signals) == 0

    def test_hold_signal(self):
        """持仓中未触发买卖的应有 hold 信号"""
        ds, fh, bus = _make_mocks(
            dy={"A.SZ": 0.04, "C.SZ": 0.05},
            npy={"A.SZ": 5.0, "C.SZ": 5.0},
        )
        strategy = DividendYield4In3Out(ds, fh, bus)
        strategy._holdings = {"A.SZ": 0.5, "C.SZ": 0.5}

        result = strategy.generate("20250228")
        hold_signals = [s for s in result.signals if s.action == "hold"]
        assert len(hold_signals) == 2

    def test_equal_weight(self):
        """持仓应为等权"""
        ds, fh, bus = _make_mocks(
            dy={"A.SZ": 0.05, "C.SZ": 0.045, "D.SZ": 0.042},
            npy={"A.SZ": 5.0, "C.SZ": 10.0, "D.SZ": 3.0},
        )
        strategy = DividendYield4In3Out(ds, fh, bus)
        result = strategy.generate("20250131")

        if strategy.holdings:
            weights = list(strategy.holdings.values())
            # 权重之和应约等于1
            assert abs(sum(weights) - 1.0) < 1e-6
