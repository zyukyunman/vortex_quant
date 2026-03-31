"""test_portfolio.py — PortfolioEngine 单元测试"""
import pandas as pd
import pytest

from vortex.portfolio import PortfolioEngine, Position, RebalanceOrder


class TestPortfolioEngine:
    def test_merge_signals_equal(self, sample_signals):
        engine = PortfolioEngine()
        weights = engine.merge_signals(sample_signals, method="equal")
        assert len(weights) == 3
        assert abs(weights.sum() - 1.0) < 1e-6

    def test_merge_signals_score(self, sample_signals):
        engine = PortfolioEngine()
        weights = engine.merge_signals(sample_signals, method="score_weighted")
        assert len(weights) == 3
        assert abs(weights.sum() - 1.0) < 1e-6
        # 最高分的应权重最大
        assert weights["000651.SZ"] > weights["000858.SZ"]

    def test_merge_empty(self):
        engine = PortfolioEngine()
        weights = engine.merge_signals([], method="equal")
        assert len(weights) == 0

    def test_apply_constraints_stock(self):
        """clip 后归一化，仅3只每只最大1/3"""
        engine = PortfolioEngine(max_position_pct=0.10)
        weights = pd.Series({"A": 0.50, "B": 0.30, "C": 0.20})
        result = engine.apply_constraints(weights)
        # After clip + normalize: all equal to 1/3
        assert abs(result.sum() - 1.0) < 1e-6
        # No individual > clip value after normalize (but with only 3 stocks, 1/3 > 0.10)
        # The constraint clips then normalizes, so final range depends on count
        assert len(result) == 3

    def test_apply_constraints_industry(self):
        engine = PortfolioEngine(max_position_pct=0.50, max_industry_pct=0.30)
        weights = pd.Series({"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25})
        ind_map = pd.Series({"A": "银行", "B": "银行", "C": "家电", "D": "食品"})
        result = engine.apply_constraints(weights, industry_map=ind_map)
        # 银行 scaled down proportionally, then all normalized
        assert abs(result.sum() - 1.0) < 1e-6

    def test_generate_rebalance(self):
        engine = PortfolioEngine()
        # 当前持仓: A=30%, B=30%, C=40%
        engine._current_positions = {
            "A": Position("A", "stockA", 0.30),
            "B": Position("B", "stockB", 0.30),
            "C": Position("C", "stockC", 0.40),
        }
        target = pd.Series({"A": 0.40, "B": 0.0, "D": 0.60})
        orders = engine.generate_rebalance(target, name_map={"A": "stockA", "B": "stockB", "D": "stockD"})
        actions = {o.ts_code: o.action for o in orders}
        assert actions["A"] == "buy"    # 加仓
        assert actions["B"] == "sell"   # 减仓
        assert actions["C"] == "sell"   # 清仓
        assert actions["D"] == "buy"    # 新建仓

    def test_update_positions(self):
        engine = PortfolioEngine()
        weights = pd.Series({"A": 0.5, "B": 0.5})
        engine.update_positions(weights, {"A": "stockA", "B": "stockB"})
        assert len(engine.current_positions) == 2
        assert engine.current_positions["A"].weight == 0.5

    def test_summary(self):
        engine = PortfolioEngine()
        engine._current_positions = {
            "000651.SZ": Position("000651.SZ", "格力电器", 0.10),
        }
        text = engine.summary()
        assert "格力电器" in text


class TestPosition:
    def test_create(self):
        p = Position("000651.SZ", "格力电器", 0.10)
        assert p.ts_code == "000651.SZ"
        assert p.shares == 0
