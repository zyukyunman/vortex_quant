"""test_risk.py — RiskManager 单元测试"""
import pandas as pd
import pytest

from vortex.risk.manager import RiskCheckResult, RiskManager, RiskReport


class TestRiskCheckResult:
    def test_passed(self):
        r = RiskCheckResult(passed=True, rule="test", detail="ok")
        assert r.passed

    def test_failed(self):
        r = RiskCheckResult(passed=False, level="CRITICAL", rule="test", detail="fail")
        assert not r.passed
        assert r.level == "CRITICAL"


class TestRiskReport:
    def test_passed_all(self):
        report = RiskReport(
            date="20250630",
            checks=[
                RiskCheckResult(passed=True, rule="a", detail="ok"),
                RiskCheckResult(passed=True, rule="b", detail="ok"),
            ],
        )
        assert report.passed

    def test_one_fail(self):
        report = RiskReport(
            date="20250630",
            checks=[
                RiskCheckResult(passed=True, rule="a"),
                RiskCheckResult(passed=False, rule="b"),
            ],
        )
        assert not report.passed

    def test_summary(self):
        report = RiskReport(date="20250630", checks=[
            RiskCheckResult(passed=True, rule="test", detail="ok"),
        ])
        text = report.summary()
        assert "通过" in text


class TestRiskManager:
    def test_pre_trade_check_pass(self):
        rm = RiskManager(max_position_pct=0.10, max_industry_pct=0.30)
        weights = pd.Series({"A": 0.05, "B": 0.05, "C": 0.05, "D": 0.05, "E": 0.05,
                            "F": 0.05, "G": 0.05, "H": 0.05, "I": 0.05, "J": 0.05,
                            "K": 0.05, "L": 0.05, "M": 0.05, "N": 0.05, "O": 0.05,
                            "P": 0.05, "Q": 0.05, "R": 0.05, "S": 0.05, "T": 0.05})
        ind_map = pd.Series({k: f"ind{i}" for i, k in enumerate(weights.index)})
        report = rm.pre_trade_check(weights, ind_map)
        assert report.passed

    def test_pre_trade_check_concentration(self):
        rm = RiskManager(max_position_pct=0.10)
        weights = pd.Series({"A": 0.50, "B": 0.50})
        report = rm.pre_trade_check(weights)
        # 单票 50% > 10%
        has_warning = any(not c.passed for c in report.checks)
        assert has_warning

    def test_pre_trade_few_positions(self):
        rm = RiskManager()
        weights = pd.Series({"A": 0.50, "B": 0.50})
        report = rm.pre_trade_check(weights)
        # 只有 2 只，分散度不足
        diversification_check = [c for c in report.checks if c.rule == "分散度"]
        assert len(diversification_check) == 1
        assert not diversification_check[0].passed

    def test_monitor_normal(self):
        rm = RiskManager(max_daily_loss=0.02, max_drawdown=0.15)
        report = rm.monitor(daily_return=0.01, nav=1.01)
        assert report.passed

    def test_monitor_daily_loss(self):
        rm = RiskManager(max_daily_loss=0.02)
        report = rm.monitor(daily_return=-0.03, nav=0.97)
        daily_check = [c for c in report.checks if c.rule == "单日亏损"]
        assert not daily_check[0].passed

    def test_monitor_drawdown(self):
        rm = RiskManager(max_drawdown=0.10)
        rm._nav_history = [1.0, 1.05, 1.10]  # peak = 1.10
        report = rm.monitor(daily_return=-0.05, nav=0.95)
        dd_check = [c for c in report.checks if c.rule == "最大回撤"]
        # drawdown = (1.10 - 0.95) / 1.10 = 13.6% > 10%
        assert not dd_check[0].passed

    def test_attribution(self):
        rm = RiskManager()
        weights = pd.Series({"A": 0.5, "B": 0.5})
        returns = pd.Series({"A": 0.02, "B": -0.01})
        result = rm.post_trade_attribution(weights, returns)
        assert abs(result["portfolio_return"] - 0.005) < 1e-6
