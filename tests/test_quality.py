"""Phase 1A — 质量门禁测试。"""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from vortex.data.quality.engine import QualityEngine
from vortex.data.quality.report import QualityContext, QualityReport
from vortex.data.quality.rules.not_empty import NotEmptyRule
from vortex.data.quality.rules.no_all_nan import NoAllNanRule
from vortex.data.quality.rules.date_continuity import DateContinuityRule
from vortex.data.quality.rules.ohlcv_range import OhlcvRangeRule
from vortex.data.quality.rules.missing_ratio import MissingRatioRule
from vortex.data.quality.rules.volume_zero_ratio import VolumeZeroRatioRule


@pytest.fixture
def all_rules():
    return [
        NotEmptyRule(),
        NoAllNanRule(),
        DateContinuityRule(),
        OhlcvRangeRule(),
        MissingRatioRule(),
        VolumeZeroRatioRule(),
    ]


@pytest.fixture
def ctx():
    return QualityContext(
        dataset="bars",
        trading_days=[date(2026, 4, d) for d in [1, 2, 3, 6, 7]],
    )


# ── NotEmpty ───────────────────────────────────────────────────────

class TestNotEmpty:
    def test_empty_df_fails(self, ctx):
        rule = NotEmptyRule()
        result = rule.check(pd.DataFrame(), ctx)
        assert not result.passed
        assert result.level == "critical"

    def test_nonempty_df_passes(self, ctx, sample_bars):
        rule = NotEmptyRule()
        result = rule.check(sample_bars, ctx)
        assert result.passed


# ── NoAllNan ───────────────────────────────────────────────────────

class TestNoAllNan:
    def test_all_nan_column_fails(self, ctx):
        rule = NoAllNanRule()
        df = pd.DataFrame({
            "symbol": ["600519.SH"], "date": ["20260401"],
            "close": [float("nan")], "open": [float("nan")],
        })
        result = rule.check(df, ctx)
        assert not result.passed

    def test_partial_nan_passes(self, ctx):
        rule = NoAllNanRule()
        df = pd.DataFrame({
            "symbol": ["600519.SH", "000001.SZ"],
            "date": ["20260401", "20260401"],
            "close": [1800.0, float("nan")],
        })
        result = rule.check(df, ctx)
        assert result.passed


# ── OhlcvRange ─────────────────────────────────────────────────────

class TestOhlcvRange:
    def test_valid_ohlcv_passes(self, ctx, sample_bars):
        rule = OhlcvRangeRule()
        result = rule.check(sample_bars, ctx)
        assert result.passed

    def test_negative_price_fails(self, ctx):
        rule = OhlcvRangeRule()
        df = pd.DataFrame({
            "symbol": ["X"], "date": ["20260401"],
            "open": [-1.0], "high": [10.0], "low": [1.0],
            "close": [5.0], "volume": [100.0], "amount": [1000.0],
        })
        result = rule.check(df, ctx)
        assert not result.passed

    def test_high_less_than_low_passes_range_only(self, ctx):
        """ohlcv_range 只检查值域（>0、>=0），不检查 high>=low 逻辑关系。"""
        rule = OhlcvRangeRule()
        df = pd.DataFrame({
            "symbol": ["X"], "date": ["20260401"],
            "open": [10.0], "high": [5.0], "low": [8.0],
            "close": [9.0], "volume": [100.0], "amount": [1000.0],
        })
        result = rule.check(df, ctx)
        assert result.passed  # 值都 > 0，range 检查通过


# ── DateContinuity ─────────────────────────────────────────────────

class TestDateContinuity:
    def test_continuous_passes(self, ctx):
        rule = DateContinuityRule()
        df = pd.DataFrame({
            "date": ["20260401", "20260402", "20260403", "20260406", "20260407"],
        })
        result = rule.check(df, ctx)
        assert result.passed

    def test_gap_fails(self):
        rule = DateContinuityRule()
        # 有 5 个交易日但数据只覆盖 3 个
        ctx = QualityContext(
            dataset="bars",
            trading_days=[date(2026, 4, d) for d in [1, 2, 3, 6, 7]],
        )
        df = pd.DataFrame({"date": ["20260401", "20260407"]})
        result = rule.check(df, ctx)
        # 缺失率 60% > 2%，应该失败
        assert not result.passed


# ── MissingRatio ───────────────────────────────────────────────────

class TestMissingRatio:
    def test_no_missing_passes(self, ctx, sample_bars):
        rule = MissingRatioRule()
        result = rule.check(sample_bars, ctx)
        assert result.passed

    def test_high_missing_warns(self, ctx):
        rule = MissingRatioRule()
        import numpy as np
        df = pd.DataFrame({
            "close": [1.0] + [np.nan] * 19,
            "volume": [100.0] * 20,
        })
        result = rule.check(df, ctx)
        # 95% 缺失 > 5% 阈值
        assert not result.passed
        assert result.level == "warning"


# ── VolumeZeroRatio ────────────────────────────────────────────────

class TestVolumeZeroRatio:
    def test_normal_passes(self, ctx, sample_bars):
        rule = VolumeZeroRatioRule()
        result = rule.check(sample_bars, ctx)
        assert result.passed

    def test_all_zero_warns(self, ctx):
        rule = VolumeZeroRatioRule()
        df = pd.DataFrame({"volume": [0.0] * 20})
        result = rule.check(df, ctx)
        assert not result.passed
        assert result.level == "warning"


# ── QualityEngine 集成 ─────────────────────────────────────────────

class TestQualityEngine:
    def test_all_pass(self, all_rules, ctx, sample_bars):
        engine = QualityEngine(rules=all_rules)
        report = engine.run("bars", sample_bars, ctx)
        assert report.overall_status == "PASSED"
        assert report.passed is True

    def test_critical_failure_blocks(self, all_rules, ctx):
        engine = QualityEngine(rules=all_rules)
        report = engine.run("bars", pd.DataFrame(), ctx)
        assert report.overall_status == "FAILED"
        assert report.passed is False

    def test_report_contains_all_results(self, all_rules, ctx, sample_bars):
        engine = QualityEngine(rules=all_rules)
        report = engine.run("bars", sample_bars, ctx)
        assert len(report.results) == len(all_rules)
