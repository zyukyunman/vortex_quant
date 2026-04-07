"""Phase 1A — PIT 对齐测试。"""
from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest

from vortex.data.pit.aligner import PitAligner
from vortex.data.pit.report import PitReport
from vortex.shared.timezone import MARKET_TZ


class TestPitAligner:
    def test_basic_alignment(self, trading_calendar):
        """正常 PIT 对齐：effective_from = ann_date 次交易日 09:30。"""
        aligner = PitAligner(calendar=trading_calendar)
        df = pd.DataFrame({
            "symbol": ["600519.SH"],
            "ann_date": [date(2026, 4, 1)],
            "report_date": [date(2025, 12, 31)],
            "revenue": [100.0],
        })
        result, report = aligner.align(df)
        assert len(result) == 1
        assert "effective_from" in result.columns
        # ann_date=4/1(周三) → effective_from=4/2(周四) 09:30
        eff = result.iloc[0]["effective_from"]
        assert "2026-04-02" in str(eff)
        assert report.blocked_count == 0

    def test_weekend_ann_date(self, trading_calendar):
        """周末公告 → 下周一 09:30。"""
        aligner = PitAligner(calendar=trading_calendar)
        df = pd.DataFrame({
            "symbol": ["600519.SH"],
            "ann_date": [date(2026, 4, 4)],  # 周六
            "report_date": [date(2025, 12, 31)],
            "revenue": [100.0],
        })
        result, report = aligner.align(df)
        assert len(result) == 1
        eff = str(result.iloc[0]["effective_from"])
        # 周六 → 下周一(4/6) 09:30
        assert "2026-04-06" in eff

    def test_missing_ann_date_blocks(self, trading_calendar):
        """ann_date 缺失 → 阻断，不静默放行。"""
        aligner = PitAligner(calendar=trading_calendar)
        df = pd.DataFrame({
            "symbol": ["600519.SH"],
            "ann_date": [pd.NaT],
            "report_date": [date(2025, 12, 31)],
            "revenue": [100.0],
        })
        result, report = aligner.align(df)
        assert len(result) == 0
        assert report.blocked_count == 1

    def test_ann_before_report_blocks(self, trading_calendar):
        """ann_date < report_date → 数据源异常，阻断。"""
        aligner = PitAligner(calendar=trading_calendar)
        df = pd.DataFrame({
            "symbol": ["600519.SH"],
            "ann_date": [date(2025, 6, 30)],  # 早于报告期
            "report_date": [date(2025, 12, 31)],
            "revenue": [100.0],
        })
        result, report = aligner.align(df)
        assert len(result) == 0
        assert report.blocked_count == 1

    def test_dedup_keeps_latest_ann_date(self, trading_calendar):
        """同 (symbol, report_date) 多条记录 → 保留最新 ann_date。"""
        aligner = PitAligner(calendar=trading_calendar)
        df = pd.DataFrame({
            "symbol": ["600519.SH", "600519.SH"],
            "ann_date": [date(2026, 3, 28), date(2026, 4, 2)],
            "report_date": [date(2025, 12, 31), date(2025, 12, 31)],
            "revenue": [100.0, 105.0],
        })
        result, report = aligner.align(df)
        assert len(result) == 1
        # 保留 ann_date=4/2 的记录（revenue=105）
        assert result.iloc[0]["revenue"] == 105.0
        assert report.overridden_count == 1

    def test_override_recorded_in_report(self, trading_calendar):
        """被覆盖的记录应在 pit_report 中有审计记录。"""
        aligner = PitAligner(calendar=trading_calendar)
        df = pd.DataFrame({
            "symbol": ["600519.SH", "600519.SH"],
            "ann_date": [date(2026, 3, 28), date(2026, 4, 2)],
            "report_date": [date(2025, 12, 31), date(2025, 12, 31)],
            "revenue": [100.0, 105.0],
        })
        _, report = aligner.align(df)
        # report 应记录 override 事件
        overridden = [r for r in report.records if r.status == "overridden"]
        assert len(overridden) >= 1

    def test_empty_df_returns_empty(self, trading_calendar):
        aligner = PitAligner(calendar=trading_calendar)
        result, report = aligner.align(pd.DataFrame())
        assert len(result) == 0

    def test_multiple_symbols(self, trading_calendar):
        """多标的并行对齐。"""
        aligner = PitAligner(calendar=trading_calendar)
        df = pd.DataFrame({
            "symbol": ["600519.SH", "000001.SZ", "300750.SZ"],
            "ann_date": [date(2026, 4, 1), date(2026, 4, 2), date(2026, 4, 3)],
            "report_date": [date(2025, 12, 31)] * 3,
            "revenue": [100.0, 200.0, 300.0],
        })
        result, report = aligner.align(df)
        assert len(result) == 3
        assert report.blocked_count == 0
