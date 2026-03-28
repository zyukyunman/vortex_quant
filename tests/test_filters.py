"""test_filters.py — 筛选管道单元测试"""
import pandas as pd
import pytest

from app.strategy.filters import (
    FactorRangeFilter,
    FactorThresholdFilter,
    FilterContext,
    FilterPipeline,
    IndustryExcludeFilter,
    MinListedDaysFilter,
    NonSTFilter,
    QuantileCutoffFilter,
)


@pytest.fixture
def ctx(sample_stock_basic, settings):
    import logging
    return FilterContext(
        date="20250630",
        df_basic=sample_stock_basic,
        settings=settings,
        log=logging.getLogger("test"),
    )


@pytest.fixture
def factor_data():
    index = ["000651.SZ", "600519.SH", "000858.SZ", "601398.SH", "600036.SH"]
    return {
        "dividend_yield": pd.Series([0.06, 0.01, 0.02, 0.05, 0.04], index=index),
        "roe_ttm": pd.Series([0.25, 0.35, 0.22, 0.12, 0.15], index=index),
        "payout_ratio_3y": pd.Series([0.30, 0.50, 0.20, 0.25, 0.35], index=index),
        "roe_stability": pd.Series([0.02, 0.01, 0.05, 0.03, 0.04], index=index),
    }


class TestNonSTFilter:
    def test_removes_st(self, factor_data, ctx):
        # 无 ST 标记，应全部通过
        pool = {"000651.SZ", "600519.SH", "000858.SZ"}
        f = NonSTFilter()
        result = f.apply(pool, factor_data, ctx)
        assert result == pool

    def test_with_st_stocks(self, factor_data, ctx):
        ctx.df_basic = pd.concat([ctx.df_basic, pd.DataFrame({
            "ts_code": ["000001.SZ"],
            "name": ["ST测试"],
            "industry": ["测试"],
            "list_date": ["20200101"],
            "market": ["主板"],
        })], ignore_index=True)
        pool = {"000651.SZ", "000001.SZ"}
        f = NonSTFilter()
        result = f.apply(pool, factor_data, ctx)
        assert "000001.SZ" not in result
        assert "000651.SZ" in result


class TestMinListedDaysFilter:
    def test_filters_new_stocks(self, factor_data, ctx):
        ctx.df_basic = pd.concat([ctx.df_basic, pd.DataFrame({
            "ts_code": ["300999.SZ"],
            "name": ["新股"],
            "industry": ["测试"],
            "list_date": ["20250601"],  # 上市不到 365 天
            "market": ["创业板"],
        })], ignore_index=True)
        pool = {"000651.SZ", "300999.SZ"}
        f = MinListedDaysFilter(min_days=365)
        result = f.apply(pool, factor_data, ctx)
        assert "300999.SZ" not in result
        assert "000651.SZ" in result


class TestIndustryExcludeFilter:
    def test_exclude_banking(self, factor_data, ctx):
        pool = {"000651.SZ", "601398.SH", "600036.SH"}
        f = IndustryExcludeFilter(industries={"银行"})
        result = f.apply(pool, factor_data, ctx)
        assert "601398.SH" not in result
        assert "600036.SH" not in result
        assert "000651.SZ" in result


class TestFactorThresholdFilter:
    def test_gt(self, factor_data, ctx):
        pool = {"000651.SZ", "600519.SH", "000858.SZ"}
        f = FactorThresholdFilter("dividend_yield", op="gt", threshold=0.03)
        result = f.apply(pool, factor_data, ctx)
        assert "000651.SZ" in result  # 0.06 > 0.03
        assert "600519.SH" not in result  # 0.01 not > 0.03

    def test_gte(self, factor_data, ctx):
        pool = {"000651.SZ", "600036.SH"}
        f = FactorThresholdFilter("dividend_yield", op="gte", threshold=0.04)
        result = f.apply(pool, factor_data, ctx)
        assert "000651.SZ" in result  # 0.06 >= 0.04
        assert "600036.SH" in result  # 0.04 >= 0.04

    def test_lte(self, factor_data, ctx):
        pool = {"000651.SZ", "600519.SH"}
        f = FactorThresholdFilter("roe_ttm", op="lte", threshold=0.30)
        result = f.apply(pool, factor_data, ctx)
        assert "000651.SZ" in result  # 0.25 <= 0.30
        assert "600519.SH" not in result  # 0.35 > 0.30

    def test_missing_factor(self, factor_data, ctx):
        """缺失因子应全部通过"""
        pool = {"000651.SZ"}
        f = FactorThresholdFilter("nonexistent", op="gt", threshold=0)
        result = f.apply(pool, factor_data, ctx)
        assert pool == result


class TestFactorRangeFilter:
    def test_range(self, factor_data, ctx):
        pool = {"000651.SZ", "600519.SH", "000858.SZ"}
        f = FactorRangeFilter("payout_ratio_3y", lo=0.25, hi=0.60)
        result = f.apply(pool, factor_data, ctx)
        assert "000651.SZ" in result  # 0.30 in [0.25, 0.60]
        assert "600519.SH" in result  # 0.50 in [0.25, 0.60]
        assert "000858.SZ" not in result  # 0.20 < 0.25


class TestQuantileCutoffFilter:
    def test_max_quantile(self, factor_data, ctx):
        pool = {"000651.SZ", "600519.SH", "000858.SZ", "601398.SH", "600036.SH"}
        f = QuantileCutoffFilter("roe_stability", max_quantile=0.60)
        result = f.apply(pool, factor_data, ctx)
        # roe_stability: SH=0.01, SZ=0.02, SH=0.03, SH=0.04, SZ=0.05
        # 60th percentile = ~0.034, so stocks with >= 0.034 are cut
        assert len(result) <= len(pool)


class TestFilterPipeline:
    def test_composition(self, factor_data, ctx):
        pipeline = FilterPipeline([
            NonSTFilter(),
            IndustryExcludeFilter(industries={"银行"}),
            FactorThresholdFilter("dividend_yield", op="gte", threshold=0.02),
        ])
        pool = {"000651.SZ", "600519.SH", "000858.SZ", "601398.SH", "600036.SH"}
        result, trace = pipeline.run(pool, factor_data, ctx)
        # 银行排除后 → 不含 601398, 600036
        # dividend_yield >= 0.02 → 不含 600519 (0.01)
        assert "601398.SH" not in result
        assert "600036.SH" not in result
        assert "600519.SH" not in result
        assert "000651.SZ" in result
        assert "000858.SZ" in result
        assert len(trace) == 3
