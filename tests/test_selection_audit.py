"""
test_selection_audit.py — 选股逻辑审计测试

验证选股流水线中每一步筛选是否严格遵守规则:
  1. 无 ST/*ST 股票
  2. 上市天数足够
  3. 无排除行业股票
  4. 因子阈值正确
  5. 分红连续性达标
  6. ROE/PB 安全标准
  7. 行业排除支持申万分类码
"""
import logging

import pandas as pd
import pytest

from vortex.strategy.filters import (
    FactorRangeFilter,
    FactorThresholdFilter,
    FilterContext,
    FilterPipeline,
    IndustryExcludeFilter,
    MinListedDaysFilter,
    NonSTFilter,
    QuantileCutoffFilter,
)


# ================================================================
#  测试数据 fixtures
# ================================================================

@pytest.fixture
def large_stock_basic():
    """更丰富的股票列表 — 包含 ST、次新、多行业"""
    return pd.DataFrame({
        "ts_code": [
            "000651.SZ",  # 格力电器 - 家用电器
            "600519.SH",  # 贵州茅台 - 白酒
            "601398.SH",  # 工商银行 - 银行
            "600036.SH",  # 招商银行 - 银行
            "601318.SH",  # 中国平安 - 保险
            "000002.SZ",  # 万科A     - 房地产
            "000001.SZ",  # *ST测试   - ST
            "300999.SZ",  # 新股       - 次新
            "600900.SH",  # 长江电力 - 电力
            "002415.SZ",  # 海康威视 - 电子
        ],
        "name": [
            "格力电器", "贵州茅台", "工商银行", "招商银行",
            "中国平安", "万科A", "*ST测试", "新股测试",
            "长江电力", "海康威视",
        ],
        "industry": [
            "家用电器", "白酒", "银行", "银行",
            "保险", "房地产开发", "测试", "测试",
            "电力", "电子",
        ],
        "list_date": [
            "19961118", "20010827", "20061027", "20020409",
            "20070301", "19910129", "20200101", "20250601",
            "20031118", "20100520",
        ],
        "market": ["主板"] * 10,
    })


@pytest.fixture
def large_factor_data():
    """完整因子数据"""
    index = [
        "000651.SZ", "600519.SH", "601398.SH", "600036.SH",
        "601318.SH", "000002.SZ", "000001.SZ", "300999.SZ",
        "600900.SH", "002415.SZ",
    ]
    return {
        "dividend_yield": pd.Series(
            [0.06, 0.015, 0.05, 0.04, 0.02, 0.01, 0.03, 0.0, 0.045, 0.01],
            index=index,
        ),
        "fcf_yield": pd.Series(
            [0.08, 0.12, 0.03, 0.04, 0.06, -0.01, 0.02, 0.0, 0.09, 0.07],
            index=index,
        ),
        "fcf_ttm": pd.Series(
            [5.0, 10.0, 2.0, 3.0, 4.0, -1.0, 1.0, 0.0, 7.0, 6.0],
            index=index,
        ),
        "roe_ttm": pd.Series(
            [0.25, 0.35, 0.12, 0.15, 0.20, 0.08, 0.05, 0.0, 0.18, 0.22],
            index=index,
        ),
        "delta_roe": pd.Series(
            [0.02, 0.01, 0.005, 0.01, 0.015, -0.02, -0.01, 0.0, 0.01, 0.02],
            index=index,
        ),
        "opcfd": pd.Series(
            [1.2, 1.5, 0.8, 0.9, 1.1, 0.5, 0.6, 0.0, 1.3, 1.0],
            index=index,
        ),
        "ep": pd.Series(
            [0.10, 0.03, 0.08, 0.07, 0.05, 0.02, -0.01, 0.0, 0.06, 0.04],
            index=index,
        ),
        "consecutive_div_years": pd.Series(
            [10, 15, 8, 7, 5, 2, 0, 0, 12, 6],
            index=index,
        ),
        "payout_ratio_3y": pd.Series(
            [0.30, 0.50, 0.25, 0.35, 0.40, 0.10, 0.0, 0.0, 0.55, 0.20],
            index=index,
        ),
        "ocf_3y_positive": pd.Series(
            [1.0, 1.0, 1.0, 1.0, 1.0, 0.0, 1.0, 0.0, 1.0, 1.0],
            index=index,
        ),
        "roe_stability": pd.Series(
            [0.02, 0.01, 0.03, 0.04, 0.02, 0.08, 0.10, 0.0, 0.015, 0.03],
            index=index,
        ),
        "netprofit_yoy": pd.Series(
            [5.0, 10.0, 3.0, 5.0, 8.0, -20.0, -30.0, 0.0, 6.0, 12.0],
            index=index,
        ),
        "debt_to_assets": pd.Series(
            [45.0, 30.0, 85.0, 80.0, 75.0, 82.0, 60.0, 0.0, 40.0, 35.0],
            index=index,
        ),
        "roe_over_pb": pd.Series(
            [12.0, 8.0, 10.0, 9.0, 7.5, 4.0, 3.0, 0.0, 11.0, 7.0],
            index=index,
        ),
    }


@pytest.fixture
def audit_ctx(large_stock_basic, settings):
    return FilterContext(
        date="20250630",
        df_basic=large_stock_basic,
        settings=settings,
        log=logging.getLogger("audit"),
    )


# ================================================================
#  审计测试
# ================================================================


class TestNonSTAudit:
    """验证 ST 股票必须被剔除"""

    def test_st_never_in_output(self, large_stock_basic, large_factor_data, audit_ctx):
        pool = set(large_stock_basic["ts_code"])
        f = NonSTFilter()
        result = f.apply(pool, large_factor_data, audit_ctx)

        # 验证: 没有任何 ST 标记的股票
        st_stocks = set(
            large_stock_basic[
                large_stock_basic["name"].str.contains(r"ST|退", na=False)
            ]["ts_code"]
        )
        assert result & st_stocks == set(), f"ST 股票未被剔除: {result & st_stocks}"


class TestMinListedDaysAudit:
    """验证次新股必须被剔除"""

    def test_new_stock_excluded(self, large_stock_basic, large_factor_data, audit_ctx):
        pool = set(large_stock_basic["ts_code"])
        f = MinListedDaysFilter(min_days=365)
        result = f.apply(pool, large_factor_data, audit_ctx)

        # 300999.SZ 上市日 20250601, 距 20250630 只有 29 天
        assert "300999.SZ" not in result, "次新股未被剔除"


class TestIndustryExcludeAudit:
    """验证行业排除器逻辑"""

    def test_legacy_mode_excludes_correctly(self, large_stock_basic, large_factor_data, audit_ctx):
        """降级模式: 用 industry 列名称匹配"""
        from vortex.strategy.dividend import EXCLUDED_INDUSTRIES
        pool = set(large_stock_basic["ts_code"])
        f = IndustryExcludeFilter(industries=EXCLUDED_INDUSTRIES)
        result = f.apply(pool, large_factor_data, audit_ctx)

        # 银行: 601398, 600036
        # 保险: 601318
        # 房地产: 000002
        assert "601398.SH" not in result, "银行股未被剔除"
        assert "600036.SH" not in result, "银行股未被剔除"
        assert "601318.SH" not in result, "保险股未被剔除"
        assert "000002.SZ" not in result, "房地产股未被剔除"

        # 非排除行业应保留
        assert "000651.SZ" in result, "家电股被误剔除"
        assert "600519.SH" in result, "白酒股被误剔除"

    def test_sw_mode_excludes_correctly(self, large_stock_basic, large_factor_data, audit_ctx):
        """申万行业码模式: 用 industry_map 精确匹配"""
        # 构造模拟的申万行业映射
        industry_map = pd.DataFrame({
            "con_code": [
                "601398.SH", "600036.SH",  # 银行
                "601318.SH",                # 保险
                "000002.SZ",                # 房地产
                "000651.SZ",                # 家电
                "600519.SH",                # 白酒
                "600900.SH",                # 电力
            ],
            "index_code": [
                "801780.SI", "801780.SI",   # 银行
                "801790.SI",                # 非银金融
                "801180.SI",                # 房地产
                "801110.SI",                # 家用电器
                "801120.SI",                # 食品饮料
                "801160.SI",                # 公用事业
            ],
            "industry_name": [
                "银行", "银行",
                "非银金融",
                "房地产",
                "家用电器",
                "食品饮料",
                "公用事业",
            ],
            "level": ["L1"] * 7,
        })
        audit_ctx.industry_map = industry_map

        pool = set(large_stock_basic["ts_code"])
        # 用行业名称排除
        f = IndustryExcludeFilter(industries={"银行", "非银金融", "房地产"})
        result = f.apply(pool, large_factor_data, audit_ctx)

        assert "601398.SH" not in result, "银行股未被剔除(申万模式)"
        assert "600036.SH" not in result, "银行股未被剔除(申万模式)"
        assert "601318.SH" not in result, "非银金融股未被剔除(申万模式)"
        assert "000002.SZ" not in result, "房地产股未被剔除(申万模式)"
        assert "000651.SZ" in result, "家电股被误剔除(申万模式)"

    def test_sw_index_code_mode(self, large_stock_basic, large_factor_data, audit_ctx):
        """申万行业码模式: 用 index_code 精确匹配"""
        industry_map = pd.DataFrame({
            "con_code": ["601398.SH", "600036.SH", "000651.SZ"],
            "index_code": ["801780.SI", "801780.SI", "801110.SI"],
            "industry_name": ["银行", "银行", "家用电器"],
            "level": ["L1"] * 3,
        })
        audit_ctx.industry_map = industry_map

        pool = {"601398.SH", "600036.SH", "000651.SZ"}
        f = IndustryExcludeFilter(
            industries=set(),
            sw_index_codes={"801780.SI"},  # 只用代码排除
        )
        result = f.apply(pool, large_factor_data, audit_ctx)

        assert "601398.SH" not in result
        assert "600036.SH" not in result
        assert "000651.SZ" in result


class TestDividendThresholdAudit:
    """验证分红相关的硬门槛"""

    def test_dividend_yield_minimum(self, large_factor_data, audit_ctx):
        """股息率低于卖出线的必须被剔除"""
        pool = set(large_factor_data["dividend_yield"].index)
        # 卖出线 = 3% (Settings default)
        f = FactorThresholdFilter("dividend_yield", op="gte", threshold=0.03)
        result = f.apply(pool, large_factor_data, audit_ctx)

        dy = large_factor_data["dividend_yield"]
        for code in result:
            assert dy[code] >= 0.03, f"{code} 股息率 {dy[code]:.4f} < 0.03"

    def test_consecutive_div_years(self, large_factor_data, audit_ctx):
        """连续分红年数不足的必须被剔除"""
        pool = set(large_factor_data["consecutive_div_years"].index)
        f = FactorThresholdFilter("consecutive_div_years", op="gte", threshold=3)
        result = f.apply(pool, large_factor_data, audit_ctx)

        cdiv = large_factor_data["consecutive_div_years"]
        for code in result:
            assert cdiv[code] >= 3, f"{code} 连续分红 {cdiv[code]} 年 < 3"

    def test_payout_ratio_range(self, large_factor_data, audit_ctx):
        """分红比例不在合理范围内的必须被剔除"""
        pool = set(large_factor_data["payout_ratio_3y"].index)
        f = FactorRangeFilter("payout_ratio_3y", lo=0.10, hi=1.0)
        result = f.apply(pool, large_factor_data, audit_ctx)

        pr = large_factor_data["payout_ratio_3y"]
        for code in result:
            assert 0.10 <= pr[code] <= 1.0, (
                f"{code} 分红比例 {pr[code]:.2f} 不在 [0.10, 1.0] 范围内"
            )


class TestQualityFilterAudit:
    """验证质量类筛选器"""

    def test_fcf_positive(self, large_factor_data, audit_ctx):
        """FCF 必须为正"""
        pool = set(large_factor_data["fcf_ttm"].index)
        f = FactorThresholdFilter("fcf_ttm", op="gt", threshold=0)
        result = f.apply(pool, large_factor_data, audit_ctx)

        fcf = large_factor_data["fcf_ttm"]
        for code in result:
            assert fcf[code] > 0, f"{code} FCF={fcf[code]} <= 0"

        # 负 FCF 的不能在池中
        assert "000002.SZ" not in result, "负FCF股票未被剔除"

    def test_ep_positive(self, large_factor_data, audit_ctx):
        """EP (盈利收益率) 必须为正"""
        pool = set(large_factor_data["ep"].index)
        f = FactorThresholdFilter("ep", op="gt", threshold=0)
        result = f.apply(pool, large_factor_data, audit_ctx)

        ep = large_factor_data["ep"]
        for code in result:
            assert ep[code] > 0, f"{code} EP={ep[code]} <= 0"

    def test_debt_ratio_ceiling(self, large_factor_data, audit_ctx):
        """资产负债率不能超过 70%"""
        pool = set(large_factor_data["debt_to_assets"].index)
        f = FactorThresholdFilter("debt_to_assets", op="lte", threshold=70)
        result = f.apply(pool, large_factor_data, audit_ctx)

        debt = large_factor_data["debt_to_assets"]
        for code in result:
            assert debt[code] <= 70, f"{code} 资产负债率={debt[code]}% > 70%"

    def test_roe_over_pb_safety(self, large_factor_data, audit_ctx):
        """ROE/PB 安全标准 >= 7%"""
        pool = set(large_factor_data["roe_over_pb"].index)
        f = FactorThresholdFilter("roe_over_pb", op="gte", threshold=7.0)
        result = f.apply(pool, large_factor_data, audit_ctx)

        roe_pb = large_factor_data["roe_over_pb"]
        for code in result:
            assert roe_pb[code] >= 7.0, f"{code} ROE/PB={roe_pb[code]} < 7.0%"


class TestFullPipelineAudit:
    """端到端管道审计: 验证完整筛选管道输出的一致性"""

    def test_pipeline_output_respects_all_rules(
        self, large_stock_basic, large_factor_data, audit_ctx
    ):
        """完整管道输出必须同时满足所有规则"""
        from vortex.strategy.dividend import EXCLUDED_INDUSTRIES, build_filter_pipeline
        pipeline = build_filter_pipeline(audit_ctx.settings)

        initial_pool = set(large_stock_basic["ts_code"])
        final_pool, trace = pipeline.run(initial_pool, large_factor_data, audit_ctx)

        # 验证规则 1: 无 ST
        st = set(
            large_stock_basic[
                large_stock_basic["name"].str.contains(r"ST|退", na=False)
            ]["ts_code"]
        )
        assert final_pool & st == set(), f"ST 股票泄漏: {final_pool & st}"

        # 验证规则 2: 无排除行业
        excluded_industry = set(
            large_stock_basic[
                large_stock_basic["industry"].isin(EXCLUDED_INDUSTRIES)
            ]["ts_code"]
        )
        assert final_pool & excluded_industry == set(), (
            f"排除行业股票泄漏: {final_pool & excluded_industry}"
        )

        # 验证规则 3: 因子约束
        for code in final_pool:
            fdata = large_factor_data
            if code in fdata["fcf_ttm"].index:
                assert fdata["fcf_ttm"][code] > 0, f"{code} FCF <= 0"
            if code in fdata["ep"].index:
                assert fdata["ep"][code] > 0, f"{code} EP <= 0"
            if code in fdata["consecutive_div_years"].index:
                assert fdata["consecutive_div_years"][code] >= 3, (
                    f"{code} 连续分红不足 3 年"
                )

        # 验证追踪记录
        assert len(trace) > 0, "筛选追踪记录为空"
        assert all(t["after"] <= t["before"] for t in trace), (
            "筛选过程中出现股票数量增加的异常"
        )

    def test_trace_records_monotonic(self, large_stock_basic, large_factor_data, audit_ctx):
        """追踪记录的 after 值应递减"""
        from vortex.strategy.dividend import build_filter_pipeline
        pipeline = build_filter_pipeline(audit_ctx.settings)

        initial_pool = set(large_stock_basic["ts_code"])
        _, trace = pipeline.run(initial_pool, large_factor_data, audit_ctx)

        afters = [t["after"] for t in trace]
        for i in range(1, len(afters)):
            assert afters[i] <= afters[i - 1], (
                f"筛选步骤 {trace[i]['filter']} 后股票数增加: "
                f"{afters[i - 1]} → {afters[i]}"
            )
