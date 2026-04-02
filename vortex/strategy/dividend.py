"""
dividend.py
红利质量现金流复合策略 — 月度选股

融合三套规则:
  ① 高股息之家「4进3出」: 股息率门槛 + 分红持续性 + 季报原则
  ② 932315 中证红利质量: 质量多因子打分 (ROE + ΔROE + OPCFD + DP)
  ③ 980092 自由现金流: FCF/EV 排序 + 现金流质量门槛

选股流水线 (7步由 FilterPipeline 和 WeightOptimizer 驱动):
  Step 1-5: 筛选管道 (filters.py 中的可复用筛选器)
  Step 6:   多因子打分 (权重由 WeightOptimizer 动态计算)
  Step 7:   等权分配 + 行业/个股约束

选股频率: 月度 (每月最后一个交易日)
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from vortex.evaluation.spec import EvalSpec, FactorRole
from vortex.factor.base import zscore
from vortex.models import SelectionResult, Signal
from vortex.strategy.base import BaseStrategy
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

logger = logging.getLogger(__name__)

# ---- 行业排除列表 (980092: 金融、房地产) ----
EXCLUDED_INDUSTRIES = {"银行", "保险", "证券", "多元金融", "房地产", "房地产开发", "房地产服务"}

# ---- 默认固定权重 (ICIR加权优化, 2024-12 IC分析) ----
DEFAULT_WEIGHTS = {
    "dividend_yield": 0.49,
    "fcf_yield": 0.0,
    "roe_ttm": 0.0,
    "delta_roe": 0.18,
    "opcfd": 0.0,
    "ep": 0.33,
}

# ---- 参与打分的因子 ----
SCORING_FACTORS = ["dividend_yield", "fcf_yield", "roe_ttm", "delta_roe", "opcfd", "ep"]


def build_filter_pipeline(cfg) -> FilterPipeline:
    """
    构建红利策略的筛选管道

    可被其他策略复用或组合部分筛选器。
    cfg: Settings 或 StrategyConfig 对象 (duck-typing, 只读属性)

    Returns
    -------
    FilterPipeline
        Step 1-5 的完整筛选管道
    """
    return FilterPipeline([
        # -- Step 1: 基础样本空间 --
        NonSTFilter(),
        MinListedDaysFilter(min_days=cfg.min_listed_days),

        # -- Step 2: 行业排除 --
        IndustryExcludeFilter(industries=EXCLUDED_INDUSTRIES),

        # -- Step 3: 盈利与现金流硬门槛 --
        FactorThresholdFilter("fcf_ttm", op="gt", threshold=0),
        FactorThresholdFilter("ocf_3y_positive", op="eq", threshold=1.0),
        FactorThresholdFilter("ep", op="gt", threshold=0),

        # -- Step 4: 分红质量门槛 --
        FactorThresholdFilter(
            "consecutive_div_years", op="gte",
            threshold=cfg.min_consecutive_dividend_years,
        ),
        FactorRangeFilter(
            "payout_ratio_3y",
            lo=cfg.payout_ratio_range[0],
            hi=cfg.payout_ratio_range[1],
        ),
        FactorThresholdFilter(
            "dividend_yield", op="gte",
            threshold=cfg.dividend_sell_threshold,
        ),

        # -- Step 5: 稳定性筛选 --
        QuantileCutoffFilter("roe_stability", max_quantile=0.80),
        FactorThresholdFilter("netprofit_yoy", op="gte", threshold=-10),
        FactorThresholdFilter("debt_to_assets", op="lte", threshold=70),

        # -- Step 5b: ROE/PB 安全标准 (实际投资报酬率 ≥ 7%) --
        FactorThresholdFilter("roe_over_pb", op="gte", threshold=7.0),
    ])


class DividendQualityFCFStrategy(BaseStrategy):
    """红利质量现金流复合策略"""

    name = "dividend_quality_fcf"
    description = (
        "融合高股息之家4进3出 + 中证红利质量(932315) + 自由现金流(980092), "
        "月度选股Top30"
    )

    def __init__(self, ds, fh, bus,
                 weights: Optional[Dict[str, float]] = None,
                 strategy_config=None):
        super().__init__(ds, fh, bus)
        # 策略配置: 优先用显式传入的 strategy_config, 否则从 ds.cfg 读取 (向后兼容)
        self.scfg = strategy_config or ds.cfg
        self.pipeline = build_filter_pipeline(self.scfg)
        # 权重在研究阶段由 WeightTuner 确定, 运行阶段只用固定权重
        self.weights = weights or DEFAULT_WEIGHTS.copy()

    # ------------------------------------------------------------------
    #  eval_specs: 声明策略使用的全部因子及其评测规格
    # ------------------------------------------------------------------
    def eval_specs(self) -> list[EvalSpec]:
        cfg = self.scfg
        specs = [
            # ---- 打分因子 (SCORING) ----
            EvalSpec("dividend_yield", FactorRole.SCORING,
                     factor_family="dividend",
                     data_source="dividend.cash_div_tax(实施,按完整年度汇总) / daily.close",
                     description="静态股息率(最近完整年度)"),
            EvalSpec("fcf_yield", FactorRole.SCORING,
                     factor_family="quality",
                     data_source="cashflow.free_cashflow 或 (n_cashflow_act-capex) / EV",
                     description="自由现金流收益率 FCF/EV"),
            EvalSpec("roe_ttm", FactorRole.SCORING,
                     factor_family="quality",
                     data_source="fina_indicator.roe (最近完整年报, 兼容旧因子名 roe_ttm)",
                     description="最新年报 ROE"),
            EvalSpec("delta_roe", FactorRole.SCORING,
                     factor_family="quality",
                     data_source="fina_indicator.roe 按最近两年年报差分",
                     description="年报 ROE 同比变化"),
            EvalSpec("opcfd", FactorRole.SCORING,
                     factor_family="quality",
                     data_source="cashflow.n_cashflow_act / balancesheet.total_liab",
                     description="经营现金流/总负债"),
            EvalSpec("ep", FactorRole.SCORING,
                     factor_family="value",
                     data_source="valuation.pe_ttm 取倒数", description="盈利收益率 E/P"),
            # ---- 过滤因子 (FILTER) ----
            EvalSpec("consecutive_div_years", FactorRole.FILTER,
                     factor_family="dividend",
                     threshold=cfg.min_consecutive_dividend_years, threshold_op=">=",
                     data_source="dividend(实施) 按年份统计", description="连续分红年数"),
            EvalSpec("fcf_ttm", FactorRole.FILTER,
                     factor_family="cashflow",
                     threshold=0, threshold_op=">",
                     data_source="cashflow.free_cashflow 或 (n_cashflow_act-capex)",
                     description="最近完整年报自由现金流>0"),
            EvalSpec("payout_ratio_3y", FactorRole.FILTER,
                     factor_family="dividend",
                     threshold=cfg.payout_ratio_range[0], threshold_op=">=",
                     data_source="dividend.cash_div_tax×base_share / income.n_income_attr_p (三年平均)",
                     description="三年平均股利支付率"),
            EvalSpec("netprofit_yoy", FactorRole.FILTER,
                     factor_family="growth",
                     threshold=-10, threshold_op=">=",
                     data_source="fina_indicator.profit_dedt 同比, 缺失时退化为 netprofit_yoy",
                     description="扣非净利润同比≥-10%"),
            # ---- 风险因子 (RISK) ----
            EvalSpec("debt_to_assets", FactorRole.RISK,
                     factor_family="risk",
                     threshold=70, threshold_op="<=",
                     data_source="fina_indicator.debt_to_assets",
                     description="资产负债率(观察杠杆尾部风险)"),
            EvalSpec("roe_stability", FactorRole.RISK,
                     factor_family="quality",
                     data_source="fina_indicator.roe 序列标准差(优先季度, 当前库退化年度)",
                     description="ROE稳定性(越小越好)"),
            EvalSpec("dividend_yield_3y", FactorRole.RISK,
                     factor_family="dividend",
                     data_source="dividend.cash_div_tax(实施,按年度汇总) / daily.close",
                     description="三年平均静态股息率(观察尾部风险)"),
        ]
        return specs

    def generate(self, date: str) -> SelectionResult:
        # ============================================================
        #  Step 0: 准备数据
        # ============================================================
        self.logger.info("Step 0: 计算全部因子...")
        factor_data = self.fh.compute_all(date)

        df_basic = self.ds.get_stock_basic()
        if df_basic.empty:
            raise RuntimeError("股票基本信息为空，请先运行 init_data.py")

        universe_size = len(df_basic)

        # ============================================================
        #  Step 1-5: 筛选管道
        # ============================================================
        self.logger.info("Step 1-5: 执行筛选管道...")

        # 尝试加载申万行业映射 (优先使用)
        industry_map = None
        try:
            industry_map = self.ds.get_stock_industry_map()
            if not industry_map.empty:
                self.logger.info("已加载申万行业映射: %d 条", len(industry_map))
        except Exception:
            pass

        ctx = FilterContext(
            date=date,
            df_basic=df_basic,
            settings=self.ds.cfg,
            log=self.logger,
            industry_map=industry_map,
        )
        initial_pool = set(df_basic["ts_code"].tolist())
        stock_pool, filter_trace = self.pipeline.run(initial_pool, factor_data, ctx)

        after_filter_size = len(stock_pool)
        self.logger.info("筛选管道完成: %d → %d 只", universe_size, after_filter_size)

        if after_filter_size == 0:
            self.logger.warning("筛选后无合格标的!")
            return SelectionResult(
                date=date, strategy=self.name, signals=[],
                universe_size=universe_size, after_filter_size=0, top_n=0,
            )

        # ============================================================
        #  Step 6: 多因子打分 (固定权重, 研究阶段已确定)
        # ============================================================
        pool_list = sorted(stock_pool)

        # 归一化权重
        raw_w = {f: self.weights.get(f, 0.0) for f in SCORING_FACTORS}
        w_sum = sum(raw_w.values())
        weights_map = {f: w / w_sum for f, w in raw_w.items()} if w_sum > 0 else {
            f: 1.0 / len(SCORING_FACTORS) for f in SCORING_FACTORS
        }
        self.logger.info("Step 6: 多因子打分 (固定权重)...")
        self.logger.info("因子权重: %s", {k: f"{v:.2%}" for k, v in weights_map.items()})

        # 构建打分矩阵
        score_components = {}
        for factor_name, weight in weights_map.items():
            if weight < 1e-6:
                continue
            fdata = factor_data.get(factor_name, pd.Series(dtype=float))
            if fdata.empty:
                continue
            f_pool = fdata.reindex(pool_list)
            if f_pool.dropna().empty:
                continue
            score_components[factor_name] = zscore(f_pool) * weight

        if not score_components:
            self.logger.error("无可用因子打分!")
            return SelectionResult(
                date=date, strategy=self.name, signals=[],
                universe_size=universe_size, after_filter_size=after_filter_size, top_n=0,
            )

        score_df = pd.DataFrame(score_components, index=pool_list)
        total_score = score_df.sum(axis=1).sort_values(ascending=False, kind='mergesort')

        top_n = min(self.scfg.top_n, len(total_score))
        selected = total_score.head(top_n)
        self.logger.info("Top %d 选出", top_n)

        # ============================================================
        #  Step 7: 等权分配 + 行业/个股约束
        # ============================================================
        self.logger.info("Step 7: 等权分配 + 行业约束...")

        pos_weights = pd.Series(1.0 / top_n, index=selected.index)

        industry_map = df_basic.set_index("ts_code").get("industry", pd.Series(dtype=str))
        pos_weights = self._apply_industry_cap(pos_weights, industry_map)

        max_w = self.scfg.max_weight_per_stock
        pos_weights = pos_weights.clip(upper=max_w)
        pos_weights = pos_weights / pos_weights.sum()

        # ============================================================
        #  构建信号
        # ============================================================
        signals = self._build_signals(
            selected, pos_weights, factor_data, df_basic, industry_map, date,
        )

        return SelectionResult(
            date=date,
            strategy=self.name,
            signals=signals,
            universe_size=universe_size,
            after_filter_size=after_filter_size,
            top_n=top_n,
            metadata={
                "score_weights": weights_map,
                "weight_method": "fixed",
                "filter_trace": filter_trace,
                "filters": {
                    "min_dividend_yield": self.scfg.dividend_sell_threshold,
                    "min_consecutive_div_years": self.scfg.min_consecutive_dividend_years,
                    "payout_ratio_range": self.scfg.payout_ratio_range,
                    "excluded_industries": list(EXCLUDED_INDUSTRIES),
                },
            },
        )

    def _build_signals(
        self,
        selected: pd.Series,
        pos_weights: pd.Series,
        factor_data: Dict[str, pd.Series],
        df_basic: pd.DataFrame,
        industry_map: pd.Series,
        date: str,
    ) -> List[Signal]:
        """将选股结果转为 Signal 列表"""
        name_map = df_basic.set_index("ts_code").get("name", pd.Series(dtype=str))
        industry_display = industry_map.reindex(selected.index).fillna("未知")

        dy = factor_data.get("dividend_yield", pd.Series(dtype=float))
        fcf_yield = factor_data.get("fcf_yield", pd.Series(dtype=float))
        roe = factor_data.get("roe_ttm", pd.Series(dtype=float))
        ep = factor_data.get("ep", pd.Series(dtype=float))
        top_n = len(selected)

        signals: List[Signal] = []
        for ts_code in selected.index:
            stock_name = name_map.get(ts_code, ts_code)
            industry = industry_display.get(ts_code, "未知")

            reasons = []
            if not dy.empty and ts_code in dy.index:
                reasons.append(f"股息率={dy[ts_code]:.1%}")
            if not fcf_yield.empty and ts_code in fcf_yield.index:
                reasons.append(f"FCF/EV={fcf_yield[ts_code]:.3f}")
            if not roe.empty and ts_code in roe.index:
                reasons.append(f"ROE={roe[ts_code]:.1f}%")
            if not ep.empty and ts_code in ep.index:
                pe_val = 1.0 / ep[ts_code] if ep[ts_code] > 0 else 0
                reasons.append(f"PE={pe_val:.1f}")
            reasons.append(f"行业={industry}")

            signals.append(Signal(
                date=date,
                strategy=self.name,
                ts_code=ts_code,
                name=stock_name,
                action="buy",
                weight=round(pos_weights.get(ts_code, 1.0 / top_n), 4),
                score=round(selected[ts_code], 4),
                reason=" | ".join(reasons),
                metadata={"industry": industry, "rank": len(signals) + 1},
            ))

        return signals

    def _apply_industry_cap(
        self, weights: pd.Series, industry_map: pd.Series
    ) -> pd.Series:
        """行业权重上限约束"""
        max_ind = self.scfg.max_weight_per_industry
        industries = industry_map.reindex(weights.index).fillna("未知")

        for _ in range(10):
            ind_weights = weights.groupby(industries).sum()
            over = ind_weights[ind_weights > max_ind]
            if over.empty:
                break
            for ind in over.index:
                mask = industries == ind
                ind_stocks = weights[mask]
                scale = max_ind / ind_stocks.sum()
                weights[mask] = ind_stocks * scale
            weights = weights / weights.sum()

        return weights
