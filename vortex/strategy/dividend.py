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

from vortex.core.weight_optimizer import (
    FixedWeightOptimizer,
    ICWeightOptimizer,
    WeightOptimizer,
)
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
                 weight_optimizer: Optional[WeightOptimizer] = None,
                 strategy_config=None):
        super().__init__(ds, fh, bus)
        # 策略配置: 优先用显式传入的 strategy_config, 否则从 ds.cfg 读取 (向后兼容)
        self.scfg = strategy_config or ds.cfg
        self.pipeline = build_filter_pipeline(self.scfg)
        self.weight_optimizer = weight_optimizer or FixedWeightOptimizer(DEFAULT_WEIGHTS)

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
        #  Step 6: 多因子打分 (权重由 optimizer 决定)
        # ============================================================
        self.logger.info("Step 6: 多因子打分 (optimizer=%s)...", self.weight_optimizer.name)

        pool_list = sorted(stock_pool)

        # 获取因子权重
        weights_map = self.weight_optimizer.optimize(SCORING_FACTORS, date)
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
                "weight_method": self.weight_optimizer.name,
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
