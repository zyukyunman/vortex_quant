"""
dividend_classic.py
高股息之家「股息率模式」经典策略 — 事件驱动型 4进3出

核心逻辑 (与 dividend.py 的月度轮动不同):
  - 买入: 股息率 ≥ 4%
  - 卖出: 股息率 < 3% 或 季报扣非净利润同比下滑 > 10%
  - 仓位: 不出则不进, 等权, 单只 ≤ 10%, 单行业 ≤ 20%
  - 持仓: ≥ 10 只

特点:
  - 低换手: 只在买卖条件触发时操作，非定期轮动
  - 持股守息: 核心操作是"持有"，买卖是例外事件
  - 反脆弱: 跌了吃股息(股息率更高)，涨了吃差价(4→3 = 33%)
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Set

import numpy as np
import pandas as pd

from vortex.models import SelectionResult, Signal
from vortex.strategy.base import BaseStrategy
from vortex.strategy.filters import (
    FactorThresholdFilter,
    FilterContext,
    FilterPipeline,
    IndustryExcludeFilter,
    NonSTFilter,
)

logger = logging.getLogger(__name__)

EXCLUDED_INDUSTRIES = {"银行", "保险", "证券", "多元金融", "房地产", "房地产开发", "房地产服务"}

# 策略参数
BUY_THRESHOLD = 0.04   # 买入门槛: 股息率 ≥ 4%
SELL_THRESHOLD = 0.03  # 卖出门槛: 股息率 < 3%
PROFIT_DECLINE = -10   # 扣非净利润同比增速 < -10%
MIN_POSITIONS = 10     # 最少持仓数
MAX_POSITIONS = 30     # 最多持仓数
MAX_SINGLE = 0.10      # 单只上限 10%
MAX_INDUSTRY = 0.20    # 单行业上限 20%
MIN_DIV_YEARS = 5      # 连续分红 ≥ 5 年


def build_candidate_pipeline() -> FilterPipeline:
    """构建候选池筛选管道 (对应 SKILL.md 2.1 节)"""
    return FilterPipeline([
        NonSTFilter(),
        IndustryExcludeFilter(industries=EXCLUDED_INDUSTRIES),
        FactorThresholdFilter("consecutive_div_years", op="gte", threshold=MIN_DIV_YEARS),
        FactorThresholdFilter("netprofit_yoy", op="gte", threshold=PROFIT_DECLINE),
        FactorThresholdFilter("dividend_yield", op="gte", threshold=BUY_THRESHOLD),
    ])


class DividendYield4In3Out(BaseStrategy):
    """
    高股息之家经典策略 — 事件驱动型 4进3出

    与 DividendQualityFCFStrategy(月度Top30轮动) 并行运行，
    产出独立的 Signal 流，供回测引擎或实盘分别跟踪。
    """

    name = "dividend_4in3out"
    description = "高股息之家4进3出: 股息率≥4%买, <3%卖, 不出则不进"

    def __init__(self, ds, fh, bus):
        super().__init__(ds, fh, bus)
        self.pipeline = build_candidate_pipeline()
        # 持仓状态 (跨期保持)
        self._holdings: Dict[str, float] = {}  # {ts_code: weight}

    @property
    def holdings(self) -> Dict[str, float]:
        return dict(self._holdings)

    def generate(self, date: str) -> SelectionResult:
        """
        事件驱动选股:
          1) 扫描持仓 → 生成卖出信号
          2) 计算可买入名额 (不出则不进)
          3) 从候选池买入
          4) 等权再平衡
        """
        self.logger.info("Step 0: 计算因子...")
        factor_data = self.fh.compute_all(date)
        df_basic = self.ds.get_stock_basic()
        if df_basic.empty:
            raise RuntimeError("股票基本信息为空")

        universe_size = len(df_basic)
        dy = factor_data.get("dividend_yield", pd.Series(dtype=float))
        npy = factor_data.get("netprofit_yoy", pd.Series(dtype=float))

        signals: List[Signal] = []
        name_map = df_basic.set_index("ts_code").get("name", pd.Series(dtype=str))
        industry_map = df_basic.set_index("ts_code").get("industry", pd.Series(dtype=str))

        # ============================================================
        #  Step 1: 扫描持仓 → 卖出信号
        # ============================================================
        self.logger.info("Step 1: 扫描持仓，检查卖出条件...")
        sell_codes: List[str] = []

        for ts_code in list(self._holdings.keys()):
            should_sell = False
            reason_parts = []

            # 条件1: 股息率 < 3%
            if ts_code in dy.index and dy[ts_code] < SELL_THRESHOLD:
                should_sell = True
                reason_parts.append(f"股息率={dy[ts_code]:.2%}<{SELL_THRESHOLD:.0%}")

            # 条件2: 扣非净利润同比下滑 > 10%
            if ts_code in npy.index and npy[ts_code] < PROFIT_DECLINE:
                should_sell = True
                reason_parts.append(f"扣非增速={npy[ts_code]:.1f}%<{PROFIT_DECLINE}%")

            if should_sell:
                sell_codes.append(ts_code)
                signals.append(Signal(
                    date=date,
                    strategy=self.name,
                    ts_code=ts_code,
                    name=name_map.get(ts_code, ts_code),
                    action="sell",
                    weight=0.0,
                    score=0.0,
                    reason=" | ".join(reason_parts),
                ))

        # 移除卖出标的
        for code in sell_codes:
            del self._holdings[code]

        self.logger.info("卖出 %d 只: %s", len(sell_codes), sell_codes)

        # ============================================================
        #  Step 2: 不出则不进 — 计算可买名额
        # ============================================================
        current_count = len(self._holdings)
        # 初始建仓: 如果持仓为空，允许买满 MIN_POSITIONS
        if current_count == 0:
            buy_slots = MIN_POSITIONS
        else:
            buy_slots = len(sell_codes)

        # 不超过最大持仓数
        buy_slots = min(buy_slots, MAX_POSITIONS - current_count)

        self.logger.info(
            "当前持仓 %d, 卖出 %d, 可买入名额 %d",
            current_count, len(sell_codes), buy_slots,
        )

        # ============================================================
        #  Step 3: 从候选池买入
        # ============================================================
        buy_codes: List[str] = []

        if buy_slots > 0:
            self.logger.info("Step 3: 筛选候选池 (4进)...")
            ctx = FilterContext(
                date=date,
                df_basic=df_basic,
                settings=self.ds.cfg,
                log=self.logger,
            )
            initial_pool = set(df_basic["ts_code"].tolist()) - set(self._holdings.keys())
            candidate_pool, _ = self.pipeline.run(initial_pool, factor_data, ctx)

            # 按股息率从高到低排序
            candidates = dy.reindex(list(candidate_pool)).dropna().sort_values(ascending=False)

            # 行业约束: 已持有行业的权重检查
            for ts_code in candidates.index:
                if len(buy_codes) >= buy_slots:
                    break

                ind = industry_map.get(ts_code, "未知")
                # 检查行业上限
                future_holdings = {**self._holdings}
                for bc in buy_codes:
                    future_holdings[bc] = 1.0  # placeholder
                ind_count = sum(
                    1 for c in future_holdings
                    if industry_map.get(c, "未知") == ind
                )
                total_future = len(future_holdings) + 1
                if total_future > 0 and ind_count / total_future > MAX_INDUSTRY:
                    continue

                buy_codes.append(ts_code)
                signals.append(Signal(
                    date=date,
                    strategy=self.name,
                    ts_code=ts_code,
                    name=name_map.get(ts_code, ts_code),
                    action="buy",
                    weight=0.0,  # 稍后等权分配
                    score=dy.get(ts_code, 0),
                    reason=f"股息率={dy.get(ts_code, 0):.2%}≥{BUY_THRESHOLD:.0%}",
                ))

        self.logger.info("买入 %d 只: %s", len(buy_codes), buy_codes)

        # ============================================================
        #  Step 4: 等权分配 + 更新持仓
        # ============================================================
        for code in buy_codes:
            self._holdings[code] = 0.0  # placeholder

        n = len(self._holdings)
        if n > 0:
            equal_w = min(1.0 / n, MAX_SINGLE)
            for code in self._holdings:
                self._holdings[code] = equal_w
            # 归一化
            total_w = sum(self._holdings.values())
            if total_w > 0:
                for code in self._holdings:
                    self._holdings[code] /= total_w

        # 更新 buy 信号的权重
        for sig in signals:
            if sig.action == "buy" and sig.ts_code in self._holdings:
                sig.weight = round(self._holdings[sig.ts_code], 4)

        # 生成 hold 信号 (当前持仓中未买未卖的)
        hold_codes = set(self._holdings.keys()) - set(buy_codes) - set(sell_codes)
        for ts_code in sorted(hold_codes):
            signals.append(Signal(
                date=date,
                strategy=self.name,
                ts_code=ts_code,
                name=name_map.get(ts_code, ts_code),
                action="hold",
                weight=round(self._holdings.get(ts_code, 0), 4),
                score=dy.get(ts_code, 0),
                reason=f"持股守息 | 股息率={dy.get(ts_code, 0):.2%}",
            ))

        after_filter = len(buy_codes) + len(hold_codes) + len(sell_codes)

        return SelectionResult(
            date=date,
            strategy=self.name,
            signals=signals,
            universe_size=universe_size,
            after_filter_size=after_filter,
            top_n=len(self._holdings),
            metadata={
                "sell_count": len(sell_codes),
                "buy_count": len(buy_codes),
                "hold_count": len(hold_codes),
                "total_positions": len(self._holdings),
            },
        )
