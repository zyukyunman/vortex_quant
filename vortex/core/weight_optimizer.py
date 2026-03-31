"""
weight_optimizer.py
因子权重优化器 — 支持固定权重、等权、IC加权

设计理念:
  策略指定参与打分的因子列表，权重由 optimizer 决定。
  - 无行情历史时 → 使用 FixedWeightOptimizer 或 EqualWeightOptimizer
  - 积累足够历史后 → 切换 ICWeightOptimizer 自适应配权
  - 两种模式可通过 fallback 机制自动切换

IC加权核心公式:
  w_i = |IC_mean_i| / Σ|IC_mean_j|   (保持IC符号作为因子方向)

参考: quant-investment SKILL.md 因子权重方法
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ================================================================
#  基类
# ================================================================


class WeightOptimizer(ABC):
    """因子权重优化器基类"""

    name: str = ""

    @abstractmethod
    def optimize(
        self,
        scoring_factors: List[str],
        date: str,
        **kwargs,
    ) -> Dict[str, float]:
        """
        计算因子权重

        Parameters
        ----------
        scoring_factors : List[str]
            参与打分的因子名列表
        date : str
            选股基准日 YYYYMMDD
        **kwargs
            实现类可能需要的额外参数 (factor_data, ds, fh 等)

        Returns
        -------
        Dict[str, float]
            {factor_name: weight}，权重之和 = 1.0
        """
        ...


# ================================================================
#  固定权重
# ================================================================


class FixedWeightOptimizer(WeightOptimizer):
    """
    固定权重 — 手动指定每个因子的权重

    Parameters
    ----------
    weights : Dict[str, float]
        {factor_name: weight}，不需要预归一化
    """

    name = "fixed"

    def __init__(self, weights: Dict[str, float]):
        self._weights = weights

    def optimize(self, scoring_factors, date, **kwargs):
        raw = {f: self._weights.get(f, 0.0) for f in scoring_factors}
        total = sum(raw.values())
        if total == 0:
            return {f: 1.0 / len(scoring_factors) for f in scoring_factors}
        return {f: w / total for f, w in raw.items()}


# ================================================================
#  等权
# ================================================================


class EqualWeightOptimizer(WeightOptimizer):
    """等权 — 所有因子权重相同"""

    name = "equal"

    def optimize(self, scoring_factors, date, **kwargs):
        n = len(scoring_factors)
        if n == 0:
            return {}
        return {f: 1.0 / n for f in scoring_factors}


# ================================================================
#  IC 加权
# ================================================================


class ICWeightOptimizer(WeightOptimizer):
    """
    IC加权因子配权器 — 滚动窗口内 IC 均值加权

    工作流:
      1. 回溯 lookback_months 个月的换仓日
      2. 每个日期计算每个因子的截面 IC (Spearman rank corr vs 下月收益)
      3. IC 均值的绝对值做归一化 → 权重
      4. 历史不足 min_periods 时, 退化到 fallback 优化器

    Parameters
    ----------
    ds : DataStore
        数据源 (读取行情用于计算收益)
    fh : FactorHub
        因子计算中心
    lookback_months : int
        IC 回看月数 (默认 12)
    min_periods : int
        最少需要多少个月的 IC 才启用 (默认 6)
    forward_days : int
        因子 IC 对应的前瞻收益天数 (默认 20 交易日 ≈ 1个月)
    fallback : WeightOptimizer, optional
        历史不足时的退化方案
    """

    name = "ic_weighted"

    def __init__(
        self,
        ds,
        fh,
        lookback_months: int = 12,
        min_periods: int = 6,
        forward_days: int = 20,
        fallback: Optional[WeightOptimizer] = None,
    ):
        self.ds = ds
        self.fh = fh
        self.lookback_months = lookback_months
        self.min_periods = min_periods
        self.forward_days = forward_days
        self.fallback = fallback or EqualWeightOptimizer()
        self._ic_cache: Dict[str, pd.Series] = {}  # factor_name → IC series

    def optimize(self, scoring_factors, date, **kwargs):
        """计算 IC 加权权重"""
        # 获取回溯换仓日
        rebalance_dates = self._get_rebalance_dates(date)

        if len(rebalance_dates) < self.min_periods:
            logger.warning(
                "IC优化器: 历史换仓日 %d 个 < min_periods %d, 退化到 %s",
                len(rebalance_dates), self.min_periods, self.fallback.name,
            )
            return self.fallback.optimize(scoring_factors, date, **kwargs)

        # 计算每个因子的 IC 序列
        ic_means: Dict[str, float] = {}
        ic_details: Dict[str, List[float]] = {}

        for factor_name in scoring_factors:
            ic_series = self._calc_rolling_ic(factor_name, rebalance_dates)
            ic_details[factor_name] = ic_series

            if len(ic_series) >= self.min_periods:
                ic_means[factor_name] = float(np.mean(ic_series))
            else:
                logger.warning("因子 %s IC 数据不足 (%d)", factor_name, len(ic_series))
                ic_means[factor_name] = 0.0

        # IC 加权: 按 |IC均值| 归一化
        abs_total = sum(abs(v) for v in ic_means.values())

        if abs_total < 1e-8:
            logger.warning("所有因子 IC 均值接近 0，退化到等权")
            return EqualWeightOptimizer().optimize(scoring_factors, date)

        weights = {f: abs(ic) / abs_total for f, ic in ic_means.items()}

        # 记录日志
        logger.info("IC加权结果 (回溯 %d 期):", len(rebalance_dates))
        for f in scoring_factors:
            logger.info(
                "  %s: IC_mean=%.4f, weight=%.2f%%",
                f, ic_means.get(f, 0), weights.get(f, 0) * 100,
            )

        return weights

    def get_ic_report(
        self, scoring_factors: List[str], date: str
    ) -> pd.DataFrame:
        """
        生成 IC 诊断报告

        Returns
        -------
        pd.DataFrame
            columns: factor, ic_mean, ic_std, ic_ir, ic_positive_pct, weight
        """
        rebalance_dates = self._get_rebalance_dates(date)
        rows = []

        for factor_name in scoring_factors:
            ic_series = self._calc_rolling_ic(factor_name, rebalance_dates)

            if len(ic_series) < 2:
                rows.append({
                    "factor": factor_name,
                    "ic_mean": np.nan,
                    "ic_std": np.nan,
                    "ic_ir": np.nan,
                    "ic_positive_pct": np.nan,
                    "n_periods": len(ic_series),
                })
                continue

            ic_arr = np.array(ic_series)
            ic_mean = ic_arr.mean()
            ic_std = ic_arr.std()
            ic_ir = ic_mean / ic_std if ic_std > 0 else 0.0
            ic_pos = (ic_arr > 0).mean()

            rows.append({
                "factor": factor_name,
                "ic_mean": round(ic_mean, 4),
                "ic_std": round(ic_std, 4),
                "ic_ir": round(ic_ir, 4),
                "ic_positive_pct": round(ic_pos, 4),
                "n_periods": len(ic_series),
            })

        df = pd.DataFrame(rows)

        # 追加权重列
        abs_total = df["ic_mean"].abs().sum()
        if abs_total > 0:
            df["weight"] = (df["ic_mean"].abs() / abs_total).round(4)
        else:
            n = len(df)
            df["weight"] = 1.0 / n if n > 0 else 0.0

        return df

    # ================================================================
    #  内部方法
    # ================================================================

    def _get_rebalance_dates(self, date: str) -> List[str]:
        """获取过去 lookback_months 个月的月末交易日列表"""
        from vortex.utils.date_utils import load_trade_cal

        cal = load_trade_cal(self.ds.data_dir)
        if cal.empty:
            return []

        cal_sorted = sorted(cal[cal <= date].tolist(), reverse=True)
        if not cal_sorted:
            return []

        # 取月末交易日: 每个月最后一个交易日
        month_ends = []
        seen_months = set()
        for d in cal_sorted:
            ym = d[:6]  # YYYYMM
            if ym not in seen_months:
                seen_months.add(ym)
                month_ends.append(d)
            if len(month_ends) >= self.lookback_months + 2:  # +2 for forward return
                break

        # 不含当月 (当月可能还没走完), 从第2个开始
        if len(month_ends) > 1:
            month_ends = month_ends[1:]

        return sorted(month_ends)

    def _calc_rolling_ic(
        self, factor_name: str, rebalance_dates: List[str]
    ) -> List[float]:
        """计算因子在多个换仓日的 IC 序列"""
        cache_key = f"{factor_name}_{'_'.join(rebalance_dates[:2])}"
        if cache_key in self._ic_cache:
            return self._ic_cache[cache_key].tolist()

        ic_list = []

        for i in range(len(rebalance_dates) - 1):
            cur_date = rebalance_dates[i]
            next_date = rebalance_dates[i + 1]

            try:
                # 计算因子截面值
                factor_values = self.fh.compute(factor_name, cur_date)
                if factor_values.dropna().empty or len(factor_values.dropna()) < 30:
                    continue

                # 计算前瞻收益 (使用月末到下月末的收益)
                forward_returns = self._calc_forward_returns(cur_date, next_date)
                if forward_returns.empty:
                    continue

                # 取交集
                common = factor_values.dropna().index.intersection(
                    forward_returns.dropna().index
                )
                if len(common) < 30:
                    continue

                # Spearman rank correlation
                ic = factor_values[common].corr(
                    forward_returns[common], method="spearman"
                )
                if np.isfinite(ic):
                    ic_list.append(ic)

            except Exception as e:
                logger.debug("IC计算跳过 %s@%s: %s", factor_name, cur_date, e)
                continue

        return ic_list

    def _calc_forward_returns(
        self, start_date: str, end_date: str
    ) -> pd.Series:
        """计算 start_date 到 end_date 的个股收益率"""
        df_start = self.ds.get_daily(trade_date=start_date)
        df_end = self.ds.get_daily(trade_date=end_date)

        if df_start.empty or df_end.empty:
            return pd.Series(dtype=float)

        close_start = df_start.set_index("ts_code")["close"]
        close_end = df_end.set_index("ts_code")["close"]

        common = close_start.index.intersection(close_end.index)
        if len(common) == 0:
            return pd.Series(dtype=float)

        returns = (close_end[common] / close_start[common]) - 1
        return returns


# ================================================================
#  IC_IR 加权 (追求稳定性)
# ================================================================


class ICIRWeightOptimizer(ICWeightOptimizer):
    """
    IC_IR 加权 — 用 IC均值/IC标准差 代替纯IC均值

    稳定性更高的因子获得更大权重。
    """

    name = "ic_ir_weighted"

    def optimize(self, scoring_factors, date, **kwargs):
        rebalance_dates = self._get_rebalance_dates(date)

        if len(rebalance_dates) < self.min_periods:
            logger.warning(
                "IC_IR优化器: 历史不足, 退化到 %s", self.fallback.name,
            )
            return self.fallback.optimize(scoring_factors, date, **kwargs)

        ir_values: Dict[str, float] = {}

        for factor_name in scoring_factors:
            ic_series = self._calc_rolling_ic(factor_name, rebalance_dates)

            if len(ic_series) >= self.min_periods:
                ic_arr = np.array(ic_series)
                ic_mean = ic_arr.mean()
                ic_std = ic_arr.std()
                ir = ic_mean / ic_std if ic_std > 0 else 0.0
                ir_values[factor_name] = ir
            else:
                ir_values[factor_name] = 0.0

        abs_total = sum(abs(v) for v in ir_values.values())

        if abs_total < 1e-8:
            return EqualWeightOptimizer().optimize(scoring_factors, date)

        weights = {f: abs(ir) / abs_total for f, ir in ir_values.items()}

        logger.info("IC_IR加权结果:")
        for f in scoring_factors:
            logger.info(
                "  %s: IC_IR=%.4f, weight=%.2f%%",
                f, ir_values.get(f, 0), weights.get(f, 0) * 100,
            )

        return weights
