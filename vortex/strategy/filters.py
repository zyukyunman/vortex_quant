"""
filters.py
可复用选股筛选器 — 管道式组合

设计理念:
  每个筛选器是独立的、可组合的最小单元。
  策略通过组装筛选器管道实现不同选股逻辑。
  新策略只需复用已有筛选器，按业务需要排列组合。

使用示例:
    pipeline = FilterPipeline([
        NonSTFilter(),
        MinListedDaysFilter(min_days=365),
        IndustryExcludeFilter(industries={"银行", "保险"}),
        FactorThresholdFilter("fcf_ttm", op="gt", threshold=0),
        FactorThresholdFilter("consecutive_div_years", op="gte", threshold=3),
    ])
    pool = pipeline.run(initial_pool, factor_data, ctx)
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import pandas as pd

from vortex.config.settings import Settings

logger = logging.getLogger(__name__)


# ================================================================
#  筛选上下文
# ================================================================


@dataclass
class FilterContext:
    """管道共享上下文 — 只传递筛选过程中不变的公共数据"""

    date: str                    # 选股基准日
    df_basic: pd.DataFrame       # 股票基本信息 (ts_code, name, industry, list_date ...)
    settings: Settings           # 全局配置
    log: logging.Logger = field(default_factory=lambda: logging.getLogger("filters"))
    industry_map: Optional[pd.DataFrame] = None  # 申万行业映射 (con_code → index_code, industry_name, level)


# ================================================================
#  筛选器基类
# ================================================================


class StockFilter(ABC):
    """筛选器抽象基类"""

    name: str = ""

    @abstractmethod
    def apply(
        self,
        stock_pool: Set[str],
        factor_data: Dict[str, pd.Series],
        ctx: FilterContext,
    ) -> Set[str]:
        """
        输入股票池，输出过滤后的股票池。

        Parameters
        ----------
        stock_pool : Set[str]
            当前候选代码集合
        factor_data : Dict[str, pd.Series]
            全部因子数据 {factor_name: Series(index=ts_code)}
        ctx : FilterContext
            共享上下文

        Returns
        -------
        Set[str]
            过滤后的代码集合 (只减不增)
        """
        ...

    def __repr__(self):
        return f"<Filter:{self.name}>"


# ================================================================
#  管道执行器
# ================================================================


class FilterPipeline:
    """
    按顺序执行一组筛选器，记录每步的进出数量。

    用法:
        pipeline = FilterPipeline([filter1, filter2, ...])
        pool, trace = pipeline.run(initial_pool, factor_data, ctx)
    """

    def __init__(self, filters: List[StockFilter]):
        self.filters = filters

    def run(
        self,
        initial_pool: Set[str],
        factor_data: Dict[str, pd.Series],
        ctx: FilterContext,
    ) -> tuple[Set[str], List[Dict]]:
        """
        执行全部筛选器，返回 (最终股票池, 筛选追踪记录)。

        追踪记录格式: [{"filter": name, "before": N, "after": M}, ...]
        """
        pool = set(initial_pool)
        trace: List[Dict] = []

        for f in self.filters:
            before = len(pool)
            pool = f.apply(pool, factor_data, ctx)
            after = len(pool)
            trace.append({
                "filter": f.name,
                "before": before,
                "after": after,
                "removed": before - after,
            })
            ctx.log.info(
                "  [%s] %d → %d (剔除 %d)",
                f.name, before, after, before - after,
            )

        return pool, trace


# ================================================================
#  具体筛选器实现
# ================================================================


class NonSTFilter(StockFilter):
    """剔除 ST / *ST / 退市股"""

    name = "non_st"

    def apply(self, stock_pool, factor_data, ctx):
        df = ctx.df_basic
        non_st = set(df[~df["name"].str.contains(r"ST|退", na=False)]["ts_code"])
        return stock_pool & non_st


class MinListedDaysFilter(StockFilter):
    """剔除上市不满 N 天的股票"""

    name = "min_listed_days"

    def __init__(self, min_days: Optional[int] = None):
        self._min_days = min_days

    def apply(self, stock_pool, factor_data, ctx):
        min_days = self._min_days or ctx.settings.min_listed_days
        df = ctx.df_basic.copy()
        df["list_date"] = pd.to_datetime(df["list_date"], format="%Y%m%d")
        cutoff = pd.Timestamp(ctx.date) - pd.Timedelta(days=min_days)
        qualified = set(df[df["list_date"] <= cutoff]["ts_code"])
        return stock_pool & qualified


class IndustryExcludeFilter(StockFilter):
    """
    剔除指定行业股票

    支持两种匹配模式 (自动选择):
    1. 优先: 若 ctx.industry_map 有数据 → 用申万行业分类代码/名称精确匹配
    2. 降级: 否则用 stock_basic.industry 列做名称匹配 (旧逻辑)

    Parameters
    ----------
    industries : Set[str]
        要排除的行业名称集合 (如 {"银行", "保险", "房地产"})
    sw_index_codes : Set[str], optional
        要排除的申万行业指数代码 (如 {"801780.SI", "801790.SI"})
        与 industries 取并集
    level : str
        匹配申万层级, "L1" 或 "L2", 默认 "L1"
    """

    name = "industry_exclude"

    def __init__(
        self,
        industries: Set[str],
        sw_index_codes: Optional[Set[str]] = None,
        level: str = "L1",
    ):
        self._industries = industries
        self._sw_index_codes = sw_index_codes or set()
        self._level = level

    def apply(self, stock_pool, factor_data, ctx):
        # 优先使用申万行业映射
        if ctx.industry_map is not None and not ctx.industry_map.empty:
            return self._apply_sw(stock_pool, ctx)
        # 降级: 字符串匹配
        return self._apply_legacy(stock_pool, ctx)

    def _apply_sw(self, stock_pool: Set[str], ctx: FilterContext) -> Set[str]:
        """用申万行业分类精确匹配"""
        imap = ctx.industry_map
        # 筛选指定层级
        if "level" in imap.columns:
            imap = imap[imap["level"] == self._level]

        # con_code 列名可能是 con_code 或 ts_code
        code_col = "con_code" if "con_code" in imap.columns else "ts_code"

        # 按行业名称匹配
        name_match = set()
        if "industry_name" in imap.columns and self._industries:
            name_match = set(
                imap[imap["industry_name"].isin(self._industries)][code_col]
            )

        # 按行业代码匹配
        code_match = set()
        if "index_code" in imap.columns and self._sw_index_codes:
            code_match = set(
                imap[imap["index_code"].isin(self._sw_index_codes)][code_col]
            )

        excluded = name_match | code_match
        ctx.log.debug("申万行业排除: %d 只 (名称匹配 %d, 代码匹配 %d)",
                       len(excluded & stock_pool), len(name_match & stock_pool),
                       len(code_match & stock_pool))
        return stock_pool - excluded

    def _apply_legacy(self, stock_pool: Set[str], ctx: FilterContext) -> Set[str]:
        """降级: 用 stock_basic.industry 列做名称匹配"""
        df = ctx.df_basic
        if "industry" not in df.columns:
            return stock_pool
        excluded = set(df[df["industry"].isin(self._industries)]["ts_code"])
        ctx.log.debug("行业排除(降级模式-名称匹配): %d 只", len(excluded & stock_pool))
        return stock_pool - excluded


class FactorThresholdFilter(StockFilter):
    """
    基于因子阈值的通用筛选器

    Parameters
    ----------
    factor_name : str
        因子名
    op : str
        比较运算符: "gt", "gte", "lt", "lte", "eq"
    threshold : float
        阈值
    """

    OPERATORS = {
        "gt":  lambda s, t: s > t,
        "gte": lambda s, t: s >= t,
        "lt":  lambda s, t: s < t,
        "lte": lambda s, t: s <= t,
        "eq":  lambda s, t: s == t,
    }

    def __init__(self, factor_name: str, op: str, threshold: float):
        self._factor = factor_name
        self._op = op
        self._threshold = threshold
        self.name = f"{factor_name}_{op}_{threshold}"

    def apply(self, stock_pool, factor_data, ctx):
        series = factor_data.get(self._factor, pd.Series(dtype=float))
        if series.empty:
            ctx.log.warning("因子 %s 数据为空，跳过阈值筛选", self._factor)
            return stock_pool
        mask_fn = self.OPERATORS[self._op]
        in_pool = series.reindex(list(stock_pool)).dropna()
        qualified = set(in_pool[mask_fn(in_pool, self._threshold)].index)
        return stock_pool & qualified


class FactorRangeFilter(StockFilter):
    """因子范围筛选: lo <= factor <= hi"""

    def __init__(self, factor_name: str, lo: float, hi: float):
        self._factor = factor_name
        self._lo = lo
        self._hi = hi
        self.name = f"{factor_name}_range_{lo}_{hi}"

    def apply(self, stock_pool, factor_data, ctx):
        series = factor_data.get(self._factor, pd.Series(dtype=float))
        if series.empty:
            ctx.log.warning("因子 %s 数据为空，跳过范围筛选", self._factor)
            return stock_pool
        in_pool = series.reindex(list(stock_pool)).dropna()
        qualified = set(in_pool[(in_pool >= self._lo) & (in_pool <= self._hi)].index)
        return stock_pool & qualified


class QuantileCutoffFilter(StockFilter):
    """
    截面分位数截断筛选

    保留因子值在 [min_quantile, max_quantile] 范围内的股票。
    适用于 "剔除后 20%" 或 "取前 80%" 这类规则。

    Parameters
    ----------
    factor_name : str
        因子名
    max_quantile : float, optional
        保留 factor <= quantile(max_quantile) 的股票 (如 ROE标准差取前80%)
    min_quantile : float, optional
        保留 factor >= quantile(min_quantile) 的股票
    """

    def __init__(
        self,
        factor_name: str,
        max_quantile: Optional[float] = None,
        min_quantile: Optional[float] = None,
    ):
        self._factor = factor_name
        self._max_q = max_quantile
        self._min_q = min_quantile
        self.name = f"{factor_name}_quantile"

    def apply(self, stock_pool, factor_data, ctx):
        series = factor_data.get(self._factor, pd.Series(dtype=float))
        if series.empty:
            return stock_pool
        in_pool = series.reindex(list(stock_pool)).dropna()
        if len(in_pool) == 0:
            return stock_pool

        mask = pd.Series(True, index=in_pool.index)
        if self._max_q is not None:
            threshold = in_pool.quantile(self._max_q)
            mask &= in_pool <= threshold
        if self._min_q is not None:
            threshold = in_pool.quantile(self._min_q)
            mask &= in_pool >= threshold

        return stock_pool & set(in_pool[mask].index)
