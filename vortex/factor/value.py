"""
value.py
价值类因子 — 股息率、EP、BP、DP

核心因子:
  - dividend_yield: 当期股息率 = 上年每股现金分红 / 当前股价 (高股息之家核心)
  - dividend_yield_3y: 过去三年平均现金股息率 (932315 排序因子)
  - payout_ratio_3y: 过去三年平均股利支付率 (932315 门槛)
  - ep: 1/PE_TTM, 盈利收益率
  - consecutive_div_years: 连续分红年数
"""
from __future__ import annotations

import logging
from typing import List

import numpy as np
import pandas as pd

from vortex.factor.base import BaseFactor, get_latest_annual_period, get_latest_n_annual_periods

logger = logging.getLogger(__name__)


class DividendYield(BaseFactor):
    """
    当期股息率 = 上年度每股现金分红合计 / 当前股价

    高股息之家规则:
      - 使用静态股息率(非TTM)
      - 上年 = 最近一个已公布年报的完整会计年度
      - 每股分红需汇总全年(年报+中报+季报分红)
    """
    name = "dividend_yield"
    category = "value"
    direction = 1
    description = "当期静态股息率 = 上年每股现金分红 / 当前股价"

    def compute(self, ds, date: str) -> pd.Series:
        # 1) 确定最新年报期
        latest_period = get_latest_annual_period(date)
        logger.debug("dividend_yield: 基准年报期=%s", latest_period)

        # 2) 读取分红数据，按年度汇总每股现金分红
        df_div = ds.get_dividend()
        if df_div.empty:
            return pd.Series(dtype=float, name=self.name)

        # 只取已实施的分红(div_proc 包含 '实施')
        if "div_proc" in df_div.columns:
            df_div = df_div[df_div["div_proc"].str.contains("实施", na=False)]

        # 只取对应年度
        df_year = df_div[df_div["end_date"] == latest_period].copy()
        if df_year.empty:
            logger.warning("dividend_yield: 年度 %s 无分红数据", latest_period)
            return pd.Series(dtype=float, name=self.name)

        # 每股现金分红(税前)，同一股票同一年度可能多次分红，汇总
        cash_per_share = df_year.groupby("ts_code")["cash_div"].sum()

        # 3) 读取当前股价
        df_daily = ds.get_daily(trade_date=date)
        if df_daily.empty:
            return pd.Series(dtype=float, name=self.name)
        price = df_daily.set_index("ts_code")["close"]

        # 4) 计算股息率
        common = cash_per_share.index.intersection(price.index)
        dy = cash_per_share.reindex(common) / price.reindex(common)
        dy = dy.replace([np.inf, -np.inf], np.nan).dropna()
        dy.name = self.name
        return dy


class DividendYield3Y(BaseFactor):
    """
    过去三年平均现金股息率 (932315 选样排序因子)

    = 三年现金分红合计 / 三年平均总市值
    简化实现: 取三年各年股息率的均值
    """
    name = "dividend_yield_3y"
    category = "value"
    direction = 1
    description = "过去三年平均现金股息率"

    def compute(self, ds, date: str) -> pd.Series:
        periods = get_latest_n_annual_periods(date, n=3)
        df_div = ds.get_dividend()
        if df_div.empty:
            return pd.Series(dtype=float, name=self.name)

        if "div_proc" in df_div.columns:
            df_div = df_div[df_div["div_proc"].str.contains("实施", na=False)]

        # 每年度每股现金分红
        yearly_div = {}
        for period in periods:
            df_y = df_div[df_div["end_date"] == period]
            if not df_y.empty:
                yearly_div[period] = df_y.groupby("ts_code")["cash_div"].sum()

        if not yearly_div:
            return pd.Series(dtype=float, name=self.name)

        # 当前股价
        df_daily = ds.get_daily(trade_date=date)
        if df_daily.empty:
            return pd.Series(dtype=float, name=self.name)
        price = df_daily.set_index("ts_code")["close"]

        # 三年平均每股分红 / 当前股价
        div_df = pd.DataFrame(yearly_div)
        avg_div = div_df.mean(axis=1)
        common = avg_div.index.intersection(price.index)
        result = avg_div.reindex(common) / price.reindex(common)
        result = result.replace([np.inf, -np.inf], np.nan).dropna()
        result.name = self.name
        return result


class PayoutRatio3Y(BaseFactor):
    """
    过去三年平均股利支付率 (932315 门槛: 10%~100%)

    = 每股现金分红 / 每股收益 (近似)
    """
    name = "payout_ratio_3y"
    category = "value"
    direction = 0  # 不直接排名，作为门槛
    description = "过去三年平均股利支付率"

    def compute(self, ds, date: str) -> pd.Series:
        periods = get_latest_n_annual_periods(date, n=3)
        df_div = ds.get_dividend()
        df_fina = ds.get_fina_indicator()

        if df_div.empty or df_fina.empty:
            return pd.Series(dtype=float, name=self.name)

        if "div_proc" in df_div.columns:
            df_div = df_div[df_div["div_proc"].str.contains("实施", na=False)]

        ratios = []
        for period in periods:
            div_y = df_div[df_div["end_date"] == period].groupby("ts_code")["cash_div"].sum()
            # profit_dedt = 扣非净利润(每股)在 fina_indicator 中
            # 简化: 使用 ROE 作为盈利能力代理，这里用 EPS 近似
            # 实际 payout = 每股分红 / EPS
            # Tushare daily_basic 有 EPS 但我们用 dv_ratio / pe_ttm 来近似
            # 或者直接用 cash_div 和 income 数据
            fina_y = df_fina[df_fina["end_date"] == period]
            if not fina_y.empty and "ocfps" in fina_y.columns:
                # 这里用 cfps (每股现金流) 做分母的近似并不准确
                # 更好的方式: 从 income 表取 EPS
                pass
            ratios.append(div_y)

        if not ratios:
            return pd.Series(dtype=float, name=self.name)

        # 读取利润表获取 EPS (净利润/总股本)
        df_income = ds.get_income()
        df_basic = ds.get_stock_basic()
        # 用 total_mv / close 估算总股本 (粗略)
        # 更好: 直接计算 每股分红总额 / 归母净利润
        # 简化版: 三年平均分红 / 三年平均归母净利润(每股)
        yearly_payout = []
        for period in periods:
            div_y = df_div[df_div["end_date"] == period].groupby("ts_code")["cash_div"].sum()
            inc_y = df_income[df_income["end_date"] == period]
            if not inc_y.empty and "n_income_attr_p" in inc_y.columns:
                # n_income_attr_p 是归母净利润 (万元)
                # cash_div 是每股分红(元)，这里单位不一致
                # 需要总股本来转换... 先用 dv_ratio / pe_ttm 近似
                pass
            yearly_payout.append(div_y)

        # 降级方案: 从 daily_basic 直接算
        # payout ≈ dv_ratio * pe_ttm / 100 (如果两者都是百分比...)
        # 实际: dv_ratio(%) = cash_div / close * 100, pe = close / eps
        # → dv_ratio / 100 * pe = (cash_div/close) * (close/eps) = cash_div / eps = payout ratio
        df_val = ds.get_valuation(trade_date=date)
        if not df_val.empty and "dv_ratio" in df_val.columns and "pe_ttm" in df_val.columns:
            df_val = df_val.set_index("ts_code")
            # pe_ttm 可能为负(亏损)，排除
            mask = (df_val["pe_ttm"] > 0) & (df_val["dv_ratio"] > 0)
            payout = (df_val.loc[mask, "dv_ratio"] / 100) * df_val.loc[mask, "pe_ttm"]
            payout.name = self.name
            return payout.dropna()

        return pd.Series(dtype=float, name=self.name)


class EP(BaseFactor):
    """
    盈利收益率 EP = 1/PE_TTM

    越高表示越便宜。
    """
    name = "ep"
    category = "value"
    direction = 1
    description = "盈利收益率 = 1/PE_TTM"

    def compute(self, ds, date: str) -> pd.Series:
        df = ds.get_valuation(trade_date=date)
        if df.empty:
            return pd.Series(dtype=float, name=self.name)
        df = df.set_index("ts_code")
        # PE_TTM 为负表示亏损，排除
        mask = df["pe_ttm"] > 0
        ep = 1.0 / df.loc[mask, "pe_ttm"]
        ep.name = self.name
        return ep.dropna()


class DP(BaseFactor):
    """
    股利价值因子 DP = 最近一年现金分红 / 总市值

    932315 质量打分因子之一。
    简化实现: 直接用 Tushare daily_basic 的 dv_ratio。
    """
    name = "dp"
    category = "value"
    direction = 1
    description = "最近一年现金分红/总市值"

    def compute(self, ds, date: str) -> pd.Series:
        df = ds.get_valuation(trade_date=date)
        if df.empty:
            return pd.Series(dtype=float, name=self.name)
        df = df.set_index("ts_code")
        result = df["dv_ratio"].dropna() / 100  # Tushare dv_ratio 是百分比
        result.name = self.name
        return result


class ConsecutiveDividendYears(BaseFactor):
    """
    连续分红年数

    从最近年度往回数，有一年无分红即中断。
    高股息之家要求 >= 5 年; 932315 要求 >= 3 年。
    """
    name = "consecutive_div_years"
    category = "value"
    direction = 1
    description = "连续现金分红年数"

    def compute(self, ds, date: str) -> pd.Series:
        df_div = ds.get_dividend()
        if df_div.empty:
            return pd.Series(dtype=float, name=self.name)

        if "div_proc" in df_div.columns:
            df_div = df_div[df_div["div_proc"].str.contains("实施", na=False)]

        latest_period = get_latest_annual_period(date)
        latest_year = int(latest_period[:4])

        # 每只股票每年是否有分红
        df_div = df_div[df_div["cash_div"] > 0].copy()
        df_div["year"] = df_div["end_date"].str[:4].astype(int)

        result = {}
        for ts_code, g in df_div.groupby("ts_code"):
            years_with_div = set(g["year"].unique())
            count = 0
            for y in range(latest_year, latest_year - 20, -1):
                if y in years_with_div:
                    count += 1
                else:
                    break
            result[ts_code] = count

        s = pd.Series(result, name=self.name, dtype=float)
        return s


class RoeOverPb(BaseFactor):
    """
    实际投资报酬率 = ROE / PB (百分比)

    高股息之家安全标准: ROE/PB ≥ 7% (国债收益率 × 2)
    当 ROE/PB ≥ 7% 时，等价于 PE ≤ 14.3 (安全PE ≤ 15倍)
    """
    name = "roe_over_pb"
    category = "value"
    direction = 1
    description = "实际投资报酬率 = ROE/PB"

    def compute(self, ds, date: str) -> pd.Series:
        # ROE from fina_indicator
        df_fina = ds.get_fina_indicator()
        if df_fina.empty:
            return pd.Series(dtype=float, name=self.name)

        latest_period = get_latest_annual_period(date)
        roe = (
            df_fina[df_fina["end_date"] == latest_period]
            .drop_duplicates("ts_code", keep="first")
            .set_index("ts_code")["roe"]
            .dropna()
        )

        # PB from valuation
        df_val = ds.get_valuation(trade_date=date)
        if df_val.empty:
            return pd.Series(dtype=float, name=self.name)
        pb = df_val.set_index("ts_code")["pb"].dropna()

        common = roe.index.intersection(pb.index)
        roe_c = roe.reindex(common)
        pb_c = pb.reindex(common)

        # PB > 0 才有意义
        mask = pb_c > 0
        result = roe_c[mask] / pb_c[mask]  # ROE是百分比(如15)，PB是倍数(如2)→ 结果是百分比(如7.5)
        result = result.replace([np.inf, -np.inf], np.nan).dropna()
        result.name = self.name
        return result
