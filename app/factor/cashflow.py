"""
cashflow.py
现金流类因子 — FCF/EV、OCF覆盖、自由现金流率

核心因子 (980092 自由现金流指数):
  - fcf_yield: 自由现金流 / 企业价值 (核心排序因子)
  - ocf_to_op: 经营活动现金流 / 营业利润 (剔除后30%)
  - fcf_ttm: 近一年自由现金流 (正值门槛)
  - ocf_3y_positive: 近三年经营活动现金流是否均为正 (门槛)
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from app.factor.base import BaseFactor, get_latest_annual_period, get_latest_n_annual_periods

logger = logging.getLogger(__name__)


class FCFYield(BaseFactor):
    """
    自由现金流率 FCF/EV (980092 核心排序因子)

    FCF = 经营活动现金流 - 资本支出
    EV  = 总市值 + 总负债 - 货币资金 (简化版企业价值)

    越高表示现金流产出能力越强。
    """
    name = "fcf_yield"
    category = "cashflow"
    direction = 1
    description = "自由现金流/企业价值 (FCF/EV)"

    def compute(self, ds, date: str) -> pd.Series:
        latest_period = get_latest_annual_period(date)

        # 现金流量表
        df_cf = ds.get_cashflow()
        if df_cf.empty:
            return pd.Series(dtype=float, name=self.name)
        cf = (
            df_cf[df_cf["end_date"] == latest_period]
            .drop_duplicates("ts_code", keep="first")
            .set_index("ts_code")
        )

        # 资产负债表 (总负债、货币资金)
        df_bs = ds.get_balancesheet()
        if df_bs.empty:
            return pd.Series(dtype=float, name=self.name)
        bs = (
            df_bs[df_bs["end_date"] == latest_period]
            .drop_duplicates("ts_code", keep="first")
            .set_index("ts_code")
        )

        # 总市值 (从 daily_basic)
        df_val = ds.get_valuation(trade_date=date)
        if df_val.empty:
            return pd.Series(dtype=float, name=self.name)
        val = df_val.set_index("ts_code")

        # FCF = n_cashflow_act - c_pay_acq_const_fiolta (资本支出)
        # Tushare cashflow 表可能有 free_cashflow 字段
        common = cf.index.intersection(bs.index).intersection(val.index)

        # 经营活动现金流
        ocf = cf.reindex(common).get("n_cashflow_act", pd.Series(dtype=float))
        # 资本支出 (购建固定资产、无形资产和其他长期资产支付的现金)
        capex = cf.reindex(common).get("c_pay_acq_const_fiolta", pd.Series(0.0, index=common))
        capex = capex.fillna(0)

        # 如果有 free_cashflow 字段直接用
        if "free_cashflow" in cf.columns:
            fcf = cf.reindex(common)["free_cashflow"]
            fcf = fcf.fillna(ocf - capex.abs())
        else:
            fcf = ocf - capex.abs()

        # EV = 总市值(万元) + 总负债(万元) - 货币资金(万元)
        total_mv = val.reindex(common).get("total_mv", pd.Series(dtype=float))
        total_liab = bs.reindex(common).get("total_liab", pd.Series(0.0, index=common))
        money_cap = bs.reindex(common).get("money_cap", pd.Series(0.0, index=common))
        total_liab = total_liab.fillna(0)
        money_cap = money_cap.fillna(0)

        # 注意单位: total_mv 是万元, 财务报表也是万元
        ev = total_mv + total_liab - money_cap

        # EV > 0 才计算
        mask = (ev > 0) & fcf.notna()
        result = fcf[mask] / ev[mask]
        result = result.replace([np.inf, -np.inf], np.nan).dropna()
        result.name = self.name
        return result


class OCFtoOP(BaseFactor):
    """
    经营活动现金流 / 营业利润 (980092 利润覆盖因子)

    衡量利润的现金含量。
    980092: 剔除排名后 30%。
    """
    name = "ocf_to_op"
    category = "cashflow"
    direction = 1
    description = "经营活动现金流/营业利润"

    def compute(self, ds, date: str) -> pd.Series:
        latest_period = get_latest_annual_period(date)

        df_cf = ds.get_cashflow()
        df_inc = ds.get_income()
        if df_cf.empty or df_inc.empty:
            return pd.Series(dtype=float, name=self.name)

        cf = (
            df_cf[df_cf["end_date"] == latest_period]
            .drop_duplicates("ts_code", keep="first")
            .set_index("ts_code")
        )
        inc = (
            df_inc[df_inc["end_date"] == latest_period]
            .drop_duplicates("ts_code", keep="first")
            .set_index("ts_code")
        )

        common = cf.index.intersection(inc.index)
        ocf = cf.reindex(common).get("n_cashflow_act", pd.Series(dtype=float))
        op = inc.reindex(common).get("operate_profit", pd.Series(dtype=float))

        # 营业利润 > 0 才有意义
        mask = (op > 0) & ocf.notna()
        result = ocf[mask] / op[mask]
        result = result.replace([np.inf, -np.inf], np.nan).dropna()
        result.name = self.name
        return result


class FCF_TTM(BaseFactor):
    """
    近一年自由现金流 (980092 门槛: > 0)

    用于硬性筛选，不参与排名打分。
    """
    name = "fcf_ttm"
    category = "cashflow"
    direction = 1
    description = "近一年自由现金流"

    def compute(self, ds, date: str) -> pd.Series:
        latest_period = get_latest_annual_period(date)

        df_cf = ds.get_cashflow()
        if df_cf.empty:
            return pd.Series(dtype=float, name=self.name)

        cf = (
            df_cf[df_cf["end_date"] == latest_period]
            .drop_duplicates("ts_code", keep="first")
            .set_index("ts_code")
        )

        ocf = cf.get("n_cashflow_act", pd.Series(dtype=float))
        capex = cf.get("c_pay_acq_const_fiolta", pd.Series(0.0, index=cf.index)).fillna(0)

        if "free_cashflow" in cf.columns:
            fcf = cf["free_cashflow"].fillna(ocf - capex.abs())
        else:
            fcf = ocf - capex.abs()

        result = fcf.dropna()
        result.name = self.name
        return result


class OCF3YPositive(BaseFactor):
    """
    近三年经营活动现金流是否全部为正 (980092 门槛)

    返回值: 1.0 = 三年均正, 0.0 = 至少一年非正
    """
    name = "ocf_3y_positive"
    category = "cashflow"
    direction = 1
    description = "近三年经营活动现金流均为正(0/1)"

    def compute(self, ds, date: str) -> pd.Series:
        periods = get_latest_n_annual_periods(date, n=3)

        df_cf = ds.get_cashflow()
        if df_cf.empty:
            return pd.Series(dtype=float, name=self.name)

        yearly_ocf = {}
        for period in periods:
            cf_y = (
                df_cf[df_cf["end_date"] == period]
                .drop_duplicates("ts_code", keep="first")
                .set_index("ts_code")
            )
            if not cf_y.empty and "n_cashflow_act" in cf_y.columns:
                yearly_ocf[period] = cf_y["n_cashflow_act"]

        if len(yearly_ocf) < 3:
            logger.warning("OCF3Y: 仅找到 %d 年数据 (需要3年), 降级为有几年算几年", len(yearly_ocf))
            if not yearly_ocf:
                return pd.Series(dtype=float, name=self.name)

        ocf_df = pd.DataFrame(yearly_ocf)
        # 要求所有年份都有数据 且 都 > 0
        all_positive = (ocf_df > 0).all(axis=1) & (ocf_df.notna().sum(axis=1) >= len(yearly_ocf))
        result = all_positive.astype(float)
        result.name = self.name
        return result
