"""
quality.py
质量类因子 — ROE、ROE稳定性、ROE变化、OPCFD

核心因子 (932315 质量打分):
  - roe_ttm: 净利润/净资产 (质量打分)
  - delta_roe: ROE同比变化值 (质量打分)
  - roe_stability: trailing 12Q ROE标准差 (稳定性筛选)
  - opcfd: 经营活动现金净流量/总负债 (非金融质量打分)
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from app.factor.base import (
    BaseFactor,
    get_latest_annual_period,
    get_latest_n_annual_periods,
)

logger = logging.getLogger(__name__)


class RoeTTM(BaseFactor):
    """
    ROE (Return on Equity)

    = 净利润 / 净资产
    使用 fina_indicator 中的 roe 字段 (归属母公司的加权 ROE)
    """
    name = "roe_ttm"
    category = "quality"
    direction = 1
    description = "净资产收益率 ROE"

    def compute(self, ds, date: str) -> pd.Series:
        df = ds.get_fina_indicator()
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        latest_period = get_latest_annual_period(date)
        df_y = df[df["end_date"] == latest_period].copy()
        if df_y.empty:
            return pd.Series(dtype=float, name=self.name)

        # 取最新一条（去重后已保留 first）
        result = df_y.drop_duplicates("ts_code", keep="first").set_index("ts_code")["roe"]
        result = result.dropna()
        result.name = self.name
        return result


class DeltaROE(BaseFactor):
    """
    ROE 同比变化值 (932315 质量打分因子)

    ΔROE = 当年 ROE - 上年 ROE
    """
    name = "delta_roe"
    category = "quality"
    direction = 1
    description = "ROE同比变化"

    def compute(self, ds, date: str) -> pd.Series:
        df = ds.get_fina_indicator()
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        periods = get_latest_n_annual_periods(date, n=2)
        if len(periods) < 2:
            return pd.Series(dtype=float, name=self.name)

        current_period, prev_period = periods[0], periods[1]

        roe_cur = (
            df[df["end_date"] == current_period]
            .drop_duplicates("ts_code", keep="first")
            .set_index("ts_code")["roe"]
        )
        roe_prev = (
            df[df["end_date"] == prev_period]
            .drop_duplicates("ts_code", keep="first")
            .set_index("ts_code")["roe"]
        )

        common = roe_cur.index.intersection(roe_prev.index)
        delta = roe_cur.reindex(common) - roe_prev.reindex(common)
        delta = delta.dropna()
        delta.name = self.name
        return delta


class ROEStability(BaseFactor):
    """
    ROE 稳定性 = 过去 12 个季度 ROE 的标准差 (越小越好)

    932315: 剔除后 20% (取前 80%)
    980092: 剔除后 10% (取前 90%)

    direction = -1: 标准差越小越好
    """
    name = "roe_stability"
    category = "quality"
    direction = -1  # 越小越好
    description = "12季度ROE标准差(越小越稳定)"

    def compute(self, ds, date: str) -> pd.Series:
        df = ds.get_fina_indicator()
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        # 需要最近 12 个季度的 ROE
        # 获取最近 12 个季报期末
        latest_period = get_latest_annual_period(date)
        latest_year = int(latest_period[:4])

        # 构造最近 12 个季度
        quarter_ends = []
        for y in range(latest_year, latest_year - 4, -1):
            for q in ["1231", "0930", "0630", "0331"]:
                quarter_ends.append(f"{y}{q}")
        quarter_ends = sorted(quarter_ends, reverse=True)[:12]

        # 筛选这些季度的数据
        df_q = df[df["end_date"].isin(quarter_ends)].copy()
        n_unique = df_q["end_date"].nunique() if not df_q.empty else 0

        if n_unique < 4:
            # 降级: 用年度数据计算 ROE 稳定性
            annual_ends = [f"{y}1231" for y in range(latest_year, latest_year - 5, -1)]
            df_a = df[df["end_date"].isin(annual_ends)].copy()
            if df_a.empty or df_a["end_date"].nunique() < 2:
                logger.warning("ROEStability: 季度/年度数据均不足, 跳过")
                return pd.Series(dtype=float, name=self.name)
            pivot = df_a.pivot_table(
                index="ts_code", columns="end_date", values="roe", aggfunc="first"
            )
            min_periods = min(2, pivot.shape[1])
            valid_mask = pivot.notna().sum(axis=1) >= min_periods
            std_roe = pivot.loc[valid_mask].std(axis=1, ddof=1).dropna()
            std_roe.name = self.name
            logger.info("ROEStability: 降级为年度数据, %d 只股票", len(std_roe))
            return std_roe

        # 每只股票在各季度的 ROE
        pivot = df_q.pivot_table(
            index="ts_code", columns="end_date", values="roe", aggfunc="first"
        )
        # 要求至少 8 个季度有数据
        valid_mask = pivot.notna().sum(axis=1) >= 8
        std_roe = pivot.loc[valid_mask].std(axis=1, ddof=1)
        std_roe = std_roe.dropna()
        std_roe.name = self.name
        return std_roe


class OPCFD(BaseFactor):
    """
    经营活动现金净流量 / 总负债 (仅非金融股)

    932315 质量打分因子。
    反映公司用经营现金流偿债的能力。
    """
    name = "opcfd"
    category = "quality"
    direction = 1
    description = "经营活动现金净流量/总负债"

    def compute(self, ds, date: str) -> pd.Series:
        latest_period = get_latest_annual_period(date)

        df_cf = ds.get_cashflow()
        df_bs = ds.get_balancesheet()
        if df_cf.empty or df_bs.empty:
            return pd.Series(dtype=float, name=self.name)

        # 经营活动现金净流量
        cf = (
            df_cf[df_cf["end_date"] == latest_period]
            .drop_duplicates("ts_code", keep="first")
            .set_index("ts_code")
        )
        # 总负债
        bs = (
            df_bs[df_bs["end_date"] == latest_period]
            .drop_duplicates("ts_code", keep="first")
            .set_index("ts_code")
        )

        if "n_cashflow_act" not in cf.columns or "total_liab" not in bs.columns:
            return pd.Series(dtype=float, name=self.name)

        common = cf.index.intersection(bs.index)
        ocf = cf.reindex(common)["n_cashflow_act"]
        liab = bs.reindex(common)["total_liab"]

        # 总负债 > 0 才有意义
        mask = liab > 0
        result = ocf[mask] / liab[mask]
        result = result.replace([np.inf, -np.inf], np.nan).dropna()
        result.name = self.name
        return result


class DebtToAssets(BaseFactor):
    """
    资产负债率 = 总负债 / 总资产

    高股息之家: 剔除 > 70% (金融股除外)
    direction = -1: 越低越好
    """
    name = "debt_to_assets"
    category = "quality"
    direction = -1
    description = "资产负债率"

    def compute(self, ds, date: str) -> pd.Series:
        df = ds.get_fina_indicator()
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        latest_period = get_latest_annual_period(date)
        df_y = df[df["end_date"] == latest_period].drop_duplicates("ts_code", keep="first")

        if "debt_to_assets" in df_y.columns:
            result = df_y.set_index("ts_code")["debt_to_assets"].dropna()
        else:
            # 从 balancesheet 计算
            df_bs = ds.get_balancesheet()
            bs = (
                df_bs[df_bs["end_date"] == latest_period]
                .drop_duplicates("ts_code", keep="first")
                .set_index("ts_code")
            )
            if "total_liab" in bs.columns and "total_assets" in bs.columns:
                mask = bs["total_assets"] > 0
                result = bs.loc[mask, "total_liab"] / bs.loc[mask, "total_assets"] * 100
                result = result.dropna()
            else:
                return pd.Series(dtype=float, name=self.name)

        result.name = self.name
        return result


class NetProfitYoY(BaseFactor):
    """
    扣非净利润同比增速 (高股息之家季报原则)

    使用 profit_dedt (扣除非经常性损益后的净利润) 的同比增长率。
    < -10% 是危险信号，需要关注。
    """
    name = "netprofit_yoy"
    category = "quality"
    direction = 1
    description = "扣非净利润同比增速"

    def compute(self, ds, date: str) -> pd.Series:
        df = ds.get_fina_indicator()
        if df.empty:
            return pd.Series(dtype=float, name=self.name)

        periods = get_latest_n_annual_periods(date, n=2)
        if len(periods) < 2:
            return pd.Series(dtype=float, name=self.name)

        current_period, prev_period = periods[0], periods[1]

        # 优先使用 profit_dedt (扣非净利润) 计算同比增速
        if "profit_dedt" in df.columns:
            cur = (
                df[df["end_date"] == current_period]
                .drop_duplicates("ts_code", keep="first")
                .set_index("ts_code")["profit_dedt"]
                .dropna()
            )
            prev = (
                df[df["end_date"] == prev_period]
                .drop_duplicates("ts_code", keep="first")
                .set_index("ts_code")["profit_dedt"]
                .dropna()
            )
            common = cur.index.intersection(prev.index)
            if len(common) > 0:
                cur_v = cur.reindex(common)
                prev_v = prev.reindex(common)
                # 只在前期非零时计算同比增速
                mask = prev_v.abs() > 1e-6
                yoy = (cur_v[mask] - prev_v[mask]) / prev_v[mask].abs() * 100
                yoy = yoy.replace([np.inf, -np.inf], np.nan).dropna()
                yoy.name = self.name
                return yoy

        # 退化: 使用 netprofit_yoy 字段
        df_y = df[df["end_date"] == periods[0]].drop_duplicates("ts_code", keep="first")
        if "netprofit_yoy" not in df_y.columns:
            return pd.Series(dtype=float, name=self.name)

        result = df_y.set_index("ts_code")["netprofit_yoy"].dropna()
        result.name = self.name
        return result
