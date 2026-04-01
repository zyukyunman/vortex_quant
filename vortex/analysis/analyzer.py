"""
analyzer.py
个股分析 & 因子诊断

职责:
  - 个股多维度分析
  - 因子有效性检验 (IC, ICIR, 分组回测)
  - 策略绩效归因
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from vortex.core.data.datastore import DataStore
from vortex.core.factorhub import FactorHub

logger = logging.getLogger(__name__)


class StockAnalyzer:
    """个股分析器"""

    def __init__(self, ds: DataStore):
        self.ds = ds

    def profile(self, ts_code: str) -> Dict:
        """
        个股画像 — 基本面 + 估值 + 分红

        Parameters
        ----------
        ts_code : str
            股票代码 e.g. "000651.SZ"

        Returns
        -------
        Dict
            包含基本信息、估值、分红历史等
        """
        basic = self.ds.get_stock_basic()
        stock = basic[basic["ts_code"] == ts_code]
        if stock.empty:
            return {"error": f"未找到 {ts_code}"}

        info = stock.iloc[0].to_dict()

        # 估值
        val = self.ds.get_valuation(
            start_date="20240101", end_date="20261231"
        )
        val_stock = val[val["ts_code"] == ts_code].sort_values("trade_date")

        # 分红
        div = self.ds.get_dividend()
        div_stock = div[div["ts_code"] == ts_code].sort_values("end_date", ascending=False)

        result = {
            "basic": info,
            "valuation_latest": val_stock.tail(1).to_dict("records") if not val_stock.empty else {},
            "dividend_history": div_stock.head(10).to_dict("records") if not div_stock.empty else [],
            "dividend_years": len(div_stock["end_date"].unique()) if not div_stock.empty else 0,
        }

        # 财务指标
        fina = self.ds.get_fina_indicator()
        fina_stock = fina[fina["ts_code"] == ts_code].sort_values("end_date", ascending=False)
        if not fina_stock.empty:
            result["fina_latest"] = fina_stock.head(4).to_dict("records")

        return result


class FactorAnalyzer:
    """因子有效性分析"""

    def __init__(self, ds: DataStore, fh: FactorHub):
        self.ds = ds
        self.fh = fh

    def calc_ic(
        self,
        factor_name: str,
        dates: List[str],
        forward_days: int = 20,
    ) -> pd.Series:
        """
        计算因子 IC 序列 (Spearman rank correlation with forward returns)

        Parameters
        ----------
        factor_name : str
            因子名
        dates : List[str]
            截面日期列表
        forward_days : int
            前瞻收益期 (交易日数)

        Returns
        -------
        pd.Series
            index=date, values=IC
        """
        from scipy.stats import spearmanr

        ic_values = {}
        for date in dates:
            try:
                factor = self.fh.compute(factor_name, date)
                if factor.dropna().empty or len(factor.dropna()) < 20:
                    continue

                # 计算前瞻收益
                fwd_ret = self._get_forward_returns(
                    date, factor.dropna().index.tolist(), forward_days
                )
                if fwd_ret.dropna().empty:
                    continue

                aligned = pd.DataFrame({
                    "factor": factor, "return": fwd_ret
                }).dropna()

                if len(aligned) < 10:
                    continue

                ic, _ = spearmanr(aligned["factor"], aligned["return"])
                ic_values[date] = ic
            except Exception as e:
                logger.debug("IC 计算失败 @ %s: %s", date, e)

        return pd.Series(ic_values, dtype=float)

    def ic_report(
        self,
        factor_names: Optional[List[str]] = None,
        dates: Optional[List[str]] = None,
        forward_days: int = 20,
    ) -> pd.DataFrame:
        """
        因子 IC 汇总报告

        Returns
        -------
        pd.DataFrame
            columns: [factor, mean_ic, ic_std, icir, ic_positive_rate, n_periods]
        """
        if factor_names is None:
            factor_names = list(self.fh.factors.keys())
        if dates is None:
            from vortex.utils.date_utils import load_trade_cal
            cal = load_trade_cal(self.ds.data_dir)
            cal_list = sorted([d.strftime("%Y%m%d") for d in cal])
            # 默认取最近12个月末
            by_month = {}
            for d in cal_list[-252:]:
                by_month[d[:6]] = d
            dates = sorted(by_month.values())[-12:]

        rows = []
        for name in factor_names:
            logger.info("计算 %s IC...", name)
            ic = self.calc_ic(name, dates, forward_days)
            if ic.empty:
                rows.append({
                    "factor": name,
                    "mean_ic": np.nan,
                    "ic_std": np.nan,
                    "icir": np.nan,
                    "ic_positive_rate": np.nan,
                    "n_periods": 0,
                })
                continue
            rows.append({
                "factor": name,
                "mean_ic": ic.mean(),
                "ic_std": ic.std(),
                "icir": ic.mean() / ic.std() if ic.std() > 0 else 0,
                "ic_positive_rate": (ic > 0).mean(),
                "n_periods": len(ic),
            })

        df = pd.DataFrame(rows)
        logger.info("IC 报告:\n%s", df.to_string())
        return df

    def _get_forward_returns(
        self, date: str, ts_codes: List[str], days: int
    ) -> pd.Series:
        """获取从 date 开始 days 交易日后的累计收益"""
        from vortex.utils.date_utils import load_trade_cal
        cal = load_trade_cal(self.ds.data_dir)
        cal_list = sorted([d.strftime("%Y%m%d") for d in cal])
        try:
            idx = cal_list.index(date)
        except ValueError:
            return pd.Series(dtype=float)

        if idx + days >= len(cal_list):
            return pd.Series(dtype=float)

        end_date = cal_list[idx + days]

        df = self.ds.get_daily(start_date=date, end_date=end_date)
        if df.empty:
            return pd.Series(dtype=float)

        pivot = df.pivot_table(
            index="trade_date", columns="ts_code", values="close"
        )
        if date not in pivot.index or end_date not in pivot.index:
            return pd.Series(dtype=float)

        ret = pivot.loc[end_date] / pivot.loc[date] - 1
        return ret.reindex(ts_codes)
