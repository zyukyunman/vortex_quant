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
            dates = self._default_month_end_dates()

        rows = []
        for name in factor_names:
            logger.info("计算 %s IC...", name)
            ic = self.calc_ic(name, dates, forward_days)
            summary = self._summarize_ic_series(ic)
            rows.append({"factor": name, **summary})

        df = pd.DataFrame(rows)
        logger.info("IC 报告:\n%s", df.to_string())
        return df

    def ic_report_multi_horizon(
        self,
        factor_names: Optional[List[str]] = None,
        dates: Optional[List[str]] = None,
        forward_days_list: Optional[List[int]] = None,
    ) -> pd.DataFrame:
        """多周期 IC 汇总报告，默认输出 1d/5d/20d。"""
        if factor_names is None:
            factor_names = list(self.fh.factors.keys())
        if dates is None:
            dates = self._default_month_end_dates()
        if forward_days_list is None:
            forward_days_list = [1, 5, 20]

        rows = []
        for name in factor_names:
            row = {"factor": name}
            best_horizon = None
            best_abs_ic = -1.0

            for days in forward_days_list:
                ic = self.calc_ic(name, dates, forward_days=days)
                summary = self._summarize_ic_series(ic)
                row[f"mean_ic_{days}d"] = summary["mean_ic"]
                row[f"ic_std_{days}d"] = summary["ic_std"]
                row[f"icir_{days}d"] = summary["icir"]
                row[f"positive_rate_{days}d"] = summary["ic_positive_rate"]
                row[f"n_periods_{days}d"] = summary["n_periods"]

                mean_ic = summary["mean_ic"]
                if pd.notna(mean_ic) and abs(mean_ic) > best_abs_ic:
                    best_abs_ic = abs(mean_ic)
                    best_horizon = days

            row["best_horizon"] = best_horizon
            rows.append(row)

        return pd.DataFrame(rows)

    def calc_long_short_returns(
        self,
        factor_name: str,
        dates: List[str],
        forward_days: int = 5,
        n_groups: int = 5,
    ) -> pd.DataFrame:
        """按分组构建多空组合，返回每期多头/空头/多空收益。"""
        rows = []
        for date in dates:
            try:
                factor = self.fh.compute(factor_name, date).dropna()
                if factor.empty or len(factor) < max(30, n_groups * 10):
                    continue

                fwd_ret = self._get_forward_returns(
                    date, factor.index.tolist(), forward_days
                ).dropna()
                aligned = pd.DataFrame({
                    "factor": factor,
                    "return": fwd_ret,
                }).dropna()
                if len(aligned) < max(30, n_groups * 10):
                    continue

                ranked = aligned["factor"].rank(method="first", pct=True)
                long_mask = ranked >= (1 - 1 / n_groups)
                short_mask = ranked <= (1 / n_groups)
                if long_mask.sum() == 0 or short_mask.sum() == 0:
                    continue

                long_ret = aligned.loc[long_mask, "return"].mean()
                short_ret = aligned.loc[short_mask, "return"].mean()
                rows.append({
                    "date": date,
                    "long": float(long_ret),
                    "short": float(short_ret),
                    "long_short": float(long_ret - short_ret),
                })
            except Exception as e:
                logger.debug("多空组合计算失败 %s@%s: %s", factor_name, date, e)

        return pd.DataFrame(rows)

    def long_short_report(
        self,
        factor_names: Optional[List[str]] = None,
        dates: Optional[List[str]] = None,
        forward_days: int = 5,
        n_groups: int = 5,
    ) -> pd.DataFrame:
        """多空组合收益报告，默认用 5d 前瞻收益。"""
        if factor_names is None:
            factor_names = list(self.fh.factors.keys())
        if dates is None:
            dates = self._default_month_end_dates()

        rows = []
        annualizer = np.sqrt(252 / max(forward_days, 1))
        for name in factor_names:
            ls = self.calc_long_short_returns(
                name, dates, forward_days=forward_days, n_groups=n_groups
            )
            if ls.empty:
                rows.append({
                    "factor": name,
                    f"long_short_{forward_days}d": np.nan,
                    "long_mean": np.nan,
                    "short_mean": np.nan,
                    "sharpe": np.nan,
                    "n_periods": 0,
                })
                continue

            ls_mean = ls["long_short"].mean()
            ls_std = ls["long_short"].std()
            sharpe = ls_mean / ls_std * annualizer if ls_std and ls_std > 0 else 0.0
            rows.append({
                "factor": name,
                f"long_short_{forward_days}d": ls_mean,
                "long_mean": ls["long"].mean(),
                "short_mean": ls["short"].mean(),
                "sharpe": sharpe,
                "n_periods": len(ls),
            })

        return pd.DataFrame(rows)

    def _default_month_end_dates(self) -> List[str]:
        from vortex.utils.date_utils import load_trade_cal

        cal = load_trade_cal(self.ds.data_dir)
        cal_list = sorted([d.strftime("%Y%m%d") for d in cal])
        by_month = {}
        for d in cal_list[-252:]:
            by_month[d[:6]] = d
        return sorted(by_month.values())[-12:]

    @staticmethod
    def _summarize_ic_series(ic: pd.Series) -> Dict[str, float]:
        if ic.empty:
            return {
                "mean_ic": np.nan,
                "ic_std": np.nan,
                "icir": np.nan,
                "ic_positive_rate": np.nan,
                "n_periods": 0,
            }
        ic_std = ic.std()
        return {
            "mean_ic": float(ic.mean()),
            "ic_std": float(ic_std),
            "icir": float(ic.mean() / ic_std) if ic_std and ic_std > 0 else 0.0,
            "ic_positive_rate": float((ic > 0).mean()),
            "n_periods": int(len(ic)),
        }

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
