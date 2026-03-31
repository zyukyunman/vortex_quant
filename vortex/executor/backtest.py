"""
backtest.py
L7 回测引擎 — 历史模拟 & 绩效评估

职责:
  - 按历史日期序列执行策略
  - 模拟持仓与收益
  - 计算绩效指标 (年化、夏普、回撤、换手率)
  - 输出回测报告
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class BacktestResult:
    """回测结果"""
    nav_series: pd.Series        # 日净值序列
    returns_series: pd.Series    # 日收益率序列
    positions_history: List[Dict] # 历次持仓记录
    rebalance_dates: List[str]   # 再平衡日期
    metrics: Dict = field(default_factory=dict)
    benchmark_returns: Optional[Dict[str, pd.Series]] = None  # {基准名: 日收益率序列}

    def summary(self) -> str:
        """输出回测摘要"""
        m = self.metrics
        lines = [
            "=" * 60,
            "  回测绩效报告",
            "=" * 60,
            f"  回测区间: {m.get('start_date', 'N/A')} ~ {m.get('end_date', 'N/A')}",
            f"  再平衡次数: {m.get('n_rebalance', 0)}",
            f"  ────────────────────────────",
            f"  总收益率:    {m.get('total_return', 0):.2%}",
            f"  年化收益率:  {m.get('annual_return', 0):.2%}",
            f"  年化波动率:  {m.get('annual_volatility', 0):.2%}",
            f"  夏普比率:    {m.get('sharpe_ratio', 0):.3f}",
            f"  Sortino比率: {m.get('sortino_ratio', 0):.3f}",
            f"  最大回撤:    {m.get('max_drawdown', 0):.2%}",
            f"  最大回撤天数:{m.get('max_dd_days', 0)}",
            f"  Calmar比率:  {m.get('calmar_ratio', 0):.3f}",
            f"  盈亏比:      {m.get('profit_factor', 0):.2f}",
            f"  平均换手率:  {m.get('avg_turnover', 0):.2%}",
            f"  胜率:        {m.get('win_rate', 0):.2%}",
            "=" * 60,
        ]
        return "\n".join(lines)

    def calc_benchmark_metrics(self) -> Dict[str, Dict]:
        """计算各基准的绩效指标 (与策略对比用)"""
        if not self.benchmark_returns:
            return {}
        result = {}
        for bname, bret in self.benchmark_returns.items():
            if bret.empty:
                continue
            b_nav = (1 + bret).cumprod()
            total = b_nav.iloc[-1] - 1 if len(b_nav) > 0 else 0
            n_days = len(b_nav)
            ann = (1 + total) ** (252 / max(n_days, 1)) - 1
            vol = bret.std() * np.sqrt(252) if bret.std() > 0 else 0
            sharpe = (ann - 0.02) / vol if vol > 0 else 0
            cummax = b_nav.cummax()
            dd = ((cummax - b_nav) / cummax).max()
            calmar = ann / dd if dd > 0 else 0
            ds_ret = bret[bret < 0]
            ds_std = ds_ret.std() * np.sqrt(252) if len(ds_ret) > 0 else 0
            sortino = (ann - 0.02) / ds_std if ds_std > 0 else 0
            result[bname] = {
                "total_return": total,
                "annual_return": ann,
                "annual_volatility": vol,
                "sharpe_ratio": sharpe,
                "max_drawdown": dd,
                "calmar_ratio": calmar,
                "sortino_ratio": sortino,
            }
        return result


class BacktestEngine:
    """
    回测引擎

    使用方式:
        engine = BacktestEngine(ds)
        result = engine.run(strategy, start_date, end_date, freq='M')
    """

    def __init__(self, ds):
        """
        Parameters
        ----------
        ds : DataStore
            数据源
        """
        self.ds = ds

    def run(
        self,
        strategy,
        start_date: str,
        end_date: str,
        freq: str = "M",
        initial_capital: float = 1_000_000.0,
        benchmark_codes: Optional[List[str]] = None,
    ) -> BacktestResult:
        """
        执行回测

        Parameters
        ----------
        strategy : BaseStrategy
            要回测的策略实例
        start_date, end_date : str
            回测区间 YYYYMMDD
        freq : str
            再平衡频率 M=月度, Q=季度
        initial_capital : float
            初始资金
        benchmark_codes : list, optional
            基准指数代码, 如 ["000300.SH", "000905.SH"]

        Returns
        -------
        BacktestResult
        """
        logger.info("=" * 60)
        logger.info("回测启动: %s [%s ~ %s] freq=%s",
                     strategy.name, start_date, end_date, freq)
        logger.info("=" * 60)

        # 获取再平衡日期
        rebalance_dates = self._get_rebalance_dates(start_date, end_date, freq)
        if not rebalance_dates:
            raise ValueError(f"区间 {start_date}~{end_date} 无有效再平衡日期")

        # 确保第一个交易日立即建仓 (不等到下一个周期末)
        from vortex.utils.date_utils import load_trade_cal
        cal = load_trade_cal(self.ds.data_dir)
        cal_list = sorted([d.strftime("%Y%m%d") for d in cal])
        first_trade_day = None
        for d in cal_list:
            if d >= start_date:
                first_trade_day = d
                break
        if first_trade_day and first_trade_day < rebalance_dates[0]:
            rebalance_dates = [first_trade_day] + rebalance_dates
            logger.info("注入首日建仓: %s (基于前一日数据选股)", first_trade_day)

        logger.info("再平衡日期: %d 个, 首尾 %s ~ %s",
                     len(rebalance_dates), rebalance_dates[0], rebalance_dates[-1])

        # 获取全量日线数据
        df_daily = self.ds.get_daily(start_date=start_date, end_date=end_date)
        if df_daily.empty:
            raise ValueError("无日线行情数据")

        # 构建收益率矩阵: index=trade_date, columns=ts_code
        pivot = df_daily.pivot_table(
            index="trade_date", columns="ts_code", values="pct_chg"
        ) / 100.0
        trade_dates = sorted(pivot.index.tolist())

        # 回测循环
        nav = initial_capital
        current_weights = pd.Series(dtype=float)
        nav_list = []
        returns_list = []
        positions_history = []
        turnovers = []

        rb_idx = 0
        for td in trade_dates:
            # 再平衡
            if rb_idx < len(rebalance_dates) and td >= rebalance_dates[rb_idx]:
                try:
                    result = strategy.run(rebalance_dates[rb_idx])
                    new_weights = pd.Series(
                        {s.ts_code: s.weight for s in result.signals},
                        dtype=float,
                    )
                    if not new_weights.empty:
                        # 换手率
                        turnover = self._calc_turnover(current_weights, new_weights)
                        turnovers.append(turnover)
                        current_weights = new_weights
                        positions_history.append({
                            "date": rebalance_dates[rb_idx],
                            "n_stocks": len(new_weights),
                            "weights": new_weights.to_dict(),
                        })
                except Exception as e:
                    logger.warning("再平衡失败 @ %s: %s", rebalance_dates[rb_idx], e)
                rb_idx += 1

            # 计算组合日收益
            if not current_weights.empty and td in pivot.index:
                daily_ret = pivot.loc[td].reindex(current_weights.index).fillna(0)
                port_ret = (current_weights * daily_ret).sum()
            else:
                port_ret = 0.0

            nav *= (1 + port_ret)
            nav_list.append({"date": td, "nav": nav})
            returns_list.append({"date": td, "return": port_ret})

        nav_series = pd.Series(
            {r["date"]: r["nav"] for r in nav_list}
        )
        returns_series = pd.Series(
            {r["date"]: r["return"] for r in returns_list}
        )

        # 计算绩效指标
        metrics = self._calc_metrics(
            nav_series, returns_series, rebalance_dates, turnovers,
        )

        # 加载基准收益率
        benchmark_returns = None
        if benchmark_codes:
            benchmark_returns = self._load_benchmark_returns(
                benchmark_codes, start_date, end_date, trade_dates,
            )

        result = BacktestResult(
            nav_series=nav_series,
            returns_series=returns_series,
            positions_history=positions_history,
            rebalance_dates=rebalance_dates,
            metrics=metrics,
            benchmark_returns=benchmark_returns,
        )
        logger.info(result.summary())
        return result

    def _load_benchmark_returns(
        self,
        benchmark_codes: List[str],
        start_date: str,
        end_date: str,
        trade_dates: List[str],
    ) -> Dict[str, pd.Series]:
        """
        加载基准指数日收益率

        Returns
        -------
        Dict[str, pd.Series]
            {指数代码: 日收益率 Series (index=trade_date)}
        """
        INDEX_NAMES = {
            "000300.SH": "沪深300",
            "000905.SH": "中证500",
            "000852.SH": "中证1000",
            "932000.CSI": "中证2000",
            "000922.CSI": "中证红利",
            "000016.SH": "上证50",
            "000985.CSI": "中证全指",
        }
        result = {}
        for code in benchmark_codes:
            try:
                df = self.ds.get_index_daily(
                    ts_code=code, start_date=start_date, end_date=end_date,
                )
                if df.empty:
                    logger.warning("基准 %s 无数据", code)
                    continue
                df = df.sort_values("trade_date")
                ret = df.set_index("trade_date")["pct_chg"] / 100.0
                # 对齐到策略的交易日
                ret = ret.reindex(trade_dates).fillna(0)
                label = INDEX_NAMES.get(code, code)
                result[label] = ret
                logger.info("基准 %s (%s): %d 日", code, label, len(ret))
            except Exception as e:
                logger.warning("加载基准 %s 失败: %s", code, e)
        return result

    def _get_rebalance_dates(
        self, start: str, end: str, freq: str
    ) -> List[str]:
        """获取再平衡日期列表 (月末/季末交易日)"""
        from vortex.utils.date_utils import load_trade_cal
        cal = load_trade_cal(self.ds.data_dir)
        cal_list = sorted([d.strftime("%Y%m%d") for d in cal])
        in_range = [d for d in cal_list if start <= d <= end]

        if not in_range:
            return []

        if freq == "M":
            # 每月最后一个交易日
            by_month = {}
            for d in in_range:
                key = d[:6]
                by_month[key] = d
            return sorted(by_month.values())
        elif freq == "Q":
            by_quarter = {}
            for d in in_range:
                month = int(d[4:6])
                q = (month - 1) // 3
                key = f"{d[:4]}Q{q}"
                by_quarter[key] = d
            return sorted(by_quarter.values())
        elif freq == "SA":
            # 半年调仓: 6月末 + 12月末
            by_half = {}
            for d in in_range:
                month = int(d[4:6])
                half = 1 if month <= 6 else 2
                key = f"{d[:4]}H{half}"
                by_half[key] = d
            return sorted(by_half.values())
        else:
            return in_range

    @staticmethod
    def _calc_turnover(old: pd.Series, new: pd.Series) -> float:
        """换手率 = 权重变化绝对值之和 / 2"""
        all_codes = set(old.index) | set(new.index)
        total = sum(abs(new.get(c, 0) - old.get(c, 0)) for c in all_codes)
        return total / 2

    @staticmethod
    def _calc_metrics(
        nav: pd.Series,
        returns: pd.Series,
        rebalance_dates: List[str],
        turnovers: List[float],
    ) -> Dict:
        """计算绩效指标"""
        if len(nav) < 2:
            return {}

        total_return = nav.iloc[-1] / nav.iloc[0] - 1
        n_days = len(nav)
        annual_return = (1 + total_return) ** (252 / max(n_days, 1)) - 1

        daily_std = returns.std()
        annual_vol = daily_std * np.sqrt(252) if daily_std > 0 else 0

        rf = 0.02  # 无风险利率
        sharpe = (annual_return - rf) / annual_vol if annual_vol > 0 else 0

        # Sortino 比率 (只考虑下行波动)
        downside = returns[returns < 0]
        downside_std = downside.std() * np.sqrt(252) if len(downside) > 0 else 0
        sortino = (annual_return - rf) / downside_std if downside_std > 0 else 0

        # 最大回撤 & 最大回撤持续天数
        cummax = nav.cummax()
        drawdown = (cummax - nav) / cummax
        max_dd = drawdown.max()

        # 回撤持续天数: 从回撤开始到恢复(或截止)的最长期间
        max_dd_days = 0
        dd_start = None
        for i, (idx, dd_val) in enumerate(drawdown.items()):
            if dd_val > 0 and dd_start is None:
                dd_start = i
            elif dd_val == 0 and dd_start is not None:
                max_dd_days = max(max_dd_days, i - dd_start)
                dd_start = None
        if dd_start is not None:
            max_dd_days = max(max_dd_days, len(drawdown) - dd_start)

        calmar = annual_return / max_dd if max_dd > 0 else 0

        # 盈亏比 (Profit Factor)
        gains = returns[returns > 0].sum()
        losses = abs(returns[returns < 0].sum())
        profit_factor = gains / losses if losses > 0 else float('inf')

        win_rate = (returns > 0).sum() / max(len(returns), 1)

        return {
            "start_date": nav.index[0] if len(nav) > 0 else "N/A",
            "end_date": nav.index[-1] if len(nav) > 0 else "N/A",
            "n_rebalance": len(rebalance_dates),
            "total_return": total_return,
            "annual_return": annual_return,
            "annual_volatility": annual_vol,
            "sharpe_ratio": sharpe,
            "sortino_ratio": sortino,
            "max_drawdown": max_dd,
            "max_dd_days": max_dd_days,
            "calmar_ratio": calmar,
            "profit_factor": profit_factor,
            "avg_turnover": np.mean(turnovers) if turnovers else 0,
            "win_rate": win_rate,
        }
