"""截面多空组合回测。

当 long-only 策略无法同时满足高收益和低回撤时，市场中性多空组合
可以把主要风险从市场 beta 转为因子 alpha。该模块只做纯计算，不假设
具体融券、股指期货或衍生品执行方式。
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from vortex.research.goal_review import (
    ExperimentQuality,
    GoalCriteria,
    GoalReviewResult,
)
from vortex.strategy.backtest import BacktestMetrics, review_backtest_metrics


@dataclass(frozen=True)
class LongShortConfig:
    """截面多空组合配置。"""

    long_n: int = 30
    short_n: int = 30
    rebalance_every: int = 5
    gross_exposure: float = 1.0
    long_exposure_fraction: float = 0.5
    transaction_cost_bps: float = 12.0
    initial_equity: float = 1.0


@dataclass(frozen=True)
class LongShortResult:
    """多空回测结果。"""

    metrics: BacktestMetrics
    goal_review: GoalReviewResult
    equity_curve: pd.Series = field(repr=False)
    weights: pd.DataFrame = field(repr=False)
    returns: pd.Series = field(repr=False)

    def to_dict(self) -> dict[str, object]:
        return {
            "metrics": self.metrics.__dict__,
            "goal_review": self.goal_review.to_dict(),
        }


def run_cross_sectional_long_short_backtest(
    signal: pd.DataFrame,
    close: pd.DataFrame,
    config: LongShortConfig | None = None,
    *,
    market_gate: pd.Series | None = None,
    quality: ExperimentQuality | None = None,
    goal_criteria: GoalCriteria | None = None,
) -> LongShortResult:
    """用 t-1 的截面信号构建 t 日多空组合收益。"""

    config = config or LongShortConfig()
    _validate_inputs(signal, close, config)
    common_dates = signal.index.intersection(close.index)
    common_symbols = signal.columns.intersection(close.columns)
    signal = signal.loc[common_dates, common_symbols].sort_index()
    close = close.loc[common_dates, common_symbols].sort_index()
    if market_gate is not None:
        market_gate = market_gate.reindex(common_dates).fillna(False).astype(bool)

    daily_returns = close.pct_change().fillna(0.0)
    current_weights = pd.Series(0.0, index=common_symbols)
    equity = config.initial_equity
    equity_rows: list[dict[str, object]] = [{"date": common_dates[0], "equity": equity}]
    return_rows: list[dict[str, object]] = []
    weight_rows: list[pd.Series] = []
    turnover_sum = 0.0
    rebalance_count = 0

    for idx in range(1, len(common_dates)):
        signal_date = common_dates[idx - 1]
        trade_date = common_dates[idx]
        if (idx - 1) % config.rebalance_every == 0:
            if market_gate is not None and not bool(market_gate.loc[signal_date]):
                target_weights = pd.Series(0.0, index=common_symbols)
            else:
                target_weights = _target_weights(signal.loc[signal_date], config)
            turnover = float((target_weights - current_weights).abs().sum())
            current_weights = target_weights
            rebalance_count += 1
        else:
            turnover = 0.0

        gross_ret = float((current_weights * daily_returns.loc[trade_date]).sum())
        cost = turnover * config.transaction_cost_bps / 10000.0
        net_ret = gross_ret - cost
        equity *= 1.0 + net_ret
        turnover_sum += turnover

        return_rows.append({"date": trade_date, "return": net_ret})
        equity_rows.append({"date": trade_date, "equity": equity})
        row = current_weights.copy()
        row.name = trade_date
        weight_rows.append(row)

    equity_curve = pd.DataFrame(equity_rows).set_index("date")["equity"]
    returns = pd.DataFrame(return_rows).set_index("date")["return"]
    weights = pd.DataFrame(weight_rows).fillna(0.0)
    metrics = _compute_metrics(equity_curve, returns, turnover_sum=turnover_sum, rebalance_count=rebalance_count)
    goal_review = review_backtest_metrics(metrics, quality=quality, goal_criteria=goal_criteria)
    return LongShortResult(
        metrics=metrics,
        goal_review=goal_review,
        equity_curve=equity_curve,
        weights=weights,
        returns=returns,
    )


def _target_weights(score: pd.Series, config: LongShortConfig) -> pd.Series:
    clean = score.dropna().sort_values(ascending=False)
    target = pd.Series(0.0, index=score.index)
    if clean.empty:
        return target
    long_names = clean.head(config.long_n).index
    short_names = clean.tail(config.short_n).index.difference(long_names)
    long_exposure = config.gross_exposure * config.long_exposure_fraction
    short_exposure = config.gross_exposure - long_exposure
    if len(long_names) > 0:
        target.loc[long_names] = long_exposure / len(long_names)
    if len(short_names) > 0:
        target.loc[short_names] = -short_exposure / len(short_names)
    return target


def _compute_metrics(
    equity_curve: pd.Series,
    returns: pd.Series,
    *,
    turnover_sum: float,
    rebalance_count: int,
) -> BacktestMetrics:
    total_return = float(equity_curve.iloc[-1] / equity_curve.iloc[0] - 1.0)
    years = max(len(returns) / 252.0, 1.0 / 252.0)
    annual_return = float((1.0 + total_return) ** (1.0 / years) - 1.0)
    running_max = equity_curve.cummax()
    drawdown = equity_curve / running_max - 1.0
    max_drawdown = float(drawdown.min())
    ret_std = float(returns.std(ddof=1)) if len(returns) > 1 else 0.0
    sharpe = float((returns.mean() / ret_std) * (252 ** 0.5)) if ret_std > 0 else 0.0
    calmar = annual_return / abs(max_drawdown) if max_drawdown < 0 else 0.0
    turnover = turnover_sum / max(rebalance_count, 1)
    return BacktestMetrics(
        annual_return=annual_return,
        max_drawdown=max_drawdown,
        sharpe=sharpe,
        calmar=calmar,
        turnover=float(turnover),
        total_return=total_return,
    )


def _validate_inputs(signal: pd.DataFrame, close: pd.DataFrame, config: LongShortConfig) -> None:
    if signal.empty:
        raise ValueError("signal 不能为空")
    if close.empty:
        raise ValueError("close 不能为空")
    if not signal.index.is_monotonic_increasing:
        raise ValueError("signal index 必须按日期升序排列")
    if not close.index.is_monotonic_increasing:
        raise ValueError("close index 必须按日期升序排列")
    if config.long_n <= 0:
        raise ValueError("long_n 必须为正整数")
    if config.short_n <= 0:
        raise ValueError("short_n 必须为正整数")
    if config.rebalance_every <= 0:
        raise ValueError("rebalance_every 必须为正整数")
    if config.gross_exposure <= 0:
        raise ValueError("gross_exposure 必须为正数")
    if not 0 < config.long_exposure_fraction < 1:
        raise ValueError("long_exposure_fraction 必须在 (0, 1) 内")
    if config.transaction_cost_bps < 0:
        raise ValueError("transaction_cost_bps 不能为负")
