"""轻量截面选股回测内核。

该模块是 Strategy MVP 的纯计算版本：消费 date × symbol 的 signal 分数
和 close 价格，按固定调仓周期构建 Top-N 组合并计算收益风险指标。
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from vortex.research.goal_review import (
    ExperimentQuality,
    GoalCriteria,
    GoalReviewResult,
    StrategyGoalInput,
    review_strategy_goal,
)


@dataclass(frozen=True)
class BacktestConfig:
    """截面策略回测配置。"""

    top_n: int = 10
    rebalance_every: int = 20
    max_weight: float = 0.10
    target_exposure: float = 1.0
    transaction_cost_bps: float = 8.0
    initial_equity: float = 1.0


@dataclass(frozen=True)
class BacktestMetrics:
    """回测核心指标。"""

    annual_return: float
    max_drawdown: float
    sharpe: float
    calmar: float
    turnover: float
    total_return: float


@dataclass(frozen=True)
class BacktestResult:
    """截面策略回测结果。"""

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


def run_cross_sectional_backtest(
    signal: pd.DataFrame,
    close: pd.DataFrame,
    config: BacktestConfig | None = None,
    *,
    quality: ExperimentQuality | None = None,
    goal_criteria: GoalCriteria | None = None,
) -> BacktestResult:
    """运行 Top-N 截面选股回测。

    为避免未来函数，日期 t 的 signal 只用于 t→t+1 的持仓收益。
    """

    config = config or BacktestConfig()
    _validate_inputs(signal, close, config)

    common_dates = signal.index.intersection(close.index)
    common_symbols = signal.columns.intersection(close.columns)
    signal = signal.loc[common_dates, common_symbols].sort_index()
    close = close.loc[common_dates, common_symbols].sort_index()
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
    metrics = _compute_metrics(
        equity_curve,
        returns,
        turnover_sum=turnover_sum,
        rebalance_count=rebalance_count,
    )
    goal_review = review_backtest_metrics(metrics, quality=quality, goal_criteria=goal_criteria)
    return BacktestResult(
        metrics=metrics,
        goal_review=goal_review,
        equity_curve=equity_curve,
        weights=weights,
        returns=returns,
    )


def review_backtest_metrics(
    metrics: BacktestMetrics,
    *,
    quality: ExperimentQuality | None = None,
    goal_criteria: GoalCriteria | None = None,
) -> GoalReviewResult:
    """把回测指标转成统一目标审查结果。"""

    return review_strategy_goal(
        StrategyGoalInput(
            annual_return=metrics.annual_return,
            max_drawdown=metrics.max_drawdown,
            sharpe=metrics.sharpe,
            calmar=metrics.calmar,
            quality=quality,
        ),
        criteria=goal_criteria,
    )


def _target_weights(score: pd.Series, config: BacktestConfig) -> pd.Series:
    clean = score.dropna().sort_values(ascending=False)
    target = pd.Series(0.0, index=score.index)
    if clean.empty:
        return target
    selected = clean.head(config.top_n).index
    raw_weight = min(1.0 / len(selected), config.max_weight)
    target.loc[selected] = raw_weight
    total = float(target.sum())
    if total > 0:
        target = target / total * config.target_exposure
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


def _validate_inputs(signal: pd.DataFrame, close: pd.DataFrame, config: BacktestConfig) -> None:
    if signal.empty:
        raise ValueError("signal 不能为空")
    if close.empty:
        raise ValueError("close 不能为空")
    if not signal.index.is_monotonic_increasing:
        raise ValueError("signal index 必须按日期升序排列")
    if not close.index.is_monotonic_increasing:
        raise ValueError("close index 必须按日期升序排列")
    if config.top_n <= 0:
        raise ValueError("top_n 必须为正整数")
    if config.rebalance_every <= 0:
        raise ValueError("rebalance_every 必须为正整数")
    if not 0 < config.max_weight <= 1:
        raise ValueError("max_weight 必须在 (0, 1] 内")
    if not 0 < config.target_exposure <= 1:
        raise ValueError("target_exposure 必须在 (0, 1] 内")
    if config.transaction_cost_bps < 0:
        raise ValueError("transaction_cost_bps 不能为负")
