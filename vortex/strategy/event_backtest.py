"""事件信号专用轻量回测器。

事件策略通常是稀疏信号：公告、热榜、龙虎榜、涨停等事件只在少数日期出现。
该模块假设输入 signal 已经映射到可交易日，returns 也已经按同一执行口径构造，
例如“公告后下一交易日开盘到收盘收益”。这样可以避免在回测器内部猜测事件时点。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd

from vortex.research.goal_review import ExperimentQuality, GoalCriteria
from vortex.strategy.backtest import BacktestConfig, BacktestResult, _compute_metrics, review_backtest_metrics


@dataclass(frozen=True)
class EventBacktestConfig:
    """事件回测配置。"""

    top_n: int = 10
    max_weight: float = 0.20
    target_exposure: float = 1.0
    transaction_cost_bps: float = 20.0
    initial_equity: float = 1.0
    position_mode: Literal["full_equal_selected", "capped_with_cash"] = "full_equal_selected"


def run_event_signal_backtest(
    signal: pd.DataFrame,
    returns: pd.DataFrame,
    config: EventBacktestConfig | None = None,
    *,
    market_gate: pd.Series | None = None,
    blocked_buy_mask: pd.DataFrame | None = None,
    blocked_sell_mask: pd.DataFrame | None = None,
    quality: ExperimentQuality | None = None,
    goal_criteria: GoalCriteria | None = None,
) -> BacktestResult:
    """运行事件信号回测。

    `signal.loc[date]` 会直接决定 `returns.loc[date]` 的持仓，因此调用方必须先把
    公告/事件映射到真实可交易日，并提供同一执行口径的收益矩阵。
    """

    config = config or EventBacktestConfig()
    _validate_event_backtest_inputs(signal, returns, config)
    common_dates = signal.index.intersection(returns.index)
    common_symbols = signal.columns.intersection(returns.columns)
    signal = signal.loc[common_dates, common_symbols].sort_index()
    returns = returns.loc[common_dates, common_symbols].sort_index().fillna(0.0)
    gate = market_gate.reindex(common_dates).fillna(False) if market_gate is not None else None
    blocked_buy = _align_trade_block_mask(blocked_buy_mask, common_dates, common_symbols)
    blocked_sell = _align_trade_block_mask(blocked_sell_mask, common_dates, common_symbols)

    current_weights = pd.Series(0.0, index=common_symbols)
    equity = config.initial_equity
    equity_rows: list[dict[str, object]] = [{"date": common_dates[0], "equity": equity}]
    return_rows: list[dict[str, object]] = []
    weight_rows: list[pd.Series] = []
    turnover_sum = 0.0

    for idx in range(1, len(common_dates)):
        trade_date = common_dates[idx]
        if gate is not None and not bool(gate.loc[trade_date]):
            target_weights = pd.Series(0.0, index=common_symbols)
        else:
            target_weights = _event_target_weights(signal.loc[trade_date], config)
        target_weights = _apply_trade_blocks(
            target_weights,
            current_weights,
            blocked_buy.loc[trade_date] if blocked_buy is not None else None,
            blocked_sell.loc[trade_date] if blocked_sell is not None else None,
            target_exposure=config.target_exposure,
        )
        turnover = float((target_weights - current_weights).abs().sum())
        current_weights = target_weights
        gross_ret = float((current_weights * returns.loc[trade_date]).sum())
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
    event_returns = pd.DataFrame(return_rows).set_index("date")["return"]
    weights = pd.DataFrame(weight_rows).fillna(0.0)
    metrics = _compute_metrics(
        equity_curve,
        event_returns,
        turnover_sum=turnover_sum,
        rebalance_count=len(event_returns),
    )
    goal_review = review_backtest_metrics(metrics, quality=quality, goal_criteria=goal_criteria)
    return BacktestResult(
        metrics=metrics,
        goal_review=goal_review,
        equity_curve=equity_curve,
        weights=weights,
        returns=event_returns,
    )


def _event_target_weights(score: pd.Series, config: EventBacktestConfig) -> pd.Series:
    if config.position_mode == "capped_with_cash":
        return _target_weights_capped_with_cash(
            score,
            config.top_n,
            config.max_weight,
            config.target_exposure,
        )
    return _target_weights(score, config.top_n, config.max_weight, config.target_exposure)


def _target_weights(score: pd.Series, top_n: int, max_weight: float, target_exposure: float) -> pd.Series:
    clean = score.dropna().sort_values(ascending=False)
    target = pd.Series(0.0, index=score.index)
    if clean.empty:
        return target
    selected = clean.head(top_n).index
    raw_weight = min(1.0 / len(selected), max_weight)
    target.loc[selected] = raw_weight
    total = float(target.sum())
    if total > 0:
        target = target / total * target_exposure
    return target


def _target_weights_capped_with_cash(
    score: pd.Series,
    top_n: int,
    max_weight: float,
    target_exposure: float,
) -> pd.Series:
    clean = score.dropna().sort_values(ascending=False)
    target = pd.Series(0.0, index=score.index)
    if clean.empty:
        return target
    selected = clean.head(top_n).index
    target.loc[selected] = min(target_exposure / len(selected), max_weight)
    return target


def _align_trade_block_mask(
    mask: pd.DataFrame | None,
    dates: pd.Index,
    symbols: pd.Index,
) -> pd.DataFrame | None:
    if mask is None:
        return None
    return mask.reindex(index=dates, columns=symbols).fillna(False).astype(bool)


def _apply_trade_blocks(
    target: pd.Series,
    current: pd.Series,
    blocked_buy: pd.Series | None,
    blocked_sell: pd.Series | None,
    *,
    target_exposure: float,
) -> pd.Series:
    adjusted = target.copy()
    locked = pd.Series(False, index=target.index)
    eps = 1e-12

    if blocked_sell is not None:
        sell_blocked = blocked_sell.reindex(target.index).fillna(False).astype(bool)
        decreases = adjusted < current - eps
        locked = sell_blocked & decreases
        adjusted.loc[locked] = current.loc[locked]

    if blocked_buy is not None:
        buy_blocked = blocked_buy.reindex(target.index).fillna(False).astype(bool)
        increases = adjusted > current + eps
        adjusted.loc[buy_blocked & increases] = current.loc[buy_blocked & increases]

    total = float(adjusted.sum())
    if total > target_exposure:
        locked_weight = float(adjusted.loc[locked].sum()) if locked.any() else 0.0
        budget = max(target_exposure - locked_weight, 0.0)
        variable = ~locked
        variable_total = float(adjusted.loc[variable].sum())
        if variable_total > 0:
            adjusted.loc[variable] = adjusted.loc[variable] * min(1.0, budget / variable_total)
    if blocked_sell is not None:
        sell_blocked = blocked_sell.reindex(target.index).fillna(False).astype(bool)
        adjusted.loc[sell_blocked & (adjusted < current - eps)] = current.loc[sell_blocked & (adjusted < current - eps)]
    if blocked_buy is not None:
        buy_blocked = blocked_buy.reindex(target.index).fillna(False).astype(bool)
        adjusted.loc[buy_blocked & (adjusted > current + eps)] = current.loc[buy_blocked & (adjusted > current + eps)]
    return adjusted


def _validate_event_backtest_inputs(
    signal: pd.DataFrame,
    returns: pd.DataFrame,
    config: EventBacktestConfig,
) -> None:
    if signal.empty:
        raise ValueError("signal 不能为空")
    if returns.empty:
        raise ValueError("returns 不能为空")
    if not signal.index.is_monotonic_increasing:
        raise ValueError("signal index 必须按日期升序排列")
    if not returns.index.is_monotonic_increasing:
        raise ValueError("returns index 必须按日期升序排列")
    if config.top_n <= 0:
        raise ValueError("top_n 必须为正整数")
    if not 0 < config.max_weight <= 1:
        raise ValueError("max_weight 必须在 (0, 1] 内")
    if not 0 < config.target_exposure <= 1:
        raise ValueError("target_exposure 必须在 (0, 1] 内")
    if config.transaction_cost_bps < 0:
        raise ValueError("transaction_cost_bps 不能为负")
    if config.position_mode not in {"full_equal_selected", "capped_with_cash"}:
        raise ValueError("position_mode 必须是 full_equal_selected 或 capped_with_cash")


def as_backtest_config(config: EventBacktestConfig) -> BacktestConfig:
    """把事件配置映射为通用截面配置，便于报告层复用字段。"""

    return BacktestConfig(
        top_n=config.top_n,
        rebalance_every=1,
        max_weight=config.max_weight,
        target_exposure=config.target_exposure,
        transaction_cost_bps=config.transaction_cost_bps,
        initial_equity=config.initial_equity,
    )
