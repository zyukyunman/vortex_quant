"""季度选股池 + 周频技术择时回测内核。

该模块用于验证“低频选股、较高频交易”的策略原型：

1. 每隔一个季度用 selection_score 选出 50 只候选股。
2. 周频只在“候选股 + 当前持仓”组成的池子里交易。
3. 买卖由日 K 动量、支撑位和压力位决定。
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from vortex.research.goal_review import ExperimentQuality, GoalCriteria, GoalReviewResult
from vortex.strategy.backtest import BacktestMetrics, review_backtest_metrics


@dataclass(frozen=True)
class TechnicalPoolConfig:
    """季度选股池技术择时策略配置。"""

    selection_size: int = 50
    max_holdings: int = 30
    selection_every: int = 63
    rebalance_every: int = 5
    momentum_window: int = 20
    support_window: int = 20
    resistance_window: int = 20
    min_buy_momentum: float = 0.0
    sell_momentum: float = -0.03
    near_support_pct: float = 0.08
    min_resistance_room: float = 0.03
    breakout_pct: float = 0.0
    support_break_pct: float = 0.02
    resistance_exit_pct: float = 0.02
    use_resistance_exit: bool = False
    require_technical_entry: bool = True
    max_weight: float = 1.0 / 30.0
    transaction_cost_bps: float = 8.0
    initial_equity: float = 1.0


@dataclass(frozen=True)
class TechnicalPoolResult:
    """季度选股池技术择时策略回测结果。"""

    metrics: BacktestMetrics
    goal_review: GoalReviewResult
    equity_curve: pd.Series = field(repr=False)
    weights: pd.DataFrame = field(repr=False)
    returns: pd.Series = field(repr=False)
    selections: dict[pd.Timestamp, list[str]] = field(repr=False)

    def to_dict(self) -> dict[str, object]:
        return {
            "metrics": self.metrics.__dict__,
            "goal_review": self.goal_review.to_dict(),
        }


def run_quarterly_pool_technical_backtest(
    selection_score: pd.DataFrame,
    close: pd.DataFrame,
    high: pd.DataFrame,
    low: pd.DataFrame,
    config: TechnicalPoolConfig | None = None,
    market_gate: pd.Series | None = None,
    *,
    quality: ExperimentQuality | None = None,
    goal_criteria: GoalCriteria | None = None,
) -> TechnicalPoolResult:
    """运行“季度选 50、周频持 30、动量+支撑压力交易”回测。

    日期 t 的所有信号只用于 t+1 的持仓收益，避免未来函数。
    """

    config = config or TechnicalPoolConfig()
    _validate_inputs(selection_score, close, high, low, config)

    common_dates = selection_score.index.intersection(close.index).intersection(high.index).intersection(low.index)
    common_symbols = (
        selection_score.columns.intersection(close.columns).intersection(high.columns).intersection(low.columns)
    )
    selection_score = selection_score.loc[common_dates, common_symbols].sort_index()
    close = close.loc[common_dates, common_symbols].sort_index()
    high = high.loc[common_dates, common_symbols].sort_index()
    low = low.loc[common_dates, common_symbols].sort_index()
    if market_gate is not None:
        market_gate = market_gate.reindex(common_dates).fillna(False).astype(bool)

    daily_returns = close.pct_change().fillna(0.0)
    momentum = close / close.shift(config.momentum_window) - 1.0
    support = low.rolling(config.support_window, min_periods=config.support_window).min().shift(1)
    resistance = high.rolling(config.resistance_window, min_periods=config.resistance_window).max().shift(1)

    current_weights = pd.Series(0.0, index=common_symbols)
    current_pool: set[str] = set()
    selections: dict[pd.Timestamp, list[str]] = {}
    equity = config.initial_equity
    equity_rows: list[dict[str, object]] = [{"date": common_dates[0], "equity": equity}]
    return_rows: list[dict[str, object]] = []
    weight_rows: list[pd.Series] = []
    turnover_sum = 0.0
    rebalance_count = 0

    for idx in range(1, len(common_dates)):
        signal_date = common_dates[idx - 1]
        trade_date = common_dates[idx]
        current_holdings = set(current_weights[current_weights > 0].index)

        if _is_selection_day(idx - 1, config):
            selected = _select_universe(selection_score.loc[signal_date], config.selection_size)
            current_pool = set(selected).union(current_holdings)
            selections[signal_date] = selected

        if (idx - 1) % config.rebalance_every == 0:
            if market_gate is not None and not bool(market_gate.loc[signal_date]):
                target_weights = pd.Series(0.0, index=common_symbols)
            else:
                target_weights = _target_weights(
                    pool=current_pool.union(current_holdings),
                    current_holdings=current_holdings,
                    momentum=momentum.loc[signal_date],
                    close=close.loc[signal_date],
                    support=support.loc[signal_date],
                    resistance=resistance.loc[signal_date],
                    config=config,
                )
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
    return TechnicalPoolResult(
        metrics=metrics,
        goal_review=goal_review,
        equity_curve=equity_curve,
        weights=weights,
        returns=returns,
        selections=selections,
    )


def _is_selection_day(offset: int, config: TechnicalPoolConfig) -> bool:
    return offset % config.selection_every == 0


def _select_universe(score: pd.Series, selection_size: int) -> list[str]:
    clean = score.dropna().sort_values(ascending=False)
    return list(clean.head(selection_size).index)


def _target_weights(
    *,
    pool: set[str],
    current_holdings: set[str],
    momentum: pd.Series,
    close: pd.Series,
    support: pd.Series,
    resistance: pd.Series,
    config: TechnicalPoolConfig,
) -> pd.Series:
    target = pd.Series(0.0, index=close.index)
    if not pool:
        return target

    pool_index = close.index.intersection(pd.Index(sorted(pool)))
    tradable = pd.DataFrame(
        {
            "momentum": momentum.reindex(pool_index),
            "close": close.reindex(pool_index),
            "support": support.reindex(pool_index),
            "resistance": resistance.reindex(pool_index),
        }
    ).dropna()
    if tradable.empty:
        return target

    held = set(tradable.index).intersection(current_holdings)
    keep = _keep_holdings(tradable, held, config)
    buyable = _buy_candidates(tradable.drop(index=list(keep.index), errors="ignore"), config)
    selected = pd.concat([keep, buyable]).sort_values("entry_score", ascending=False)
    selected_symbols = list(selected.head(config.max_holdings).index)

    if selected_symbols:
        raw_weight = min(1.0 / len(selected_symbols), config.max_weight)
        target.loc[selected_symbols] = raw_weight
        total = float(target.sum())
        if total > 0:
            target = target / total
    return target


def _keep_holdings(
    frame: pd.DataFrame,
    current_holdings: set[str],
    config: TechnicalPoolConfig,
) -> pd.DataFrame:
    if not current_holdings:
        return frame.iloc[0:0].assign(entry_score=pd.Series(dtype=float))

    held = frame.loc[list(current_holdings)].copy()
    support_break = held["close"] < held["support"] * (1.0 - config.support_break_pct)
    momentum_break = held["momentum"] < config.sell_momentum
    resistance_exit = pd.Series(False, index=held.index)
    if config.use_resistance_exit:
        near_resistance = held["close"] >= held["resistance"] * (1.0 - config.resistance_exit_pct)
        resistance_exit = near_resistance & (held["momentum"] <= config.min_buy_momentum)
    keep = held.loc[~(support_break | momentum_break | resistance_exit)].copy()
    keep["entry_score"] = _entry_score(keep)
    return keep


def _buy_candidates(frame: pd.DataFrame, config: TechnicalPoolConfig) -> pd.DataFrame:
    if frame.empty:
        return frame.assign(entry_score=pd.Series(dtype=float))

    scored = frame.copy()
    scored["entry_score"] = _entry_score(scored)
    if not config.require_technical_entry:
        return scored.loc[scored["momentum"] >= config.min_buy_momentum]

    distance_to_support = frame["close"] / frame["support"] - 1.0
    resistance_room = frame["resistance"] / frame["close"] - 1.0
    near_support = (distance_to_support >= 0) & (distance_to_support <= config.near_support_pct)
    has_room = resistance_room >= config.min_resistance_room
    breakout = frame["close"] >= frame["resistance"] * (1.0 + config.breakout_pct)
    momentum_ok = frame["momentum"] >= config.min_buy_momentum
    candidates = frame.loc[momentum_ok & ((near_support & has_room) | breakout)].copy()
    candidates["entry_score"] = _entry_score(candidates)
    return candidates


def _entry_score(frame: pd.DataFrame) -> pd.Series:
    distance_to_support = frame["close"] / frame["support"] - 1.0
    resistance_room = frame["resistance"] / frame["close"] - 1.0
    support_score = 1.0 / (1.0 + distance_to_support.clip(lower=0.0))
    return frame["momentum"] + 0.25 * support_score + 0.10 * resistance_room


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


def _validate_inputs(
    selection_score: pd.DataFrame,
    close: pd.DataFrame,
    high: pd.DataFrame,
    low: pd.DataFrame,
    config: TechnicalPoolConfig,
) -> None:
    if selection_score.empty:
        raise ValueError("selection_score 不能为空")
    if close.empty or high.empty or low.empty:
        raise ValueError("OHLC 数据不能为空")
    for name, frame in {"selection_score": selection_score, "close": close, "high": high, "low": low}.items():
        if not frame.index.is_monotonic_increasing:
            raise ValueError(f"{name} index 必须按日期升序排列")
    if config.selection_size <= 0:
        raise ValueError("selection_size 必须为正整数")
    if config.max_holdings <= 0:
        raise ValueError("max_holdings 必须为正整数")
    if config.max_holdings > config.selection_size:
        raise ValueError("max_holdings 不能大于 selection_size")
    if config.selection_every <= 0 or config.rebalance_every <= 0:
        raise ValueError("selection_every 和 rebalance_every 必须为正整数")
    if config.momentum_window <= 0 or config.support_window <= 1 or config.resistance_window <= 1:
        raise ValueError("窗口参数必须为正")
    if not 0 < config.max_weight <= 1:
        raise ValueError("max_weight 必须在 (0, 1] 内")
    if config.transaction_cost_bps < 0:
        raise ValueError("transaction_cost_bps 不能为负")
