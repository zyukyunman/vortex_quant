"""Strategy evaluation protocol metrics."""
from __future__ import annotations

from dataclasses import dataclass
import math

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class StrategyProtocolMetrics:
    """Supplemental metrics for promotion/shadow/reject decisions."""

    sortino: float
    cvar_5pct: float
    worst_5d_return: float
    worst_20d_return: float
    max_drawdown_duration_days: int
    max_drawdown_recovery_days: int | None
    max_drawdown_recovered: bool
    positive_month_rate: float
    annual_win_rate: float


def compute_strategy_protocol_metrics(
    equity_curve: pd.Series,
    returns: pd.Series,
) -> StrategyProtocolMetrics:
    """Compute path, downside, and tail-risk metrics from daily returns."""

    clean_returns = pd.to_numeric(returns, errors="coerce").dropna().sort_index()
    clean_equity = pd.to_numeric(equity_curve, errors="coerce").dropna().sort_index()
    return StrategyProtocolMetrics(
        sortino=_sortino(clean_returns),
        cvar_5pct=_cvar(clean_returns, tail_fraction=0.05),
        worst_5d_return=_worst_window_return(clean_returns, window=5),
        worst_20d_return=_worst_window_return(clean_returns, window=20),
        **_drawdown_path_metrics(clean_equity),
        positive_month_rate=_period_positive_rate(clean_returns, freq="ME"),
        annual_win_rate=_period_positive_rate(clean_returns, freq="YE"),
    )


def _sortino(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    downside = returns.clip(upper=0.0)
    downside_deviation = float(np.sqrt(np.mean(np.square(downside))))
    if downside_deviation <= 0:
        return 0.0
    return float((returns.mean() / downside_deviation) * math.sqrt(252.0))


def _cvar(returns: pd.Series, *, tail_fraction: float) -> float:
    if returns.empty:
        return 0.0
    tail_count = max(1, int(math.ceil(len(returns) * tail_fraction)))
    return float(returns.sort_values().head(tail_count).mean())


def _worst_window_return(returns: pd.Series, *, window: int) -> float:
    if returns.empty:
        return 0.0
    compounded = (1.0 + returns).rolling(window=window, min_periods=1).apply(np.prod, raw=True) - 1.0
    return float(compounded.min())


def _drawdown_path_metrics(equity_curve: pd.Series) -> dict[str, object]:
    if equity_curve.empty:
        return {
            "max_drawdown_duration_days": 0,
            "max_drawdown_recovery_days": None,
            "max_drawdown_recovered": False,
        }
    values = equity_curve.to_numpy(dtype=float)
    running_max = np.maximum.accumulate(values)
    drawdown = values / running_max - 1.0
    trough_pos = int(np.nanargmin(drawdown))
    if float(drawdown[trough_pos]) >= 0:
        return {
            "max_drawdown_duration_days": 0,
            "max_drawdown_recovery_days": 0,
            "max_drawdown_recovered": True,
        }

    pre_trough_max = float(running_max[trough_pos])
    peak_candidates = np.flatnonzero(values[: trough_pos + 1] >= pre_trough_max)
    peak_pos = int(peak_candidates[-1]) if len(peak_candidates) else 0
    duration = int(trough_pos - peak_pos)

    recovered = np.flatnonzero(values[trough_pos:] >= pre_trough_max)
    if len(recovered) == 0:
        return {
            "max_drawdown_duration_days": duration,
            "max_drawdown_recovery_days": None,
            "max_drawdown_recovered": False,
        }
    recovery_days = int(recovered[0])
    return {
        "max_drawdown_duration_days": duration,
        "max_drawdown_recovery_days": recovery_days,
        "max_drawdown_recovered": True,
    }


def _period_positive_rate(returns: pd.Series, *, freq: str) -> float:
    if returns.empty:
        return 0.0
    index = pd.to_datetime(returns.index.astype(str), errors="coerce")
    series = pd.Series(returns.to_numpy(dtype=float), index=index).dropna()
    if series.empty:
        return 0.0
    period_returns = (1.0 + series).resample(freq).prod() - 1.0
    period_returns = period_returns.dropna()
    if period_returns.empty:
        return 0.0
    return float((period_returns > 0).mean())
