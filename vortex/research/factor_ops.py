"""PIT-safe dataframe operators for daily factor research.

The functions operate on wide dataframes indexed by date with symbols as
columns. They are intentionally small building blocks for Alpha101-style
formula research and avoid look-ahead by using pandas rolling windows.
"""
from __future__ import annotations

from collections.abc import Mapping

import numpy as np
import pandas as pd


def cs_rank(frame: pd.DataFrame, *, pct: bool = True) -> pd.DataFrame:
    """Cross-sectional rank by date."""

    return frame.rank(axis=1, pct=pct)


def cs_zscore(frame: pd.DataFrame) -> pd.DataFrame:
    """Cross-sectional z-score by date."""

    mean = frame.mean(axis=1)
    std = frame.std(axis=1).replace(0.0, np.nan)
    return frame.sub(mean, axis=0).div(std, axis=0)


def delay(frame: pd.DataFrame, periods: int = 1) -> pd.DataFrame:
    """Lag a factor or price matrix by trading days."""

    return frame.shift(periods)


def delta(frame: pd.DataFrame, periods: int = 1) -> pd.DataFrame:
    """Difference from `periods` trading days ago."""

    return frame - frame.shift(periods)


def ts_rank(frame: pd.DataFrame, window: int, *, min_periods: int | None = None) -> pd.DataFrame:
    """Rolling percentile rank of the latest value within each symbol history."""

    resolved_min_periods = min_periods or window
    return frame.rolling(window, min_periods=resolved_min_periods).apply(_last_percentile_rank, raw=True)


def ts_sum(frame: pd.DataFrame, window: int, *, min_periods: int | None = None) -> pd.DataFrame:
    """Rolling time-series sum."""

    return frame.rolling(window, min_periods=min_periods or window).sum()


def ts_mean(frame: pd.DataFrame, window: int, *, min_periods: int | None = None) -> pd.DataFrame:
    """Rolling time-series mean."""

    return frame.rolling(window, min_periods=min_periods or window).mean()


def ts_std(frame: pd.DataFrame, window: int, *, min_periods: int | None = None) -> pd.DataFrame:
    """Rolling time-series standard deviation."""

    return frame.rolling(window, min_periods=min_periods or window).std()


def correlation(
    left: pd.DataFrame,
    right: pd.DataFrame,
    window: int,
    *,
    min_periods: int | None = None,
) -> pd.DataFrame:
    """Rolling correlation by symbol."""

    aligned_left, aligned_right = left.align(right, join="outer", axis=None)
    return aligned_left.rolling(window, min_periods=min_periods or window).corr(aligned_right)


def covariance(
    left: pd.DataFrame,
    right: pd.DataFrame,
    window: int,
    *,
    min_periods: int | None = None,
) -> pd.DataFrame:
    """Rolling covariance by symbol."""

    aligned_left, aligned_right = left.align(right, join="outer", axis=None)
    return aligned_left.rolling(window, min_periods=min_periods or window).cov(aligned_right)


def decay_linear(frame: pd.DataFrame, window: int, *, min_periods: int | None = None) -> pd.DataFrame:
    """Rolling linearly weighted mean, newest observation gets largest weight."""

    resolved_min_periods = min_periods or window
    return frame.rolling(window, min_periods=resolved_min_periods).apply(_weighted_latest_mean, raw=True)


def signed_power(frame: pd.DataFrame, exponent: float) -> pd.DataFrame:
    """Alpha101-style signed power."""

    return np.sign(frame) * np.power(np.abs(frame), exponent)


def scale(frame: pd.DataFrame, k: float = 1.0) -> pd.DataFrame:
    """Scale each date's absolute exposure sum to `k`."""

    denom = frame.abs().sum(axis=1).replace(0.0, np.nan)
    return frame.mul(k, axis=0).div(denom, axis=0)


def neutralize_by_group(frame: pd.DataFrame, groups: Mapping[str, str]) -> pd.DataFrame:
    """Subtract same-date group means from each symbol."""

    group_series = pd.Series(groups)
    result = frame.copy()
    for group in sorted(group_series.dropna().unique()):
        columns = [symbol for symbol in group_series[group_series == group].index if symbol in result.columns]
        if not columns:
            continue
        group_mean = result[columns].mean(axis=1)
        result[columns] = result[columns].sub(group_mean, axis=0)
    return result


def _last_percentile_rank(values: np.ndarray) -> float:
    valid = values[~np.isnan(values)]
    if len(valid) == 0 or np.isnan(values[-1]):
        return np.nan
    return float(pd.Series(valid).rank(pct=True).iloc[-1])


def _weighted_latest_mean(values: np.ndarray) -> float:
    valid_mask = ~np.isnan(values)
    if not valid_mask.any():
        return np.nan
    weights = np.arange(1, len(values) + 1, dtype="float64")
    valid_weights = weights[valid_mask]
    valid_values = values[valid_mask]
    return float(np.dot(valid_values, valid_weights) / valid_weights.sum())
