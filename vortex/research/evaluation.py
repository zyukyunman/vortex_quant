"""因子评测内核。

该模块提供 Research MVP 的纯计算能力：多周期 RankIC、稳定性指标与
多空组合检验。输入统一为 date × symbol 的宽表，便于后续从 Data
snapshot、factor cache 或外部因子表装配。
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass(frozen=True)
class ICStats:
    """单个 horizon 的 IC 统计。"""

    horizon: int
    ic_mean: float
    ic_std: float
    icir: float
    positive_rate: float
    count: int


@dataclass(frozen=True)
class LongShortStats:
    """多空组合统计。"""

    horizon: int
    long_mean: float
    short_mean: float
    long_short_mean: float
    sharpe: float
    count: int


@dataclass(frozen=True)
class FactorEvaluationResult:
    """一次因子评测的汇总结果。"""

    ic_stats: dict[int, ICStats]
    long_short: LongShortStats
    ic_series: dict[int, pd.Series] = field(repr=False)
    long_short_series: pd.DataFrame = field(repr=False)

    def to_dict(self) -> dict[str, object]:
        return {
            "ic_stats": {
                str(horizon): stats.__dict__
                for horizon, stats in self.ic_stats.items()
            },
            "long_short": self.long_short.__dict__,
        }


@dataclass(frozen=True)
class FactorAdmissionRule:
    """因子准入规则。

    该规则用于 candidate hardening：先用统一阈值判断因子是否值得进入
    策略回测，而不是看到一次漂亮回测后再补理由。
    """

    min_ic_mean: float = 0.02
    min_positive_rate: float = 0.55
    min_long_short_mean: float = 0.0
    max_correlation: float = 0.85


@dataclass(frozen=True)
class FactorCandidate:
    """批量因子评测后的候选摘要。"""

    name: str
    result: FactorEvaluationResult = field(repr=False)
    score: float
    admitted: bool
    rejection_reasons: tuple[str, ...]
    max_abs_correlation: float = 0.0

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "score": self.score,
            "admitted": self.admitted,
            "rejection_reasons": list(self.rejection_reasons),
            "max_abs_correlation": self.max_abs_correlation,
            "result": self.result.to_dict(),
        }


def forward_returns(close: pd.DataFrame, horizons: list[int] | tuple[int, ...]) -> dict[int, pd.DataFrame]:
    """计算未来 N 日收益率。

    close 必须是 date × symbol 的收盘价宽表，按日期升序排列。
    """

    _validate_wide_frame(close, "close")
    returns: dict[int, pd.DataFrame] = {}
    for horizon in horizons:
        if horizon <= 0:
            raise ValueError("horizon 必须为正整数")
        returns[horizon] = close.shift(-horizon) / close - 1.0
    return returns


def rank_ic_series(
    factor: pd.DataFrame,
    forward_return: pd.DataFrame,
    *,
    min_periods: int = 30,
) -> pd.Series:
    """计算逐截面 RankIC 序列。"""

    _validate_wide_frame(factor, "factor")
    _validate_wide_frame(forward_return, "forward_return")
    common_dates = factor.index.intersection(forward_return.index)
    values: list[dict[str, object]] = []
    for dt in common_dates:
        f = factor.loc[dt]
        r = forward_return.loc[dt]
        valid = pd.concat([f.rename("factor"), r.rename("ret")], axis=1).dropna()
        if len(valid) < min_periods:
            continue
        ic = valid["factor"].rank().corr(valid["ret"].rank())
        if pd.notna(ic):
            values.append({"date": dt, "ic": float(ic)})
    if not values:
        return pd.Series(dtype="float64", name="ic")
    return pd.DataFrame(values).set_index("date")["ic"]


def summarize_ic(ic: pd.Series, horizon: int) -> ICStats:
    """汇总 IC 序列。"""

    clean = ic.dropna()
    if clean.empty:
        return ICStats(horizon, 0.0, 0.0, 0.0, 0.0, 0)
    ic_mean = float(clean.mean())
    ic_std = float(clean.std(ddof=1)) if len(clean) > 1 else 0.0
    icir = ic_mean / ic_std if ic_std > 0 else 0.0
    positive_rate = float((clean > 0).mean())
    return ICStats(
        horizon=horizon,
        ic_mean=ic_mean,
        ic_std=ic_std,
        icir=icir,
        positive_rate=positive_rate,
        count=int(len(clean)),
    )


def long_short_series(
    factor: pd.DataFrame,
    forward_return: pd.DataFrame,
    *,
    groups: int = 5,
    min_periods: int = 30,
) -> pd.DataFrame:
    """按因子分组计算 top-bottom 多空收益序列。"""

    _validate_wide_frame(factor, "factor")
    _validate_wide_frame(forward_return, "forward_return")
    if groups < 2:
        raise ValueError("groups 必须不少于 2")

    rows: list[dict[str, object]] = []
    common_dates = factor.index.intersection(forward_return.index)
    for dt in common_dates:
        f = factor.loc[dt]
        r = forward_return.loc[dt]
        valid = pd.concat([f.rename("factor"), r.rename("ret")], axis=1).dropna()
        if len(valid) < max(min_periods, groups * 2):
            continue
        rank_pct = valid["factor"].rank(pct=True, method="first")
        long_ret = valid.loc[rank_pct > 1.0 - 1.0 / groups, "ret"].mean()
        short_ret = valid.loc[rank_pct <= 1.0 / groups, "ret"].mean()
        rows.append(
            {
                "date": dt,
                "long": float(long_ret),
                "short": float(short_ret),
                "long_short": float(long_ret - short_ret),
            }
        )
    if not rows:
        return pd.DataFrame(columns=["long", "short", "long_short"])
    return pd.DataFrame(rows).set_index("date")


def summarize_long_short(series: pd.DataFrame, horizon: int) -> LongShortStats:
    """汇总多空收益序列。"""

    if series.empty:
        return LongShortStats(horizon, 0.0, 0.0, 0.0, 0.0, 0)
    spread = series["long_short"].dropna()
    spread_std = float(spread.std(ddof=1)) if len(spread) > 1 else 0.0
    sharpe = float(spread.mean() / spread_std) if spread_std > 0 else 0.0
    return LongShortStats(
        horizon=horizon,
        long_mean=float(series["long"].mean()),
        short_mean=float(series["short"].mean()),
        long_short_mean=float(spread.mean()) if not spread.empty else 0.0,
        sharpe=sharpe,
        count=int(len(spread)),
    )


def evaluate_factor(
    factor: pd.DataFrame,
    close: pd.DataFrame,
    *,
    horizons: list[int] | tuple[int, ...] = (1, 5, 20),
    long_short_horizon: int = 5,
    groups: int = 5,
    min_periods: int = 30,
) -> FactorEvaluationResult:
    """执行标准单因子评测。"""

    all_horizons = tuple(dict.fromkeys([*horizons, long_short_horizon]))
    returns = forward_returns(close, all_horizons)
    ic_series_by_horizon: dict[int, pd.Series] = {}
    ic_stats: dict[int, ICStats] = {}
    for horizon in horizons:
        series = rank_ic_series(factor, returns[horizon], min_periods=min_periods)
        ic_series_by_horizon[horizon] = series
        ic_stats[horizon] = summarize_ic(series, horizon)

    ls_series = long_short_series(
        factor,
        returns[long_short_horizon],
        groups=groups,
        min_periods=min_periods,
    )
    return FactorEvaluationResult(
        ic_stats=ic_stats,
        long_short=summarize_long_short(ls_series, long_short_horizon),
        ic_series=ic_series_by_horizon,
        long_short_series=ls_series,
    )


def evaluate_factor_batch(
    factors: dict[str, pd.DataFrame],
    close: pd.DataFrame,
    *,
    horizons: list[int] | tuple[int, ...] = (1, 5, 20),
    long_short_horizon: int = 5,
    groups: int = 5,
    min_periods: int = 30,
    admission_rule: FactorAdmissionRule | None = None,
) -> list[FactorCandidate]:
    """批量评测因子并输出候选排序。

    排名分数采用主 horizon IC、正 IC 占比和多空收益的简单组合。它不是
    最终策略模型，只用于把大量 Research Spike 结果压缩成可审查候选池。
    """

    if not factors:
        raise ValueError("factors 不能为空")
    rule = admission_rule or FactorAdmissionRule()
    results: dict[str, FactorEvaluationResult] = {}
    for name, factor in factors.items():
        results[name] = evaluate_factor(
            factor,
            close,
            horizons=horizons,
            long_short_horizon=long_short_horizon,
            groups=groups,
            min_periods=min_periods,
        )

    correlations = _factor_correlations(factors)
    preliminary: list[tuple[str, FactorEvaluationResult, ICStats, float]] = []
    for name, result in results.items():
        primary = result.ic_stats[horizons[0]]
        score = (
            primary.ic_mean
            + 0.1 * primary.positive_rate
            + result.long_short.long_short_mean
        )
        preliminary.append((name, result, primary, float(score)))

    candidates: list[FactorCandidate] = []
    admitted_names: list[str] = []
    for name, result, primary, score in sorted(preliminary, key=lambda item: item[3], reverse=True):
        max_corr = _max_abs_selected_correlation(correlations, name, admitted_names)
        reasons = _admission_reasons(result, primary, max_corr, rule)
        admitted = not reasons
        if admitted:
            admitted_names.append(name)
        candidates.append(
            FactorCandidate(
                name=name,
                result=result,
                score=score,
                admitted=admitted,
                rejection_reasons=tuple(reasons),
                max_abs_correlation=max_corr,
            )
        )
    return candidates


def _validate_wide_frame(df: pd.DataFrame, name: str) -> None:
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"{name} 必须是 pandas DataFrame")
    if df.empty:
        raise ValueError(f"{name} 不能为空")
    if not df.index.is_monotonic_increasing:
        raise ValueError(f"{name} index 必须按日期升序排列")


def _factor_correlations(factors: dict[str, pd.DataFrame]) -> pd.DataFrame:
    flattened = {
        name: frame.stack().dropna().rename(name)
        for name, frame in factors.items()
    }
    panel = pd.concat(flattened.values(), axis=1, join="inner")
    if panel.empty or len(panel.columns) <= 1:
        return pd.DataFrame(index=factors.keys(), columns=factors.keys(), dtype=float)
    return panel.corr(method="spearman").fillna(0.0)


def _max_abs_selected_correlation(correlations: pd.DataFrame, name: str, selected: list[str]) -> float:
    if correlations.empty or name not in correlations.index or not selected:
        return 0.0
    peers = correlations.loc[name, [item for item in selected if item in correlations.columns]].abs()
    return float(peers.max()) if not peers.empty else 0.0


def _admission_reasons(
    result: FactorEvaluationResult,
    primary: ICStats,
    max_corr: float,
    rule: FactorAdmissionRule,
) -> list[str]:
    reasons: list[str] = []
    if primary.ic_mean < rule.min_ic_mean:
        reasons.append("ic_mean_below_rule")
    if primary.positive_rate < rule.min_positive_rate:
        reasons.append("positive_rate_below_rule")
    if result.long_short.long_short_mean < rule.min_long_short_mean:
        reasons.append("long_short_below_rule")
    if max_corr > rule.max_correlation:
        reasons.append("correlation_too_high")
    return reasons
