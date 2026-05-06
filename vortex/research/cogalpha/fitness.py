"""CogAlpha fitness evaluation built on the existing research kernel."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace

import numpy as np
import pandas as pd

from vortex.research.cogalpha.quality import QualityGateRule, run_quality_gate
from vortex.research.cogalpha.schema import AlphaCandidate, CogAlphaEvaluationResult, FitnessStats
from vortex.research.evaluation import evaluate_factor, forward_returns, summarize_ic


@dataclass(frozen=True)
class CogAlphaFitnessRule:
    """Admission thresholds for CogAlpha candidates."""

    min_abs_rank_ic_mean: float = 0.02
    min_rank_icir: float = 0.30
    min_positive_rate: float = 0.55
    min_long_short_mean: float = 0.0
    min_coverage: float = 0.70
    min_distinct_ratio: float = 0.05
    min_group_monotonicity: float = 0.50
    max_abs_correlation: float = 0.85
    elite_quantile: float = 0.80


def pearson_ic_series(
    factor: pd.DataFrame,
    forward_return: pd.DataFrame,
    *,
    min_periods: int = 30,
) -> pd.Series:
    """Compute cross-sectional Pearson IC by date."""

    _validate_wide_frame(factor, "factor")
    _validate_wide_frame(forward_return, "forward_return")
    rows: list[dict[str, object]] = []
    for dt in factor.index.intersection(forward_return.index):
        valid = pd.concat(
            [factor.loc[dt].rename("factor"), forward_return.loc[dt].rename("ret")],
            axis=1,
        ).dropna()
        if len(valid) < min_periods:
            continue
        if valid["factor"].nunique(dropna=True) < 2 or valid["ret"].nunique(dropna=True) < 2:
            continue
        ic = valid["factor"].corr(valid["ret"])
        if pd.notna(ic):
            rows.append({"date": dt, "ic": float(ic)})
    if not rows:
        return pd.Series(dtype="float64", name="ic")
    return pd.DataFrame(rows).set_index("date")["ic"]


def group_return_means(
    factor: pd.DataFrame,
    forward_return: pd.DataFrame,
    *,
    groups: int = 5,
    min_periods: int = 30,
) -> dict[int, float]:
    """Return mean forward return for factor quantile groups."""

    if groups < 2:
        raise ValueError("groups must be at least 2")
    group_returns: dict[int, list[float]] = {group: [] for group in range(1, groups + 1)}
    for dt in factor.index.intersection(forward_return.index):
        valid = pd.concat(
            [factor.loc[dt].rename("factor"), forward_return.loc[dt].rename("ret")],
            axis=1,
        ).dropna()
        if len(valid) < max(min_periods, groups * 2):
            continue
        rank_pct = valid["factor"].rank(pct=True, method="first")
        for group in range(1, groups + 1):
            lower = (group - 1) / groups
            upper = group / groups
            if group == 1:
                mask = rank_pct <= upper
            else:
                mask = (rank_pct > lower) & (rank_pct <= upper)
            value = valid.loc[mask, "ret"].mean()
            if pd.notna(value):
                group_returns[group].append(float(value))
    return {
        group: float(np.mean(values)) if values else 0.0
        for group, values in group_returns.items()
    }


def group_monotonicity_score(group_means: Mapping[int, float]) -> float:
    """Score how often adjacent group returns are non-decreasing."""

    ordered = [group_means[group] for group in sorted(group_means)]
    if len(ordered) < 2:
        return 0.0
    comparisons = [
        right >= left
        for left, right in zip(ordered, ordered[1:])
    ]
    return float(sum(comparisons) / len(comparisons))


def evaluate_cogalpha_candidate(
    candidate: AlphaCandidate,
    factor: pd.DataFrame,
    close: pd.DataFrame,
    *,
    available_fields: set[str] | frozenset[str],
    quality_rule: QualityGateRule | None = None,
    fitness_rule: CogAlphaFitnessRule | None = None,
    groups: int = 5,
    min_periods: int = 30,
    max_abs_correlation: float = 0.0,
) -> CogAlphaEvaluationResult:
    """Run quality gates and fitness evaluation for one candidate."""

    resolved_quality_rule = quality_rule or QualityGateRule()
    resolved_fitness_rule = fitness_rule or CogAlphaFitnessRule()
    quality = run_quality_gate(
        candidate,
        available_fields=available_fields,
        factor=factor,
        rule=resolved_quality_rule,
    )
    if quality.has_blocking_issues:
        return CogAlphaEvaluationResult(
            candidate=candidate,
            quality=quality,
            decision="invalid",
            rejection_reasons=quality.blocking_codes,
        )

    primary_horizon = candidate.horizons[0]
    oriented_factor = _orient_factor(candidate, factor, close, primary_horizon, min_periods)
    evaluation = evaluate_factor(
        oriented_factor,
        close,
        horizons=candidate.horizons,
        long_short_horizon=primary_horizon,
        groups=groups,
        min_periods=min_periods,
    )
    returns = forward_returns(close, (primary_horizon,))[primary_horizon]
    pearson_series = pearson_ic_series(oriented_factor, returns, min_periods=min_periods)
    pearson_stats = summarize_ic(pearson_series, primary_horizon)
    rank_stats = evaluation.ic_stats[primary_horizon]
    group_means = group_return_means(oriented_factor, returns, groups=groups, min_periods=min_periods)
    quality_metrics = dict(quality.metrics)
    fitness = FitnessStats(
        primary_horizon=primary_horizon,
        pearson_ic_mean=pearson_stats.ic_mean,
        pearson_icir=_stable_icir(pearson_stats.ic_mean, pearson_stats.ic_std),
        pearson_positive_rate=pearson_stats.positive_rate,
        pearson_count=pearson_stats.count,
        rank_ic_mean=rank_stats.ic_mean,
        rank_icir=_stable_icir(rank_stats.ic_mean, rank_stats.ic_std),
        rank_positive_rate=rank_stats.positive_rate,
        rank_count=rank_stats.count,
        long_short_mean=evaluation.long_short.long_short_mean,
        long_short_sharpe=evaluation.long_short.sharpe,
        coverage=float(quality_metrics.get("coverage", 0.0)),
        distinct_ratio=float(quality_metrics.get("distinct_ratio", 0.0)),
        group_monotonicity=group_monotonicity_score(group_means),
        max_abs_correlation=max_abs_correlation,
    )
    rejection_reasons = _fitness_rejection_reasons(fitness, resolved_fitness_rule)
    decision = "qualified" if not rejection_reasons else "rejected"
    score = _fitness_score(fitness)
    return CogAlphaEvaluationResult(
        candidate=candidate,
        quality=quality,
        fitness=fitness,
        decision=decision,
        score=score,
        rejection_reasons=tuple(rejection_reasons),
    )


def rank_cogalpha_candidates(
    candidate_factors: Sequence[tuple[AlphaCandidate, pd.DataFrame]],
    close: pd.DataFrame,
    *,
    available_fields: set[str] | frozenset[str],
    quality_rule: QualityGateRule | None = None,
    fitness_rule: CogAlphaFitnessRule | None = None,
    groups: int = 5,
    min_periods: int = 30,
) -> list[CogAlphaEvaluationResult]:
    """Evaluate and rank one generation of CogAlpha candidates."""

    if not candidate_factors:
        raise ValueError("candidate_factors must be non-empty")
    resolved_rule = fitness_rule or CogAlphaFitnessRule()
    correlations = _factor_correlations({candidate.alpha_id: factor for candidate, factor in candidate_factors})
    raw_results = [
        evaluate_cogalpha_candidate(
            candidate,
            factor,
            close,
            available_fields=available_fields,
            quality_rule=quality_rule,
            fitness_rule=resolved_rule,
            groups=groups,
            min_periods=min_periods,
        )
        for candidate, factor in candidate_factors
    ]

    ranked: list[CogAlphaEvaluationResult] = []
    accepted: list[str] = []
    for result in sorted(raw_results, key=lambda item: item.score, reverse=True):
        if result.decision != "qualified" or result.fitness is None:
            ranked.append(result)
            continue
        max_corr = _max_abs_selected_correlation(correlations, result.candidate.alpha_id, accepted)
        fitness = replace(result.fitness, max_abs_correlation=max_corr)
        if max_corr > resolved_rule.max_abs_correlation:
            ranked.append(
                replace(
                    result,
                    decision="rejected",
                    fitness=fitness,
                    rejection_reasons=result.rejection_reasons + ("correlation_too_high",),
                )
            )
            continue
        accepted.append(result.candidate.alpha_id)
        ranked.append(replace(result, fitness=fitness))

    qualified_scores = [result.score for result in ranked if result.decision == "qualified"]
    if not qualified_scores:
        return ranked
    elite_cutoff = float(np.quantile(qualified_scores, resolved_rule.elite_quantile))
    return [
        replace(result, decision="elite")
        if result.decision == "qualified" and result.score >= elite_cutoff
        else result
        for result in ranked
    ]


def _orient_factor(
    candidate: AlphaCandidate,
    factor: pd.DataFrame,
    close: pd.DataFrame,
    primary_horizon: int,
    min_periods: int,
) -> pd.DataFrame:
    if candidate.direction == "positive":
        return factor
    if candidate.direction == "negative":
        return -factor
    return factor


def _fitness_rejection_reasons(
    fitness: FitnessStats,
    rule: CogAlphaFitnessRule,
) -> list[str]:
    reasons: list[str] = []
    if abs(fitness.rank_ic_mean) < rule.min_abs_rank_ic_mean:
        reasons.append("rank_ic_below_rule")
    if fitness.rank_icir < rule.min_rank_icir:
        reasons.append("rank_icir_below_rule")
    if fitness.rank_positive_rate < rule.min_positive_rate:
        reasons.append("positive_rate_below_rule")
    if fitness.long_short_mean < rule.min_long_short_mean:
        reasons.append("long_short_below_rule")
    if fitness.coverage < rule.min_coverage:
        reasons.append("coverage_below_rule")
    if fitness.distinct_ratio < rule.min_distinct_ratio:
        reasons.append("distinct_ratio_below_rule")
    if fitness.group_monotonicity < rule.min_group_monotonicity:
        reasons.append("group_monotonicity_below_rule")
    if fitness.max_abs_correlation > rule.max_abs_correlation:
        reasons.append("correlation_too_high")
    return reasons


def _fitness_score(fitness: FitnessStats) -> float:
    return float(
        abs(fitness.rank_ic_mean)
        + 0.1 * fitness.rank_positive_rate
        + fitness.long_short_mean
        + 0.05 * fitness.group_monotonicity
    )


def _stable_icir(mean: float, std: float) -> float:
    if std > 0:
        return float(mean / std)
    if mean > 0:
        return 999.0
    if mean < 0:
        return -999.0
    return 0.0


def _factor_correlations(factors: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    flattened = {
        name: frame.stack().dropna().rename(name)
        for name, frame in factors.items()
    }
    panel = pd.concat(flattened.values(), axis=1, join="inner")
    if panel.empty or len(panel.columns) <= 1:
        return pd.DataFrame(index=factors.keys(), columns=factors.keys(), dtype=float)
    panel = panel.loc[:, panel.nunique(dropna=True) >= 2]
    if panel.empty or len(panel.columns) <= 1:
        return pd.DataFrame(index=factors.keys(), columns=factors.keys(), dtype=float)
    return panel.corr(method="spearman").fillna(0.0)


def _max_abs_selected_correlation(correlations: pd.DataFrame, name: str, selected: list[str]) -> float:
    if correlations.empty or name not in correlations.index or not selected:
        return 0.0
    peers = correlations.loc[name, [item for item in selected if item in correlations.columns]].abs()
    return float(peers.max()) if not peers.empty else 0.0


def _validate_wide_frame(df: pd.DataFrame, name: str) -> None:
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"{name} must be a pandas DataFrame")
    if df.empty:
        raise ValueError(f"{name} must be non-empty")
    if not df.index.is_monotonic_increasing:
        raise ValueError(f"{name} index must be sorted ascending")
