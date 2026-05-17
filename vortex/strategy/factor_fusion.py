"""Candidate-pool factor fusion utilities for strategy research."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

import pandas as pd


@dataclass(frozen=True)
class FusionLeg:
    """One factor leg used to rerank a candidate pool."""

    name: str
    weight: float
    ascending: bool = False
    neutral_score: float = 0.5


@dataclass(frozen=True)
class CandidateFusionRecipe:
    """Configuration for candidate-pool fusion."""

    candidate_pool_size: int = 160
    base_weight: float = 1.0
    legs: tuple[FusionLeg, ...] = ()


def build_fused_candidate_signal(
    base_signal: pd.DataFrame,
    factors: Mapping[str, pd.DataFrame],
    recipe: CandidateFusionRecipe,
) -> pd.DataFrame:
    """Build a fused signal by reranking the top-N base candidates each day.

    The output keeps scores only for each day's candidate pool. Downstream
    strategy code can then select Top80 from the fused Top160 instead of
    applying factor overlays after Top80 is already selected.
    """

    if recipe.candidate_pool_size <= 0:
        raise ValueError("candidate_pool_size must be positive")
    if recipe.base_weight <= 0:
        raise ValueError("base_weight must be positive")
    for leg in recipe.legs:
        if leg.name not in factors:
            raise KeyError(f"missing fusion factor: {leg.name}")

    aligned_factors = {
        name: frame.reindex(index=base_signal.index, columns=base_signal.columns)
        for name, frame in factors.items()
    }
    fused = pd.DataFrame(index=base_signal.index, columns=base_signal.columns, dtype=float)
    for date, row in base_signal.iterrows():
        clean = pd.to_numeric(row, errors="coerce").dropna().sort_values(ascending=False)
        if clean.empty:
            continue
        candidates = clean.head(recipe.candidate_pool_size).index
        score = recipe.base_weight * _rank_percentile(clean.reindex(candidates), ascending=False)
        for leg in recipe.legs:
            factor_row = aligned_factors[leg.name].loc[date].reindex(candidates)
            leg_rank = _rank_percentile(
                factor_row,
                ascending=leg.ascending,
                neutral_score=leg.neutral_score,
            )
            score = score.add(float(leg.weight) * leg_rank, fill_value=0.0)
        fused.loc[date, candidates] = score
    return fused


def _rank_percentile(
    values: pd.Series,
    *,
    ascending: bool,
    neutral_score: float = 0.5,
) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    if numeric.notna().sum() == 0:
        return pd.Series(float(neutral_score), index=values.index)
    ranked = numeric.rank(pct=True, ascending=not ascending)
    return ranked.reindex(values.index).fillna(float(neutral_score)).astype(float)
