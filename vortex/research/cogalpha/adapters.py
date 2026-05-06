"""Adapters between CogAlpha candidates and existing research formulas."""
from __future__ import annotations

from collections.abc import Iterable

from vortex.research.alpha101_registry import FormulaSpec, registered_specs
from vortex.research.cogalpha.recipes import CogAlphaAgentRecipe
from vortex.research.cogalpha.schema import AlphaCandidate, LineageRecord


_FAMILY_AGENT_HINTS: tuple[tuple[str, str], ...] = (
    ("reversal", "AgentReversal"),
    ("momentum", "AgentDailyTrend"),
    ("trend", "AgentDailyTrend"),
    ("low_risk", "AgentRangeVol"),
    ("lowvol", "AgentRangeVol"),
    ("range", "AgentRangeVol"),
    ("volume", "AgentPriceVolumeCoherence"),
    ("amount", "AgentLiquidity"),
    ("liquidity", "AgentLiquidity"),
    ("ohlc", "AgentBarShape"),
    ("bar", "AgentBarShape"),
)


def infer_agent_for_formula_spec(spec: FormulaSpec) -> str:
    """Infer a CogAlpha agent from FormulaSpec metadata."""

    text = " ".join([spec.family, spec.name, spec.role_hint, spec.description]).lower()
    for keyword, agent in _FAMILY_AGENT_HINTS:
        if keyword in text:
            return agent
    return "AgentComposite"


def candidate_from_formula_spec(
    spec: FormulaSpec,
    *,
    agent: str | None = None,
    alpha_id_prefix: str = "vtx_cogalpha_from_formula",
) -> AlphaCandidate:
    """Wrap an existing FormulaSpec as a CogAlpha candidate."""

    resolved_agent = agent or infer_agent_for_formula_spec(spec)
    return AlphaCandidate(
        alpha_id=f"{alpha_id_prefix}_{spec.formula_id}",
        name=spec.name,
        agent=resolved_agent,
        hypothesis=spec.description,
        expression=f"formula_spec:{spec.formula_id}",
        expression_type="field",
        required_fields=spec.required_fields,
        horizons=spec.default_horizons,
        direction="unknown",
        lineage=LineageRecord(generation=0, guidance_type="registered_formula"),
        metadata={
            "formula_id": spec.formula_id,
            "family": spec.family,
            "role_hint": spec.role_hint,
        },
    )


def formula_spec_from_recipe(recipe: CogAlphaAgentRecipe, *, formula_id: str | None = None) -> FormulaSpec:
    """Build a FormulaSpec from an executable CogAlpha recipe."""

    return recipe.build_formula_spec(formula_id=formula_id)


def candidate_from_recipe(recipe: CogAlphaAgentRecipe, *, alpha_id: str | None = None) -> AlphaCandidate:
    """Build an AlphaCandidate from an executable CogAlpha recipe."""

    return recipe.build_candidate(alpha_id=alpha_id)


def cogalpha_candidates_from_registered_specs(specs: Iterable[FormulaSpec] | None = None) -> tuple[AlphaCandidate, ...]:
    """Wrap registered Vortex formulas as CogAlpha candidates."""

    source = tuple(specs) if specs is not None else registered_specs()
    return tuple(candidate_from_formula_spec(spec) for spec in source)
