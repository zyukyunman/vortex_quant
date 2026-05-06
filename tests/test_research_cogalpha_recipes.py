from __future__ import annotations

import pandas as pd

from vortex.research.alpha101_registry import DailyFactorInputs, compute_formula, specs_by_id
from vortex.research.cogalpha import (
    all_agent_recipes,
    candidate_from_formula_spec,
    candidate_from_recipe,
    cogalpha_candidates_from_registered_specs,
    executable_recipe_by_template,
    executable_recipes,
    formula_spec_from_recipe,
    planned_recipes,
)


def _inputs(days: int = 220, symbols: int = 60) -> DailyFactorInputs:
    dates = pd.date_range("2020-01-01", periods=days, freq="B")
    columns = [f"S{i:03d}" for i in range(symbols)]
    day_grid = pd.Series(range(days), index=dates, dtype=float)
    base = pd.DataFrame(index=dates, columns=columns, dtype=float)
    for idx, column in enumerate(columns):
        base[column] = 100.0 + idx * 0.5 + day_grid * (0.01 + idx / 10000)
    return DailyFactorInputs(
        open=base * 0.995,
        high=base * 1.01,
        low=base * 0.99,
        close=base,
        volume=base * 100.0,
        amount=base * base * 100.0,
    )


def test_all_twenty_one_agents_have_executable_recipes():
    executable = executable_recipes()
    planned = planned_recipes()
    all_recipes = all_agent_recipes()

    assert len(executable) == 21
    assert len(all_recipes) == 21
    assert planned == ()
    assert {recipe.status for recipe in executable} == {"executable"}
    assert len({recipe.agent for recipe in executable}) == 21
    assert len({recipe.template_id for recipe in executable}) == 21
    assert executable_recipe_by_template("daily_trend_20d").agent == "AgentDailyTrend"


def test_recipe_builds_candidate_and_formula_spec_with_matching_metadata():
    recipe = executable_recipe_by_template("daily_trend_20d")

    candidate = candidate_from_recipe(recipe)
    spec = formula_spec_from_recipe(recipe)

    assert candidate.agent == "AgentDailyTrend"
    assert candidate.required_fields == spec.required_fields
    assert candidate.horizons == spec.default_horizons
    assert candidate.metadata["template_id"] == "daily_trend_20d"
    assert candidate.metadata["semantic_status"] == "proxy"
    assert candidate.metadata["semantic_notes"] == ""
    assert candidate.metadata["parent_templates"] == []
    assert spec.formula_id == "cogalpha_daily_trend_20d"


def test_review_flagged_agents_have_stronger_semantic_metadata():
    recipes = {recipe.agent: recipe for recipe in executable_recipes()}

    assert recipes["AgentMarketCycle"].semantic_status == "faithful_proxy"
    assert "regime" in recipes["AgentMarketCycle"].semantic_notes
    assert recipes["AgentCrashPredictor"].semantic_status == "faithful_proxy"
    assert "脆弱性" in recipes["AgentCrashPredictor"].semantic_notes
    assert recipes["AgentFractal"].semantic_status == "faithful_proxy"
    assert "roughness" in recipes["AgentFractal"].semantic_notes
    assert recipes["AgentHerding"].semantic_status == "faithful_proxy"
    assert "羊群" in recipes["AgentHerding"].semantic_notes
    assert recipes["AgentCreative"].semantic_status == "mutation_proxy"
    assert recipes["AgentCreative"].parent_templates == (
        "daily_trend_20d",
        "short_reversal_5d",
        "liquidity_range_impact",
    )


def test_every_recipe_builds_formula_and_factor():
    inputs = _inputs()

    for recipe in executable_recipes():
        candidate = candidate_from_recipe(recipe)
        spec = formula_spec_from_recipe(recipe)
        factor = compute_formula(spec, inputs)

        assert candidate.agent == recipe.agent
        assert not factor.empty
        assert list(factor.columns) == list(inputs.close.columns)
        assert factor.index.is_monotonic_increasing
        assert factor.notna().any().any(), recipe.template_id


def test_existing_formula_spec_can_be_wrapped_as_cogalpha_candidate():
    spec = specs_by_id()["vtx_alpha_001"]

    candidate = candidate_from_formula_spec(spec)

    assert candidate.agent == "AgentReversal"
    assert candidate.expression == "formula_spec:vtx_alpha_001"
    assert candidate.expression_type == "field"
    assert candidate.metadata["formula_id"] == "vtx_alpha_001"


def test_registered_specs_can_enter_cogalpha_candidate_layer():
    candidates = cogalpha_candidates_from_registered_specs()

    assert len(candidates) == len(specs_by_id())
    assert {candidate.metadata["formula_id"] for candidate in candidates} == set(specs_by_id())
