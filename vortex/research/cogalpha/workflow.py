"""End-to-end CogAlpha research workflow helpers."""
from __future__ import annotations

from pathlib import Path

from vortex.research.alpha101_registry import DailyFactorInputs, compute_formula
from vortex.research.cogalpha.adapters import candidate_from_recipe, formula_spec_from_recipe
from vortex.research.cogalpha.fitness import CogAlphaFitnessRule, rank_cogalpha_candidates
from vortex.research.cogalpha.quality import QualityGateRule
from vortex.research.cogalpha.recipes import CogAlphaAgentRecipe, executable_recipes
from vortex.research.cogalpha.reports import write_generation_report_json
from vortex.research.cogalpha.schema import CogAlphaEvaluationResult


def available_fields_from_inputs(inputs: DailyFactorInputs) -> frozenset[str]:
    """Return visible field names for DailyFactorInputs."""

    return frozenset(("open", "high", "low", "close", "volume", "amount"))


def run_cogalpha_generation(
    inputs: DailyFactorInputs,
    *,
    recipes: tuple[CogAlphaAgentRecipe, ...] | None = None,
    available_fields: set[str] | frozenset[str] | None = None,
    quality_rule: QualityGateRule | None = None,
    fitness_rule: CogAlphaFitnessRule | None = None,
    report_path: str | Path | None = None,
    min_periods: int = 30,
    groups: int = 5,
) -> list[CogAlphaEvaluationResult]:
    """Run executable CogAlpha recipes through the existing research kernel."""

    selected_recipes = recipes or executable_recipes()
    if not selected_recipes:
        raise ValueError("recipes must be non-empty")

    candidate_factors = []
    recipe_payloads: list[dict[str, object]] = []
    for recipe in selected_recipes:
        candidate = candidate_from_recipe(recipe)
        spec = formula_spec_from_recipe(recipe)
        factor = compute_formula(spec, inputs)
        candidate_factors.append((candidate, factor))
        recipe_payloads.append(
            {
                "template_id": recipe.template_id,
                "agent": recipe.agent,
                "formula_id": spec.formula_id,
                "name": spec.name,
                "semantic_status": recipe.semantic_status,
                "semantic_notes": recipe.semantic_notes,
                "parent_templates": list(recipe.parent_templates),
            }
        )

    results = rank_cogalpha_candidates(
        candidate_factors,
        inputs.close,
        available_fields=available_fields or available_fields_from_inputs(inputs),
        quality_rule=quality_rule,
        fitness_rule=fitness_rule,
        min_periods=min_periods,
        groups=groups,
    )
    if report_path is not None:
        write_generation_report_json(
            results,
            report_path,
            metadata={
                "recipes": recipe_payloads,
                "summary": summarize_generation_results(results),
            },
        )
    return results


def summarize_generation_results(
    results: list[CogAlphaEvaluationResult],
    *,
    top_n: int = 10,
) -> dict[str, object]:
    """Summarize one CogAlpha generation for review or later LLM feedback."""

    if not results:
        raise ValueError("results must be non-empty")
    decision_counts: dict[str, int] = {}
    semantic_status_counts: dict[str, int] = {}
    agent_results: list[dict[str, object]] = []
    for result in results:
        metadata = dict(result.candidate.metadata)
        semantic_status = str(metadata.get("semantic_status", "unknown"))
        decision_counts[result.decision] = decision_counts.get(result.decision, 0) + 1
        semantic_status_counts[semantic_status] = semantic_status_counts.get(semantic_status, 0) + 1
        agent_results.append(
            {
                "agent": result.candidate.agent,
                "alpha_id": result.candidate.alpha_id,
                "name": result.candidate.name,
                "template_id": metadata.get("template_id"),
                "semantic_status": semantic_status,
                "semantic_notes": metadata.get("semantic_notes", ""),
                "parent_templates": list(metadata.get("parent_templates", [])),
                "decision": result.decision,
                "score": result.score,
                "rejection_reasons": list(result.rejection_reasons),
                "fitness": result.fitness.to_dict() if result.fitness else None,
            }
        )

    ranked = sorted(agent_results, key=lambda item: float(item["score"]), reverse=True)
    by_decision = {
        decision: [
            item["alpha_id"]
            for item in ranked
            if item["decision"] == decision
        ]
        for decision in ("elite", "qualified", "rejected", "invalid", "generated")
    }
    return {
        "recipe_count": len(results),
        "decision_counts": decision_counts,
        "semantic_status_counts": semantic_status_counts,
        "by_decision": by_decision,
        "top_candidates": ranked[:top_n],
        "agent_results": sorted(agent_results, key=lambda item: str(item["agent"])),
    }
