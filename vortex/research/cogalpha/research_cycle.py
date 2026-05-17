"""CogAlpha research-cycle automation helpers."""
from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path

from vortex.research.alpha101_registry import DailyFactorInputs
from vortex.research.cogalpha.fitness import CogAlphaFitnessRule
from vortex.research.cogalpha.quality import QualityGateRule
from vortex.research.cogalpha.recipes import CogAlphaAgentRecipe, executable_recipes
from vortex.research.cogalpha.schema import CogAlphaEvaluationResult
from vortex.research.cogalpha.workflow import run_cogalpha_generation, summarize_generation_results


@dataclass(frozen=True)
class CogAlphaResearchDirection:
    """A concrete factor-mining direction that can select CogAlpha recipes."""

    direction_id: str
    name: str
    hypothesis: str
    target_horizons: tuple[int, ...]
    agents: tuple[str, ...]
    recipe_templates: tuple[str, ...]
    archive_tags: tuple[str, ...] = ()
    known_risks: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "direction_id": self.direction_id,
            "name": self.name,
            "hypothesis": self.hypothesis,
            "target_horizons": list(self.target_horizons),
            "agents": list(self.agents),
            "recipe_templates": list(self.recipe_templates),
            "archive_tags": list(self.archive_tags),
            "known_risks": list(self.known_risks),
        }


@dataclass(frozen=True)
class CogAlphaResearchCycleConfig:
    """Runtime options for one CogAlpha research cycle."""

    output_dir: str | Path = "workspace/cogalpha/price_volume_101_defensive/latest"
    quality_rule: QualityGateRule | None = None
    fitness_rule: CogAlphaFitnessRule | None = None
    min_periods: int = 30
    groups: int = 5
    top_n: int = 10
    input_type: str = "daily_factor_inputs"
    input_note: str = "Caller supplied PIT-safe DailyFactorInputs."

    def resolved_quality_rule(self) -> QualityGateRule:
        return self.quality_rule or QualityGateRule()

    def resolved_fitness_rule(self) -> CogAlphaFitnessRule:
        return self.fitness_rule or CogAlphaFitnessRule()

    def to_dict(self) -> dict[str, object]:
        quality = self.resolved_quality_rule()
        fitness = self.resolved_fitness_rule()
        return {
            "output_dir": str(self.output_dir),
            "quality_rule": {
                "min_coverage": quality.min_coverage,
                "min_distinct_ratio": quality.min_distinct_ratio,
                "min_valid_dates": quality.min_valid_dates,
                "min_valid_symbols": quality.min_valid_symbols,
                "allowed_expression_types": list(quality.allowed_expression_types),
            },
            "fitness_rule": {
                "min_abs_rank_ic_mean": fitness.min_abs_rank_ic_mean,
                "min_rank_icir": fitness.min_rank_icir,
                "min_positive_rate": fitness.min_positive_rate,
                "min_long_short_mean": fitness.min_long_short_mean,
                "min_coverage": fitness.min_coverage,
                "min_distinct_ratio": fitness.min_distinct_ratio,
                "min_group_monotonicity": fitness.min_group_monotonicity,
                "max_abs_correlation": fitness.max_abs_correlation,
                "elite_quantile": fitness.elite_quantile,
            },
            "min_periods": self.min_periods,
            "groups": self.groups,
            "top_n": self.top_n,
            "input_type": self.input_type,
            "input_note": self.input_note,
        }


def price_volume_101_defensive_direction() -> CogAlphaResearchDirection:
    """Default direction for the current 101/price-volume defensive queue."""

    return CogAlphaResearchDirection(
        direction_id="cogalpha_101_price_volume_defensive_evolution",
        name="101/量价低波拥挤反转自动化研究循环",
        hypothesis=(
            "A 股日频 101/量价体系中，低波、反转、成交拥挤、静默流动性、"
            "路径质量和风险门控更适合作为排序、过滤和组合腿，而不是独立 long-only 策略。"
        ),
        target_horizons=(5, 20, 60, 120),
        agents=(
            "AgentMarketCycle",
            "AgentCrashPredictor",
            "AgentLiquidity",
            "AgentPriceVolumeCoherence",
            "AgentVolumeStructure",
            "AgentDailyTrend",
            "AgentReversal",
            "AgentRangeVol",
            "AgentDrawdown",
            "AgentFractal",
            "AgentRegimeGating",
            "AgentComposite",
            "AgentCreative",
            "AgentHerding",
        ),
        recipe_templates=(
            "market_cycle_relative_trend_60d",
            "crash_fragility_high_range_low_liquidity_20d",
            "liquidity_range_impact",
            "price_volume_coherence_20d",
            "volume_structure_surge_decay_20d",
            "daily_trend_20d",
            "short_reversal_5d",
            "range_vol_20d",
            "drawdown_recovery_position_60d",
            "fractal_multiscale_consistency_20_60d",
            "regime_gated_trend_lowvol_60d",
            "composite_trend_reversal_liquidity",
            "creative_soft_rank_range_liquidity",
            "herding_amount_crowding_reversal_20d",
        ),
        archive_tags=("price_volume", "alpha101", "low_volatility", "crowding_reversal", "cogalpha"),
        known_risks=(
            "强 IC 可能主要来自小市值、低波、低换手等风格暴露",
            "直接 TopN long-only 历史回撤较大，必须先定位为排序/过滤/组合腿",
            "synthetic proof artifact 不能当作 A 股结论",
        ),
    )


def select_recipes_for_direction(
    direction: CogAlphaResearchDirection,
    recipes: tuple[CogAlphaAgentRecipe, ...] | None = None,
) -> tuple[CogAlphaAgentRecipe, ...]:
    """Select executable recipes that match the direction's agent/template scope."""

    available = recipes or executable_recipes()
    template_set = set(direction.recipe_templates)
    agent_set = set(direction.agents)
    selected = tuple(
        recipe
        for recipe in available
        if recipe.template_id in template_set or recipe.agent in agent_set
    )
    if not selected:
        raise ValueError(f"no executable recipes selected for direction: {direction.direction_id}")
    return selected


def run_cogalpha_research_cycle(
    inputs: DailyFactorInputs,
    *,
    direction: CogAlphaResearchDirection | None = None,
    config: CogAlphaResearchCycleConfig | None = None,
    recipes: tuple[CogAlphaAgentRecipe, ...] | None = None,
) -> dict[str, object]:
    """Run a direction-aware CogAlpha research cycle and write JSON artifacts."""

    resolved_direction = direction or price_volume_101_defensive_direction()
    resolved_config = config or CogAlphaResearchCycleConfig()
    selected_recipes = select_recipes_for_direction(resolved_direction, recipes)

    output_dir = Path(resolved_config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    generation_report_path = output_dir / "generation_report.json"
    generation_summary_path = output_dir / "generation_summary.json"
    research_cycle_path = output_dir / "research_cycle.json"

    results = run_cogalpha_generation(
        inputs,
        recipes=selected_recipes,
        quality_rule=resolved_config.resolved_quality_rule(),
        fitness_rule=resolved_config.resolved_fitness_rule(),
        report_path=generation_report_path,
        min_periods=resolved_config.min_periods,
        groups=resolved_config.groups,
    )
    summary = summarize_generation_results(results, top_n=resolved_config.top_n)
    summary_payload = {
        "schema": "vortex.cogalpha_generation_summary.v1",
        "research_direction": resolved_direction.to_dict(),
        **summary,
    }
    generation_summary_path.write_text(
        json.dumps(summary_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    cycle_payload = {
        "schema": "vortex.cogalpha_research_cycle.v1",
        "research_direction": resolved_direction.to_dict(),
        "config": resolved_config.to_dict(),
        "input": {
            "input_type": resolved_config.input_type,
            "input_note": resolved_config.input_note,
            "dates": len(inputs.close.index),
            "symbols": len(inputs.close.columns),
        },
        "artifacts": {
            "generation_report": str(generation_report_path),
            "generation_summary": str(generation_summary_path),
            "research_cycle": str(research_cycle_path),
        },
        "selected_recipes": [recipe.to_dict() for recipe in selected_recipes],
        "summary": summary,
        "parent_pool": _parent_pool(results),
        "rejected_pool": _rejected_pool(results),
        "next_generation_queue": _next_generation_queue(results),
    }
    research_cycle_path.write_text(
        json.dumps(cycle_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return {
        "results": results,
        "summary": summary_payload,
        "cycle": cycle_payload,
        "report_path": str(generation_report_path),
        "summary_path": str(generation_summary_path),
        "cycle_path": str(research_cycle_path),
    }


def _parent_pool(results: list[CogAlphaEvaluationResult]) -> list[dict[str, object]]:
    ranked = sorted(
        (result for result in results if result.decision in {"elite", "qualified"}),
        key=lambda result: result.score,
        reverse=True,
    )
    return [_result_seed(result) for result in ranked]


def _rejected_pool(results: list[CogAlphaEvaluationResult]) -> list[dict[str, object]]:
    ranked = sorted(
        (result for result in results if result.decision in {"rejected", "invalid"}),
        key=lambda result: result.score,
        reverse=True,
    )
    return [
        {
            **_result_seed(result),
            "rejection_reasons": list(result.rejection_reasons),
            "quality_status": result.quality.status,
        }
        for result in ranked
    ]


def _next_generation_queue(results: list[CogAlphaEvaluationResult]) -> list[dict[str, object]]:
    parents = _parent_pool(results)
    queue: list[dict[str, object]] = []
    for parent in parents[:5]:
        queue.append(
            {
                "action": "mutate",
                "mutation_type": "window_or_gate_variant",
                "parent_alpha_id": parent["alpha_id"],
                "parent_template": parent["template_id"],
                "reason": "保留同一经济假设，优先尝试 horizon/window、低波/流动性门控或截面归一化变化。",
            }
        )
    if len(parents) >= 2:
        queue.append(
            {
                "action": "crossover",
                "crossover_type": "orthogonal_defensive_blend",
                "parent_alpha_ids": [parents[0]["alpha_id"], parents[1]["alpha_id"]],
                "parent_templates": [parents[0]["template_id"], parents[1]["template_id"]],
                "reason": "将排名信号与防守/流动性门控组合，优先验证增量 IC 和回撤改善。",
            }
        )
    if not queue:
        queue.append(
            {
                "action": "regenerate",
                "mutation_type": "agent_scope_refresh",
                "reason": "本轮没有 qualified/elite parent，下一轮应调整 agent scope 或降低候选表达复杂度。",
            }
        )
    return queue


def _result_seed(result: CogAlphaEvaluationResult) -> dict[str, object]:
    metadata = dict(result.candidate.metadata)
    return {
        "agent": result.candidate.agent,
        "alpha_id": result.candidate.alpha_id,
        "name": result.candidate.name,
        "template_id": metadata.get("template_id"),
        "semantic_status": metadata.get("semantic_status", "unknown"),
        "parent_templates": list(metadata.get("parent_templates", [])),
        "decision": result.decision,
        "score": result.score,
        "fitness": result.fitness.to_dict() if result.fitness else None,
    }
