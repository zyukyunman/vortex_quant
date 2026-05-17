"""CogAlpha research primitives and executable recipe workflow."""
from vortex.research.cogalpha.agent_catalog import AgentSpec, agent_by_name, is_registered_agent, registered_agents
from vortex.research.cogalpha.adapters import (
    candidate_from_formula_spec,
    candidate_from_recipe,
    cogalpha_candidates_from_registered_specs,
    formula_spec_from_recipe,
    infer_agent_for_formula_spec,
)
from vortex.research.cogalpha.fitness import CogAlphaFitnessRule, evaluate_cogalpha_candidate, rank_cogalpha_candidates
from vortex.research.cogalpha.demo import build_demo_daily_inputs, run_cogalpha_demo
from vortex.research.cogalpha.quality import QualityGateRule, check_candidate_metadata, check_factor_output, run_quality_gate
from vortex.research.cogalpha.recipes import (
    CogAlphaAgentRecipe,
    all_agent_recipes,
    executable_recipe_by_template,
    executable_recipes,
    planned_recipes,
)
from vortex.research.cogalpha.research_cycle import (
    CogAlphaResearchCycleConfig,
    CogAlphaResearchDirection,
    price_volume_101_defensive_direction,
    run_cogalpha_research_cycle,
    select_recipes_for_direction,
)
from vortex.research.cogalpha.reports import write_generation_report_json
from vortex.research.cogalpha.schema import (
    AlphaCandidate,
    CogAlphaEvaluationResult,
    FitnessStats,
    LineageRecord,
    QualityCheckResult,
    QualityIssue,
)
from vortex.research.cogalpha.workflow import available_fields_from_inputs, run_cogalpha_generation, summarize_generation_results

__all__ = [
    "AgentSpec",
    "AlphaCandidate",
    "CogAlphaAgentRecipe",
    "CogAlphaEvaluationResult",
    "CogAlphaFitnessRule",
    "FitnessStats",
    "LineageRecord",
    "QualityCheckResult",
    "QualityGateRule",
    "QualityIssue",
    "CogAlphaResearchCycleConfig",
    "CogAlphaResearchDirection",
    "agent_by_name",
    "all_agent_recipes",
    "available_fields_from_inputs",
    "build_demo_daily_inputs",
    "candidate_from_formula_spec",
    "candidate_from_recipe",
    "check_candidate_metadata",
    "check_factor_output",
    "cogalpha_candidates_from_registered_specs",
    "evaluate_cogalpha_candidate",
    "executable_recipe_by_template",
    "executable_recipes",
    "formula_spec_from_recipe",
    "infer_agent_for_formula_spec",
    "is_registered_agent",
    "planned_recipes",
    "price_volume_101_defensive_direction",
    "rank_cogalpha_candidates",
    "registered_agents",
    "run_quality_gate",
    "run_cogalpha_generation",
    "run_cogalpha_research_cycle",
    "run_cogalpha_demo",
    "select_recipes_for_direction",
    "write_generation_report_json",
    "summarize_generation_results",
]
