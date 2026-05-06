from __future__ import annotations

import json

from vortex.research.cogalpha import (
    CogAlphaFitnessRule,
    CogAlphaResearchCycleConfig,
    QualityGateRule,
    build_demo_daily_inputs,
    executable_recipes,
    price_volume_101_defensive_direction,
    run_cogalpha_research_cycle,
    select_recipes_for_direction,
)


def _relaxed_config(tmp_path) -> CogAlphaResearchCycleConfig:
    return CogAlphaResearchCycleConfig(
        output_dir=tmp_path,
        quality_rule=QualityGateRule(min_valid_dates=40, min_valid_symbols=30),
        fitness_rule=CogAlphaFitnessRule(
            min_abs_rank_ic_mean=0.0,
            min_rank_icir=0.0,
            min_positive_rate=0.0,
            min_long_short_mean=-1.0,
            min_coverage=0.10,
            min_distinct_ratio=0.01,
            min_group_monotonicity=0.0,
            max_abs_correlation=1.0,
        ),
        min_periods=20,
        input_type="deterministic_synthetic_ohlcv",
        input_note="Test fixture for research-cycle artifact schema.",
    )


def test_default_price_volume_direction_selects_cogalpha_recipes():
    direction = price_volume_101_defensive_direction()
    selected = select_recipes_for_direction(direction, executable_recipes())

    template_ids = {recipe.template_id for recipe in selected}

    assert direction.direction_id == "cogalpha_101_price_volume_defensive_evolution"
    assert "creative_soft_rank_range_liquidity" in template_ids
    assert "herding_amount_crowding_reversal_20d" in template_ids
    assert "liquidity_range_impact" in template_ids
    assert len(selected) >= 10


def test_research_cycle_writes_generation_and_cycle_artifacts(tmp_path):
    inputs = build_demo_daily_inputs(days=180, symbols=50)

    result = run_cogalpha_research_cycle(
        inputs,
        config=_relaxed_config(tmp_path),
    )

    summary = json.loads((tmp_path / "generation_summary.json").read_text(encoding="utf-8"))
    cycle = json.loads((tmp_path / "research_cycle.json").read_text(encoding="utf-8"))
    report = json.loads((tmp_path / "generation_report.json").read_text(encoding="utf-8"))

    assert result["summary_path"] == str(tmp_path / "generation_summary.json")
    assert summary["schema"] == "vortex.cogalpha_generation_summary.v1"
    assert summary["research_direction"]["direction_id"] == "cogalpha_101_price_volume_defensive_evolution"
    assert cycle["schema"] == "vortex.cogalpha_research_cycle.v1"
    assert cycle["input"]["input_type"] == "deterministic_synthetic_ohlcv"
    assert cycle["artifacts"]["generation_report"] == str(tmp_path / "generation_report.json")
    assert cycle["summary"]["recipe_count"] == len(cycle["selected_recipes"])
    assert cycle["parent_pool"]
    assert cycle["next_generation_queue"]
    assert report["schema"] == "vortex.cogalpha_generation_report.v1"


def test_research_cycle_next_generation_queue_keeps_parent_lineage(tmp_path):
    inputs = build_demo_daily_inputs(days=180, symbols=50)

    result = run_cogalpha_research_cycle(
        inputs,
        config=_relaxed_config(tmp_path),
    )

    cycle = result["cycle"]
    parent_templates = {
        parent["template_id"]
        for parent in cycle["parent_pool"]
        if parent["template_id"]
    }
    queue_templates = {
        item["parent_template"]
        for item in cycle["next_generation_queue"]
        if item["action"] == "mutate"
    }

    assert queue_templates
    assert queue_templates.issubset(parent_templates)
