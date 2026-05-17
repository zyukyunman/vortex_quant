from __future__ import annotations

import json

import numpy as np
import pandas as pd

from vortex.research.alpha101_registry import DailyFactorInputs
from vortex.research.cogalpha import (
    CogAlphaFitnessRule,
    QualityGateRule,
    available_fields_from_inputs,
    executable_recipe_by_template,
    executable_recipes,
    run_cogalpha_generation,
    summarize_generation_results,
)


def _inputs(days: int = 220, symbols: int = 60) -> DailyFactorInputs:
    dates = pd.date_range("2020-01-01", periods=days, freq="B")
    columns = [f"S{i:03d}" for i in range(symbols)]
    quality = pd.Series(np.linspace(0.0, 1.0, symbols), index=columns)
    close = pd.DataFrame(100.0, index=dates, columns=columns)
    for idx in range(1, days):
        daily_ret = 0.0002 + quality * 0.002
        close.iloc[idx] = close.iloc[idx - 1] * (1.0 + daily_ret)
    open_ = close * 0.995
    high = close * (1.005 + quality * 0.002)
    low = close * (0.995 - quality * 0.001)
    volume = pd.DataFrame(
        np.tile(np.linspace(1000.0, 5000.0, symbols), (days, 1)),
        index=dates,
        columns=columns,
    )
    amount = volume * close
    return DailyFactorInputs(open=open_, high=high, low=low, close=close, volume=volume, amount=amount)


def test_available_fields_from_daily_inputs():
    assert available_fields_from_inputs(_inputs()) == frozenset(("open", "high", "low", "close", "volume", "amount"))


def test_run_cogalpha_generation_executes_recipe_to_fitness_result(tmp_path):
    inputs = _inputs()
    report_path = tmp_path / "cogalpha_generation.json"

    results = run_cogalpha_generation(
        inputs,
        recipes=(executable_recipe_by_template("daily_trend_20d"),),
        quality_rule=QualityGateRule(min_valid_dates=40, min_valid_symbols=30),
        fitness_rule=CogAlphaFitnessRule(min_rank_icir=0.0),
        report_path=report_path,
        min_periods=20,
    )

    assert len(results) == 1
    assert results[0].decision in {"qualified", "elite"}
    assert results[0].candidate.agent == "AgentDailyTrend"
    assert results[0].fitness is not None
    assert results[0].fitness.rank_ic_mean > 0.99
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["schema"] == "vortex.cogalpha_generation_report.v1"
    assert payload["metadata"]["recipes"][0]["formula_id"] == "cogalpha_daily_trend_20d"


def test_run_cogalpha_generation_keeps_bad_recipe_as_invalid_not_executable_claim():
    inputs = _inputs()

    results = run_cogalpha_generation(
        inputs,
        recipes=executable_recipes(),
        quality_rule=QualityGateRule(min_valid_dates=40, min_valid_symbols=30),
        fitness_rule=CogAlphaFitnessRule(min_rank_icir=0.0),
        min_periods=20,
    )

    assert len(results) == 21
    assert any(result.decision in {"qualified", "elite"} for result in results)
    assert any(result.decision in {"invalid", "rejected"} for result in results)


def test_generation_summary_explains_agent_level_results():
    inputs = _inputs()

    results = run_cogalpha_generation(
        inputs,
        recipes=executable_recipes(),
        quality_rule=QualityGateRule(min_valid_dates=40, min_valid_symbols=30),
        fitness_rule=CogAlphaFitnessRule(min_rank_icir=0.0),
        min_periods=20,
    )
    summary = summarize_generation_results(results)

    assert summary["recipe_count"] == 21
    assert sum(summary["decision_counts"].values()) == 21
    assert sum(summary["semantic_status_counts"].values()) == 21
    assert summary["semantic_status_counts"]["faithful_proxy"] >= 4
    assert summary["semantic_status_counts"]["mutation_proxy"] == 1
    assert len(summary["agent_results"]) == 21
    assert summary["top_candidates"]
    creative = next(item for item in summary["agent_results"] if item["agent"] == "AgentCreative")
    assert creative["semantic_status"] == "mutation_proxy"
    assert creative["parent_templates"] == [
        "daily_trend_20d",
        "short_reversal_5d",
        "liquidity_range_impact",
    ]
