from __future__ import annotations

import json

import pandas as pd

from vortex.research.cogalpha import (
    AlphaCandidate,
    CogAlphaFitnessRule,
    QualityGateRule,
    evaluate_cogalpha_candidate,
    rank_cogalpha_candidates,
    write_generation_report_json,
)


def _panel(days: int = 80, symbols: int = 40) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.date_range("2020-01-01", periods=days, freq="B")
    columns = [f"S{i:03d}" for i in range(symbols)]
    quality = pd.Series(range(symbols), index=columns, dtype=float)
    close = pd.DataFrame(100.0, index=dates, columns=columns)
    for idx in range(1, days):
        daily_ret = 0.0002 + quality / quality.max() * 0.002
        close.iloc[idx] = close.iloc[idx - 1] * (1.0 + daily_ret)
    factor = pd.DataFrame([quality.values] * days, index=dates, columns=columns)
    return factor, close


def _candidate(alpha_id: str = "vtx_cogalpha_0001", expression: str = "cs_rank(close)") -> AlphaCandidate:
    return AlphaCandidate(
        alpha_id=alpha_id,
        name=f"{alpha_id}_daily_trend",
        agent="AgentDailyTrend",
        hypothesis="Stable relative trend strength should rank future returns.",
        expression=expression,
        required_fields=("close",),
        horizons=(1, 5),
        direction="unknown",
    )


def test_evaluate_cogalpha_candidate_outputs_fitness_and_decision():
    factor, close = _panel()

    result = evaluate_cogalpha_candidate(
        _candidate(),
        factor,
        close,
        available_fields={"close"},
        quality_rule=QualityGateRule(min_valid_dates=20, min_valid_symbols=20),
        fitness_rule=CogAlphaFitnessRule(min_rank_icir=0.0),
        min_periods=20,
    )

    assert result.decision == "qualified"
    assert result.fitness is not None
    assert result.fitness.rank_ic_mean > 0.99
    assert result.fitness.pearson_ic_mean > 0.99
    assert result.fitness.coverage == 1.0
    assert result.fitness.group_monotonicity == 1.0


def test_unknown_direction_does_not_auto_flip_on_same_sample_forward_returns():
    factor, close = _panel()

    result = evaluate_cogalpha_candidate(
        _candidate(alpha_id="vtx_cogalpha_negative_unknown"),
        -factor,
        close,
        available_fields={"close"},
        quality_rule=QualityGateRule(min_valid_dates=20, min_valid_symbols=20),
        fitness_rule=CogAlphaFitnessRule(min_rank_icir=0.0),
        min_periods=20,
    )

    assert result.decision == "rejected"
    assert result.fitness is not None
    assert result.fitness.rank_ic_mean < -0.99
    assert "positive_rate_below_rule" in result.rejection_reasons
    assert "long_short_below_rule" in result.rejection_reasons


def test_evaluate_cogalpha_candidate_fails_closed_on_quality_errors():
    factor, close = _panel()

    result = evaluate_cogalpha_candidate(
        _candidate(expression="close.shift(-1)"),
        factor,
        close,
        available_fields={"close"},
        quality_rule=QualityGateRule(min_valid_dates=20, min_valid_symbols=20),
        min_periods=20,
    )

    assert result.decision == "invalid"
    assert "negative_shift" in result.rejection_reasons
    assert result.fitness is None


def test_rank_cogalpha_candidates_marks_elite_and_rejects_duplicate_correlation():
    factor, close = _panel()
    results = rank_cogalpha_candidates(
        [
            (_candidate("vtx_cogalpha_0001"), factor),
            (_candidate("vtx_cogalpha_0002"), factor.copy()),
        ],
        close,
        available_fields={"close"},
        quality_rule=QualityGateRule(min_valid_dates=20, min_valid_symbols=20),
        fitness_rule=CogAlphaFitnessRule(min_rank_icir=0.0, max_abs_correlation=0.50),
        min_periods=20,
    )

    assert results[0].decision == "elite"
    assert results[1].decision == "rejected"
    assert "correlation_too_high" in results[1].rejection_reasons


def test_generation_report_json_writer(tmp_path):
    factor, close = _panel()
    result = evaluate_cogalpha_candidate(
        _candidate(),
        factor,
        close,
        available_fields={"close"},
        quality_rule=QualityGateRule(min_valid_dates=20, min_valid_symbols=20),
        fitness_rule=CogAlphaFitnessRule(min_rank_icir=0.0),
        min_periods=20,
    )

    output = write_generation_report_json([result], tmp_path / "generation.json", metadata={"generation": 0})
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert payload["schema"] == "vortex.cogalpha_generation_report.v1"
    assert payload["metadata"] == {"generation": 0}
    assert payload["results"][0]["candidate"]["alpha_id"] == "vtx_cogalpha_0001"
