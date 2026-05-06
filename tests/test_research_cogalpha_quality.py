from __future__ import annotations

import numpy as np
import pandas as pd

from vortex.research.cogalpha import AlphaCandidate, QualityGateRule, check_candidate_metadata, check_factor_output, run_quality_gate


def _candidate(expression: str = "cs_rank((high - low) / amount)") -> AlphaCandidate:
    return AlphaCandidate(
        alpha_id="vtx_cogalpha_0001",
        name="liquidity_impact_reversal_20d",
        agent="AgentLiquidity",
        hypothesis="Large range under thin liquidity may predict a premium.",
        expression=expression,
        required_fields=("high", "low", "amount"),
        lookback_windows=(20,),
        horizons=(1, 5, 20),
    )


def _factor(value: float = 1.0) -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=30, freq="B")
    columns = [f"S{i:03d}" for i in range(5)]
    data = np.arange(len(dates) * len(columns), dtype=float).reshape(len(dates), len(columns))
    return pd.DataFrame(data + value, index=dates, columns=columns)


def test_metadata_gate_rejects_future_and_dangerous_expression():
    candidate = _candidate("eval(close.shift(-1)) + future_return")

    result = check_candidate_metadata(candidate, available_fields={"high", "low", "amount", "close"})
    codes = set(result.blocking_codes)

    assert not result.passed
    assert {"negative_shift", "future_return", "eval_call"}.issubset(codes)


def test_metadata_gate_rejects_fields_used_but_not_declared():
    candidate = _candidate("cs_rank(close / amount)")

    result = check_candidate_metadata(candidate, available_fields={"high", "low", "amount", "close"})

    assert "field_not_declared" in result.blocking_codes


def test_factor_output_gate_rejects_inf_low_coverage_and_low_distinct():
    factor = pd.DataFrame(
        {
            "A": [1.0, np.inf, np.nan, 1.0],
            "B": [1.0, np.nan, np.nan, 1.0],
            "C": [1.0, np.nan, np.nan, 1.0],
        },
        index=pd.date_range("2020-01-01", periods=4, freq="B"),
    )

    result = check_factor_output(
        _candidate(),
        factor,
        rule=QualityGateRule(min_coverage=0.80, min_distinct_ratio=0.50, min_valid_dates=4, min_valid_symbols=2),
    )

    assert {"factor_has_inf", "coverage_too_low", "valid_dates_too_low", "distinct_ratio_too_low"}.issubset(
        set(result.blocking_codes)
    )
    assert result.metrics["coverage"] < 0.80


def test_quality_gate_passes_clean_candidate_and_factor():
    result = run_quality_gate(
        _candidate(),
        available_fields={"high", "low", "amount"},
        factor=_factor(),
        rule=QualityGateRule(min_valid_dates=20, min_valid_symbols=3),
    )

    assert result.passed
    assert result.metrics["coverage"] == 1.0
