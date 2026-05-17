"""Candidate-pool factor fusion tests."""
from __future__ import annotations

import pandas as pd

from vortex.strategy.factor_fusion import (
    CandidateFusionRecipe,
    FusionLeg,
    build_fused_candidate_signal,
)


def test_fused_signal_reranks_only_base_candidate_pool():
    base = pd.DataFrame(
        {
            "A": [10.0],
            "B": [9.0],
            "C": [8.0],
            "D": [7.0],
        },
        index=["20250102"],
    )
    factor = pd.DataFrame(
        {
            "A": [0.0],
            "B": [0.0],
            "C": [100.0],
            "D": [1000.0],
        },
        index=["20250102"],
    )

    fused = build_fused_candidate_signal(
        base,
        {"quality": factor},
            CandidateFusionRecipe(
                candidate_pool_size=3,
                base_weight=1.0,
            legs=(FusionLeg("quality", 1.5),),
        ),
    )

    row = fused.loc["20250102"].dropna().sort_values(ascending=False)
    assert "D" not in row.index
    assert row.index[0] == "C"
    assert set(row.index) == {"A", "B", "C"}


def test_fused_signal_supports_negative_risk_leg():
    base = pd.DataFrame(
        {"A": [10.0], "B": [9.0]},
        index=["20250102"],
    )
    risk = pd.DataFrame(
        {"A": [100.0], "B": [0.0]},
        index=["20250102"],
    )

    fused = build_fused_candidate_signal(
        base,
        {"risk": risk},
        CandidateFusionRecipe(
            candidate_pool_size=2,
            base_weight=1.0,
            legs=(FusionLeg("risk", -1.1),),
        ),
    )

    assert fused.loc["20250102", "B"] > fused.loc["20250102", "A"]
