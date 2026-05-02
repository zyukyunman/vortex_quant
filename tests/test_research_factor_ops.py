from __future__ import annotations

import numpy as np
import pandas as pd

from vortex.research.factor_ops import (
    correlation,
    cs_rank,
    cs_zscore,
    decay_linear,
    delta,
    neutralize_by_group,
    scale,
    signed_power,
    ts_rank,
)


def test_cs_rank_and_zscore_by_date():
    frame = pd.DataFrame(
        {"A": [1.0, 3.0], "B": [2.0, 1.0], "C": [3.0, 2.0]},
        index=["20260101", "20260102"],
    )

    ranked = cs_rank(frame)
    zscored = cs_zscore(frame)

    assert ranked.loc["20260101"].to_dict() == {"A": 1 / 3, "B": 2 / 3, "C": 1.0}
    assert np.isclose(float(zscored.loc["20260101"].mean()), 0.0)
    assert np.isclose(float(zscored.loc["20260101"].std()), 1.0)


def test_delta_and_ts_rank_are_pit_safe():
    frame = pd.DataFrame({"A": [1.0, 3.0, 2.0, 4.0], "B": [4.0, 3.0, 2.0, 1.0]})

    diff = delta(frame, 2)
    ranked = ts_rank(frame, 3)

    assert np.isnan(diff["A"].iloc[1])
    assert diff["A"].iloc[2] == 1.0
    assert ranked["A"].iloc[2] == 2 / 3
    assert ranked["A"].iloc[3] == 1.0
    assert ranked["B"].iloc[3] == 1 / 3


def test_correlation_and_decay_linear():
    left = pd.DataFrame({"A": [1.0, 2.0, 3.0, 4.0]})
    right = pd.DataFrame({"A": [2.0, 4.0, 6.0, 8.0]})

    corr = correlation(left, right, 3)
    decayed = decay_linear(left, 3)

    assert np.isclose(corr["A"].iloc[-1], 1.0)
    assert np.isclose(decayed["A"].iloc[-1], (2 * 1 + 3 * 2 + 4 * 3) / 6)


def test_signed_power_scale_and_group_neutralize():
    frame = pd.DataFrame(
        {"A": [-2.0, 1.0], "B": [1.0, 2.0], "C": [3.0, 5.0]},
        index=["20260101", "20260102"],
    )

    powered = signed_power(frame, 2)
    scaled = scale(frame)
    neutralized = neutralize_by_group(frame, {"A": "g1", "B": "g1", "C": "g2"})

    assert powered["A"].iloc[0] == -4.0
    assert np.isclose(float(scaled.loc["20260101"].abs().sum()), 1.0)
    assert np.isclose(float(neutralized[["A", "B"]].loc["20260101"].mean()), 0.0)
    assert neutralized["C"].iloc[0] == 0.0
