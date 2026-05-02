from __future__ import annotations

import pandas as pd
import pytest

from vortex.research.walk_forward import WalkForwardConfig, generate_walk_forward_splits


def test_walk_forward_generates_rolling_splits():
    dates = pd.date_range("2020-01-01", periods=100, freq="B")

    splits = generate_walk_forward_splits(
        dates,
        WalkForwardConfig(train_size=40, validation_size=10, test_size=10, step_size=20),
    )

    assert len(splits) == 3
    assert splits[0].train[0] == dates[0]
    assert splits[0].test[-1] == dates[59]
    assert splits[1].train[0] == dates[20]


def test_walk_forward_supports_expanding_train():
    dates = pd.date_range("2020-01-01", periods=90, freq="B")

    splits = generate_walk_forward_splits(
        dates,
        WalkForwardConfig(train_size=30, validation_size=10, test_size=10, step_size=20, expanding_train=True),
    )

    assert splits[0].train[0] == dates[0]
    assert splits[1].train[0] == dates[0]
    assert len(splits[1].train) == 50


def test_walk_forward_fails_when_dates_too_short():
    dates = pd.date_range("2020-01-01", periods=10, freq="B")

    with pytest.raises(ValueError, match="长度不足"):
        generate_walk_forward_splits(
            dates,
            WalkForwardConfig(train_size=10, validation_size=5, test_size=5, step_size=5),
        )


def test_walk_forward_rejects_invalid_config():
    dates = pd.date_range("2020-01-01", periods=100, freq="B")

    with pytest.raises(ValueError, match="train_size"):
        generate_walk_forward_splits(
            dates,
            WalkForwardConfig(train_size=0, validation_size=5, test_size=5, step_size=5),
        )

