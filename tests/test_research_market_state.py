from __future__ import annotations

import pandas as pd
import pytest

from vortex.research.market_state import (
    MarketStateConfig,
    build_market_state,
    market_gate_from_state,
)


def _index_close(days: int = 180) -> pd.DataFrame:
    dates = pd.date_range("2020-01-01", periods=days, freq="B")
    up = pd.Series(100.0, index=dates)
    down = pd.Series(100.0, index=dates)
    for idx in range(1, days):
        up.iloc[idx] = up.iloc[idx - 1] * 1.002
        down.iloc[idx] = down.iloc[idx - 1] * (0.998 if idx > 130 else 1.001)
    return pd.DataFrame(
        {
            "000300.SH": up,
            "000905.SH": up * 1.01,
            "000852.SH": down,
        },
        index=dates,
    )


def test_build_market_state_turns_risk_on_after_confirmation_windows():
    close = _index_close()
    state = build_market_state(
        close,
        MarketStateConfig(
            momentum_window=20,
            support_window=30,
            min_risk_on_confirmations=2,
        ),
    )

    assert state["risk_on"].iloc[:30].sum() == 0
    assert state["risk_on"].iloc[-1]
    assert state["risk_on_confirmations"].iloc[-1] >= 2


def test_market_gate_from_state_returns_boolean_series():
    close = _index_close()
    state = build_market_state(close, MarketStateConfig(momentum_window=20, support_window=30))
    gate = market_gate_from_state(state)

    assert gate.dtype == bool
    assert gate.index.equals(close.index)


def test_build_market_state_fails_without_enough_indices():
    close = _index_close()[["000300.SH"]]

    with pytest.raises(ValueError, match="确认指数"):
        build_market_state(
            close,
            MarketStateConfig(
                confirmation_indices=("000300.SH", "000905.SH"),
                min_risk_on_confirmations=2,
            ),
        )
