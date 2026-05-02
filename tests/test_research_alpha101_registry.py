from __future__ import annotations

import numpy as np
import pandas as pd

from vortex.research.alpha101_registry import (
    DailyFactorInputs,
    batch_a_specs,
    batch_b_specs,
    batch_c_specs,
    batch_d_specs,
    batch_e_specs,
    batch_f_specs,
    batch_g_specs,
    batch_h_specs,
    batch_i_specs,
    batch_j_specs,
    batch_k_specs,
    compute_formula,
    compute_formula_batch,
    registered_specs,
    specs_by_id,
)


def _inputs() -> DailyFactorInputs:
    index = pd.RangeIndex(160)
    symbols = ["A", "B", "C"]
    base = pd.DataFrame(
        {
            "A": np.linspace(10.0, 18.0, len(index)),
            "B": np.linspace(20.0, 14.0, len(index)),
            "C": np.linspace(15.0, 16.0, len(index)) + np.sin(np.arange(len(index))) * 0.2,
        },
        index=index,
    )
    open_ = base * 0.99
    high = base * 1.02
    low = base * 0.98
    volume = pd.DataFrame(
        {
            "A": np.linspace(1000.0, 2000.0, len(index)),
            "B": np.linspace(2000.0, 1500.0, len(index)),
            "C": np.linspace(1500.0, 1700.0, len(index)),
        },
        index=index,
    )
    amount = volume * base
    return DailyFactorInputs(open=open_, high=high, low=low, close=base, volume=volume, amount=amount)


def test_batch_a_registry_has_unique_ids_and_metadata():
    specs = batch_a_specs()
    ids = [spec.formula_id for spec in specs]

    assert len(specs) >= 10
    assert len(ids) == len(set(ids))
    assert specs_by_id()["vtx_alpha_001"].name == "short_reversal_5d"
    assert all(spec.required_fields for spec in specs)


def test_registered_specs_include_batch_b_without_id_collisions():
    specs = registered_specs()
    ids = [spec.formula_id for spec in specs]

    assert len(batch_b_specs()) >= 10
    assert len(batch_c_specs()) >= 10
    assert len(batch_d_specs()) >= 10
    assert len(batch_e_specs()) >= 10
    assert len(batch_f_specs()) >= 10
    assert len(batch_g_specs()) >= 10
    assert len(batch_h_specs()) >= 10
    assert len(batch_i_specs()) >= 10
    assert len(batch_j_specs()) >= 10
    assert len(batch_k_specs()) >= 1
    assert len(specs) >= 101
    assert len(ids) == len(set(ids))
    assert specs_by_id()["vtx_alpha_020"].name == "volume_acceleration_reversal_20d"
    assert specs_by_id()["vtx_alpha_030"].name == "liquidity_dryup_lowvol_20d"
    assert specs_by_id()["vtx_alpha_040"].name == "range_reversal_lowrisk_20d"
    assert specs_by_id()["vtx_alpha_050"].name == "defensive_crowding_combo_20d"
    assert specs_by_id()["vtx_alpha_060"].name == "execution_safe_defensive_combo_20d"
    assert specs_by_id()["vtx_alpha_070"].name == "defensive_residual_combo_20d"
    assert specs_by_id()["vtx_alpha_080"].name == "defensive_execution_combo_v2_20d"
    assert specs_by_id()["vtx_alpha_090"].name == "balanced_crowding_residual_combo_20d"
    assert specs_by_id()["vtx_alpha_100"].name == "all_weather_defensive_combo_20d"
    assert specs_by_id()["vtx_alpha_101"].name == "research_gatekeeper_shadow_20d"


def test_compute_formula_batch_returns_wide_frames():
    inputs = _inputs()
    factors = compute_formula_batch(inputs)

    assert "vtx_alpha_001" in factors
    assert set(factors) == set(specs_by_id())
    for frame in factors.values():
        assert list(frame.columns) == ["A", "B", "C"]
        assert frame.index.is_monotonic_increasing


def test_short_reversal_uses_only_past_prices():
    inputs = _inputs()
    spec = specs_by_id()["vtx_alpha_001"]
    baseline = compute_formula(spec, inputs)

    changed_close = inputs.close.copy()
    changed_close.iloc[-1, :] = changed_close.iloc[-1, :] * 10.0
    changed = compute_formula(
        spec,
        DailyFactorInputs(
            open=inputs.open,
            high=inputs.high,
            low=inputs.low,
            close=changed_close,
            volume=inputs.volume,
            amount=inputs.amount,
        ),
    )

    pd.testing.assert_series_equal(baseline.iloc[-2], changed.iloc[-2])


def test_price_amount_corr_produces_finite_values_after_warmup():
    inputs = _inputs()
    spec = specs_by_id()["vtx_alpha_006"]
    factor = compute_formula(spec, inputs)

    assert factor.iloc[25:].notna().any().any()


def test_batch_b_gap_formula_is_pit_safe():
    inputs = _inputs()
    spec = specs_by_id()["vtx_alpha_013"]
    baseline = compute_formula(spec, inputs)

    changed_open = inputs.open.copy()
    changed_open.iloc[-1, :] = changed_open.iloc[-1, :] * 3.0
    changed = compute_formula(
        spec,
        DailyFactorInputs(
            open=changed_open,
            high=inputs.high,
            low=inputs.low,
            close=inputs.close,
            volume=inputs.volume,
            amount=inputs.amount,
        ),
    )

    pd.testing.assert_series_equal(baseline.iloc[-2], changed.iloc[-2])


def test_batch_b_formulas_produce_finite_values_after_warmup():
    inputs = _inputs()
    factors = compute_formula_batch(inputs, batch_b_specs())

    assert set(factors) == {spec.formula_id for spec in batch_b_specs()}
    for frame in factors.values():
        assert list(frame.columns) == ["A", "B", "C"]
        assert frame.iloc[40:].notna().any().any()


def test_batch_c_formulas_produce_finite_values_after_warmup():
    inputs = _inputs()
    factors = compute_formula_batch(inputs, batch_c_specs())

    assert set(factors) == {spec.formula_id for spec in batch_c_specs()}
    for frame in factors.values():
        assert list(frame.columns) == ["A", "B", "C"]
        assert frame.iloc[40:].notna().any().any()


def test_batch_d_formulas_produce_finite_values_after_warmup():
    inputs = _inputs()
    factors = compute_formula_batch(inputs, batch_d_specs())

    assert set(factors) == {spec.formula_id for spec in batch_d_specs()}
    for frame in factors.values():
        assert list(frame.columns) == ["A", "B", "C"]
        assert frame.iloc[40:].notna().any().any()


def test_batch_e_formulas_produce_finite_values_after_warmup():
    inputs = _inputs()
    factors = compute_formula_batch(inputs, batch_e_specs())

    assert set(factors) == {spec.formula_id for spec in batch_e_specs()}
    for frame in factors.values():
        assert list(frame.columns) == ["A", "B", "C"]
        assert frame.iloc[80:].notna().any().any()


def test_batch_f_formulas_produce_finite_values_after_warmup():
    inputs = _inputs()
    factors = compute_formula_batch(inputs, batch_f_specs())

    assert set(factors) == {spec.formula_id for spec in batch_f_specs()}
    for frame in factors.values():
        assert list(frame.columns) == ["A", "B", "C"]
        assert frame.iloc[100:].notna().any().any()


def test_batch_g_formulas_produce_finite_values_after_warmup():
    inputs = _inputs()
    factors = compute_formula_batch(inputs, batch_g_specs())

    assert set(factors) == {spec.formula_id for spec in batch_g_specs()}
    for frame in factors.values():
        assert list(frame.columns) == ["A", "B", "C"]
        assert frame.iloc[100:].notna().any().any()


def test_batches_h_to_k_formulas_produce_finite_values_after_warmup():
    inputs = _inputs()
    for specs in (batch_h_specs(), batch_i_specs(), batch_j_specs(), batch_k_specs()):
        factors = compute_formula_batch(inputs, specs)

        assert set(factors) == {spec.formula_id for spec in specs}
        for frame in factors.values():
            assert list(frame.columns) == ["A", "B", "C"]
            assert frame.iloc[120:].notna().any().any()
