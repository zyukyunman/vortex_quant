"""Alpha101-style daily factor formula registry.

This registry is not a claim that every public Alpha101 formula is already
replicated. It provides a typed, testable queue of Vortex-owned formulas built
from the same operator grammar so research can expand formula coverage in
batches and archive both successful and failed results.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import pandas as pd

from vortex.research.factor_ops import (
    correlation,
    cs_zscore,
    decay_linear,
    delta,
    ts_mean,
    ts_rank,
    ts_std,
)


@dataclass(frozen=True)
class DailyFactorInputs:
    """Wide daily market matrices indexed by date with symbols as columns."""

    open: pd.DataFrame
    high: pd.DataFrame
    low: pd.DataFrame
    close: pd.DataFrame
    volume: pd.DataFrame
    amount: pd.DataFrame


FormulaBuilder = Callable[[DailyFactorInputs], pd.DataFrame]


@dataclass(frozen=True)
class FormulaSpec:
    """Research metadata for one formula candidate."""

    formula_id: str
    name: str
    family: str
    role_hint: str
    required_fields: tuple[str, ...]
    default_horizons: tuple[int, ...]
    builder: FormulaBuilder
    description: str


def batch_a_specs() -> tuple[FormulaSpec, ...]:
    """Initial Vortex Alpha101-style batch focused on known A-share signals."""

    return (
        FormulaSpec(
            formula_id="vtx_alpha_001",
            name="short_reversal_5d",
            family="reversal",
            role_hint="short_horizon_ranking",
            required_fields=("close",),
            default_horizons=(1, 5, 10),
            builder=_short_reversal_5d,
            description="Negative 5-day return; short-horizon reversal candidate.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_002",
            name="medium_reversal_20d",
            family="reversal",
            role_hint="candidate_pool_ranking",
            required_fields=("close",),
            default_horizons=(5, 10, 20),
            builder=_medium_reversal_20d,
            description="Negative 20-day return; medium-horizon reversal candidate.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_003",
            name="low_volatility_20d",
            family="low_risk",
            role_hint="defensive_filter",
            required_fields=("close",),
            default_horizons=(10, 20, 60),
            builder=_low_volatility_20d,
            description="Negative 20-day realized volatility.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_004",
            name="lowvol_reversal_20d",
            family="low_risk_reversal",
            role_hint="defensive_ranking",
            required_fields=("close",),
            default_horizons=(10, 20, 60),
            builder=_lowvol_reversal_20d,
            description="Blend of 20-day reversal and 20-day low volatility.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_005",
            name="amount_crowding_reversal_20d",
            family="volume_reversal",
            role_hint="crowding_risk_filter",
            required_fields=("close", "amount"),
            default_horizons=(5, 20, 60),
            builder=_amount_crowding_reversal_20d,
            description="Avoid high recent amount surge combined with 20-day price run-up.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_006",
            name="price_amount_corr_20d",
            family="price_volume_correlation",
            role_hint="secondary_ranking",
            required_fields=("close", "amount"),
            default_horizons=(5, 20, 60),
            builder=_price_amount_corr_20d,
            description="20-day rolling price-amount correlation.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_007",
            name="amount_surge_reversal_5d",
            family="volume_reversal",
            role_hint="short_horizon_filter",
            required_fields=("close", "amount"),
            default_horizons=(1, 5, 10),
            builder=_amount_surge_reversal_5d,
            description="Short reversal after abnormal 20-day amount surge.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_008",
            name="intraday_strength",
            family="ohlc_shape",
            role_hint="short_horizon_ranking",
            required_fields=("open", "high", "low", "close"),
            default_horizons=(1, 5, 10),
            builder=_intraday_strength,
            description="Close location relative to intraday open and range.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_009",
            name="volume_price_divergence_20d",
            family="price_volume_divergence",
            role_hint="secondary_ranking",
            required_fields=("close", "volume"),
            default_horizons=(5, 20, 60),
            builder=_volume_price_divergence_20d,
            description="20-day volume rank minus price rank; captures volume-price divergence.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_010",
            name="risk_adjusted_momentum_60_skip20",
            family="momentum",
            role_hint="regime_or_risk_diagnostic",
            required_fields=("close",),
            default_horizons=(20, 60, 120),
            builder=_risk_adjusted_momentum_60_skip20,
            description="60-day momentum skipping the last 20 days, divided by 60-day volatility.",
        ),
    )


def batch_b_specs() -> tuple[FormulaSpec, ...]:
    """Second batch focused on trend state, gap behavior, and OHLC structure."""

    return (
        FormulaSpec(
            formula_id="vtx_alpha_011",
            name="medium_momentum_120_skip20",
            family="momentum",
            role_hint="trend_state_diagnostic",
            required_fields=("close",),
            default_horizons=(20, 60, 120),
            builder=_medium_momentum_120_skip20,
            description="120-day momentum skipping the most recent 20 days.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_012",
            name="risk_adjusted_momentum_120_skip20",
            family="momentum",
            role_hint="trend_state_diagnostic",
            required_fields=("close",),
            default_horizons=(20, 60, 120),
            builder=_risk_adjusted_momentum_120_skip20,
            description="120-day skip-20 momentum divided by 120-day realized volatility.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_013",
            name="gap_reversal_5d",
            family="gap_reversal",
            role_hint="short_horizon_execution_filter",
            required_fields=("open", "close"),
            default_horizons=(1, 5, 10),
            builder=_gap_reversal_5d,
            description="Avoid repeated positive opening gaps; short-horizon gap reversal.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_014",
            name="overnight_reversal_20d",
            family="gap_reversal",
            role_hint="execution_risk_filter",
            required_fields=("open", "close"),
            default_horizons=(1, 5, 20),
            builder=_overnight_reversal_20d,
            description="Negative 20-day overnight return accumulation.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_015",
            name="intraday_momentum_10d",
            family="ohlc_shape",
            role_hint="short_horizon_ranking",
            required_fields=("open", "close"),
            default_horizons=(1, 5, 10),
            builder=_intraday_momentum_10d,
            description="10-day accumulation of open-to-close strength.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_016",
            name="intraday_reversal_10d",
            family="ohlc_shape",
            role_hint="short_horizon_risk_filter",
            required_fields=("open", "close"),
            default_horizons=(1, 5, 10),
            builder=_intraday_reversal_10d,
            description="Negative 10-day accumulation of open-to-close strength.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_017",
            name="range_position_20d",
            family="ohlc_shape",
            role_hint="candidate_pool_ranking",
            required_fields=("high", "low", "close"),
            default_horizons=(5, 20, 60),
            builder=_range_position_20d,
            description="Close location within the recent 20-day high-low range.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_018",
            name="range_compression_20d",
            family="low_risk",
            role_hint="defensive_filter",
            required_fields=("high", "low", "close"),
            default_horizons=(10, 20, 60),
            builder=_range_compression_20d,
            description="Low 20-day high-low range relative to price.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_019",
            name="downside_volatility_reversal_20d",
            family="low_risk_reversal",
            role_hint="defensive_ranking",
            required_fields=("close",),
            default_horizons=(10, 20, 60),
            builder=_downside_volatility_reversal_20d,
            description="Blend of 20-day reversal and low downside volatility.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_020",
            name="volume_acceleration_reversal_20d",
            family="volume_reversal",
            role_hint="crowding_risk_filter",
            required_fields=("close", "amount"),
            default_horizons=(5, 20, 60),
            builder=_volume_acceleration_reversal_20d,
            description="Avoid accelerating amount crowding combined with recent price strength.",
        ),
    )


def registered_specs() -> tuple[FormulaSpec, ...]:
    """Return all currently registered Vortex Alpha101-style formulas."""

    return (
        batch_a_specs()
        + batch_b_specs()
        + batch_c_specs()
        + batch_d_specs()
        + batch_e_specs()
        + batch_f_specs()
        + batch_g_specs()
        + batch_h_specs()
        + batch_i_specs()
        + batch_j_specs()
        + batch_k_specs()
    )


def batch_c_specs() -> tuple[FormulaSpec, ...]:
    """Third batch focused on price-volume correlation and divergence."""

    return (
        FormulaSpec(
            formula_id="vtx_alpha_021",
            name="price_amount_corr_reversal_20d",
            family="price_volume_correlation",
            role_hint="crowding_risk_filter",
            required_fields=("close", "amount"),
            default_horizons=(5, 20, 60),
            builder=_price_amount_corr_reversal_20d,
            description="Negative 20-day price-amount correlation; reverses Batch A raw direction.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_022",
            name="return_amount_corr_reversal_20d",
            family="price_volume_correlation",
            role_hint="crowding_risk_filter",
            required_fields=("close", "amount"),
            default_horizons=(5, 20, 60),
            builder=_return_amount_corr_reversal_20d,
            description="Avoid high correlation between daily returns and abnormal amount.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_023",
            name="decayed_volume_price_divergence_20d",
            family="price_volume_divergence",
            role_hint="secondary_ranking",
            required_fields=("close", "volume"),
            default_horizons=(5, 20, 60),
            builder=_decayed_volume_price_divergence_20d,
            description="Linearly decayed volume rank minus price rank divergence.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_024",
            name="quiet_reversal_20d",
            family="volume_reversal",
            role_hint="defensive_ranking",
            required_fields=("close", "amount"),
            default_horizons=(5, 20, 60),
            builder=_quiet_reversal_20d,
            description="20-day reversal strengthened when recent amount is quiet rather than crowded.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_025",
            name="down_volume_pressure_20d",
            family="price_volume_correlation",
            role_hint="risk_filter",
            required_fields=("close", "amount"),
            default_horizons=(5, 20, 60),
            builder=_down_volume_pressure_20d,
            description="Avoid heavy amount on down-return days; selling pressure proxy.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_026",
            name="up_volume_exhaustion_reversal_20d",
            family="volume_reversal",
            role_hint="crowding_risk_filter",
            required_fields=("close", "amount"),
            default_horizons=(5, 20, 60),
            builder=_up_volume_exhaustion_reversal_20d,
            description="Avoid heavy amount on up-return days; chase exhaustion proxy.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_027",
            name="volume_volatility_corr_reversal_20d",
            family="price_volume_correlation",
            role_hint="defensive_filter",
            required_fields=("close", "amount"),
            default_horizons=(5, 20, 60),
            builder=_volume_volatility_corr_reversal_20d,
            description="Avoid high amount-volatility coupling.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_028",
            name="amount_trend_reversal_20d",
            family="volume_reversal",
            role_hint="crowding_risk_filter",
            required_fields=("amount",),
            default_horizons=(5, 20, 60),
            builder=_amount_trend_reversal_20d,
            description="Negative short-term acceleration of amount trend.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_029",
            name="price_volume_rank_gap_10d",
            family="price_volume_divergence",
            role_hint="short_horizon_filter",
            required_fields=("close", "volume"),
            default_horizons=(1, 5, 10),
            builder=_price_volume_rank_gap_10d,
            description="10-day time-series volume rank minus price rank.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_030",
            name="liquidity_dryup_lowvol_20d",
            family="low_risk",
            role_hint="defensive_filter",
            required_fields=("close", "amount"),
            default_horizons=(10, 20, 60),
            builder=_liquidity_dryup_lowvol_20d,
            description="Blend of quiet amount and low realized volatility.",
        ),
    )


def batch_d_specs() -> tuple[FormulaSpec, ...]:
    """Fourth batch focused on OHLC shape and range structure."""

    return (
        FormulaSpec(
            formula_id="vtx_alpha_031",
            name="upper_shadow_reversal_10d",
            family="ohlc_shape",
            role_hint="chase_exhaustion_filter",
            required_fields=("open", "high", "low", "close"),
            default_horizons=(1, 5, 10),
            builder=_upper_shadow_reversal_10d,
            description="Avoid repeated upper shadows, a short-horizon chase exhaustion proxy.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_032",
            name="lower_shadow_support_10d",
            family="ohlc_shape",
            role_hint="short_horizon_ranking",
            required_fields=("open", "high", "low", "close"),
            default_horizons=(1, 5, 10),
            builder=_lower_shadow_support_10d,
            description="Repeated lower shadows as short-horizon support/reversal candidate.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_033",
            name="close_to_high_20d",
            family="ohlc_shape",
            role_hint="trend_state_diagnostic",
            required_fields=("high", "low", "close"),
            default_horizons=(5, 20, 60),
            builder=_close_to_high_20d,
            description="Close location near recent 20-day high.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_034",
            name="close_to_low_reversal_20d",
            family="ohlc_shape",
            role_hint="candidate_pool_ranking",
            required_fields=("high", "low", "close"),
            default_horizons=(5, 20, 60),
            builder=_close_to_low_reversal_20d,
            description="Reversal candidate when close is near recent 20-day low.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_035",
            name="range_expansion_reversal_10d",
            family="ohlc_shape",
            role_hint="volatility_risk_filter",
            required_fields=("high", "low", "close"),
            default_horizons=(1, 5, 20),
            builder=_range_expansion_reversal_10d,
            description="Avoid recent high-low range expansion.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_036",
            name="body_compression_10d",
            family="ohlc_shape",
            role_hint="defensive_filter",
            required_fields=("open", "close"),
            default_horizons=(1, 5, 20),
            builder=_body_compression_10d,
            description="Low average open-close body size.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_037",
            name="gap_up_exhaustion_10d",
            family="gap_reversal",
            role_hint="execution_risk_filter",
            required_fields=("open", "close", "high", "low"),
            default_horizons=(1, 5, 10),
            builder=_gap_up_exhaustion_10d,
            description="Avoid positive gaps followed by weak close location.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_038",
            name="gap_down_rebound_10d",
            family="gap_reversal",
            role_hint="short_horizon_ranking",
            required_fields=("open", "close", "high", "low"),
            default_horizons=(1, 5, 10),
            builder=_gap_down_rebound_10d,
            description="Positive signal for negative gaps followed by stronger close location.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_039",
            name="true_range_lowrisk_20d",
            family="low_risk",
            role_hint="defensive_filter",
            required_fields=("high", "low", "close"),
            default_horizons=(10, 20, 60),
            builder=_true_range_lowrisk_20d,
            description="Low average true range relative to close.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_040",
            name="range_reversal_lowrisk_20d",
            family="low_risk_reversal",
            role_hint="defensive_ranking",
            required_fields=("high", "low", "close"),
            default_horizons=(10, 20, 60),
            builder=_range_reversal_lowrisk_20d,
            description="Blend of close-near-low reversal and low true range.",
        ),
    )


def batch_e_specs() -> tuple[FormulaSpec, ...]:
    """Fifth batch of mixed formulas combining the strongest prior themes."""

    return (
        FormulaSpec(
            formula_id="vtx_alpha_041",
            name="defensive_lowrisk_ensemble_20d",
            family="mixed_low_risk",
            role_hint="defensive_ranking",
            required_fields=("close", "high", "low", "amount"),
            default_horizons=(10, 20, 60),
            builder=_defensive_lowrisk_ensemble_20d,
            description="Blend of low-vol reversal, quiet low-vol, and true-range low-risk.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_042",
            name="crowding_exhaustion_ensemble_20d",
            family="mixed_volume_reversal",
            role_hint="crowding_risk_filter",
            required_fields=("close", "amount"),
            default_horizons=(5, 20, 60),
            builder=_crowding_exhaustion_ensemble_20d,
            description="Blend of amount crowding reversal, price-amount corr reversal, and up-volume exhaustion.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_043",
            name="quiet_range_reversal_20d",
            family="mixed_reversal",
            role_hint="candidate_pool_ranking",
            required_fields=("close", "high", "low", "amount"),
            default_horizons=(5, 20, 60),
            builder=_quiet_range_reversal_20d,
            description="Quiet reversal plus range-reversal low-risk.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_044",
            name="lowrisk_intraday_reversal_20d",
            family="mixed_ohlc_low_risk",
            role_hint="short_horizon_risk_filter",
            required_fields=("open", "close", "high", "low"),
            default_horizons=(1, 5, 20),
            builder=_lowrisk_intraday_reversal_20d,
            description="Intraday reversal conditioned by true-range low-risk.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_045",
            name="anti_momentum_crowding_60d",
            family="mixed_momentum_reversal",
            role_hint="regime_or_risk_diagnostic",
            required_fields=("close", "amount"),
            default_horizons=(20, 60, 120),
            builder=_anti_momentum_crowding_60d,
            description="Reverse risk-adjusted momentum combined with crowding reversal.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_046",
            name="compression_lowrisk_ensemble_20d",
            family="mixed_low_risk",
            role_hint="defensive_filter",
            required_fields=("open", "close", "high", "low", "amount"),
            default_horizons=(10, 20, 60),
            builder=_compression_lowrisk_ensemble_20d,
            description="Range compression, body compression, liquidity dry-up, and low volatility.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_047",
            name="gap_execution_risk_ensemble_10d",
            family="mixed_gap_reversal",
            role_hint="execution_risk_filter",
            required_fields=("open", "close", "high", "low"),
            default_horizons=(1, 5, 10),
            builder=_gap_execution_risk_ensemble_10d,
            description="Gap-up exhaustion and inverse gap-down rebound for execution risk control.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_048",
            name="short_reversal_execution_ensemble_10d",
            family="mixed_short_reversal",
            role_hint="short_horizon_filter",
            required_fields=("open", "close", "high", "low", "amount"),
            default_horizons=(1, 5, 10),
            builder=_short_reversal_execution_ensemble_10d,
            description="Short reversal, amount-surge reversal, and upper-shadow reversal.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_049",
            name="medium_defensive_reversal_20d",
            family="mixed_low_risk_reversal",
            role_hint="defensive_ranking",
            required_fields=("close", "high", "low"),
            default_horizons=(10, 20, 60),
            builder=_medium_defensive_reversal_20d,
            description="Medium reversal combined with downside volatility and true-range low-risk.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_050",
            name="defensive_crowding_combo_20d",
            family="mixed_combo",
            role_hint="candidate_pool_overlay",
            required_fields=("close", "high", "low", "amount"),
            default_horizons=(5, 20, 60),
            builder=_defensive_crowding_combo_20d,
            description="Balanced defensive low-risk and crowding-exhaustion composite.",
        ),
    )


def batch_f_specs() -> tuple[FormulaSpec, ...]:
    """Sixth batch focused on execution-aware liquidity, beta, and path structure."""

    return (
        FormulaSpec(
            formula_id="vtx_alpha_051",
            name="liquidity_adjusted_reversal_20d",
            family="liquidity_reversal",
            role_hint="candidate_pool_ranking",
            required_fields=("close", "amount"),
            default_horizons=(5, 20, 60),
            builder=_liquidity_adjusted_reversal_20d,
            description="20-day reversal strengthened when recent amount is not crowded.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_052",
            name="low_market_beta_reversal_60d",
            family="low_risk",
            role_hint="defensive_filter",
            required_fields=("close",),
            default_horizons=(20, 60, 120),
            builder=_low_market_beta_reversal_60d,
            description="Low market beta blended with 20-day reversal.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_053",
            name="volatility_trend_reversal_60d",
            family="low_risk",
            role_hint="risk_filter",
            required_fields=("close",),
            default_horizons=(20, 60, 120),
            builder=_volatility_trend_reversal_60d,
            description="Avoid rising realized volatility relative to its medium-term baseline.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_054",
            name="price_efficiency_reversal_20d",
            family="path_structure",
            role_hint="secondary_ranking",
            required_fields=("close",),
            default_horizons=(5, 20, 60),
            builder=_price_efficiency_reversal_20d,
            description="Blend of 20-day reversal and low directional path efficiency.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_055",
            name="short_down_amount_pressure_10d",
            family="volume_reversal",
            role_hint="short_horizon_filter",
            required_fields=("close", "amount"),
            default_horizons=(1, 5, 10),
            builder=_short_down_amount_pressure_10d,
            description="Avoid recent abnormal amount on down-return days.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_056",
            name="upper_shadow_crowding_reversal_10d",
            family="ohlc_volume",
            role_hint="chase_exhaustion_filter",
            required_fields=("open", "high", "low", "close", "amount"),
            default_horizons=(1, 5, 10),
            builder=_upper_shadow_crowding_reversal_10d,
            description="Avoid upper-shadow exhaustion when amount is crowded.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_057",
            name="overnight_lowrisk_reversal_20d",
            family="gap_reversal",
            role_hint="execution_risk_filter",
            required_fields=("open", "close"),
            default_horizons=(1, 5, 20),
            builder=_overnight_lowrisk_reversal_20d,
            description="Negative overnight return accumulation with low overnight volatility.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_058",
            name="close_location_crowding_reversal_10d",
            family="ohlc_volume",
            role_hint="crowding_risk_filter",
            required_fields=("open", "high", "low", "close", "amount"),
            default_horizons=(1, 5, 20),
            builder=_close_location_crowding_reversal_10d,
            description="Avoid crowded high close-location moves.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_059",
            name="volume_dryup_rebound_5d",
            family="volume_reversal",
            role_hint="short_horizon_ranking",
            required_fields=("close", "amount"),
            default_horizons=(1, 5, 10),
            builder=_volume_dryup_rebound_5d,
            description="Short reversal candidate after quiet recent amount.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_060",
            name="execution_safe_defensive_combo_20d",
            family="mixed_execution_low_risk",
            role_hint="candidate_pool_overlay",
            required_fields=("open", "close", "high", "low", "amount"),
            default_horizons=(5, 20, 60),
            builder=_execution_safe_defensive_combo_20d,
            description="Defensive low-risk composite penalizing gap and upper-shadow execution risk.",
        ),
    )


def batch_g_specs() -> tuple[FormulaSpec, ...]:
    """Seventh batch focused on residual reversal and volatility-adjusted path quality."""

    return (
        FormulaSpec(
            formula_id="vtx_alpha_061",
            name="long_reversal_60d",
            family="reversal",
            role_hint="medium_horizon_ranking",
            required_fields=("close",),
            default_horizons=(20, 60, 120),
            builder=_long_reversal_60d,
            description="Negative 60-day return as a medium-horizon reversal candidate.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_062",
            name="volatility_adjusted_reversal_20d",
            family="low_risk_reversal",
            role_hint="defensive_ranking",
            required_fields=("close",),
            default_horizons=(10, 20, 60),
            builder=_volatility_adjusted_reversal_20d,
            description="20-day reversal divided by realized volatility.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_063",
            name="market_residual_reversal_20d",
            family="residual_reversal",
            role_hint="style_residual_candidate",
            required_fields=("close",),
            default_horizons=(5, 20, 60),
            builder=_market_residual_reversal_20d,
            description="20-day reversal after removing equal-weight market return proxy.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_064",
            name="amount_dryup_price_stability_20d",
            family="low_risk",
            role_hint="defensive_filter",
            required_fields=("close", "amount"),
            default_horizons=(10, 20, 60),
            builder=_amount_dryup_price_stability_20d,
            description="Quiet amount combined with low short-term realized volatility.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_065",
            name="smooth_trend_quality_60d",
            family="path_structure",
            role_hint="trend_state_diagnostic",
            required_fields=("close",),
            default_horizons=(20, 60, 120),
            builder=_smooth_trend_quality_60d,
            description="60-day return adjusted by realized path length.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_066",
            name="range_expansion_reversal_20d",
            family="ohlc_shape",
            role_hint="volatility_risk_filter",
            required_fields=("high", "low", "close"),
            default_horizons=(5, 20, 60),
            builder=_range_expansion_reversal_20d,
            description="Avoid recent 20-day relative range expansion.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_067",
            name="lower_shadow_lowrisk_10d",
            family="ohlc_shape",
            role_hint="short_horizon_ranking",
            required_fields=("open", "high", "low", "close"),
            default_horizons=(1, 5, 10),
            builder=_lower_shadow_lowrisk_10d,
            description="Lower-shadow support conditioned by low true-range risk.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_068",
            name="gap_volatility_lowrisk_20d",
            family="gap_reversal",
            role_hint="execution_risk_filter",
            required_fields=("open", "close"),
            default_horizons=(1, 5, 20),
            builder=_gap_volatility_lowrisk_20d,
            description="Low volatility of overnight gaps.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_069",
            name="volume_decay_reversal_20d",
            family="volume_reversal",
            role_hint="crowding_risk_filter",
            required_fields=("close", "amount"),
            default_horizons=(5, 20, 60),
            builder=_volume_decay_reversal_20d,
            description="20-day reversal plus decayed negative amount crowding.",
        ),
        FormulaSpec(
            formula_id="vtx_alpha_070",
            name="defensive_residual_combo_20d",
            family="mixed_residual_low_risk",
            role_hint="candidate_pool_overlay",
            required_fields=("close", "high", "low", "amount"),
            default_horizons=(5, 20, 60),
            builder=_defensive_residual_combo_20d,
            description="Blend of residual reversal, volatility-adjusted reversal, and quiet low-risk legs.",
        ),
    )


def batch_h_specs() -> tuple[FormulaSpec, ...]:
    """Eighth batch focused on execution risk and refined low-risk reversals."""

    return (
        FormulaSpec("vtx_alpha_071", "momentum_reversal_blend_120d", "momentum_reversal", "trend_state_diagnostic", ("close",), (20, 60, 120), _momentum_reversal_blend_120d, "Reverse risk-adjusted medium momentum blended with 60-day reversal."),
        FormulaSpec("vtx_alpha_072", "amount_shock_reversal_10d", "volume_reversal", "short_horizon_filter", ("close", "amount"), (1, 5, 10), _amount_shock_reversal_10d, "Avoid short-term amount shocks when price is overextended."),
        FormulaSpec("vtx_alpha_073", "close_location_lowrisk_20d", "ohlc_shape", "defensive_filter", ("high", "low", "close"), (5, 20, 60), _close_location_lowrisk_20d, "Low close-location risk blended with true-range low-risk."),
        FormulaSpec("vtx_alpha_074", "high_low_skew_reversal_20d", "ohlc_shape", "volatility_risk_filter", ("high", "low", "close"), (5, 20, 60), _high_low_skew_reversal_20d, "Avoid asymmetric upside range expansion."),
        FormulaSpec("vtx_alpha_075", "intraday_volatility_lowrisk_20d", "ohlc_shape", "execution_risk_filter", ("open", "close"), (1, 5, 20), _intraday_volatility_lowrisk_20d, "Low volatility of open-to-close returns."),
        FormulaSpec("vtx_alpha_076", "negative_gap_exhaustion_reversal_10d", "gap_reversal", "execution_risk_filter", ("open", "close", "high", "low"), (1, 5, 10), _negative_gap_exhaustion_reversal_10d, "Avoid negative gaps followed by weak closes."),
        FormulaSpec("vtx_alpha_077", "volume_price_corr_lowrisk_20d", "price_volume_correlation", "crowding_risk_filter", ("close", "amount"), (5, 20, 60), _volume_price_corr_lowrisk_20d, "Price-amount correlation reversal blended with low volatility."),
        FormulaSpec("vtx_alpha_078", "amount_volatility_lowrisk_20d", "low_risk", "defensive_filter", ("close", "amount"), (10, 20, 60), _amount_volatility_lowrisk_20d, "Low abnormal amount volatility and low price volatility."),
        FormulaSpec("vtx_alpha_079", "rebound_quality_reversal_20d", "reversal", "secondary_ranking", ("close", "high", "low"), (5, 20, 60), _rebound_quality_reversal_20d, "20-day reversal conditioned by low drawdown volatility."),
        FormulaSpec("vtx_alpha_080", "defensive_execution_combo_v2_20d", "mixed_execution_low_risk", "candidate_pool_overlay", ("open", "close", "high", "low", "amount"), (5, 20, 60), _defensive_execution_combo_v2_20d, "Execution-aware low-risk blend using amount and OHLC risk legs."),
    )


def batch_i_specs() -> tuple[FormulaSpec, ...]:
    """Ninth batch focused on crowding persistence and overreaction diagnostics."""

    return (
        FormulaSpec("vtx_alpha_081", "amount_dryup_60d", "liquidity_reversal", "defensive_filter", ("amount",), (20, 60, 120), _amount_dryup_60d, "Longer-horizon quiet amount proxy."),
        FormulaSpec("vtx_alpha_082", "return_skew_reversal_20d", "path_structure", "risk_filter", ("close",), (5, 20, 60), _return_skew_reversal_20d, "Avoid positive return skew and chase-like paths."),
        FormulaSpec("vtx_alpha_083", "downside_amount_exhaustion_20d", "volume_reversal", "risk_filter", ("close", "amount"), (5, 20, 60), _downside_amount_exhaustion_20d, "Downside amount pressure reversal."),
        FormulaSpec("vtx_alpha_084", "upper_shadow_lowrisk_20d", "ohlc_shape", "chase_exhaustion_filter", ("open", "high", "low", "close"), (1, 5, 20), _upper_shadow_lowrisk_20d, "Upper-shadow reversal conditioned by low true-range risk."),
        FormulaSpec("vtx_alpha_085", "amount_return_beta_reversal_60d", "price_volume_correlation", "crowding_risk_filter", ("close", "amount"), (20, 60, 120), _amount_return_beta_reversal_60d, "Avoid high medium-term return/amount coupling."),
        FormulaSpec("vtx_alpha_086", "opening_gap_crowding_10d", "gap_reversal", "execution_risk_filter", ("open", "close", "amount"), (1, 5, 10), _opening_gap_crowding_10d, "Avoid crowded positive opening gaps."),
        FormulaSpec("vtx_alpha_087", "vwap_proxy_reversal_20d", "price_volume_microstructure", "execution_risk_filter", ("close", "volume", "amount"), (5, 20, 60), _vwap_proxy_reversal_20d, "Close-to-VWAP proxy reversal using amount divided by volume."),
        FormulaSpec("vtx_alpha_088", "range_position_voladjusted_20d", "ohlc_shape", "secondary_ranking", ("high", "low", "close"), (5, 20, 60), _range_position_voladjusted_20d, "Range-position reversal adjusted by true range."),
        FormulaSpec("vtx_alpha_089", "short_overreaction_3d", "reversal", "short_horizon_ranking", ("close",), (1, 5, 10), _short_overreaction_3d, "Very short-term overreaction reversal."),
        FormulaSpec("vtx_alpha_090", "balanced_crowding_residual_combo_20d", "mixed_volume_reversal", "candidate_pool_overlay", ("open", "close", "high", "low", "amount"), (5, 20, 60), _balanced_crowding_residual_combo_20d, "Residual reversal plus crowding and execution-risk legs."),
    )


def batch_j_specs() -> tuple[FormulaSpec, ...]:
    """Tenth batch focused on drawdown, breakout failure, and all-weather defenses."""

    return (
        FormulaSpec("vtx_alpha_091", "monthly_reversal_lowrisk_40d", "low_risk_reversal", "defensive_ranking", ("close",), (20, 60, 120), _monthly_reversal_lowrisk_40d, "40-day reversal blended with low volatility."),
        FormulaSpec("vtx_alpha_092", "quarterly_anti_momentum_120d", "momentum_reversal", "trend_state_diagnostic", ("close",), (20, 60, 120), _quarterly_anti_momentum_120d, "Reverse long momentum while avoiding recent reversal overlap."),
        FormulaSpec("vtx_alpha_093", "amount_persistence_reversal_20d", "volume_reversal", "crowding_risk_filter", ("amount",), (5, 20, 60), _amount_persistence_reversal_20d, "Avoid persistent abnormal amount."),
        FormulaSpec("vtx_alpha_094", "volume_stability_20d", "low_risk", "defensive_filter", ("volume",), (10, 20, 60), _volume_stability_20d, "Low coefficient of variation in volume."),
        FormulaSpec("vtx_alpha_095", "price_acceleration_reversal_10d", "reversal", "short_horizon_filter", ("close",), (1, 5, 20), _price_acceleration_reversal_10d, "Reverse short-term price acceleration."),
        FormulaSpec("vtx_alpha_096", "low_drawdown_60d", "low_risk", "defensive_filter", ("close",), (20, 60, 120), _low_drawdown_60d, "Low 60-day peak-to-trough drawdown."),
        FormulaSpec("vtx_alpha_097", "drawdown_rebound_quality_20d", "low_risk_reversal", "secondary_ranking", ("close",), (5, 20, 60), _drawdown_rebound_quality_20d, "Reversal after drawdown with low volatility."),
        FormulaSpec("vtx_alpha_098", "breakout_failure_reversal_20d", "ohlc_shape", "chase_exhaustion_filter", ("high", "low", "close"), (5, 20, 60), _breakout_failure_reversal_20d, "Avoid close-near-high breakout failure risk."),
        FormulaSpec("vtx_alpha_099", "liquidity_shock_absorption_20d", "volume_reversal", "risk_filter", ("close", "amount"), (5, 20, 60), _liquidity_shock_absorption_20d, "Reward price stability during amount shocks."),
        FormulaSpec("vtx_alpha_100", "all_weather_defensive_combo_20d", "mixed_low_risk", "candidate_pool_overlay", ("open", "close", "high", "low", "amount"), (5, 20, 60), _all_weather_defensive_combo_20d, "All-weather blend of low-risk, reversal, and crowding defenses."),
    )


def batch_k_specs() -> tuple[FormulaSpec, ...]:
    """Final one-formula batch to complete the 101-formula registry milestone."""

    return (
        FormulaSpec("vtx_alpha_101", "research_gatekeeper_shadow_20d", "mixed_shadow_gatekeeper", "shadow_only", ("open", "close", "high", "low", "amount"), (5, 20, 60), _research_gatekeeper_shadow_20d, "Shadow-only gatekeeper blend; never promoted without overlay evidence."),
    )


def specs_by_id(specs: tuple[FormulaSpec, ...] | None = None) -> dict[str, FormulaSpec]:
    """Return formula specs keyed by formula id."""

    selected = specs or registered_specs()
    return {spec.formula_id: spec for spec in selected}


def compute_formula(spec: FormulaSpec, inputs: DailyFactorInputs) -> pd.DataFrame:
    """Compute one formula after validating required input fields."""

    missing = [field for field in spec.required_fields if getattr(inputs, field) is None]
    if missing:
        raise ValueError(f"{spec.formula_id} 缺少字段: {missing}")
    result = spec.builder(inputs)
    return result.sort_index()


def compute_formula_batch(
    inputs: DailyFactorInputs,
    specs: tuple[FormulaSpec, ...] | None = None,
) -> dict[str, pd.DataFrame]:
    """Compute a batch of registered formulas."""

    selected = specs or registered_specs()
    return {spec.formula_id: compute_formula(spec, inputs) for spec in selected}


def _returns(close: pd.DataFrame) -> pd.DataFrame:
    return close.pct_change(fill_method=None)


def _ret(close: pd.DataFrame, window: int) -> pd.DataFrame:
    return close / close.shift(window) - 1.0


def _short_reversal_5d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return cs_zscore(-_ret(inputs.close, 5))


def _medium_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return cs_zscore(-_ret(inputs.close, 20))


def _low_volatility_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return cs_zscore(-ts_std(_returns(inputs.close), 20, min_periods=15))


def _lowvol_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    reversal = cs_zscore(-_ret(inputs.close, 20))
    lowvol = cs_zscore(-ts_std(_returns(inputs.close), 20, min_periods=15))
    return cs_zscore((reversal + lowvol) / 2.0)


def _amount_crowding_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    amount_surge = inputs.amount / ts_mean(inputs.amount, 20, min_periods=15) - 1.0
    reversal = cs_zscore(-_ret(inputs.close, 20))
    uncrowded = cs_zscore(-amount_surge)
    return cs_zscore((0.45 * reversal) + (0.55 * uncrowded))


def _price_amount_corr_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return cs_zscore(correlation(inputs.close, inputs.amount, 20, min_periods=15))


def _amount_surge_reversal_5d(inputs: DailyFactorInputs) -> pd.DataFrame:
    amount_surge = inputs.amount / ts_mean(inputs.amount, 20, min_periods=15) - 1.0
    reversal = cs_zscore(-_ret(inputs.close, 5))
    uncrowded = cs_zscore(-amount_surge)
    return cs_zscore((0.50 * reversal) + (0.50 * uncrowded))


def _intraday_strength(inputs: DailyFactorInputs) -> pd.DataFrame:
    day_range = (inputs.high - inputs.low).replace(0.0, pd.NA)
    return cs_zscore((inputs.close - inputs.open) / day_range)


def _volume_price_divergence_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    volume_rank = ts_rank(inputs.volume, 20, min_periods=15)
    price_rank = ts_rank(inputs.close, 20, min_periods=15)
    return cs_zscore(volume_rank - price_rank)


def _risk_adjusted_momentum_60_skip20(inputs: DailyFactorInputs) -> pd.DataFrame:
    momentum = inputs.close.shift(20) / inputs.close.shift(60) - 1.0
    volatility = ts_std(_returns(inputs.close), 60, min_periods=40)
    return cs_zscore(momentum / volatility.replace(0.0, pd.NA))


def _medium_momentum_120_skip20(inputs: DailyFactorInputs) -> pd.DataFrame:
    return cs_zscore(inputs.close.shift(20) / inputs.close.shift(120) - 1.0)


def _risk_adjusted_momentum_120_skip20(inputs: DailyFactorInputs) -> pd.DataFrame:
    momentum = inputs.close.shift(20) / inputs.close.shift(120) - 1.0
    volatility = ts_std(_returns(inputs.close), 120, min_periods=80)
    return cs_zscore(momentum / volatility.replace(0.0, pd.NA))


def _gap_reversal_5d(inputs: DailyFactorInputs) -> pd.DataFrame:
    overnight = inputs.open / inputs.close.shift(1) - 1.0
    return cs_zscore(-ts_mean(overnight, 5, min_periods=3))


def _overnight_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    overnight = inputs.open / inputs.close.shift(1) - 1.0
    return cs_zscore(-ts_mean(overnight, 20, min_periods=15))


def _intraday_momentum_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    intraday = inputs.close / inputs.open - 1.0
    return cs_zscore(ts_mean(intraday, 10, min_periods=7))


def _intraday_reversal_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    intraday = inputs.close / inputs.open - 1.0
    return cs_zscore(-ts_mean(intraday, 10, min_periods=7))


def _range_position_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    rolling_high = inputs.high.rolling(20, min_periods=15).max()
    rolling_low = inputs.low.rolling(20, min_periods=15).min()
    span = (rolling_high - rolling_low).replace(0.0, pd.NA)
    return cs_zscore((inputs.close - rolling_low) / span)


def _range_compression_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    rolling_high = inputs.high.rolling(20, min_periods=15).max()
    rolling_low = inputs.low.rolling(20, min_periods=15).min()
    relative_range = (rolling_high - rolling_low) / inputs.close.replace(0.0, pd.NA)
    return cs_zscore(-relative_range)


def _downside_volatility_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    returns = _returns(inputs.close)
    downside = returns.where(returns < 0.0, 0.0)
    downside_vol = ts_std(downside, 20, min_periods=15)
    reversal = cs_zscore(-_ret(inputs.close, 20))
    low_downside = cs_zscore(-downside_vol)
    return cs_zscore((0.5 * reversal) + (0.5 * low_downside))


def _volume_acceleration_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    amount_ratio = inputs.amount / ts_mean(inputs.amount, 20, min_periods=15) - 1.0
    acceleration = delta(amount_ratio, 5)
    reversal = cs_zscore(-_ret(inputs.close, 20))
    deceleration = cs_zscore(-acceleration)
    return cs_zscore((0.45 * reversal) + (0.55 * deceleration))


def _amount_ratio(amount: pd.DataFrame, window: int = 20) -> pd.DataFrame:
    return amount / ts_mean(amount, window, min_periods=max(3, int(window * 0.75))) - 1.0


def _price_amount_corr_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return cs_zscore(-correlation(inputs.close, inputs.amount, 20, min_periods=15))


def _return_amount_corr_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return cs_zscore(-correlation(_returns(inputs.close), _amount_ratio(inputs.amount), 20, min_periods=15))


def _decayed_volume_price_divergence_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    volume_rank = ts_rank(inputs.volume, 20, min_periods=15)
    price_rank = ts_rank(inputs.close, 20, min_periods=15)
    return cs_zscore(decay_linear(volume_rank - price_rank, 10, min_periods=7))


def _quiet_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    reversal = cs_zscore(-_ret(inputs.close, 20))
    quiet_amount = cs_zscore(-_amount_ratio(inputs.amount))
    return cs_zscore((0.60 * reversal) + (0.40 * quiet_amount))


def _down_volume_pressure_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    returns = _returns(inputs.close)
    down_pressure = ts_mean(_amount_ratio(inputs.amount) * returns.clip(upper=0.0).abs(), 20, min_periods=15)
    return cs_zscore(-down_pressure)


def _up_volume_exhaustion_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    returns = _returns(inputs.close)
    up_exhaustion = ts_mean(_amount_ratio(inputs.amount) * returns.clip(lower=0.0), 20, min_periods=15)
    reversal = cs_zscore(-_ret(inputs.close, 20))
    return cs_zscore((0.45 * reversal) + (0.55 * cs_zscore(-up_exhaustion)))


def _volume_volatility_corr_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return cs_zscore(-correlation(_amount_ratio(inputs.amount).abs(), _returns(inputs.close).abs(), 20, min_periods=15))


def _amount_trend_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    short_amount = ts_mean(inputs.amount, 5, min_periods=3)
    long_amount = ts_mean(inputs.amount, 20, min_periods=15).replace(0.0, pd.NA)
    trend = short_amount / long_amount - 1.0
    return cs_zscore(-delta(trend, 5))


def _price_volume_rank_gap_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    volume_rank = ts_rank(inputs.volume, 10, min_periods=7)
    price_rank = ts_rank(inputs.close, 10, min_periods=7)
    return cs_zscore(volume_rank - price_rank)


def _liquidity_dryup_lowvol_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    quiet_amount = cs_zscore(-_amount_ratio(inputs.amount))
    lowvol = cs_zscore(-ts_std(_returns(inputs.close), 20, min_periods=15))
    return cs_zscore((0.45 * quiet_amount) + (0.55 * lowvol))


def _daily_range(inputs: DailyFactorInputs) -> pd.DataFrame:
    return (inputs.high - inputs.low).replace(0.0, float("nan"))


def _close_location(inputs: DailyFactorInputs) -> pd.DataFrame:
    return (inputs.close - inputs.low) / _daily_range(inputs)


def _upper_shadow_reversal_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    body_top = pd.concat([inputs.open.stack(), inputs.close.stack()], axis=1).max(axis=1).unstack()
    upper_shadow = (inputs.high - body_top) / _daily_range(inputs)
    return cs_zscore(-ts_mean(upper_shadow, 10, min_periods=7))


def _lower_shadow_support_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    body_bottom = pd.concat([inputs.open.stack(), inputs.close.stack()], axis=1).min(axis=1).unstack()
    lower_shadow = (body_bottom - inputs.low) / _daily_range(inputs)
    return cs_zscore(ts_mean(lower_shadow, 10, min_periods=7))


def _close_to_high_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    rolling_high = inputs.high.rolling(20, min_periods=15).max()
    rolling_low = inputs.low.rolling(20, min_periods=15).min()
    span = (rolling_high - rolling_low).replace(0.0, pd.NA)
    return cs_zscore((inputs.close - rolling_low) / span)


def _close_to_low_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return cs_zscore(-_close_to_high_20d(inputs))


def _range_expansion_reversal_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    relative_range = _daily_range(inputs) / inputs.close.replace(0.0, float("nan"))
    expansion = relative_range / ts_mean(relative_range, 20, min_periods=15) - 1.0
    return cs_zscore(-ts_mean(expansion, 10, min_periods=7))


def _body_compression_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    body = (inputs.close - inputs.open).abs() / inputs.open.replace(0.0, float("nan"))
    return cs_zscore(-ts_mean(body, 10, min_periods=7))


def _gap_up_exhaustion_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    gap = inputs.open / inputs.close.shift(1) - 1.0
    weak_close = 1.0 - _close_location(inputs)
    exhaustion = gap.clip(lower=0.0) * weak_close
    return cs_zscore(-ts_mean(exhaustion, 10, min_periods=7))


def _gap_down_rebound_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    gap = inputs.open / inputs.close.shift(1) - 1.0
    rebound = gap.clip(upper=0.0).abs() * _close_location(inputs)
    return cs_zscore(ts_mean(rebound, 10, min_periods=7))


def _true_range_lowrisk_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    prev_close = inputs.close.shift(1)
    true_range = pd.concat(
        [
            (inputs.high - inputs.low).stack(),
            (inputs.high - prev_close).abs().stack(),
            (inputs.low - prev_close).abs().stack(),
        ],
        axis=1,
    ).max(axis=1).unstack()
    atr = ts_mean(true_range / inputs.close.replace(0.0, float("nan")), 20, min_periods=15)
    return cs_zscore(-atr)


def _range_reversal_lowrisk_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    near_low_reversal = _close_to_low_reversal_20d(inputs)
    lowrisk = _true_range_lowrisk_20d(inputs)
    return cs_zscore((0.45 * near_low_reversal) + (0.55 * lowrisk))


def _weighted_blend(*weighted_frames: tuple[float, pd.DataFrame]) -> pd.DataFrame:
    total = None
    for weight, frame in weighted_frames:
        leg = cs_zscore(frame)
        total = leg * weight if total is None else total.add(leg * weight, fill_value=0.0)
    return cs_zscore(total)


def _defensive_lowrisk_ensemble_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.30, _lowvol_reversal_20d(inputs)),
        (0.25, _downside_volatility_reversal_20d(inputs)),
        (0.25, _liquidity_dryup_lowvol_20d(inputs)),
        (0.20, _true_range_lowrisk_20d(inputs)),
    )


def _crowding_exhaustion_ensemble_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.30, _amount_crowding_reversal_20d(inputs)),
        (0.30, _price_amount_corr_reversal_20d(inputs)),
        (0.25, _up_volume_exhaustion_reversal_20d(inputs)),
        (0.15, _volume_acceleration_reversal_20d(inputs)),
    )


def _quiet_range_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.45, _quiet_reversal_20d(inputs)),
        (0.35, _range_reversal_lowrisk_20d(inputs)),
        (0.20, _close_to_low_reversal_20d(inputs)),
    )


def _lowrisk_intraday_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.45, _intraday_reversal_10d(inputs)),
        (0.35, _true_range_lowrisk_20d(inputs)),
        (0.20, _body_compression_10d(inputs)),
    )


def _anti_momentum_crowding_60d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.35, -_risk_adjusted_momentum_60_skip20(inputs)),
        (0.25, -_risk_adjusted_momentum_120_skip20(inputs)),
        (0.25, _price_amount_corr_reversal_20d(inputs)),
        (0.15, _up_volume_exhaustion_reversal_20d(inputs)),
    )


def _compression_lowrisk_ensemble_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.30, _range_compression_20d(inputs)),
        (0.25, _body_compression_10d(inputs)),
        (0.25, _true_range_lowrisk_20d(inputs)),
        (0.20, _liquidity_dryup_lowvol_20d(inputs)),
    )


def _gap_execution_risk_ensemble_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.40, _gap_up_exhaustion_10d(inputs)),
        (0.35, -_gap_down_rebound_10d(inputs)),
        (0.25, _intraday_reversal_10d(inputs)),
    )


def _short_reversal_execution_ensemble_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.35, _short_reversal_5d(inputs)),
        (0.30, _amount_surge_reversal_5d(inputs)),
        (0.20, _upper_shadow_reversal_10d(inputs)),
        (0.15, _gap_execution_risk_ensemble_10d(inputs)),
    )


def _medium_defensive_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.35, _medium_reversal_20d(inputs)),
        (0.30, _downside_volatility_reversal_20d(inputs)),
        (0.20, _true_range_lowrisk_20d(inputs)),
        (0.15, _range_reversal_lowrisk_20d(inputs)),
    )


def _defensive_crowding_combo_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.55, _defensive_lowrisk_ensemble_20d(inputs)),
        (0.30, _crowding_exhaustion_ensemble_20d(inputs)),
        (0.15, _quiet_range_reversal_20d(inputs)),
    )


def _liquidity_adjusted_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    reversal = cs_zscore(-_ret(inputs.close, 20))
    quiet_amount = cs_zscore(-_amount_ratio(inputs.amount, 20))
    return cs_zscore((0.65 * reversal) + (0.35 * quiet_amount))


def _market_return_matrix(close: pd.DataFrame) -> pd.DataFrame:
    returns = _returns(close)
    market_return = returns.mean(axis=1)
    return pd.DataFrame({column: market_return for column in close.columns}, index=close.index)


def _low_market_beta_reversal_60d(inputs: DailyFactorInputs) -> pd.DataFrame:
    returns = _returns(inputs.close)
    market_returns = _market_return_matrix(inputs.close)
    beta_proxy = correlation(returns, market_returns, 60, min_periods=40)
    reversal = cs_zscore(-_ret(inputs.close, 20))
    return cs_zscore((0.55 * cs_zscore(-beta_proxy)) + (0.45 * reversal))


def _volatility_trend_reversal_60d(inputs: DailyFactorInputs) -> pd.DataFrame:
    volatility_20d = ts_std(_returns(inputs.close), 20, min_periods=15)
    volatility_60d = ts_mean(volatility_20d, 60, min_periods=40).replace(0.0, float("nan"))
    volatility_trend = volatility_20d / volatility_60d - 1.0
    return cs_zscore(-volatility_trend)


def _price_efficiency_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    gross_path = _returns(inputs.close).abs().rolling(20, min_periods=15).sum().replace(0.0, float("nan"))
    net_path = _ret(inputs.close, 20).abs()
    efficiency = net_path / gross_path
    reversal = cs_zscore(-_ret(inputs.close, 20))
    return cs_zscore((0.55 * reversal) + (0.45 * cs_zscore(-efficiency)))


def _short_down_amount_pressure_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    returns = _returns(inputs.close)
    pressure = _amount_ratio(inputs.amount, 10) * returns.clip(upper=0.0).abs()
    return cs_zscore(-ts_mean(pressure, 10, min_periods=7))


def _upper_shadow_crowding_reversal_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    body_top = pd.concat([inputs.open.stack(), inputs.close.stack()], axis=1).max(axis=1).unstack()
    upper_shadow = (inputs.high - body_top) / _daily_range(inputs)
    crowded_shadow = upper_shadow * (1.0 + _amount_ratio(inputs.amount, 10).clip(lower=0.0))
    return cs_zscore(-ts_mean(crowded_shadow, 10, min_periods=7))


def _overnight_lowrisk_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    overnight = inputs.open / inputs.close.shift(1) - 1.0
    overnight_reversal = cs_zscore(-ts_mean(overnight, 20, min_periods=15))
    overnight_lowrisk = cs_zscore(-ts_std(overnight, 20, min_periods=15))
    return cs_zscore((0.55 * overnight_reversal) + (0.45 * overnight_lowrisk))


def _close_location_crowding_reversal_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    close_location = _close_location(inputs)
    amount_surge = _amount_ratio(inputs.amount, 10).clip(lower=0.0)
    crowded_high_close = close_location * (1.0 + amount_surge)
    return cs_zscore(-ts_mean(crowded_high_close, 10, min_periods=7))


def _volume_dryup_rebound_5d(inputs: DailyFactorInputs) -> pd.DataFrame:
    short_reversal = cs_zscore(-_ret(inputs.close, 5))
    amount_dryup = cs_zscore(-_amount_ratio(inputs.amount, 5))
    return cs_zscore((0.55 * short_reversal) + (0.45 * amount_dryup))


def _execution_safe_defensive_combo_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.40, _defensive_lowrisk_ensemble_20d(inputs)),
        (0.25, _overnight_lowrisk_reversal_20d(inputs)),
        (0.20, _upper_shadow_crowding_reversal_10d(inputs)),
        (0.15, _gap_execution_risk_ensemble_10d(inputs)),
    )


def _long_reversal_60d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return cs_zscore(-_ret(inputs.close, 60))


def _volatility_adjusted_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    reversal = -_ret(inputs.close, 20)
    volatility = ts_std(_returns(inputs.close), 20, min_periods=15).replace(0.0, float("nan"))
    return cs_zscore(reversal / volatility)


def _market_residual_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    returns_20d = _ret(inputs.close, 20)
    market_20d = returns_20d.mean(axis=1)
    market_frame = pd.DataFrame({column: market_20d for column in inputs.close.columns}, index=inputs.close.index)
    residual = returns_20d - market_frame
    return cs_zscore(-residual)


def _amount_dryup_price_stability_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    quiet_amount = cs_zscore(-_amount_ratio(inputs.amount, 20))
    stable_price = cs_zscore(-ts_std(_returns(inputs.close), 10, min_periods=7))
    return cs_zscore((0.55 * quiet_amount) + (0.45 * stable_price))


def _smooth_trend_quality_60d(inputs: DailyFactorInputs) -> pd.DataFrame:
    gross_path = _returns(inputs.close).abs().rolling(60, min_periods=40).sum().replace(0.0, float("nan"))
    net_return = _ret(inputs.close, 60)
    return cs_zscore(net_return / gross_path)


def _range_expansion_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    relative_range = _daily_range(inputs) / inputs.close.replace(0.0, float("nan"))
    expansion = relative_range / ts_mean(relative_range, 60, min_periods=40).replace(0.0, float("nan")) - 1.0
    return cs_zscore(-ts_mean(expansion, 20, min_periods=15))


def _lower_shadow_lowrisk_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    lower_support = _lower_shadow_support_10d(inputs)
    lowrisk = _true_range_lowrisk_20d(inputs)
    return cs_zscore((0.55 * lower_support) + (0.45 * lowrisk))


def _gap_volatility_lowrisk_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    overnight = inputs.open / inputs.close.shift(1) - 1.0
    return cs_zscore(-ts_std(overnight, 20, min_periods=15))


def _volume_decay_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    reversal = cs_zscore(-_ret(inputs.close, 20))
    decayed_uncrowded = cs_zscore(-decay_linear(_amount_ratio(inputs.amount, 20), 10, min_periods=7))
    return cs_zscore((0.55 * reversal) + (0.45 * decayed_uncrowded))


def _defensive_residual_combo_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.35, _market_residual_reversal_20d(inputs)),
        (0.30, _volatility_adjusted_reversal_20d(inputs)),
        (0.20, _amount_dryup_price_stability_20d(inputs)),
        (0.15, _true_range_lowrisk_20d(inputs)),
    )


def _momentum_reversal_blend_120d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.55, -_risk_adjusted_momentum_120_skip20(inputs)),
        (0.45, _long_reversal_60d(inputs)),
    )


def _amount_shock_reversal_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    shock = _amount_ratio(inputs.amount, 10).abs()
    overreaction = cs_zscore(_ret(inputs.close, 10))
    return cs_zscore(-(shock * overreaction))


def _close_location_lowrisk_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    high_close_risk = cs_zscore(-ts_mean(_close_location(inputs), 20, min_periods=15))
    return cs_zscore((0.45 * high_close_risk) + (0.55 * _true_range_lowrisk_20d(inputs)))


def _high_low_skew_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    up_range = (inputs.high - inputs.close.shift(1)).clip(lower=0.0) / inputs.close.shift(1).replace(0.0, float("nan"))
    down_range = (inputs.close.shift(1) - inputs.low).clip(lower=0.0) / inputs.close.shift(1).replace(0.0, float("nan"))
    skew = ts_mean(up_range - down_range, 20, min_periods=15)
    return cs_zscore(-skew)


def _intraday_volatility_lowrisk_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    intraday = inputs.close / inputs.open - 1.0
    return cs_zscore(-ts_std(intraday, 20, min_periods=15))


def _negative_gap_exhaustion_reversal_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    gap = inputs.open / inputs.close.shift(1) - 1.0
    weak_close = 1.0 - _close_location(inputs)
    exhaustion = gap.clip(upper=0.0).abs() * weak_close
    return cs_zscore(-ts_mean(exhaustion, 10, min_periods=7))


def _volume_price_corr_lowrisk_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.60, _price_amount_corr_reversal_20d(inputs)),
        (0.40, _low_volatility_20d(inputs)),
    )


def _amount_volatility_lowrisk_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    amount_vol = ts_std(_amount_ratio(inputs.amount, 20), 20, min_periods=15)
    price_vol = ts_std(_returns(inputs.close), 20, min_periods=15)
    return cs_zscore((0.50 * cs_zscore(-amount_vol)) + (0.50 * cs_zscore(-price_vol)))


def _rebound_quality_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    drawdown = inputs.close / inputs.close.rolling(20, min_periods=15).max() - 1.0
    drawdown_vol = ts_std(drawdown, 20, min_periods=15)
    return cs_zscore((0.60 * cs_zscore(-_ret(inputs.close, 20))) + (0.40 * cs_zscore(-drawdown_vol)))


def _defensive_execution_combo_v2_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.35, _execution_safe_defensive_combo_20d(inputs)),
        (0.25, _amount_volatility_lowrisk_20d(inputs)),
        (0.20, _negative_gap_exhaustion_reversal_10d(inputs)),
        (0.20, _volume_price_corr_lowrisk_20d(inputs)),
    )


def _amount_dryup_60d(inputs: DailyFactorInputs) -> pd.DataFrame:
    amount_ratio = inputs.amount / ts_mean(inputs.amount, 60, min_periods=40).replace(0.0, float("nan")) - 1.0
    return cs_zscore(-amount_ratio)


def _return_skew_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    skew = _returns(inputs.close).rolling(20, min_periods=15).skew()
    return cs_zscore(-skew)


def _downside_amount_exhaustion_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    returns = _returns(inputs.close)
    down_exhaustion = ts_mean(_amount_ratio(inputs.amount, 20).clip(lower=0.0) * returns.clip(upper=0.0).abs(), 20, min_periods=15)
    return cs_zscore(-down_exhaustion)


def _upper_shadow_lowrisk_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.55, _upper_shadow_reversal_10d(inputs)),
        (0.45, _true_range_lowrisk_20d(inputs)),
    )


def _amount_return_beta_reversal_60d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return cs_zscore(-correlation(_returns(inputs.close), _amount_ratio(inputs.amount, 20), 60, min_periods=40))


def _opening_gap_crowding_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    gap = inputs.open / inputs.close.shift(1) - 1.0
    crowding = _amount_ratio(inputs.amount, 10).clip(lower=0.0)
    return cs_zscore(-ts_mean(gap.clip(lower=0.0) * (1.0 + crowding), 10, min_periods=7))


def _vwap_proxy_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    vwap_proxy = inputs.amount / inputs.volume.replace(0.0, float("nan"))
    premium = inputs.close / vwap_proxy.replace(0.0, float("nan")) - 1.0
    return cs_zscore(-ts_mean(premium, 20, min_periods=15))


def _range_position_voladjusted_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    range_reversal = _close_to_low_reversal_20d(inputs)
    lowrisk = _true_range_lowrisk_20d(inputs)
    return cs_zscore((0.60 * range_reversal) + (0.40 * lowrisk))


def _short_overreaction_3d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return cs_zscore(-_ret(inputs.close, 3))


def _balanced_crowding_residual_combo_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.30, _market_residual_reversal_20d(inputs)),
        (0.30, _crowding_exhaustion_ensemble_20d(inputs)),
        (0.20, _opening_gap_crowding_10d(inputs)),
        (0.20, _amount_return_beta_reversal_60d(inputs)),
    )


def _monthly_reversal_lowrisk_40d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.55, cs_zscore(-_ret(inputs.close, 40))),
        (0.45, _low_volatility_20d(inputs)),
    )


def _quarterly_anti_momentum_120d(inputs: DailyFactorInputs) -> pd.DataFrame:
    momentum = inputs.close.shift(20) / inputs.close.shift(120) - 1.0
    return cs_zscore(-momentum)


def _amount_persistence_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    persistent_amount = ts_mean(_amount_ratio(inputs.amount, 20).clip(lower=0.0), 20, min_periods=15)
    return cs_zscore(-persistent_amount)


def _volume_stability_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    mean_volume = ts_mean(inputs.volume, 20, min_periods=15).replace(0.0, float("nan"))
    volume_cv = ts_std(inputs.volume, 20, min_periods=15) / mean_volume
    return cs_zscore(-volume_cv)


def _price_acceleration_reversal_10d(inputs: DailyFactorInputs) -> pd.DataFrame:
    acceleration = delta(_ret(inputs.close, 5), 5)
    return cs_zscore(-acceleration)


def _low_drawdown_60d(inputs: DailyFactorInputs) -> pd.DataFrame:
    drawdown = inputs.close / inputs.close.rolling(60, min_periods=40).max() - 1.0
    return cs_zscore(drawdown)


def _drawdown_rebound_quality_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    drawdown = inputs.close / inputs.close.rolling(20, min_periods=15).max() - 1.0
    reversal = cs_zscore(-_ret(inputs.close, 20))
    return cs_zscore((0.50 * reversal) + (0.50 * cs_zscore(drawdown)))


def _breakout_failure_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    rolling_high = inputs.high.rolling(20, min_periods=15).max()
    close_slippage = inputs.close / rolling_high.replace(0.0, float("nan")) - 1.0
    return cs_zscore(close_slippage)


def _liquidity_shock_absorption_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    shock = _amount_ratio(inputs.amount, 20).abs()
    stable_return = -_returns(inputs.close).abs()
    return cs_zscore(ts_mean(shock * stable_return, 20, min_periods=15))


def _all_weather_defensive_combo_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.30, _defensive_lowrisk_ensemble_20d(inputs)),
        (0.25, _defensive_residual_combo_20d(inputs)),
        (0.20, _amount_volatility_lowrisk_20d(inputs)),
        (0.15, _gap_volatility_lowrisk_20d(inputs)),
        (0.10, _volume_stability_20d(inputs)),
    )


def _research_gatekeeper_shadow_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _weighted_blend(
        (0.30, _all_weather_defensive_combo_20d(inputs)),
        (0.25, _defensive_crowding_combo_20d(inputs)),
        (0.20, _balanced_crowding_residual_combo_20d(inputs)),
        (0.15, _defensive_execution_combo_v2_20d(inputs)),
        (0.10, _short_reversal_execution_ensemble_10d(inputs)),
    )
