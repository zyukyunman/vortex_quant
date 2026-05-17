"""Shared strategy-spec resolution for earnings-forecast live runners."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd

from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend
from vortex.research.alpha101_registry import DailyFactorInputs, compute_formula
from vortex.research.cogalpha.recipes import executable_recipe_by_template
from vortex.runtime.workspace import Workspace
from vortex.strategy.factor_fusion import CandidateFusionRecipe, FusionLeg, build_fused_candidate_signal


@dataclass(frozen=True)
class EarningsForecastOverlaySpec:
    """One overlay applied on top of a base earnings-forecast preset."""

    name: str
    role: str
    factor_template: str | None
    overlay_type: str
    weight: float = 0.0
    candidate_pool_size: int = 160
    filter_quantile: float = 0.0
    secondary_factor_template: str | None = None
    secondary_weight: float = 0.0
    penalty_strength: float = 0.0


@dataclass(frozen=True)
class EarningsForecastStrategySpec:
    """Resolved live/shadow strategy version."""

    name: str
    base_preset_name: str
    overlay: EarningsForecastOverlaySpec | None = None

    def diagnostics(self) -> dict[str, object]:
        overlay = self.overlay
        return {
            "name": self.name,
            "base_preset": self.base_preset_name,
            "overlay_variant": overlay.name if overlay is not None else None,
            "overlay_type": overlay.overlay_type if overlay is not None else None,
            "factor_template": overlay.factor_template if overlay is not None else None,
            "penalty_strength": overlay.penalty_strength if overlay is not None else None,
        }


TAIL_RISK_SOFT_Q10_P25 = EarningsForecastOverlaySpec(
    name="tail_risk_soft_q10_p25",
    role="tail_risk_soft_trim",
    factor_template="tail_risk_downside_vol_20d",
    overlay_type="soft_penalty",
    filter_quantile=0.10,
    penalty_strength=0.25,
)

PROMOTED_OVERLAY_SPECS: dict[str, EarningsForecastOverlaySpec] = {
    TAIL_RISK_SOFT_Q10_P25.name: TAIL_RISK_SOFT_Q10_P25,
}


def resolve_earnings_forecast_strategy_spec(
    name: str,
    *,
    base_preset_names: Iterable[str],
) -> EarningsForecastStrategySpec:
    """Resolve a public strategy version into a base preset plus optional overlay."""

    strategy_name = str(name).strip()
    base_names = tuple(str(item) for item in base_preset_names)
    if strategy_name in base_names:
        return EarningsForecastStrategySpec(name=strategy_name, base_preset_name=strategy_name)
    if strategy_name in PROMOTED_OVERLAY_SPECS:
        base_preset_name = "baseline_top110_large"
        if base_preset_name not in base_names:
            raise KeyError(
                f"strategy {strategy_name} requires base preset {base_preset_name}; "
                f"allowed base presets: {', '.join(sorted(base_names))}"
            )
        return EarningsForecastStrategySpec(
            name=strategy_name,
            base_preset_name=base_preset_name,
            overlay=PROMOTED_OVERLAY_SPECS[strategy_name],
        )
    allowed = ", ".join(sorted((*base_names, *PROMOTED_OVERLAY_SPECS)))
    raise KeyError(f"unknown earnings forecast strategy preset: {strategy_name}; allowed: {allowed}")


def list_earnings_forecast_strategy_names(*, base_preset_names: Iterable[str]) -> list[str]:
    names = [str(item) for item in base_preset_names]
    for overlay_name in PROMOTED_OVERLAY_SPECS:
        if overlay_name not in names:
            names.append(overlay_name)
    return names


def build_earnings_forecast_strategy_signal(
    base_signal: pd.DataFrame,
    *,
    workspace: Workspace,
    start: str,
    end: str,
    spec: EarningsForecastStrategySpec,
) -> pd.DataFrame:
    """Apply the resolved overlay to a base preset signal matrix."""

    if spec.overlay is None:
        return base_signal
    factor_inputs = load_daily_factor_inputs_for_strategy(workspace, start=start, end=end)
    factors = compute_earnings_forecast_overlay_factors(factor_inputs, (spec.overlay,))
    return apply_earnings_forecast_overlay_signal(base_signal, factors, spec.overlay)


def compute_earnings_forecast_overlay_factors(
    inputs: DailyFactorInputs,
    overlays: Iterable[EarningsForecastOverlaySpec],
) -> dict[str, pd.DataFrame]:
    templates = sorted({template for overlay in overlays for template in _overlay_factor_templates(overlay)})
    factors: dict[str, pd.DataFrame] = {}
    for template_id in templates:
        recipe = executable_recipe_by_template(template_id)
        factors[template_id] = compute_formula(recipe.build_formula_spec(), inputs)
    return factors


def apply_earnings_forecast_overlay_signal(
    base_signal: pd.DataFrame,
    factors: dict[str, pd.DataFrame],
    overlay: EarningsForecastOverlaySpec,
) -> pd.DataFrame:
    if overlay.overlay_type == "baseline":
        return base_signal
    if overlay.factor_template is None:
        raise ValueError(f"overlay requires factor_template: {overlay.name}")
    factor = _as_visible_factor(
        factors[overlay.factor_template],
        index=base_signal.index,
        columns=base_signal.columns,
    )
    if overlay.overlay_type == "candidate_rerank":
        return build_fused_candidate_signal(
            base_signal,
            {overlay.factor_template: factor},
            CandidateFusionRecipe(
                candidate_pool_size=overlay.candidate_pool_size,
                base_weight=1.0,
                legs=(FusionLeg(overlay.factor_template, overlay.weight),),
            ),
        )
    if overlay.overlay_type == "candidate_rerank_combo":
        if overlay.secondary_factor_template is None:
            raise ValueError(f"combo overlay requires secondary_factor_template: {overlay.name}")
        secondary = _as_visible_factor(
            factors[overlay.secondary_factor_template],
            index=base_signal.index,
            columns=base_signal.columns,
        )
        return build_fused_candidate_signal(
            base_signal,
            {
                overlay.factor_template: factor,
                overlay.secondary_factor_template: secondary,
            },
            CandidateFusionRecipe(
                candidate_pool_size=overlay.candidate_pool_size,
                base_weight=1.0,
                legs=(
                    FusionLeg(overlay.factor_template, overlay.weight),
                    FusionLeg(overlay.secondary_factor_template, overlay.secondary_weight),
                ),
            ),
        )
    if overlay.overlay_type == "bottom_filter":
        percentile = factor.rank(axis=1, pct=True)
        return base_signal.where(percentile.isna() | (percentile >= overlay.filter_quantile))
    if overlay.overlay_type == "soft_penalty":
        percentile = factor.rank(axis=1, pct=True)
        penalty_mask = percentile.notna() & (percentile < overlay.filter_quantile)
        return base_signal.where(~penalty_mask, base_signal * (1.0 - overlay.penalty_strength))
    if overlay.overlay_type == "soft_penalty_ramp":
        percentile = factor.rank(axis=1, pct=True)
        active = percentile.notna() & (percentile < overlay.filter_quantile)
        scaled = (percentile / overlay.filter_quantile).clip(lower=0.0, upper=1.0)
        multiplier = 1.0 - overlay.penalty_strength * (1.0 - scaled)
        return base_signal.where(~active, base_signal * multiplier)
    raise ValueError(f"unknown overlay_type: {overlay.overlay_type}")


def _as_visible_factor(
    factor: pd.DataFrame,
    *,
    index: pd.Index,
    columns: pd.Index,
) -> pd.DataFrame:
    """Align factor values to the signal matrix and expose only T-1 data on T."""

    return factor.reindex(index=index, columns=columns).shift(1)


def load_daily_factor_inputs_for_strategy(workspace: Workspace, *, start: str, end: str) -> DailyFactorInputs:
    """Load daily bars into the wide matrices required by CogAlpha recipes."""

    storage = ParquetDuckDBBackend(workspace.data_dir)
    bars = storage.read("bars", filters={"date": (">=", int(start))})
    bars = _filter_date_frame(bars, start=start, end=end)
    if bars.empty:
        raise ValueError("dataset is empty or missing: bars")
    required = {"date", "symbol", "open", "close", "amount"}
    missing = sorted(required - set(bars.columns))
    if missing:
        raise ValueError(f"bars missing required columns: {', '.join(missing)}")

    bars = bars.copy()
    if "high" not in bars.columns:
        bars["high"] = bars[["open", "close"]].max(axis=1)
    if "low" not in bars.columns:
        bars["low"] = bars[["open", "close"]].min(axis=1)
    if "volume" not in bars.columns:
        close = pd.to_numeric(bars["close"], errors="coerce").replace(0.0, np.nan)
        bars["volume"] = pd.to_numeric(bars["amount"], errors="coerce") / close

    return DailyFactorInputs(
        open=_pivot_market_frame(bars, "open"),
        high=_pivot_market_frame(bars, "high"),
        low=_pivot_market_frame(bars, "low"),
        close=_pivot_market_frame(bars, "close"),
        volume=_pivot_market_frame(bars, "volume"),
        amount=_pivot_market_frame(bars, "amount"),
    )


def _overlay_factor_templates(overlay: EarningsForecastOverlaySpec) -> tuple[str, ...]:
    return tuple(
        template
        for template in (overlay.factor_template, overlay.secondary_factor_template)
        if template is not None
    )


def _filter_date_frame(frame: pd.DataFrame, *, start: str, end: str) -> pd.DataFrame:
    if frame.empty or "date" not in frame.columns:
        return frame
    dates = pd.to_numeric(frame["date"], errors="coerce")
    mask = dates.between(int(start), int(end), inclusive="both")
    return frame.loc[mask].copy()


def _pivot_market_frame(frame: pd.DataFrame, value: str) -> pd.DataFrame:
    data = frame[["date", "symbol", value]].copy()
    data["date"] = data["date"].astype(str)
    data[value] = pd.to_numeric(data[value], errors="coerce")
    pivot = data.pivot_table(index="date", columns="symbol", values=value, aggfunc="last")
    return pivot.sort_index()


__all__ = [
    "EarningsForecastOverlaySpec",
    "EarningsForecastStrategySpec",
    "TAIL_RISK_SOFT_Q10_P25",
    "apply_earnings_forecast_overlay_signal",
    "build_earnings_forecast_strategy_signal",
    "compute_earnings_forecast_overlay_factors",
    "list_earnings_forecast_strategy_names",
    "load_daily_factor_inputs_for_strategy",
    "resolve_earnings_forecast_strategy_spec",
]
