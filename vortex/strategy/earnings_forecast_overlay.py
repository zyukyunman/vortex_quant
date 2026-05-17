"""Role-based factor overlay challenges for earnings-forecast presets."""
from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from vortex.research.alpha101_registry import compute_formula
from vortex.research.cogalpha.recipes import executable_recipe_by_template
from vortex.research.goal_review import ExperimentQuality
from vortex.runtime.workspace import Workspace
from vortex.strategy.earnings_forecast_cogalpha import load_daily_factor_inputs_for_cogalpha
from vortex.strategy.earnings_forecast_drift import open_to_close_returns
from vortex.strategy.earnings_forecast_runner import (
    DEFAULT_VERSION_REVIEW_LABEL,
    _build_version_signal_context,
    _jsonable,
    get_earnings_forecast_version_preset,
    load_earnings_forecast_inputs,
)
from vortex.strategy.event_backtest import EventBacktestConfig, run_event_signal_backtest
from vortex.strategy.factor_fusion import CandidateFusionRecipe, FusionLeg, build_fused_candidate_signal
from vortex.strategy.small_capital import SmallCapitalExecutionConfig, run_lot_constrained_backtest
from vortex.strategy.backtest import BacktestResult, _compute_metrics, review_backtest_metrics


DEFAULT_FACTOR_OVERLAY_LABEL = "业绩预告因子角色融合挑战"
DEFAULT_CPCV_BACKTEST_LABEL = "业绩预告tail-risk冻结CPCV回测"


@dataclass(frozen=True)
class OverlayVariant:
    """One role-based overlay experiment."""

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
class EarningsForecastFactorOverlayArtifacts:
    """Factor overlay challenge artifacts."""

    json_path: Path
    metrics_path: Path
    md_path: Path
    summary: dict[str, object]


@dataclass(frozen=True)
class EarningsForecastRobustnessArtifacts:
    """Robustness matrix artifacts for one promoted overlay."""

    json_path: Path
    matrix_path: Path
    md_path: Path
    summary: dict[str, object]


@dataclass(frozen=True)
class EarningsForecastCpcvBacktestArtifacts:
    """CPCV-style test artifacts for one frozen overlay challenger."""

    json_path: Path
    matrix_path: Path
    md_path: Path
    summary: dict[str, object]


@dataclass(frozen=True)
class EarningsForecastDailyMutationArtifacts:
    """Daily-only mutation-grid artifacts."""

    json_path: Path
    metrics_path: Path
    md_path: Path
    summary: dict[str, object]


@dataclass(frozen=True)
class EarningsForecastOverlayExecutionArtifacts:
    """Lot and minute-capacity execution review artifacts for an overlay."""

    json_path: Path
    metrics_path: Path
    md_path: Path
    summary: dict[str, object]


@dataclass(frozen=True)
class EarningsForecastRegimeBudgetArtifacts:
    """Daily regime-budget challenge artifacts."""

    json_path: Path
    metrics_path: Path
    md_path: Path
    summary: dict[str, object]


@dataclass(frozen=True)
class EarningsForecastPrvTargetPoolArtifacts:
    """PRV target-pool review artifacts."""

    json_path: Path
    factor_metrics_path: Path
    strategy_metrics_path: Path
    md_path: Path
    summary: dict[str, object]


DEFAULT_OVERLAY_VARIANTS: tuple[OverlayVariant, ...] = (
    OverlayVariant("baseline_top110_large", "baseline", None, "baseline"),
    OverlayVariant("rerank_range_vol_w005", "bad_holder_rerank", "range_vol_20d", "candidate_rerank", weight=0.05),
    OverlayVariant("rerank_range_vol_w010", "bad_holder_rerank", "range_vol_20d", "candidate_rerank", weight=0.10),
    OverlayVariant("rerank_tail_risk_w005", "bad_holder_rerank", "tail_risk_downside_vol_20d", "candidate_rerank", weight=0.05),
    OverlayVariant("rerank_tail_risk_w010", "bad_holder_rerank", "tail_risk_downside_vol_20d", "candidate_rerank", weight=0.10),
    OverlayVariant(
        "rerank_market_cycle_w005",
        "regime_execution_rerank",
        "market_cycle_relative_trend_60d",
        "candidate_rerank",
        weight=0.05,
    ),
    OverlayVariant(
        "rerank_market_cycle_w010",
        "regime_execution_rerank",
        "market_cycle_relative_trend_60d",
        "candidate_rerank",
        weight=0.10,
    ),
    OverlayVariant(
        "rerank_regime_lowvol_w005",
        "regime_execution_rerank",
        "regime_gated_trend_lowvol_60d",
        "candidate_rerank",
        weight=0.05,
    ),
    OverlayVariant(
        "rerank_vol_compression_w005",
        "regime_execution_rerank",
        "volatility_regime_compression_20d",
        "candidate_rerank",
        weight=0.05,
    ),
    OverlayVariant("filter_range_vol_bottom10", "bad_holder_filter", "range_vol_20d", "bottom_filter", filter_quantile=0.10),
    OverlayVariant("filter_tail_risk_bottom10", "bad_holder_filter", "tail_risk_downside_vol_20d", "bottom_filter", filter_quantile=0.10),
)


def run_earnings_forecast_factor_overlay_challenge(
    root: str | Path,
    *,
    preset_name: str = "baseline_top110_large",
    start: str,
    end: str,
    output_dir: str | Path | None = None,
    artifact_dir: str | Path | None = None,
    label: str = DEFAULT_FACTOR_OVERLAY_LABEL,
    require_precise_data: bool = True,
) -> EarningsForecastFactorOverlayArtifacts:
    """Challenge a preset with role-based CogAlpha factor overlays."""

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir) if output_dir is not None else workspace.strategy_dir
    artifact_root = Path(artifact_dir) if artifact_dir is not None else workspace.strategy_dir / "artifacts"
    output_root.mkdir(parents=True, exist_ok=True)
    artifact_root.mkdir(parents=True, exist_ok=True)

    preset = get_earnings_forecast_version_preset(preset_name)
    inputs = load_earnings_forecast_inputs(
        workspace,
        start=start,
        end=end,
        require_precise_data=require_precise_data,
    )
    factor_inputs = load_daily_factor_inputs_for_cogalpha(workspace, start=start, end=end)
    factors = _compute_overlay_factors(factor_inputs, DEFAULT_OVERLAY_VARIANTS)
    base_signal, market_gate, blocked_buy, blocked_sell = _build_version_signal_context(inputs, preset=preset)
    returns = open_to_close_returns(inputs.open_prices, inputs.close_prices)
    quality = ExperimentQuality(
        pit_safe=True,
        adjusted_prices=True,
        cost_included=True,
        no_future_leakage=True,
        out_of_sample_checked=True,
    )

    rows: list[dict[str, object]] = []
    for variant in DEFAULT_OVERLAY_VARIANTS:
        variant_signal = _build_overlay_signal(base_signal, factors, variant)
        backtest = run_event_signal_backtest(
            variant_signal,
            returns,
            EventBacktestConfig(
                top_n=preset.top_n,
                max_weight=preset.max_weight,
                target_exposure=1.0,
                transaction_cost_bps=preset.transaction_cost_bps,
                position_mode=preset.position_mode,
            ),
            market_gate=market_gate,
            blocked_buy_mask=blocked_buy,
            blocked_sell_mask=blocked_sell,
            quality=quality,
            goal_criteria=None,
        )
        rows.append(
            {
                "variant": variant.name,
                "preset": preset.name,
                "role": variant.role,
                "overlay_type": variant.overlay_type,
                "factor_template": variant.factor_template,
                "weight": variant.weight,
                "candidate_pool_size": variant.candidate_pool_size if variant.overlay_type == "candidate_rerank" else None,
                "filter_quantile": variant.filter_quantile if variant.overlay_type == "bottom_filter" else None,
                **backtest.metrics.__dict__,
            }
        )

    metrics = pd.DataFrame(rows)
    metrics = _classify_overlay_decisions(metrics)
    safe_label = _artifact_label(label)
    metrics_path = artifact_root / f"{safe_label}指标.csv"
    json_path = output_root / f"{safe_label}.json"
    md_path = output_root / f"{safe_label}.md"
    metrics.to_csv(metrics_path, index=False)
    summary = {
        "label": label,
        "preset": preset.name,
        "start": start,
        "end": end,
        "json_path": str(json_path),
        "metrics_path": str(metrics_path),
        "md_path": str(md_path),
        "variant_count": int(len(metrics)),
        "best_by_calmar": _best_variant(metrics, "calmar"),
        "best_by_annual_return": _best_variant(metrics, "annual_return"),
        "decisions": metrics["decision"].value_counts().to_dict(),
        "next_step": _overlay_next_step(metrics),
    }
    json_path.write_text(json.dumps(_jsonable(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_overlay_markdown(label, summary, metrics), encoding="utf-8")
    return EarningsForecastFactorOverlayArtifacts(
        json_path=json_path,
        metrics_path=metrics_path,
        md_path=md_path,
        summary=summary,
    )


def run_earnings_forecast_strategy_robustness_matrix(
    root: str | Path,
    *,
    preset_name: str = "baseline_top110_large",
    challenger_name: str = "rerank_tail_risk_w010",
    start: str,
    end: str,
    output_dir: str | Path | None = None,
    artifact_dir: str | Path | None = None,
    label: str = "业绩预告策略鲁棒性矩阵",
    require_precise_data: bool = True,
) -> EarningsForecastRobustnessArtifacts:
    """Run perturbation checks for a promoted overlay challenger."""

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir) if output_dir is not None else workspace.strategy_dir
    artifact_root = Path(artifact_dir) if artifact_dir is not None else workspace.strategy_dir / "artifacts"
    output_root.mkdir(parents=True, exist_ok=True)
    artifact_root.mkdir(parents=True, exist_ok=True)

    preset = get_earnings_forecast_version_preset(preset_name)
    challenger = _variant_by_name(challenger_name)
    inputs = load_earnings_forecast_inputs(
        workspace,
        start=start,
        end=end,
        require_precise_data=require_precise_data,
    )
    factor_inputs = load_daily_factor_inputs_for_cogalpha(workspace, start=start, end=end)
    factors = _compute_overlay_factors(factor_inputs, (challenger,))
    base_signal, market_gate, blocked_buy, blocked_sell = _build_version_signal_context(inputs, preset=preset)
    challenger_signal = _build_overlay_signal(base_signal, factors, challenger)
    returns = open_to_close_returns(inputs.open_prices, inputs.close_prices)
    quality = ExperimentQuality(
        pit_safe=True,
        adjusted_prices=True,
        cost_included=True,
        no_future_leakage=True,
        out_of_sample_checked=True,
    )

    rows: list[dict[str, object]] = []
    for scenario in _robustness_scenarios(start, end, preset_top_n=preset.top_n, base_cost=preset.transaction_cost_bps):
        scenario_start = str(scenario["start"])
        scenario_end = str(scenario["end"])
        for variant_name, signal in (("baseline", base_signal), (challenger.name, challenger_signal)):
            backtest = run_event_signal_backtest(
                _slice_by_date(signal, scenario_start, scenario_end),
                _slice_by_date(returns, scenario_start, scenario_end),
                EventBacktestConfig(
                    top_n=int(scenario["top_n"]),
                    max_weight=preset.max_weight,
                    target_exposure=1.0,
                    transaction_cost_bps=float(scenario["cost_bps"]),
                    position_mode=preset.position_mode,
                ),
                market_gate=_slice_series_by_date(market_gate, scenario_start, scenario_end),
                blocked_buy_mask=_slice_optional_frame_by_date(blocked_buy, scenario_start, scenario_end),
                blocked_sell_mask=_slice_optional_frame_by_date(blocked_sell, scenario_start, scenario_end),
                quality=quality,
                goal_criteria=None,
            )
            rows.append(
                {
                    "scenario": scenario["scenario"],
                    "scenario_type": scenario["scenario_type"],
                    "variant": variant_name,
                    "start": scenario_start,
                    "end": scenario_end,
                    "top_n": int(scenario["top_n"]),
                    "cost_bps": float(scenario["cost_bps"]),
                    **backtest.metrics.__dict__,
                }
            )
    matrix = _robustness_delta_frame(pd.DataFrame(rows), challenger.name)
    safe_label = _artifact_label(label)
    matrix_path = artifact_root / f"{safe_label}矩阵.csv"
    json_path = output_root / f"{safe_label}.json"
    md_path = output_root / f"{safe_label}.md"
    matrix.to_csv(matrix_path, index=False)
    summary = {
        "label": label,
        "preset": preset.name,
        "challenger": challenger.name,
        "start": start,
        "end": end,
        "json_path": str(json_path),
        "matrix_path": str(matrix_path),
        "md_path": str(md_path),
        "scenario_count": int(matrix["scenario"].nunique()) if not matrix.empty else 0,
        "challenger_win_rate_calmar": float((matrix["calmar_delta"] > 0).mean()) if not matrix.empty else 0.0,
        "worst_drawdown_delta": float(matrix["max_drawdown_delta"].min()) if not matrix.empty else None,
        "robustness_status": _robustness_status(matrix),
    }
    json_path.write_text(json.dumps(_jsonable(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_robustness_markdown(label, summary, matrix), encoding="utf-8")
    return EarningsForecastRobustnessArtifacts(
        json_path=json_path,
        matrix_path=matrix_path,
        md_path=md_path,
        summary=summary,
    )


def run_earnings_forecast_cpcv_backtest(
    root: str | Path,
    *,
    preset_name: str = "baseline_top110_large",
    reference_name: str | None = None,
    challenger_name: str = "tail_risk_soft_q10_p25",
    start: str,
    end: str,
    n_groups: int = 8,
    n_test_groups: int = 2,
    purge_horizon: int = 40,
    embargo: int = 20,
    output_dir: str | Path | None = None,
    artifact_dir: str | Path | None = None,
    label: str = DEFAULT_CPCV_BACKTEST_LABEL,
    require_precise_data: bool = True,
    max_combinations: int | None = None,
) -> EarningsForecastCpcvBacktestArtifacts:
    """Run CPCV-style out-of-sample checks for one frozen overlay challenger.

    The challenger parameters are fixed before this function runs. Train groups
    are therefore used for leakage accounting and overfit-gap context, not for
    re-fitting or selecting a new parameter.
    """

    if n_groups < 2:
        raise ValueError("n_groups must be at least 2")
    if n_test_groups <= 0 or n_test_groups >= n_groups:
        raise ValueError("n_test_groups must be in [1, n_groups)")
    if purge_horizon < 0:
        raise ValueError("purge_horizon must be non-negative")
    if embargo < 0:
        raise ValueError("embargo must be non-negative")

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir) if output_dir is not None else workspace.strategy_dir
    artifact_root = Path(artifact_dir) if artifact_dir is not None else workspace.strategy_dir / "artifacts"
    output_root.mkdir(parents=True, exist_ok=True)
    artifact_root.mkdir(parents=True, exist_ok=True)

    preset = get_earnings_forecast_version_preset(preset_name)
    challenger = _variant_by_name(challenger_name)
    reference = _variant_by_name(reference_name) if reference_name else OverlayVariant(preset.name, "baseline", None, "baseline")
    variants = (reference, challenger)
    inputs = load_earnings_forecast_inputs(
        workspace,
        start=start,
        end=end,
        require_precise_data=require_precise_data,
    )
    factor_inputs = load_daily_factor_inputs_for_cogalpha(workspace, start=start, end=end)
    factors = _compute_overlay_factors(factor_inputs, variants)
    base_signal, market_gate, blocked_buy, blocked_sell = _build_version_signal_context(inputs, preset=preset)
    returns = open_to_close_returns(inputs.open_prices, inputs.close_prices)
    quality = ExperimentQuality(
        pit_safe=True,
        adjusted_prices=True,
        cost_included=True,
        no_future_leakage=True,
        out_of_sample_checked=True,
    )
    config = EventBacktestConfig(
        top_n=preset.top_n,
        max_weight=preset.max_weight,
        target_exposure=1.0,
        transaction_cost_bps=preset.transaction_cost_bps,
        position_mode=preset.position_mode,
    )

    backtests: dict[str, BacktestResult] = {}
    for variant in variants:
        signal = _build_overlay_signal(base_signal, factors, variant)
        backtests[variant.name] = run_event_signal_backtest(
            signal,
            returns,
            config,
            market_gate=market_gate,
            blocked_buy_mask=blocked_buy,
            blocked_sell_mask=blocked_sell,
            quality=quality,
            goal_criteria=None,
        )

    baseline_result = backtests[reference.name]
    challenger_result = backtests[challenger.name]
    common_dates = baseline_result.returns.index.intersection(challenger_result.returns.index).sort_values()
    groups = _cpcv_contiguous_groups(common_dates, n_groups)
    combo_items = list(combinations(range(n_groups), n_test_groups))
    if max_combinations is not None:
        combo_items = combo_items[: max(0, int(max_combinations))]

    rows: list[dict[str, object]] = []
    for fold_index, test_group_ids in enumerate(combo_items, start=1):
        split = _cpcv_split_dates(
            common_dates,
            groups,
            test_group_ids,
            purge_horizon=purge_horizon,
            embargo=embargo,
        )
        test_dates = split["test_dates"]
        train_dates = split["train_dates"]
        row = {
            "fold": f"fold_{fold_index:02d}",
            "test_groups": ",".join(str(group_id + 1) for group_id in test_group_ids),
            "train_days": int(len(train_dates)),
            "test_days": int(len(test_dates)),
            "purged_or_embargoed_days": int(split["excluded_days"]),
            "test_start": _format_cpcv_date(test_dates[0]) if len(test_dates) else None,
            "test_end": _format_cpcv_date(test_dates[-1]) if len(test_dates) else None,
        }
        row.update(_cpcv_metric_delta_columns(baseline_result, challenger_result, test_dates, "test"))
        row.update(_cpcv_metric_delta_columns(baseline_result, challenger_result, train_dates, "train"))
        row["decision"] = _cpcv_fold_decision(row)
        rows.append(row)

    matrix = pd.DataFrame(rows)
    safe_label = _artifact_label(label)
    matrix_path = artifact_root / f"{safe_label}矩阵.csv"
    json_path = output_root / f"{safe_label}.json"
    md_path = output_root / f"{safe_label}.md"
    matrix.to_csv(matrix_path, index=False)

    full_baseline = baseline_result.metrics.__dict__
    full_challenger = challenger_result.metrics.__dict__
    group_rows = [
        {
            "group": int(group_id + 1),
            "start": _format_cpcv_date(group_dates[0]),
            "end": _format_cpcv_date(group_dates[-1]),
            "days": int(len(group_dates)),
        }
        for group_id, group_dates in enumerate(groups)
    ]
    summary = {
        "label": label,
        "preset": preset.name,
        "baseline": reference.name,
        "reference": reference.name,
        "reference_logic": _overlay_variant_logic(reference),
        "challenger": challenger.name,
        "challenger_logic": _overlay_variant_logic(challenger),
        "start": start,
        "end": end,
        "n_groups": int(n_groups),
        "n_test_groups": int(n_test_groups),
        "purge_horizon": int(purge_horizon),
        "embargo": int(embargo),
        "evaluation_mode": "frozen_full_path_test_mask",
        "trade_days": int(len(common_dates)),
        "first_trade_date": _format_cpcv_date(common_dates[0]) if len(common_dates) else None,
        "last_trade_date": _format_cpcv_date(common_dates[-1]) if len(common_dates) else None,
        "group_calendar": group_rows,
        "split_count": int(len(matrix)),
        "json_path": str(json_path),
        "matrix_path": str(matrix_path),
        "md_path": str(md_path),
        "full_sample": {
            "baseline": full_baseline,
            "challenger": full_challenger,
            "annual_return_delta": float(full_challenger["annual_return"] - full_baseline["annual_return"]),
            "max_drawdown_delta": float(full_challenger["max_drawdown"] - full_baseline["max_drawdown"]),
            "calmar_delta": float(full_challenger["calmar"] - full_baseline["calmar"]),
        },
        "cpcv_status": _cpcv_status(matrix),
    }
    json_path.write_text(json.dumps(_jsonable(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_cpcv_markdown(label, summary, matrix), encoding="utf-8")
    return EarningsForecastCpcvBacktestArtifacts(
        json_path=json_path,
        matrix_path=matrix_path,
        md_path=md_path,
        summary=summary,
    )


def run_earnings_forecast_daily_mutation_grid(
    root: str | Path,
    *,
    preset_name: str = "baseline_top110_large",
    start: str,
    end: str,
    output_dir: str | Path | None = None,
    artifact_dir: str | Path | None = None,
    label: str = "业绩预告日频tail-risk mutation网格",
    require_precise_data: bool = True,
) -> EarningsForecastDailyMutationArtifacts:
    """Run a daily-data-only tail-risk mutation grid against a preset."""

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir) if output_dir is not None else workspace.strategy_dir
    artifact_root = Path(artifact_dir) if artifact_dir is not None else workspace.strategy_dir / "artifacts"
    output_root.mkdir(parents=True, exist_ok=True)
    artifact_root.mkdir(parents=True, exist_ok=True)

    preset = get_earnings_forecast_version_preset(preset_name)
    variants = _tail_risk_mutation_variants()
    inputs = load_earnings_forecast_inputs(
        workspace,
        start=start,
        end=end,
        require_precise_data=require_precise_data,
    )
    factor_inputs = load_daily_factor_inputs_for_cogalpha(workspace, start=start, end=end)
    factors = _compute_overlay_factors(factor_inputs, variants)
    base_signal, market_gate, blocked_buy, blocked_sell = _build_version_signal_context(inputs, preset=preset)
    returns = open_to_close_returns(inputs.open_prices, inputs.close_prices)
    quality = ExperimentQuality(
        pit_safe=True,
        adjusted_prices=True,
        cost_included=True,
        no_future_leakage=True,
        out_of_sample_checked=True,
    )

    rows: list[dict[str, object]] = []
    for variant in variants:
        variant_signal = _build_overlay_signal(base_signal, factors, variant)
        backtest = run_event_signal_backtest(
            variant_signal,
            returns,
            EventBacktestConfig(
                top_n=preset.top_n,
                max_weight=preset.max_weight,
                target_exposure=1.0,
                transaction_cost_bps=preset.transaction_cost_bps,
                position_mode=preset.position_mode,
            ),
            market_gate=market_gate,
            blocked_buy_mask=blocked_buy,
            blocked_sell_mask=blocked_sell,
            quality=quality,
            goal_criteria=None,
        )
        rows.append(
            {
                "variant": variant.name,
                "preset": preset.name,
                "role": variant.role,
                "overlay_type": variant.overlay_type,
                "factor_template": variant.factor_template,
                "secondary_factor_template": variant.secondary_factor_template,
                "weight": variant.weight,
                "secondary_weight": variant.secondary_weight,
                "candidate_pool_size": variant.candidate_pool_size if "rerank" in variant.overlay_type else None,
                "filter_quantile": variant.filter_quantile if variant.overlay_type in {"bottom_filter", "soft_penalty"} else None,
                "penalty_strength": variant.penalty_strength if variant.overlay_type == "soft_penalty" else None,
                **backtest.metrics.__dict__,
            }
        )

    metrics = _classify_overlay_decisions(pd.DataFrame(rows))
    safe_label = _artifact_label(label)
    metrics_path = artifact_root / f"{safe_label}指标.csv"
    json_path = output_root / f"{safe_label}.json"
    md_path = output_root / f"{safe_label}.md"
    metrics.to_csv(metrics_path, index=False)
    summary = {
        "label": label,
        "preset": preset.name,
        "start": start,
        "end": end,
        "json_path": str(json_path),
        "metrics_path": str(metrics_path),
        "md_path": str(md_path),
        "variant_count": int(len(metrics)),
        "best_by_calmar": _best_variant(metrics, "calmar"),
        "best_by_annual_return": _best_variant(metrics, "annual_return"),
        "decisions": metrics["decision"].value_counts().to_dict(),
        "next_step": _mutation_next_step(metrics),
    }
    json_path.write_text(json.dumps(_jsonable(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_mutation_markdown(label, summary, metrics), encoding="utf-8")
    return EarningsForecastDailyMutationArtifacts(
        json_path=json_path,
        metrics_path=metrics_path,
        md_path=md_path,
        summary=summary,
    )


def run_earnings_forecast_overlay_execution_review(
    root: str | Path,
    *,
    preset_name: str = "baseline_top110_large",
    challenger_name: str = "tail_risk_soft_q10_p25",
    start: str,
    end: str,
    output_dir: str | Path | None = None,
    artifact_dir: str | Path | None = None,
    label: str = "业绩预告overlay整手分钟执行复核",
    capital_tiers: tuple[float, ...] = (10_000_000.0, 50_000_000.0, 100_000_000.0),
    participation_rates: tuple[float, ...] = (0.10, 0.20, 0.30),
    require_precise_data: bool = True,
) -> EarningsForecastOverlayExecutionArtifacts:
    """Review an overlay with lot execution and target-price minute capacity."""

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir) if output_dir is not None else workspace.strategy_dir
    artifact_root = Path(artifact_dir) if artifact_dir is not None else workspace.strategy_dir / "artifacts"
    output_root.mkdir(parents=True, exist_ok=True)
    artifact_root.mkdir(parents=True, exist_ok=True)

    preset = get_earnings_forecast_version_preset(preset_name)
    challenger = _variant_by_name(challenger_name)
    variants = (
        OverlayVariant(preset.name, "baseline", None, "baseline"),
        challenger,
    )
    inputs = load_earnings_forecast_inputs(
        workspace,
        start=start,
        end=end,
        require_precise_data=require_precise_data,
    )
    factor_inputs = load_daily_factor_inputs_for_cogalpha(workspace, start=start, end=end)
    factors = _compute_overlay_factors(factor_inputs, variants)
    base_signal, market_gate, blocked_buy, blocked_sell = _build_version_signal_context(inputs, preset=preset)
    returns = open_to_close_returns(inputs.open_prices, inputs.close_prices)
    quality = ExperimentQuality(
        pit_safe=True,
        adjusted_prices=True,
        cost_included=True,
        no_future_leakage=True,
        out_of_sample_checked=True,
    )

    rows: list[dict[str, object]] = []
    coverage_rows: list[dict[str, object]] = []
    for variant in variants:
        signal = _build_overlay_signal(base_signal, factors, variant)
        theory = run_event_signal_backtest(
            signal,
            returns,
            EventBacktestConfig(
                top_n=preset.top_n,
                max_weight=preset.max_weight,
                target_exposure=1.0,
                transaction_cost_bps=preset.transaction_cost_bps,
                position_mode=preset.position_mode,
            ),
            market_gate=market_gate,
            blocked_buy_mask=blocked_buy,
            blocked_sell_mask=blocked_sell,
            quality=quality,
            goal_criteria=None,
        )
        rows.append(
            _execution_metric_row(
                variant=variant.name,
                mode="event_theory",
                capital=None,
                participation_rate=None,
                metrics=theory.metrics.__dict__,
                order_summary={},
            )
        )
        limit_cache: dict[float, pd.DataFrame] = {}
        for participation_rate in participation_rates:
            limits, coverage = _build_minute_target_price_buy_limits(
                workspace,
                theory.weights,
                inputs.open_prices,
                participation_rate=float(participation_rate),
            )
            limit_cache[float(participation_rate)] = limits
            coverage_rows.append({"variant": variant.name, "participation_rate": float(participation_rate), **coverage})
        for capital in capital_tiers:
            lot = run_lot_constrained_backtest(
                theory.weights,
                inputs.open_prices,
                inputs.close_prices,
                SmallCapitalExecutionConfig(initial_cash=float(capital)),
                market_gate=market_gate,
                signal=signal,
            )
            rows.append(
                _execution_metric_row(
                    variant=variant.name,
                    mode="lot_unconstrained",
                    capital=float(capital),
                    participation_rate=None,
                    metrics=lot.metrics.__dict__,
                    order_summary=_buy_order_summary(lot.order_intents),
                    lot_summary=lot.summary,
                )
            )
            for participation_rate in participation_rates:
                constrained = run_lot_constrained_backtest(
                    theory.weights,
                    inputs.open_prices,
                    inputs.close_prices,
                    SmallCapitalExecutionConfig(initial_cash=float(capital), allow_partial_buy_fills=True),
                    market_gate=market_gate,
                    signal=signal,
                    buy_share_limits=limit_cache[float(participation_rate)],
                )
                rows.append(
                    _execution_metric_row(
                        variant=variant.name,
                        mode="minute_target_price_partial",
                        capital=float(capital),
                        participation_rate=float(participation_rate),
                        metrics=constrained.metrics.__dict__,
                        order_summary=_buy_order_summary(constrained.order_intents),
                        lot_summary=constrained.summary,
                    )
                )

    metrics = pd.DataFrame(rows)
    coverage = pd.DataFrame(coverage_rows)
    metrics = _add_execution_deltas(metrics)
    safe_label = _artifact_label(label)
    metrics_path = artifact_root / f"{safe_label}指标.csv"
    coverage_path = artifact_root / f"{safe_label}分钟覆盖.csv"
    json_path = output_root / f"{safe_label}.json"
    md_path = output_root / f"{safe_label}.md"
    metrics.to_csv(metrics_path, index=False)
    coverage.to_csv(coverage_path, index=False)
    summary = {
        "label": label,
        "preset": preset.name,
        "challenger": challenger.name,
        "start": start,
        "end": end,
        "capital_tiers": [float(value) for value in capital_tiers],
        "participation_rates": [float(value) for value in participation_rates],
        "json_path": str(json_path),
        "metrics_path": str(metrics_path),
        "coverage_path": str(coverage_path),
        "md_path": str(md_path),
        "best_challenger_rows": _best_execution_rows(metrics, challenger.name),
        "minute_coverage": coverage.to_dict(orient="records"),
        "decision": _execution_review_decision(metrics, challenger.name),
    }
    json_path.write_text(json.dumps(_jsonable(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_execution_review_markdown(label, summary, metrics, coverage), encoding="utf-8")
    return EarningsForecastOverlayExecutionArtifacts(
        json_path=json_path,
        metrics_path=metrics_path,
        md_path=md_path,
        summary=summary,
    )


def run_earnings_forecast_regime_budget_challenge(
    root: str | Path,
    *,
    preset_name: str = "baseline_top110_large",
    challenger_name: str = "tail_risk_soft_q10_p25",
    start: str,
    end: str,
    output_dir: str | Path | None = None,
    artifact_dir: str | Path | None = None,
    label: str = "业绩预告regime风险预算挑战",
    require_precise_data: bool = True,
) -> EarningsForecastRegimeBudgetArtifacts:
    """Test light daily market/regime exposure budgets on an overlay challenger."""

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir) if output_dir is not None else workspace.strategy_dir
    artifact_root = Path(artifact_dir) if artifact_dir is not None else workspace.strategy_dir / "artifacts"
    output_root.mkdir(parents=True, exist_ok=True)
    artifact_root.mkdir(parents=True, exist_ok=True)

    preset = get_earnings_forecast_version_preset(preset_name)
    challenger = _variant_by_name(challenger_name)
    inputs = load_earnings_forecast_inputs(workspace, start=start, end=end, require_precise_data=require_precise_data)
    factor_inputs = load_daily_factor_inputs_for_cogalpha(workspace, start=start, end=end)
    factors = _compute_overlay_factors(factor_inputs, (challenger,))
    base_signal, market_gate, blocked_buy, blocked_sell = _build_version_signal_context(inputs, preset=preset)
    challenger_signal = _build_overlay_signal(base_signal, factors, challenger)
    returns = open_to_close_returns(inputs.open_prices, inputs.close_prices)
    quality = ExperimentQuality(
        pit_safe=True,
        adjusted_prices=True,
        cost_included=True,
        no_future_leakage=True,
        out_of_sample_checked=True,
    )
    config = EventBacktestConfig(
        top_n=preset.top_n,
        max_weight=preset.max_weight,
        target_exposure=1.0,
        transaction_cost_bps=preset.transaction_cost_bps,
        position_mode=preset.position_mode,
    )
    base_backtest = run_event_signal_backtest(
        base_signal,
        returns,
        config,
        market_gate=market_gate,
        blocked_buy_mask=blocked_buy,
        blocked_sell_mask=blocked_sell,
        quality=quality,
        goal_criteria=None,
    )
    challenger_backtest = run_event_signal_backtest(
        challenger_signal,
        returns,
        config,
        market_gate=market_gate,
        blocked_buy_mask=blocked_buy,
        blocked_sell_mask=blocked_sell,
        quality=quality,
        goal_criteria=None,
    )

    rows = [
        {
            "variant": preset.name,
            "budget": "baseline_no_budget",
            "avg_exposure_scale": 1.0,
            "min_exposure_scale": 1.0,
            "scaled_day_rate": 0.0,
            **base_backtest.metrics.__dict__,
        },
        {
            "variant": challenger.name,
            "budget": "challenger_no_budget",
            "avg_exposure_scale": 1.0,
            "min_exposure_scale": 1.0,
            "scaled_day_rate": 0.0,
            **challenger_backtest.metrics.__dict__,
        },
    ]
    budgets = _regime_budget_scales(inputs.index_close, challenger_backtest.weights.index)
    for budget_name, scale in budgets.items():
        scaled = _run_scaled_weight_backtest(
            challenger_backtest.weights,
            returns,
            scale,
            transaction_cost_bps=preset.transaction_cost_bps,
            quality=quality,
        )
        rows.append(
            {
                "variant": challenger.name,
                "budget": budget_name,
                "avg_exposure_scale": float(scale.mean()),
                "min_exposure_scale": float(scale.min()),
                "scaled_day_rate": float((scale < 0.999).mean()),
                **scaled.metrics.__dict__,
            }
        )

    metrics = _classify_regime_budget_metrics(pd.DataFrame(rows), challenger.name)
    safe_label = _artifact_label(label)
    metrics_path = artifact_root / f"{safe_label}指标.csv"
    json_path = output_root / f"{safe_label}.json"
    md_path = output_root / f"{safe_label}.md"
    metrics.to_csv(metrics_path, index=False)
    summary = {
        "label": label,
        "preset": preset.name,
        "challenger": challenger.name,
        "start": start,
        "end": end,
        "json_path": str(json_path),
        "metrics_path": str(metrics_path),
        "md_path": str(md_path),
        "best_by_calmar": _best_regime_budget(metrics, "calmar"),
        "decision": _regime_budget_decision(metrics),
    }
    json_path.write_text(json.dumps(_jsonable(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_regime_budget_markdown(label, summary, metrics), encoding="utf-8")
    return EarningsForecastRegimeBudgetArtifacts(
        json_path=json_path,
        metrics_path=metrics_path,
        md_path=md_path,
        summary=summary,
    )


def run_earnings_forecast_prv_target_pool_review(
    root: str | Path,
    *,
    preset_name: str = "baseline_top110_large",
    challenger_name: str = "tail_risk_soft_q10_p25",
    start: str,
    end: str,
    output_dir: str | Path | None = None,
    artifact_dir: str | Path | None = None,
    label: str = "业绩预告PRV目标池复验",
    require_precise_data: bool = True,
) -> EarningsForecastPrvTargetPoolArtifacts:
    """Review existing PRV panels against baseline/challenger target pools."""

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir) if output_dir is not None else workspace.strategy_dir
    artifact_root = Path(artifact_dir) if artifact_dir is not None else workspace.strategy_dir / "artifacts"
    output_root.mkdir(parents=True, exist_ok=True)
    artifact_root.mkdir(parents=True, exist_ok=True)

    preset = get_earnings_forecast_version_preset(preset_name)
    challenger = _variant_by_name(challenger_name)
    inputs = load_earnings_forecast_inputs(workspace, start=start, end=end, require_precise_data=require_precise_data)
    factor_inputs = load_daily_factor_inputs_for_cogalpha(workspace, start=start, end=end)
    factors = _compute_overlay_factors(factor_inputs, (challenger,))
    base_signal, market_gate, blocked_buy, blocked_sell = _build_version_signal_context(inputs, preset=preset)
    challenger_signal = _build_overlay_signal(base_signal, factors, challenger)
    returns = open_to_close_returns(inputs.open_prices, inputs.close_prices)
    quality = ExperimentQuality(True, True, True, True, True)
    config = EventBacktestConfig(
        top_n=preset.top_n,
        max_weight=preset.max_weight,
        target_exposure=1.0,
        transaction_cost_bps=preset.transaction_cost_bps,
        position_mode=preset.position_mode,
    )
    baseline_bt = run_event_signal_backtest(
        base_signal,
        returns,
        config,
        market_gate=market_gate,
        blocked_buy_mask=blocked_buy,
        blocked_sell_mask=blocked_sell,
        quality=quality,
        goal_criteria=None,
    )
    challenger_bt = run_event_signal_backtest(
        challenger_signal,
        returns,
        config,
        market_gate=market_gate,
        blocked_buy_mask=blocked_buy,
        blocked_sell_mask=blocked_sell,
        quality=quality,
        goal_criteria=None,
    )
    target_sets = {
        preset.name: baseline_bt.weights,
        challenger.name: challenger_bt.weights,
    }
    panel_specs = _default_prv_panel_specs(workspace)
    factor_rows: list[dict[str, object]] = []
    strategy_rows: list[dict[str, object]] = [
        {"panel": "none", "variant": preset.name, "overlay": "baseline_no_prv", **baseline_bt.metrics.__dict__},
        {"panel": "none", "variant": challenger.name, "overlay": "challenger_no_prv", **challenger_bt.metrics.__dict__},
    ]
    for panel_name, panel_path in panel_specs:
        panel = pd.read_parquet(panel_path)
        factor_map = _prv_factor_columns(panel)
        if not factor_map:
            continue
        for variant, weights in target_sets.items():
            for role, column in factor_map.items():
                factor_frame = _panel_factor_frame(panel, column, weights.index, weights.columns)
                factor_rows.append(
                    {
                        "panel": panel_name,
                        "variant": variant,
                        "role": role,
                        "factor": column,
                        **_target_pool_factor_stats(weights, factor_frame, returns),
                    }
                )
        ridge_column = factor_map.get("ridge_risk")
        if ridge_column is not None:
            ridge = _panel_factor_frame(panel, ridge_column, challenger_bt.weights.index, challenger_bt.weights.columns)
            deweighted = _deweight_top_quantile(challenger_bt.weights, ridge, quantile=0.80, multiplier=0.50)
            deweighted_bt = _run_weight_matrix_backtest(
                deweighted,
                returns,
                transaction_cost_bps=preset.transaction_cost_bps,
                quality=quality,
            )
            strategy_rows.append(
                {
                    "panel": panel_name,
                    "variant": challenger.name,
                    "overlay": f"{ridge_column}_top20_half_holding",
                    **deweighted_bt.metrics.__dict__,
                }
            )

    factor_metrics = pd.DataFrame(factor_rows)
    strategy_metrics = _classify_prv_strategy_metrics(pd.DataFrame(strategy_rows), challenger.name)
    safe_label = _artifact_label(label)
    factor_path = artifact_root / f"{safe_label}因子指标.csv"
    strategy_path = artifact_root / f"{safe_label}策略指标.csv"
    json_path = output_root / f"{safe_label}.json"
    md_path = output_root / f"{safe_label}.md"
    factor_metrics.to_csv(factor_path, index=False)
    strategy_metrics.to_csv(strategy_path, index=False)
    summary = {
        "label": label,
        "preset": preset.name,
        "challenger": challenger.name,
        "start": start,
        "end": end,
        "panels": [{"name": name, "path": str(path)} for name, path in panel_specs],
        "json_path": str(json_path),
        "factor_metrics_path": str(factor_path),
        "strategy_metrics_path": str(strategy_path),
        "md_path": str(md_path),
        "decision": _prv_target_pool_decision(factor_metrics, strategy_metrics),
    }
    json_path.write_text(json.dumps(_jsonable(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_prv_target_pool_markdown(label, summary, factor_metrics, strategy_metrics), encoding="utf-8")
    return EarningsForecastPrvTargetPoolArtifacts(
        json_path=json_path,
        factor_metrics_path=factor_path,
        strategy_metrics_path=strategy_path,
        md_path=md_path,
        summary=summary,
    )


def _compute_overlay_factors(
    inputs,
    variants: tuple[OverlayVariant, ...],
) -> dict[str, pd.DataFrame]:
    templates = sorted({template for variant in variants for template in _variant_factor_templates(variant)})
    factors: dict[str, pd.DataFrame] = {}
    for template_id in templates:
        if template_id is None:
            continue
        recipe = executable_recipe_by_template(template_id)
        factors[template_id] = compute_formula(recipe.build_formula_spec(), inputs)
    return factors


def _tail_risk_mutation_variants() -> tuple[OverlayVariant, ...]:
    variants: list[OverlayVariant] = [OverlayVariant("baseline_top110_large", "baseline", None, "baseline")]
    for pool in (140, 150, 160, 180):
        for weight in (0.07, 0.10, 0.12, 0.15):
            variants.append(
                OverlayVariant(
                    name=f"tail_risk_pool{pool}_w{int(weight * 100):03d}",
                    role="tail_risk_rerank_grid",
                    factor_template="tail_risk_downside_vol_20d",
                    overlay_type="candidate_rerank",
                    weight=weight,
                    candidate_pool_size=pool,
                )
            )
    for quantile in (0.10, 0.20):
        for penalty in (0.10, 0.25):
            variants.append(
                OverlayVariant(
                    name=f"tail_risk_soft_q{int(quantile * 100):02d}_p{int(penalty * 100):02d}",
                    role="tail_risk_soft_trim",
                    factor_template="tail_risk_downside_vol_20d",
                    overlay_type="soft_penalty",
                    filter_quantile=quantile,
                    penalty_strength=penalty,
                )
            )
    variants.append(
        OverlayVariant(
            name="tail_risk_ramp_q50_p25",
            role="tail_risk_soft_ramp",
            factor_template="tail_risk_downside_vol_20d",
            overlay_type="soft_penalty_ramp",
            filter_quantile=0.50,
            penalty_strength=0.25,
        )
    )
    for quantile, penalty in ((0.20, 0.20), (0.25, 0.20), (0.25, 0.25)):
        variants.append(
            OverlayVariant(
                name=f"tail_risk_ramp_q{int(quantile * 100):02d}_p{int(penalty * 100):02d}",
                role="tail_risk_soft_ramp",
                factor_template="tail_risk_downside_vol_20d",
                overlay_type="soft_penalty_ramp",
                filter_quantile=quantile,
                penalty_strength=penalty,
            )
        )
    for pool in (150, 160):
        for tail_weight in (0.10, 0.12):
            variants.append(
                OverlayVariant(
                    name=f"tail_regime_pool{pool}_w{int(tail_weight * 100):03d}_r003",
                    role="tail_risk_regime_combo",
                    factor_template="tail_risk_downside_vol_20d",
                    overlay_type="candidate_rerank_combo",
                    weight=tail_weight,
                    candidate_pool_size=pool,
                    secondary_factor_template="regime_gated_trend_lowvol_60d",
                    secondary_weight=0.03,
                )
            )
    return tuple(variants)


def _variant_factor_templates(variant: OverlayVariant) -> tuple[str, ...]:
    templates = [template for template in (variant.factor_template, variant.secondary_factor_template) if template]
    return tuple(templates)


def _variant_by_name(name: str) -> OverlayVariant:
    variants = {variant.name: variant for variant in (*DEFAULT_OVERLAY_VARIANTS, *_tail_risk_mutation_variants())}
    try:
        return variants[name]
    except KeyError as exc:
        allowed = ", ".join(sorted(variants))
        raise KeyError(f"unknown overlay variant: {name}; allowed: {allowed}") from exc


def _build_overlay_signal(
    base_signal: pd.DataFrame,
    factors: dict[str, pd.DataFrame],
    variant: OverlayVariant,
) -> pd.DataFrame:
    if variant.overlay_type == "baseline":
        return base_signal
    if variant.factor_template is None:
        raise ValueError(f"variant requires factor_template: {variant.name}")
    factor = _visible_overlay_factor(
        factors[variant.factor_template],
        index=base_signal.index,
        columns=base_signal.columns,
    )
    if variant.overlay_type == "candidate_rerank":
        return build_fused_candidate_signal(
            base_signal,
            {variant.factor_template: factor},
            CandidateFusionRecipe(
                candidate_pool_size=variant.candidate_pool_size,
                base_weight=1.0,
                legs=(FusionLeg(variant.factor_template, variant.weight),),
            ),
        )
    if variant.overlay_type == "candidate_rerank_combo":
        if variant.secondary_factor_template is None:
            raise ValueError(f"combo variant requires secondary_factor_template: {variant.name}")
        return build_fused_candidate_signal(
            base_signal,
            {
                variant.factor_template: factor,
                variant.secondary_factor_template: _visible_overlay_factor(
                    factors[variant.secondary_factor_template],
                    index=base_signal.index,
                    columns=base_signal.columns,
                ),
            },
            CandidateFusionRecipe(
                candidate_pool_size=variant.candidate_pool_size,
                base_weight=1.0,
                legs=(
                    FusionLeg(variant.factor_template, variant.weight),
                    FusionLeg(variant.secondary_factor_template, variant.secondary_weight),
                ),
            ),
        )
    if variant.overlay_type == "bottom_filter":
        percentile = factor.rank(axis=1, pct=True)
        return base_signal.where(percentile.isna() | (percentile >= variant.filter_quantile))
    if variant.overlay_type == "soft_penalty":
        percentile = factor.rank(axis=1, pct=True)
        penalty_mask = percentile.notna() & (percentile < variant.filter_quantile)
        return base_signal.where(~penalty_mask, base_signal * (1.0 - variant.penalty_strength))
    if variant.overlay_type == "soft_penalty_ramp":
        percentile = factor.rank(axis=1, pct=True)
        active = percentile.notna() & (percentile < variant.filter_quantile)
        scaled = (percentile / variant.filter_quantile).clip(lower=0.0, upper=1.0)
        multiplier = 1.0 - variant.penalty_strength * (1.0 - scaled)
        return base_signal.where(~active, base_signal * multiplier)
    raise ValueError(f"unknown overlay_type: {variant.overlay_type}")


def _visible_overlay_factor(
    factor: pd.DataFrame,
    *,
    index: pd.Index,
    columns: pd.Index,
) -> pd.DataFrame:
    return factor.reindex(index=index, columns=columns).shift(1)


def _build_minute_target_price_buy_limits(
    workspace: Workspace,
    target_weights: pd.DataFrame,
    target_prices: pd.DataFrame,
    *,
    participation_rate: float,
    lot_size: int = 100,
) -> tuple[pd.DataFrame, dict[str, object]]:
    if participation_rate < 0:
        raise ValueError("participation_rate must be non-negative")
    common_dates = target_weights.index.intersection(target_prices.index)
    common_symbols = target_weights.columns.intersection(target_prices.columns)
    empty_limits = pd.DataFrame(0.0, index=target_weights.index, columns=target_weights.columns)
    if len(common_dates) == 0 or len(common_symbols) == 0:
        return empty_limits, {"active_target_cells": 0, "minute_files": 0}

    active = target_weights.loc[common_dates, common_symbols].fillna(0.0).gt(0)
    active_rows = active.stack()
    active_rows = active_rows[active_rows]
    if active_rows.empty:
        return empty_limits, {"active_target_cells": 0, "minute_files": 0}

    requests = active_rows.rename("active").reset_index()
    requests.columns = ["date", "symbol", "active"]
    price_values = target_prices.loc[common_dates, common_symbols].stack().rename("target_price").reset_index()
    price_values.columns = ["date", "symbol", "target_price"]
    requests = requests.merge(price_values, on=["date", "symbol"], how="left")
    requests = requests.dropna(subset=["target_price"])
    requests["date"] = requests["date"].astype(str)
    requests["symbol"] = requests["symbol"].astype(str)
    requests["target_price"] = pd.to_numeric(requests["target_price"], errors="raise").astype(float)
    requests = requests.loc[requests["target_price"] > 0, ["date", "symbol", "target_price"]].copy()
    requests["_row_id"] = range(len(requests))
    if requests.empty:
        return empty_limits, {"active_target_cells": 0, "minute_files": 0}

    minute_root = workspace.root / "data" / "stk_mins"
    paths = _minute_symbol_year_paths(minute_root, requests)
    if not paths:
        return empty_limits, {
            "active_target_cells": int(len(requests)),
            "minute_files": 0,
            "minute_data_status": "missing",
        }

    con = duckdb.connect(database=":memory:", read_only=False)
    try:
        con.register("requests", requests)
        parquet_sources = _duckdb_path_list(paths)
        matched = con.execute(
            f"""
            SELECT
              r._row_id,
              r.date,
              r.symbol,
              r.target_price,
              SUM(CASE WHEN m.low <= r.target_price AND m.high >= r.target_price THEN m.amount ELSE 0 END) AS matched_amount,
              SUM(CASE WHEN m.low <= r.target_price AND m.high >= r.target_price THEN 1 ELSE 0 END) AS matched_minutes,
              COUNT(m.amount) AS minute_rows
            FROM requests r
            LEFT JOIN read_parquet({parquet_sources}, hive_partitioning=true, union_by_name=true) m
              ON m.date = r.date AND m.symbol = r.symbol
            GROUP BY r._row_id, r.date, r.symbol, r.target_price
            ORDER BY r._row_id
            """
        ).fetchdf()
    finally:
        con.close()

    matched["available_lot_shares"] = (
        (matched["matched_amount"].fillna(0.0) * float(participation_rate) / matched["target_price"])
        .fillna(0.0)
        .clip(lower=0.0)
        // lot_size
        * lot_size
    ).astype(int)
    limits = matched.pivot_table(
        index="date",
        columns="symbol",
        values="available_lot_shares",
        aggfunc="max",
    )
    limits = limits.reindex(index=target_weights.index.astype(str), columns=target_weights.columns.astype(str)).fillna(0.0)
    limits.index = target_weights.index
    limits.columns = target_weights.columns
    coverage = {
        "active_target_cells": int(len(requests)),
        "minute_files": int(len(paths)),
        "date_count": int(requests["date"].nunique()),
        "symbol_count": int(requests["symbol"].nunique()),
        "start": str(requests["date"].min()),
        "end": str(requests["date"].max()),
        "target_price_touch_rate": float((matched["matched_minutes"] > 0).mean()) if len(matched) else 0.0,
        "positive_capacity_rate": float((matched["available_lot_shares"] > 0).mean()) if len(matched) else 0.0,
        "median_available_lot_shares": float(matched["available_lot_shares"].median()) if len(matched) else 0.0,
    }
    return limits, coverage


def _regime_budget_scales(index_close: pd.DataFrame, target_index: pd.Index) -> dict[str, pd.Series]:
    available = [code for code in ("000300.SH", "000905.SH", "000852.SH") if code in index_close.columns]
    if len(available) < 2:
        return {}
    close = index_close[available].sort_index()
    momentum_60 = (close / close.shift(60) - 1.0).shift(1)
    confirmations = momentum_60.gt(0).sum(axis=1).reindex(target_index).fillna(len(available)).astype(int)
    benchmark = close["000300.SH"] if "000300.SH" in close.columns else close[available[0]]
    drawdown_120 = (benchmark / benchmark.rolling(120, min_periods=60).max() - 1.0).shift(1).reindex(target_index).fillna(0.0)
    scales: dict[str, pd.Series] = {}
    base = pd.Series(1.0, index=target_index)
    scales["broad_60d_weak_scale075"] = base.where(confirmations >= 2, 0.75)
    scales["broad_60d_weak_scale050"] = base.where(confirmations >= 2, 0.50)
    scales["drawdown120_10pct_scale075"] = base.where(drawdown_120 > -0.10, 0.75)
    scales["drawdown120_10pct_scale050"] = base.where(drawdown_120 > -0.10, 0.50)
    combo = pd.concat(
        [
            scales["broad_60d_weak_scale075"],
            scales["drawdown120_10pct_scale050"],
        ],
        axis=1,
    ).min(axis=1)
    scales["combo_broad075_drawdown050"] = combo
    return scales


def _run_scaled_weight_backtest(
    weights: pd.DataFrame,
    returns: pd.DataFrame,
    exposure_scale: pd.Series,
    *,
    transaction_cost_bps: float,
    quality: ExperimentQuality,
) -> BacktestResult:
    common_dates = weights.index.intersection(returns.index).intersection(exposure_scale.index)
    common_symbols = weights.columns.intersection(returns.columns)
    weights = weights.loc[common_dates, common_symbols].fillna(0.0)
    returns = returns.loc[common_dates, common_symbols].fillna(0.0)
    scale = exposure_scale.reindex(common_dates).fillna(1.0).clip(lower=0.0, upper=1.0)
    scaled_weights = weights.mul(scale, axis=0)
    equity = 1.0
    current = pd.Series(0.0, index=common_symbols)
    equity_rows = [{"date": common_dates[0], "equity": equity}]
    return_rows: list[dict[str, object]] = []
    turnover_sum = 0.0
    for idx in range(1, len(common_dates)):
        date = common_dates[idx]
        target = scaled_weights.loc[date]
        turnover = float((target - current).abs().sum())
        current = target
        gross_ret = float((current * returns.loc[date]).sum())
        cost = turnover * float(transaction_cost_bps) / 10000.0
        net_ret = gross_ret - cost
        equity *= 1.0 + net_ret
        turnover_sum += turnover
        return_rows.append({"date": date, "return": net_ret})
        equity_rows.append({"date": date, "equity": equity})
    equity_curve = pd.DataFrame(equity_rows).set_index("date")["equity"]
    scaled_returns = pd.DataFrame(return_rows).set_index("date")["return"]
    metrics = _compute_metrics(equity_curve, scaled_returns, turnover_sum=turnover_sum, rebalance_count=len(scaled_returns))
    return BacktestResult(
        metrics=metrics,
        goal_review=review_backtest_metrics(metrics, quality=quality, goal_criteria=None),
        equity_curve=equity_curve,
        weights=scaled_weights,
        returns=scaled_returns,
    )


def _classify_regime_budget_metrics(metrics: pd.DataFrame, challenger_name: str) -> pd.DataFrame:
    frame = metrics.copy()
    base = frame.loc[frame["budget"] == "challenger_no_budget"].iloc[0]
    for column in ["annual_return", "max_drawdown", "calmar", "turnover"]:
        frame[f"{column}_delta_vs_challenger"] = frame[column] - float(base[column])
    decisions = []
    for row in frame.to_dict(orient="records"):
        if row["budget"] in {"baseline_no_budget", "challenger_no_budget"}:
            decisions.append("reference")
        elif row["calmar_delta_vs_challenger"] > 0 and row["annual_return_delta_vs_challenger"] > -0.02:
            decisions.append("promote_budget")
        elif row["max_drawdown_delta_vs_challenger"] > 0 and row["annual_return_delta_vs_challenger"] > -0.05:
            decisions.append("risk_shadow")
        else:
            decisions.append("reject")
    frame["decision"] = decisions
    return frame.sort_values(["decision", "calmar"], ascending=[True, False])


def _regime_budget_decision(metrics: pd.DataFrame) -> dict[str, object]:
    promoted = metrics.loc[metrics["decision"] == "promote_budget"]
    if not promoted.empty:
        best = promoted.sort_values("calmar", ascending=False).iloc[0]
        return {
            "status": "promote_regime_budget",
            "budget": str(best["budget"]),
            "reason": "Calmar improves without material annual-return decay.",
        }
    shadow = metrics.loc[metrics["decision"] == "risk_shadow"]
    if not shadow.empty:
        best = shadow.sort_values("max_drawdown_delta_vs_challenger", ascending=False).iloc[0]
        return {
            "status": "keep_risk_shadow",
            "budget": str(best["budget"]),
            "reason": "Risk improves, but return/Calmar trade-off is not yet good enough.",
        }
    return {"status": "reject_regime_budgets", "reason": "No light budget improved the challenger."}


def _best_regime_budget(metrics: pd.DataFrame, column: str) -> dict[str, object]:
    row = metrics.sort_values(column, ascending=False).iloc[0]
    return {
        "variant": str(row["variant"]),
        "budget": str(row["budget"]),
        "decision": str(row["decision"]),
        "annual_return": float(row["annual_return"]),
        "max_drawdown": float(row["max_drawdown"]),
        "calmar": float(row["calmar"]),
    }


def _default_prv_panel_specs(workspace: Workspace) -> list[tuple[str, Path]]:
    root = workspace.root / "research" / "factor-reports" / "volume-peak-ridge-valley"
    candidates = [
        ("all_a_2025_2026", root / "all-a-2025-2026-prv" / "volume_prv_all_a_panel_2025_2026.parquet"),
        ("liquid300_2017_2026", root / "volume_prv_liquid300_panel_2017_2026_20260502.parquet"),
    ]
    return [(name, path) for name, path in candidates if path.exists()]


def _prv_factor_columns(panel: pd.DataFrame) -> dict[str, str]:
    candidates = {
        "ridge_risk": ["ridge3_volume_ratio", "ridge_volume_ratio", "adjacent_ridge_volume_ratio"],
        "peak_alpha": ["isolated_peak_volume_ratio", "peak_volume_ratio"],
        "valley_alpha": ["valley_relative_vwap", "valley_minutes", "valley_volume_ratio"],
        "first30_execution": ["first30_volume_ratio"],
    }
    result: dict[str, str] = {}
    for role, columns in candidates.items():
        for column in columns:
            if column in panel.columns:
                result[role] = column
                break
    return result


def _panel_factor_frame(
    panel: pd.DataFrame,
    column: str,
    target_dates: pd.Index,
    target_symbols: pd.Index,
) -> pd.DataFrame:
    date_values = pd.Index([str(date) for date in target_dates], name="date")
    frame = panel[["date", "symbol", column]].copy()
    frame["date"] = frame["date"].astype(str)
    factor = frame.pivot_table(index="date", columns="symbol", values=column, aggfunc="last")
    factor = factor.sort_index().shift(1)
    return factor.reindex(index=date_values, columns=target_symbols)


def _target_pool_factor_stats(
    weights: pd.DataFrame,
    factor: pd.DataFrame,
    returns: pd.DataFrame,
) -> dict[str, object]:
    common_dates = weights.index.intersection(returns.index).intersection(factor.index)
    common_symbols = weights.columns.intersection(returns.columns).intersection(factor.columns)
    weights = weights.loc[common_dates, common_symbols].fillna(0.0)
    factor = factor.loc[common_dates, common_symbols]
    returns = returns.loc[common_dates, common_symbols]
    active = weights.gt(0)
    target_cells = int(active.sum().sum())
    covered_cells = int((active & factor.notna()).sum().sum())
    ic_values: list[float] = []
    ls_values: list[float] = []
    for date in common_dates:
        mask = active.loc[date] & factor.loc[date].notna() & returns.loc[date].notna()
        if int(mask.sum()) < 30:
            continue
        f = factor.loc[date, mask]
        r = returns.loc[date, mask]
        if f.nunique() >= 5:
            ic_values.append(float(f.rank().corr(r.rank())))
        if f.nunique() >= 10 and int(mask.sum()) >= 60:
            rank = f.rank(pct=True)
            top = rank >= 0.80
            bottom = rank <= 0.20
            ls_values.append(float(r.loc[top].mean() - r.loc[bottom].mean()))
    ic = pd.Series(ic_values, dtype=float)
    ls = pd.Series(ls_values, dtype=float)
    return {
        "target_cells": target_cells,
        "covered_cells": covered_cells,
        "coverage": float(covered_cells / target_cells) if target_cells else np.nan,
        "ic_mean": float(ic.mean()) if not ic.empty else np.nan,
        "ic_ir": float(ic.mean() / ic.std()) if len(ic) > 1 and float(ic.std()) else np.nan,
        "ic_positive_rate": float((ic > 0).mean()) if not ic.empty else np.nan,
        "ic_days": int(len(ic)),
        "long_short_mean": float(ls.mean()) if not ls.empty else np.nan,
        "long_short_positive_rate": float((ls > 0).mean()) if not ls.empty else np.nan,
        "long_short_days": int(len(ls)),
    }


def _deweight_top_quantile(
    weights: pd.DataFrame,
    factor: pd.DataFrame,
    *,
    quantile: float,
    multiplier: float,
) -> pd.DataFrame:
    adjusted = weights.copy().fillna(0.0)
    for date in adjusted.index.intersection(factor.index):
        active = adjusted.loc[date].gt(0) & factor.loc[date].notna()
        if int(active.sum()) < 10:
            continue
        cutoff = factor.loc[date, active].quantile(quantile)
        deweight = active & factor.loc[date].ge(cutoff)
        adjusted.loc[date, deweight] *= multiplier
    return adjusted


def _run_weight_matrix_backtest(
    weights: pd.DataFrame,
    returns: pd.DataFrame,
    *,
    transaction_cost_bps: float,
    quality: ExperimentQuality,
) -> BacktestResult:
    return _run_scaled_weight_backtest(
        weights,
        returns,
        pd.Series(1.0, index=weights.index),
        transaction_cost_bps=transaction_cost_bps,
        quality=quality,
    )


def _classify_prv_strategy_metrics(metrics: pd.DataFrame, challenger_name: str) -> pd.DataFrame:
    frame = metrics.copy()
    base = frame.loc[(frame["variant"] == challenger_name) & (frame["overlay"] == "challenger_no_prv")]
    if base.empty:
        frame["annual_return_delta_vs_challenger"] = np.nan
        frame["max_drawdown_delta_vs_challenger"] = np.nan
        frame["calmar_delta_vs_challenger"] = np.nan
        frame["decision"] = "audit_only"
        return frame
    challenger = base.iloc[0]
    for column in ["annual_return", "max_drawdown", "calmar", "turnover"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["annual_return_delta_vs_challenger"] = frame["annual_return"] - float(challenger["annual_return"])
    frame["max_drawdown_delta_vs_challenger"] = frame["max_drawdown"] - float(challenger["max_drawdown"])
    frame["calmar_delta_vs_challenger"] = frame["calmar"] - float(challenger["calmar"])
    frame["decision"] = "audit_only"
    overlay_mask = frame["overlay"].astype(str).str.contains("_top20_half_holding")
    frame.loc[overlay_mask & frame["calmar_delta_vs_challenger"].le(0), "decision"] = "reject"
    frame.loc[
        overlay_mask & frame["calmar_delta_vs_challenger"].gt(0) & frame["annual_return_delta_vs_challenger"].ge(-0.02),
        "decision",
    ] = "keep_shadow"
    frame.loc[
        overlay_mask & frame["calmar_delta_vs_challenger"].gt(0.10) & frame["annual_return_delta_vs_challenger"].gt(0),
        "decision",
    ] = "promote_challenger"
    return frame


def _prv_target_pool_decision(factor_metrics: pd.DataFrame, strategy_metrics: pd.DataFrame) -> dict[str, object]:
    if factor_metrics.empty:
        return {"status": "blocked", "reason": "没有可用 PRV panel。"}
    best = strategy_metrics[strategy_metrics["decision"].isin(["promote_challenger", "keep_shadow"])]
    if not best.empty:
        row = best.sort_values("calmar_delta_vs_challenger", ascending=False).iloc[0]
        return {
            "status": "keep_shadow",
            "reason": "PRV 在至少一个持仓风险 overlay 中改善 Calmar，但需要更长 all-A panel 再晋级。",
            "best_overlay": row.to_dict(),
        }
    if set(factor_metrics["panel"].unique()) >= {"all_a_2025_2026", "liquid300_2017_2026"}:
        return {
            "status": "risk_shadow_or_reject",
            "reason": "已有 all-A 近端与 long-window liquid300 复验，但策略层 PRV overlay 未打赢 tail-risk soft challenger。",
        }
    return {
        "status": "partial_coverage",
        "reason": "PRV 数据可用于方向审计，但尚无 2017-2026 全 A 面板；优先补目标池/全 A long-window panel。",
    }


def _minute_symbol_year_paths(minute_root: Path, requests: pd.DataFrame) -> list[str]:
    request_keys = requests.assign(year=requests["date"].astype(str).str[:4])
    paths: list[str] = []
    for row in request_keys[["year", "symbol"]].drop_duplicates().to_dict(orient="records"):
        path = minute_root / f"year={row['year']}" / "universe=all_active" / f"symbol={row['symbol']}" / "data.parquet"
        if path.exists():
            paths.append(str(path))
    return sorted(paths)


def _duckdb_path_list(paths: list[str]) -> str:
    quoted = ["'" + path.replace("'", "''") + "'" for path in paths]
    return "[" + ", ".join(quoted) + "]"


def _execution_metric_row(
    *,
    variant: str,
    mode: str,
    capital: float | None,
    participation_rate: float | None,
    metrics: dict[str, object],
    order_summary: dict[str, object],
    lot_summary: dict[str, object] | None = None,
) -> dict[str, object]:
    row = {
        "variant": variant,
        "mode": mode,
        "capital": capital,
        "participation_rate": participation_rate,
        **metrics,
    }
    if lot_summary:
        row["avg_holding_count"] = lot_summary.get("avg_holding_count")
        row["avg_cash_ratio"] = lot_summary.get("avg_cash_ratio")
    row.update(order_summary)
    return row


def _buy_order_summary(order_intents: pd.DataFrame) -> dict[str, object]:
    if order_intents.empty:
        return {
            "buy_order_count": 0,
            "filled_buy_count": 0,
            "partial_buy_count": 0,
            "skipped_buy_count": 0,
            "buy_share_fill_ratio": 0.0,
            "buy_notional_fill_ratio": 0.0,
        }
    buys = order_intents.loc[(order_intents["side"] == "buy") & (order_intents["requested_shares"] > 0)].copy()
    if buys.empty:
        return {
            "buy_order_count": 0,
            "filled_buy_count": 0,
            "partial_buy_count": 0,
            "skipped_buy_count": 0,
            "buy_share_fill_ratio": 0.0,
            "buy_notional_fill_ratio": 0.0,
        }
    requested_shares = float(buys["requested_shares"].sum())
    executed_shares = float(buys["executed_shares"].sum()) if "executed_shares" in buys.columns else 0.0
    requested_notional = float(buys["requested_notional"].sum()) if "requested_notional" in buys.columns else 0.0
    executed_notional = (
        float((buys["executed_shares"] * buys["open_price"]).sum())
        if {"executed_shares", "open_price"}.issubset(buys.columns)
        else 0.0
    )
    return {
        "buy_order_count": int(len(buys)),
        "filled_buy_count": int((buys["status"] == "filled").sum()) if "status" in buys.columns else 0,
        "partial_buy_count": int((buys["status"] == "partial").sum()) if "status" in buys.columns else 0,
        "skipped_buy_count": int((buys["status"] == "skipped").sum()) if "status" in buys.columns else 0,
        "buy_share_fill_ratio": executed_shares / requested_shares if requested_shares else 0.0,
        "buy_notional_fill_ratio": executed_notional / requested_notional if requested_notional else 0.0,
    }


def _add_execution_deltas(metrics: pd.DataFrame) -> pd.DataFrame:
    frame = metrics.copy()
    delta_cols = ["annual_return", "max_drawdown", "calmar"]
    for column in delta_cols:
        frame[f"{column}_delta_vs_baseline"] = np.nan
    key_cols = ["mode", "capital", "participation_rate"]
    baseline_variant = str(frame["variant"].iloc[0]) if not frame.empty else ""
    baseline = frame.loc[frame["variant"] == baseline_variant].copy()
    for idx, row in frame.iterrows():
        peers = baseline
        for key in key_cols:
            if pd.isna(row.get(key)):
                peers = peers.loc[peers[key].isna()]
            else:
                peers = peers.loc[peers[key] == row.get(key)]
        if peers.empty:
            continue
        base = peers.iloc[0]
        for column in delta_cols:
            frame.loc[idx, f"{column}_delta_vs_baseline"] = float(row[column]) - float(base[column])
    return frame


def _best_execution_rows(metrics: pd.DataFrame, challenger_name: str) -> list[dict[str, object]]:
    challenger = metrics.loc[metrics["variant"] == challenger_name].copy()
    if challenger.empty:
        return []
    columns = [
        "variant",
        "mode",
        "capital",
        "participation_rate",
        "annual_return",
        "max_drawdown",
        "calmar",
        "annual_return_delta_vs_baseline",
        "max_drawdown_delta_vs_baseline",
        "calmar_delta_vs_baseline",
        "buy_share_fill_ratio",
        "avg_cash_ratio",
    ]
    return challenger.sort_values("calmar", ascending=False)[columns].head(12).to_dict(orient="records")


def _execution_review_decision(metrics: pd.DataFrame, challenger_name: str) -> dict[str, object]:
    comparable = metrics.loc[
        (metrics["variant"] == challenger_name)
        & (metrics["mode"] == "minute_target_price_partial")
        & metrics["calmar_delta_vs_baseline"].notna()
    ].copy()
    if comparable.empty:
        return {"status": "insufficient_execution_evidence", "reason": "No minute-constrained comparable rows."}
    win_rate = float((comparable["calmar_delta_vs_baseline"] > 0).mean())
    worst_drawdown_delta = float(comparable["max_drawdown_delta_vs_baseline"].min())
    status = "execution_robust_challenger" if win_rate >= 0.6 and worst_drawdown_delta >= -0.01 else "keep_shadow"
    return {
        "status": status,
        "minute_calmar_win_rate": win_rate,
        "worst_drawdown_delta": worst_drawdown_delta,
        "scenario_count": int(len(comparable)),
    }


def _robustness_scenarios(start: str, end: str, *, preset_top_n: int, base_cost: float) -> list[dict[str, object]]:
    years = sorted({start[:4], end[:4], "2019", "2020", "2022", "2023"})
    split_1_end = "20191231" if start <= "20191231" and end >= "20170101" else end
    split_2_start = "20200101" if start <= "20200101" else start
    split_2_end = "20221231" if end >= "20221231" else end
    split_3_start = "20230101" if start <= "20230101" else start
    scenarios = [
        {"scenario": "full_base_cost", "scenario_type": "full", "start": start, "end": end, "top_n": preset_top_n, "cost_bps": base_cost},
        {"scenario": "cost_50bps", "scenario_type": "cost", "start": start, "end": end, "top_n": preset_top_n, "cost_bps": 50.0},
        {"scenario": "cost_100bps", "scenario_type": "cost", "start": start, "end": end, "top_n": preset_top_n, "cost_bps": 100.0},
        {"scenario": "topn_minus10", "scenario_type": "top_n", "start": start, "end": end, "top_n": max(10, preset_top_n - 10), "cost_bps": base_cost},
        {"scenario": "topn_plus10", "scenario_type": "top_n", "start": start, "end": end, "top_n": preset_top_n + 10, "cost_bps": base_cost},
    ]
    if start <= split_1_end:
        scenarios.append({"scenario": f"{years[0]}_to_2019", "scenario_type": "time", "start": start, "end": split_1_end, "top_n": preset_top_n, "cost_bps": base_cost})
    if split_2_start <= split_2_end:
        scenarios.append({"scenario": "2020_to_2022", "scenario_type": "time", "start": split_2_start, "end": split_2_end, "top_n": preset_top_n, "cost_bps": base_cost})
    if split_3_start <= end:
        scenarios.append({"scenario": "2023_to_end", "scenario_type": "time", "start": split_3_start, "end": end, "top_n": preset_top_n, "cost_bps": base_cost})
    return scenarios


def _slice_by_date(frame: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    index = frame.index.astype(str)
    return frame.loc[(index >= start) & (index <= end)]


def _slice_series_by_date(series: pd.Series | None, start: str, end: str) -> pd.Series | None:
    if series is None:
        return None
    index = series.index.astype(str)
    return series.loc[(index >= start) & (index <= end)]


def _slice_optional_frame_by_date(frame: pd.DataFrame | None, start: str, end: str) -> pd.DataFrame | None:
    if frame is None:
        return None
    return _slice_by_date(frame, start, end)


def _cpcv_contiguous_groups(dates: pd.Index, n_groups: int) -> list[pd.Index]:
    if len(dates) < n_groups:
        raise ValueError(f"not enough trade dates for CPCV: {len(dates)} dates < {n_groups} groups")
    positions = np.array_split(np.arange(len(dates)), n_groups)
    return [dates.take(position) for position in positions if len(position) > 0]


def _cpcv_split_dates(
    dates: pd.Index,
    groups: list[pd.Index],
    test_group_ids: tuple[int, ...],
    *,
    purge_horizon: int,
    embargo: int,
) -> dict[str, object]:
    test_mask = np.zeros(len(dates), dtype=bool)
    excluded_mask = np.zeros(len(dates), dtype=bool)
    for group_id in test_group_ids:
        group = groups[group_id]
        start_pos = int(dates.get_loc(group[0]))
        end_pos = int(dates.get_loc(group[-1]))
        test_mask[start_pos : end_pos + 1] = True
        excluded_start = max(0, start_pos - purge_horizon)
        excluded_end = min(len(dates) - 1, end_pos + embargo)
        excluded_mask[excluded_start : excluded_end + 1] = True
    train_mask = (~test_mask) & (~excluded_mask)
    return {
        "test_dates": dates[test_mask],
        "train_dates": dates[train_mask],
        "excluded_days": int((excluded_mask & ~test_mask).sum()),
    }


def _cpcv_metric_delta_columns(
    baseline: BacktestResult,
    challenger: BacktestResult,
    dates: pd.Index,
    prefix: str,
) -> dict[str, object]:
    baseline_metrics = _cpcv_metrics_for_dates(baseline, dates)
    challenger_metrics = _cpcv_metrics_for_dates(challenger, dates)
    columns: dict[str, object] = {}
    metric_names = (
        "annual_return",
        "max_drawdown",
        "sharpe",
        "calmar",
        "turnover",
        "total_return",
        "sortino",
        "cvar_5pct",
        "worst_5d_return",
        "worst_20d_return",
        "positive_month_rate",
        "annual_win_rate",
    )
    for name in metric_names:
        base_value = getattr(baseline_metrics, name) if baseline_metrics is not None else None
        challenger_value = getattr(challenger_metrics, name) if challenger_metrics is not None else None
        columns[f"{prefix}_baseline_{name}"] = base_value
        columns[f"{prefix}_challenger_{name}"] = challenger_value
        columns[f"{prefix}_{name}_delta"] = (
            float(challenger_value - base_value)
            if base_value is not None and challenger_value is not None
            else None
        )
    return columns


def _cpcv_metrics_for_dates(result: BacktestResult, dates: pd.Index) -> object | None:
    if len(dates) == 0:
        return None
    returns = result.returns.reindex(dates).fillna(0.0)
    if returns.empty:
        return None
    equity_values = [1.0, *((1.0 + returns).cumprod().to_list())]
    equity_curve = pd.Series(equity_values, index=pd.RangeIndex(len(equity_values)))
    weights = result.weights.reindex(dates).fillna(0.0)
    if weights.empty:
        turnover_sum = 0.0
    else:
        turnover = weights.diff().abs().sum(axis=1)
        turnover.iloc[0] = float(weights.iloc[0].abs().sum())
        turnover_sum = float(turnover.sum())
    return _compute_metrics(
        equity_curve,
        returns,
        turnover_sum=turnover_sum,
        rebalance_count=len(returns),
    )


def _cpcv_fold_decision(row: dict[str, object]) -> str:
    annual_delta = row.get("test_annual_return_delta")
    drawdown_delta = row.get("test_max_drawdown_delta")
    calmar_delta = row.get("test_calmar_delta")
    if annual_delta is None or drawdown_delta is None or calmar_delta is None:
        return "empty"
    if float(annual_delta) > 0 and float(drawdown_delta) >= 0 and float(calmar_delta) > 0:
        return "win"
    if float(calmar_delta) > 0 and float(drawdown_delta) >= -0.01:
        return "risk_acceptable_win"
    if float(annual_delta) > 0 or float(calmar_delta) > 0:
        return "mixed"
    return "lose"


def _cpcv_status(matrix: pd.DataFrame) -> dict[str, object]:
    if matrix.empty:
        return {"status": "blocked", "reason": "empty CPCV matrix"}
    calmar_delta = pd.to_numeric(matrix["test_calmar_delta"], errors="coerce").dropna()
    annual_delta = pd.to_numeric(matrix["test_annual_return_delta"], errors="coerce").dropna()
    drawdown_delta = pd.to_numeric(matrix["test_max_drawdown_delta"], errors="coerce").dropna()
    if calmar_delta.empty or annual_delta.empty or drawdown_delta.empty:
        return {"status": "blocked", "reason": "no valid CPCV metric deltas"}
    calmar_win_rate = float((calmar_delta > 0).mean())
    annual_win_rate = float((annual_delta > 0).mean())
    median_calmar_delta = float(calmar_delta.median())
    median_annual_delta = float(annual_delta.median())
    drawdown_delta_p25 = float(drawdown_delta.quantile(0.25))
    if (
        median_calmar_delta > 0
        and median_annual_delta > 0
        and calmar_win_rate >= 0.70
        and drawdown_delta_p25 >= -0.01
    ):
        status = "cpcv_pass_reference_baseline_candidate"
        reason = "Frozen challenger beats baseline across the CPCV test distribution."
    elif median_calmar_delta > 0 and median_annual_delta > 0 and calmar_win_rate >= 0.60:
        status = "cpcv_conditional_shadow"
        reason = "Frozen challenger is positive but not strong enough for automatic baseline replacement."
    else:
        status = "cpcv_fail_keep_baseline"
        reason = "Frozen challenger does not clear sample-out replacement thresholds."
    return {
        "status": status,
        "reason": reason,
        "test_calmar_win_rate": calmar_win_rate,
        "test_annual_return_win_rate": annual_win_rate,
        "median_test_calmar_delta": median_calmar_delta,
        "median_test_annual_return_delta": median_annual_delta,
        "test_max_drawdown_delta_p25": drawdown_delta_p25,
        "split_count": int(len(matrix)),
    }


def _overlay_variant_logic(variant: OverlayVariant) -> dict[str, object]:
    return {
        "variant": variant.name,
        "role": variant.role,
        "overlay_type": variant.overlay_type,
        "factor_template": variant.factor_template,
        "secondary_factor_template": variant.secondary_factor_template,
        "weight": variant.weight,
        "secondary_weight": variant.secondary_weight,
        "candidate_pool_size": variant.candidate_pool_size,
        "filter_quantile": variant.filter_quantile,
        "penalty_strength": variant.penalty_strength,
    }


def _format_cpcv_date(value: object) -> str:
    parsed = pd.to_datetime(str(value), errors="coerce")
    if pd.isna(parsed):
        return str(value)
    return parsed.strftime("%Y%m%d")


def _robustness_delta_frame(rows: pd.DataFrame, challenger_name: str) -> pd.DataFrame:
    baseline = rows.loc[rows["variant"] == "baseline"].set_index("scenario")
    challenger = rows.loc[rows["variant"] == challenger_name].set_index("scenario")
    records: list[dict[str, object]] = []
    for scenario in challenger.index.intersection(baseline.index):
        base = baseline.loc[scenario]
        chal = challenger.loc[scenario]
        records.append(
            {
                "scenario": scenario,
                "scenario_type": chal["scenario_type"],
                "start": chal["start"],
                "end": chal["end"],
                "top_n": int(chal["top_n"]),
                "cost_bps": float(chal["cost_bps"]),
                "baseline_annual_return": float(base["annual_return"]),
                "challenger_annual_return": float(chal["annual_return"]),
                "annual_return_delta": float(chal["annual_return"] - base["annual_return"]),
                "baseline_max_drawdown": float(base["max_drawdown"]),
                "challenger_max_drawdown": float(chal["max_drawdown"]),
                "max_drawdown_delta": float(chal["max_drawdown"] - base["max_drawdown"]),
                "baseline_calmar": float(base["calmar"]),
                "challenger_calmar": float(chal["calmar"]),
                "calmar_delta": float(chal["calmar"] - base["calmar"]),
                "decision": _scenario_decision(chal, base),
            }
        )
    return pd.DataFrame(records).sort_values(["scenario_type", "scenario"])


def _scenario_decision(challenger: pd.Series, baseline: pd.Series) -> str:
    if challenger["annual_return"] >= baseline["annual_return"] and challenger["max_drawdown"] >= baseline["max_drawdown"] and challenger["calmar"] >= baseline["calmar"]:
        return "win"
    if challenger["max_drawdown"] >= baseline["max_drawdown"] and challenger["calmar"] >= baseline["calmar"] * 0.98:
        return "risk_shadow"
    if challenger["annual_return"] >= baseline["annual_return"] * 0.99 or challenger["calmar"] >= baseline["calmar"] * 0.99:
        return "near"
    return "lose"


def _robustness_status(matrix: pd.DataFrame) -> dict[str, object]:
    if matrix.empty:
        return {"status": "blocked", "reason": "empty robustness matrix"}
    win_rate = float((matrix["calmar_delta"] > 0).mean())
    drawdown_worse = float(matrix["max_drawdown_delta"].min())
    if win_rate >= 0.6 and drawdown_worse >= -0.01:
        status = "robust_challenger"
    elif win_rate >= 0.4 and drawdown_worse >= -0.02:
        status = "tradable_shadow"
    else:
        status = "fragile"
    return {
        "status": status,
        "calmar_win_rate": win_rate,
        "worst_drawdown_delta": drawdown_worse,
    }


def _robustness_markdown(label: str, summary: dict[str, object], matrix: pd.DataFrame) -> str:
    lines = [
        f"# {label}",
        "",
        "## Robustness status",
        "",
        f"`{summary['robustness_status']['status']}`; Calmar win rate {summary['challenger_win_rate_calmar'] * 100:.2f}%, worst drawdown delta {summary['worst_drawdown_delta']:.6f}.",
        "",
        "## Matrix",
        "",
    ]
    columns = [
        "scenario",
        "scenario_type",
        "top_n",
        "cost_bps",
        "annual_return_delta",
        "max_drawdown_delta",
        "calmar_delta",
        "decision",
    ]
    lines.extend(_markdown_table(matrix[columns]))
    return "\n".join(lines) + "\n"


def _cpcv_markdown(label: str, summary: dict[str, object], matrix: pd.DataFrame) -> str:
    status = summary.get("cpcv_status", {})
    full_sample = summary.get("full_sample", {})
    lines = [
        f"# {label}",
        "",
        "## Frozen Candidate",
        "",
        f"- Baseline: `{summary['baseline']}`",
        f"- Challenger: `{summary['challenger']}`",
        f"- Evaluation mode: `{summary['evaluation_mode']}`",
        "- Train blocks are used for leakage accounting and train/test gap context; no parameter is refit inside folds.",
        "",
        "## CPCV Status",
        "",
        f"`{status.get('status', 'unknown')}`: {status.get('reason', '')}",
        f"- Test Calmar win rate: {float(status.get('test_calmar_win_rate', 0.0)) * 100:.2f}%",
        f"- Median test annual-return delta: {float(status.get('median_test_annual_return_delta', 0.0)):.6f}",
        f"- Median test Calmar delta: {float(status.get('median_test_calmar_delta', 0.0)):.6f}",
        f"- Test drawdown-delta p25: {float(status.get('test_max_drawdown_delta_p25', 0.0)):.6f}",
        "",
        "## Full Sample Delta",
        "",
        f"- Annual-return delta: {float(full_sample.get('annual_return_delta', 0.0)):.6f}",
        f"- Max-drawdown delta: {float(full_sample.get('max_drawdown_delta', 0.0)):.6f}",
        f"- Calmar delta: {float(full_sample.get('calmar_delta', 0.0)):.6f}",
        "",
        "## Groups",
        "",
    ]
    lines.extend(_markdown_table(pd.DataFrame(summary.get("group_calendar", []))))
    lines.extend(["", "## Fold Matrix", ""])
    columns = [
        "fold",
        "test_groups",
        "train_days",
        "test_days",
        "purged_or_embargoed_days",
        "test_annual_return_delta",
        "test_max_drawdown_delta",
        "test_calmar_delta",
        "test_turnover_delta",
        "train_calmar_delta",
        "decision",
    ]
    available_columns = [column for column in columns if column in matrix.columns]
    lines.extend(_markdown_table(matrix[available_columns] if available_columns else matrix))
    return "\n".join(lines) + "\n"


def _classify_overlay_decisions(metrics: pd.DataFrame) -> pd.DataFrame:
    frame = metrics.copy()
    baseline = frame.loc[frame["overlay_type"] == "baseline"]
    if baseline.empty:
        raise ValueError("baseline variant is required")
    base = baseline.iloc[0]
    annual_base = float(base["annual_return"])
    drawdown_base = float(base["max_drawdown"])
    calmar_base = float(base["calmar"])
    decisions: list[str] = []
    reasons: list[str] = []
    for row in frame.to_dict(orient="records"):
        annual = float(row["annual_return"])
        drawdown = float(row["max_drawdown"])
        calmar = float(row["calmar"])
        if row["overlay_type"] == "baseline":
            decisions.append("baseline")
            reasons.append("reference")
        elif annual >= annual_base and drawdown >= drawdown_base and calmar >= calmar_base:
            decisions.append("promote_challenger")
            reasons.append("annual_return_drawdown_calmar_not_worse")
        elif drawdown > drawdown_base and calmar >= calmar_base * 0.98:
            decisions.append("keep_shadow")
            reasons.append("risk_improved_without_large_calmar_decay")
        elif calmar >= calmar_base * 0.99 or annual >= annual_base * 0.99:
            decisions.append("mutate")
            reasons.append("near_baseline_but_not_dominant")
        else:
            decisions.append("reject")
            reasons.append("underperformed_baseline")
    frame["decision"] = decisions
    frame["decision_reason"] = reasons
    frame["annual_return_delta"] = frame["annual_return"] - annual_base
    frame["max_drawdown_delta"] = frame["max_drawdown"] - drawdown_base
    frame["calmar_delta"] = frame["calmar"] - calmar_base
    return frame.sort_values(["decision", "calmar"], ascending=[True, False])


def _best_variant(metrics: pd.DataFrame, column: str) -> dict[str, object]:
    row = metrics.sort_values(column, ascending=False).iloc[0]
    return {
        "variant": str(row["variant"]),
        "decision": str(row["decision"]),
        "annual_return": float(row["annual_return"]),
        "max_drawdown": float(row["max_drawdown"]),
        "calmar": float(row["calmar"]),
    }


def _overlay_next_step(metrics: pd.DataFrame) -> dict[str, object]:
    promoted = metrics.loc[metrics["decision"] == "promote_challenger"]
    shadow = metrics.loc[metrics["decision"] == "keep_shadow"]
    if not promoted.empty:
        return {
            "decision": "run_robustness_matrix",
            "variants": promoted["variant"].tolist(),
            "reason": "Promoted challengers must pass perturbation and execution tests before preset mutation.",
        }
    if not shadow.empty:
        return {
            "decision": "keep_shadow_and_mutate",
            "variants": shadow["variant"].tolist(),
            "reason": "Shadow variants improved risk but need role-specific mutation before promotion.",
        }
    return {
        "decision": "mutate_or_reject_overlays",
        "variants": metrics.loc[metrics["decision"] == "mutate", "variant"].tolist(),
        "reason": "No overlay dominated the baseline in this first role challenge.",
    }


def _mutation_next_step(metrics: pd.DataFrame) -> dict[str, object]:
    promoted = metrics.loc[metrics["decision"] == "promote_challenger"]
    if not promoted.empty:
        top = promoted.sort_values("calmar", ascending=False).head(5)
        return {
            "decision": "run_walk_forward_acceptance",
            "variants": top["variant"].tolist(),
            "reason": "Mutation variants beat the baseline and should move to annual/time-split walk-forward before preset mutation.",
        }
    near = metrics.loc[metrics["decision"].isin(["keep_shadow", "mutate"])]
    return {
        "decision": "mutate_or_keep_shadow",
        "variants": near.sort_values("calmar", ascending=False).head(5)["variant"].tolist(),
        "reason": "No mutation fully dominated the baseline; keep only near variants for further mutation.",
    }


def _overlay_markdown(label: str, summary: dict[str, object], metrics: pd.DataFrame) -> str:
    lines = [
        f"# {label}",
        "",
        "## Decision",
        "",
        str(summary["next_step"].get("reason", "")),
        "",
        "## Best variants",
        "",
        f"- Best by Calmar: `{summary['best_by_calmar']['variant']}` ({summary['best_by_calmar']['decision']})",
        f"- Best by annual return: `{summary['best_by_annual_return']['variant']}` ({summary['best_by_annual_return']['decision']})",
        "",
        "## Metrics",
        "",
    ]
    columns = [
        "variant",
        "role",
        "overlay_type",
        "factor_template",
        "annual_return",
        "max_drawdown",
        "calmar",
        "annual_return_delta",
        "max_drawdown_delta",
        "calmar_delta",
        "decision",
        "decision_reason",
    ]
    lines.extend(_markdown_table(metrics[columns]))
    return "\n".join(lines) + "\n"


def _mutation_markdown(label: str, summary: dict[str, object], metrics: pd.DataFrame) -> str:
    lines = [
        f"# {label}",
        "",
        "## Decision",
        "",
        str(summary["next_step"].get("reason", "")),
        "",
        "## Best variants",
        "",
        f"- Best by Calmar: `{summary['best_by_calmar']['variant']}` ({summary['best_by_calmar']['decision']})",
        f"- Best by annual return: `{summary['best_by_annual_return']['variant']}` ({summary['best_by_annual_return']['decision']})",
        "",
        "## Top metrics",
        "",
    ]
    columns = [
        "variant",
        "role",
        "overlay_type",
        "factor_template",
        "secondary_factor_template",
        "weight",
        "secondary_weight",
        "candidate_pool_size",
        "filter_quantile",
        "penalty_strength",
        "annual_return",
        "max_drawdown",
        "calmar",
        "annual_return_delta",
        "max_drawdown_delta",
        "calmar_delta",
        "decision",
        "decision_reason",
    ]
    lines.extend(_markdown_table(metrics[columns].sort_values("calmar", ascending=False).head(40)))
    return "\n".join(lines) + "\n"


def _execution_review_markdown(
    label: str,
    summary: dict[str, object],
    metrics: pd.DataFrame,
    coverage: pd.DataFrame,
) -> str:
    lines = [
        f"# {label}",
        "",
        "## Decision",
        "",
        f"`{summary['decision']['status']}`; minute Calmar win rate {summary['decision'].get('minute_calmar_win_rate', 0) * 100:.2f}%, worst drawdown delta {summary['decision'].get('worst_drawdown_delta')}.",
        "",
        "## Best challenger rows",
        "",
    ]
    metric_columns = [
        "variant",
        "mode",
        "capital",
        "participation_rate",
        "annual_return",
        "max_drawdown",
        "calmar",
        "annual_return_delta_vs_baseline",
        "max_drawdown_delta_vs_baseline",
        "calmar_delta_vs_baseline",
        "buy_share_fill_ratio",
        "avg_cash_ratio",
    ]
    challenger = metrics.loc[metrics["variant"] == summary["challenger"]].sort_values("calmar", ascending=False)
    lines.extend(_markdown_table(challenger[metric_columns].head(24)))
    lines.extend(["", "## Minute coverage", ""])
    coverage_columns = [
        "variant",
        "participation_rate",
        "active_target_cells",
        "minute_files",
        "date_count",
        "symbol_count",
        "start",
        "end",
        "target_price_touch_rate",
        "positive_capacity_rate",
        "median_available_lot_shares",
    ]
    lines.extend(_markdown_table(coverage[coverage_columns] if not coverage.empty else coverage))
    return "\n".join(lines) + "\n"


def _regime_budget_markdown(label: str, summary: dict[str, object], metrics: pd.DataFrame) -> str:
    lines = [
        f"# {label}",
        "",
        "## Decision",
        "",
        f"`{summary['decision']['status']}`: {summary['decision'].get('reason', '')}",
        "",
        "## Metrics",
        "",
    ]
    columns = [
        "variant",
        "budget",
        "annual_return",
        "max_drawdown",
        "calmar",
        "turnover",
        "avg_exposure_scale",
        "scaled_day_rate",
        "annual_return_delta_vs_challenger",
        "max_drawdown_delta_vs_challenger",
        "calmar_delta_vs_challenger",
        "decision",
    ]
    lines.extend(_markdown_table(metrics[columns]))
    return "\n".join(lines) + "\n"


def _prv_target_pool_markdown(
    label: str,
    summary: dict[str, object],
    factor_metrics: pd.DataFrame,
    strategy_metrics: pd.DataFrame,
) -> str:
    decision = summary.get("decision", {})
    lines = [
        f"# {label}",
        "",
        "## Decision",
        "",
        f"`{decision.get('status', 'unknown')}`: {decision.get('reason', '')}",
        "",
        "## Available PRV panels",
        "",
    ]
    panel_rows = pd.DataFrame(summary.get("panels", []))
    lines.extend(_markdown_table(panel_rows))
    lines.extend(["", "## Target-pool factor metrics", ""])
    factor_cols = [
        "panel",
        "variant",
        "role",
        "factor",
        "coverage",
        "ic_mean",
        "ic_ir",
        "ic_positive_rate",
        "long_short_mean",
        "ic_days",
    ]
    available_factor_cols = [column for column in factor_cols if column in factor_metrics.columns]
    lines.extend(_markdown_table(factor_metrics[available_factor_cols] if available_factor_cols else factor_metrics))
    lines.extend(["", "## Strategy overlay metrics", ""])
    strategy_cols = [
        "panel",
        "variant",
        "overlay",
        "annual_return",
        "max_drawdown",
        "calmar",
        "annual_return_delta_vs_challenger",
        "max_drawdown_delta_vs_challenger",
        "calmar_delta_vs_challenger",
        "decision",
    ]
    available_strategy_cols = [column for column in strategy_cols if column in strategy_metrics.columns]
    lines.extend(_markdown_table(strategy_metrics[available_strategy_cols] if available_strategy_cols else strategy_metrics))
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "- 本复验优先使用已有 PRV panel，避免在研究入口中盲扫 2017-2026 全市场分钟数据。",
            "- `all_a_2025_2026` 用于检验全 A 近端方向；`liquid300_2017_2026` 用于长窗口稳健性参照。",
            "- 若 PRV 只能改善解释或个别风险 overlay，不直接替换 `tail_risk_soft_q10_p25`；应保留为 shadow / warning。",
        ]
    )
    return "\n".join(lines) + "\n"


def _markdown_table(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return ["_No rows._"]
    printable = frame.copy()
    for column in printable.columns:
        if pd.api.types.is_float_dtype(printable[column]):
            printable[column] = printable[column].map(lambda value: "" if pd.isna(value) else f"{float(value):.6f}")
        else:
            printable[column] = printable[column].map(lambda value: "" if pd.isna(value) else str(value))
    columns = [str(column) for column in printable.columns]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in printable.to_dict(orient="records"):
        lines.append("| " + " | ".join(str(row[column]).replace("|", "\\|") for column in printable.columns) + " |")
    return lines


def _artifact_label(label: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in label.strip())
    return cleaned.strip("-") or DEFAULT_VERSION_REVIEW_LABEL
