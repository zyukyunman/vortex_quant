"""Selection-stability research reports for the earnings-forecast strategy."""
from __future__ import annotations

from dataclasses import dataclass
import dataclasses
import json
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from vortex.research.goal_review import ExperimentQuality
from vortex.runtime.workspace import Workspace
from vortex.strategy.earnings_forecast_drift import open_to_close_returns
from vortex.strategy.earnings_forecast_runner import (
    DEFAULT_VERSION_REVIEW_LABEL,
    EarningsForecastVersionPreset,
    _build_version_signal_context,
    _jsonable,
    get_earnings_forecast_version_preset,
    load_earnings_forecast_inputs,
)
from vortex.strategy.event_backtest import EventBacktestConfig, run_event_signal_backtest


DEFAULT_SELECTION_STABILITY_LABEL = "业绩预告选股稳定性审判"
DEFAULT_SELECTION_PRESETS: tuple[str, ...] = (
    "baseline_top110_large",
    "stable_100w",
    "aggressive_100w",
)
DEFAULT_SELECTION_HORIZONS: tuple[int, ...] = (1, 5, 20)


@dataclass(frozen=True)
class EarningsForecastSelectionStabilityArtifacts:
    """Selection-stability report outputs."""

    json_path: Path
    md_path: Path
    event_bucket_path: Path
    rank_bucket_path: Path
    holding_profile_path: Path
    style_exposure_path: Path
    summary: dict[str, object]


def run_earnings_forecast_selection_stability_review(
    root: str | Path,
    *,
    start: str,
    end: str,
    presets: Iterable[str] = DEFAULT_SELECTION_PRESETS,
    horizons: Iterable[int] = DEFAULT_SELECTION_HORIZONS,
    output_dir: str | Path | None = None,
    artifact_dir: str | Path | None = None,
    label: str = DEFAULT_SELECTION_STABILITY_LABEL,
    require_precise_data: bool = True,
) -> EarningsForecastSelectionStabilityArtifacts:
    """Audit whether preset selection logic is stable and explainable."""

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir) if output_dir is not None else workspace.strategy_dir
    artifact_root = Path(artifact_dir) if artifact_dir is not None else workspace.strategy_dir / "artifacts"
    output_root.mkdir(parents=True, exist_ok=True)
    artifact_root.mkdir(parents=True, exist_ok=True)

    preset_names = tuple(dict.fromkeys(str(item) for item in presets))
    if not preset_names:
        raise ValueError("presets must be non-empty")
    horizon_values = tuple(int(item) for item in horizons)
    if not horizon_values or any(item <= 0 for item in horizon_values):
        raise ValueError("horizons must contain positive integers")

    inputs = load_earnings_forecast_inputs(
        workspace,
        start=start,
        end=end,
        require_precise_data=require_precise_data,
    )
    returns = open_to_close_returns(inputs.open_prices, inputs.close_prices)
    quality = ExperimentQuality(
        pit_safe=True,
        adjusted_prices=True,
        cost_included=True,
        no_future_leakage=True,
        out_of_sample_checked=True,
    )

    event_bucket_frames: list[pd.DataFrame] = []
    rank_bucket_frames: list[pd.DataFrame] = []
    holding_profile_frames: list[pd.DataFrame] = []
    style_exposure_frames: list[pd.DataFrame] = []
    preset_summaries: list[dict[str, object]] = []

    for preset_name in preset_names:
        preset = get_earnings_forecast_version_preset(preset_name)
        signal, market_gate, blocked_buy, blocked_sell = _build_version_signal_context(inputs, preset=preset)
        backtest = run_event_signal_backtest(
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

        signal_observations = _signal_observations(signal, inputs.open_prices, inputs.close_prices, horizon_values)
        event_buckets = _event_bucket_metrics(signal_observations, preset=preset, horizons=horizon_values)
        rank_buckets = _rank_bucket_metrics(signal_observations, preset=preset, horizons=horizon_values)
        holding_profile = _holding_contribution_profile(
            backtest.weights,
            returns,
            signal,
            inputs.amount,
            preset=preset,
        )
        style_exposure = _style_exposure_summary(
            signal,
            backtest.weights,
            inputs.open_prices,
            inputs.close_prices,
            inputs.amount,
            preset=preset,
        )

        event_bucket_frames.append(event_buckets)
        rank_bucket_frames.append(rank_buckets)
        holding_profile_frames.append(holding_profile)
        style_exposure_frames.append(style_exposure)
        preset_summaries.append(
            _preset_selection_summary(
                preset,
                backtest_metrics=backtest.metrics.__dict__,
                signal_observations=signal_observations,
                holding_profile=holding_profile,
                style_exposure=style_exposure,
                horizons=horizon_values,
            )
        )

    event_bucket = _concat_or_empty(event_bucket_frames)
    rank_bucket = _concat_or_empty(rank_bucket_frames)
    holding_profile_all = _concat_or_empty(holding_profile_frames)
    style_exposure_all = _concat_or_empty(style_exposure_frames)

    safe_label = _artifact_label(label)
    event_bucket_path = artifact_root / f"{safe_label}事件alpha分桶.csv"
    rank_bucket_path = artifact_root / f"{safe_label}排名层级分桶.csv"
    holding_profile_path = artifact_root / f"{safe_label}持仓赢家输家画像.csv"
    style_exposure_path = artifact_root / f"{safe_label}风格暴露.csv"
    event_bucket.to_csv(event_bucket_path, index=False)
    rank_bucket.to_csv(rank_bucket_path, index=False)
    holding_profile_all.to_csv(holding_profile_path, index=False)
    style_exposure_all.to_csv(style_exposure_path, index=False)

    summary = {
        "label": label,
        "start": start,
        "end": end,
        "presets": [dataclasses.asdict(get_earnings_forecast_version_preset(name)) for name in preset_names],
        "horizons": list(horizon_values),
        "json_path": str(output_root / f"{safe_label}.json"),
        "md_path": str(output_root / f"{safe_label}.md"),
        "event_bucket_path": str(event_bucket_path),
        "rank_bucket_path": str(rank_bucket_path),
        "holding_profile_path": str(holding_profile_path),
        "style_exposure_path": str(style_exposure_path),
        "preset_summaries": preset_summaries,
        "research_decision": _selection_research_decision(preset_summaries),
    }
    json_path = output_root / f"{safe_label}.json"
    md_path = output_root / f"{safe_label}.md"
    json_path.write_text(json.dumps(_jsonable(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(
        _selection_stability_markdown(label, summary, event_bucket, rank_bucket, holding_profile_all, style_exposure_all),
        encoding="utf-8",
    )
    return EarningsForecastSelectionStabilityArtifacts(
        json_path=json_path,
        md_path=md_path,
        event_bucket_path=event_bucket_path,
        rank_bucket_path=rank_bucket_path,
        holding_profile_path=holding_profile_path,
        style_exposure_path=style_exposure_path,
        summary=summary,
    )


def _signal_observations(
    signal: pd.DataFrame,
    open_prices: pd.DataFrame,
    close_prices: pd.DataFrame,
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    active_signal = signal.replace([np.inf, -np.inf], np.nan)
    rows = _stack_frame(active_signal, "signal", dropna=True)
    rows.columns = ["date", "symbol", "signal"]
    rows = rows.loc[rows["signal"].notna()]
    if rows.empty:
        return pd.DataFrame(columns=["date", "symbol", "signal", "rank_in_day", "score_percentile"])

    rank_in_day = active_signal.rank(axis=1, ascending=False, method="first")
    score_percentile = active_signal.rank(axis=1, ascending=True, pct=True)
    rows = rows.merge(
        _stack_frame(rank_in_day, "rank_in_day", dropna=True),
        on=["date", "symbol"],
        how="left",
    )
    rows = rows.merge(
        _stack_frame(score_percentile, "score_percentile", dropna=True),
        on=["date", "symbol"],
        how="left",
    )
    for horizon in horizons:
        future_return = _forward_open_to_close_return(open_prices, close_prices, horizon)
        rows = rows.merge(
            _stack_frame(future_return, f"forward_return_{horizon}d", dropna=False),
            on=["date", "symbol"],
            how="left",
        )
    rows["score_bucket"] = pd.cut(
        rows["score_percentile"],
        bins=[0.0, 0.2, 0.4, 0.6, 0.8, 1.0],
        labels=["q1_lowest", "q2", "q3", "q4", "q5_highest"],
        include_lowest=True,
    ).astype("string")
    rows["rank_bucket"] = rows["rank_in_day"].map(_rank_bucket)
    return rows


def _forward_open_to_close_return(open_prices: pd.DataFrame, close_prices: pd.DataFrame, horizon: int) -> pd.DataFrame:
    future_close = close_prices.shift(-(horizon - 1))
    result = future_close / open_prices - 1.0
    return result.replace([np.inf, -np.inf], np.nan)


def _rank_bucket(rank: object) -> str:
    if pd.isna(rank):
        return "missing"
    value = int(rank)
    if value <= 30:
        return "top001_030"
    if value <= 50:
        return "top031_050"
    if value <= 80:
        return "top051_080"
    if value <= 110:
        return "top081_110"
    if value <= 160:
        return "top111_160"
    if value <= 200:
        return "top161_200"
    return "tail201_plus"


def _event_bucket_metrics(
    observations: pd.DataFrame,
    *,
    preset: EarningsForecastVersionPreset,
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    if observations.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    grouped = observations.groupby("score_bucket", dropna=False)
    for bucket, group in grouped:
        row: dict[str, object] = {
            "preset": preset.name,
            "bucket_type": "score_percentile",
            "bucket": str(bucket),
            "observation_count": int(len(group)),
            "active_dates": int(group["date"].nunique()),
            "avg_signal": float(group["signal"].mean()),
            "avg_rank_in_day": float(group["rank_in_day"].mean()),
        }
        for horizon in horizons:
            values = group[f"forward_return_{horizon}d"].dropna()
            row[f"mean_forward_return_{horizon}d"] = float(values.mean()) if not values.empty else np.nan
            row[f"hit_rate_{horizon}d"] = float((values > 0).mean()) if not values.empty else np.nan
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["preset", "bucket"])


def _rank_bucket_metrics(
    observations: pd.DataFrame,
    *,
    preset: EarningsForecastVersionPreset,
    horizons: tuple[int, ...],
) -> pd.DataFrame:
    if observations.empty:
        return pd.DataFrame()
    rows: list[dict[str, object]] = []
    grouped = observations.groupby("rank_bucket", dropna=False)
    for bucket, group in grouped:
        row: dict[str, object] = {
            "preset": preset.name,
            "bucket_type": "rank_layer",
            "bucket": str(bucket),
            "observation_count": int(len(group)),
            "active_dates": int(group["date"].nunique()),
            "avg_signal": float(group["signal"].mean()),
            "avg_rank_in_day": float(group["rank_in_day"].mean()),
        }
        for horizon in horizons:
            values = group[f"forward_return_{horizon}d"].dropna()
            row[f"mean_forward_return_{horizon}d"] = float(values.mean()) if not values.empty else np.nan
            row[f"hit_rate_{horizon}d"] = float((values > 0).mean()) if not values.empty else np.nan
        rows.append(row)
    order = ["top001_030", "top031_050", "top051_080", "top081_110", "top111_160", "top161_200", "tail201_plus"]
    frame = pd.DataFrame(rows)
    frame["bucket_order"] = frame["bucket"].map({name: idx for idx, name in enumerate(order)}).fillna(999).astype(int)
    return frame.sort_values(["preset", "bucket_order"]).drop(columns=["bucket_order"])


def _holding_contribution_profile(
    weights: pd.DataFrame,
    returns: pd.DataFrame,
    signal: pd.DataFrame,
    amount: pd.DataFrame,
    *,
    preset: EarningsForecastVersionPreset,
) -> pd.DataFrame:
    aligned_returns = returns.reindex(index=weights.index, columns=weights.columns).fillna(0.0)
    active_weights = weights.where(weights.abs() > 1e-12)
    contributions = active_weights.fillna(0.0) * aligned_returns
    amount20 = amount.rolling(20, min_periods=5).mean().shift(1).reindex(index=weights.index, columns=weights.columns)
    signal_aligned = signal.reindex(index=weights.index, columns=weights.columns)

    stacked = pd.concat(
        [
            _stack_frame(active_weights, "weight", dropna=True).set_index(["date", "symbol"])["weight"],
            _stack_frame(aligned_returns, "daily_return", dropna=False).set_index(["date", "symbol"])["daily_return"],
            _stack_frame(contributions, "daily_contribution", dropna=False).set_index(["date", "symbol"])["daily_contribution"],
            _stack_frame(signal_aligned, "signal", dropna=False).set_index(["date", "symbol"])["signal"],
            _stack_frame(amount20, "amount20", dropna=False).set_index(["date", "symbol"])["amount20"],
        ],
        axis=1,
        join="inner",
    ).reset_index()
    stacked.columns = ["date", "symbol", "weight", "daily_return", "daily_contribution", "signal", "amount20"]
    if stacked.empty:
        return pd.DataFrame()
    grouped = stacked.groupby("symbol", dropna=False)
    frame = grouped.agg(
        holding_days=("date", "nunique"),
        avg_weight=("weight", "mean"),
        total_contribution=("daily_contribution", "sum"),
        avg_daily_contribution=("daily_contribution", "mean"),
        hit_rate=("daily_return", lambda series: float((series > 0).mean())),
        worst_daily_return=("daily_return", "min"),
        best_daily_return=("daily_return", "max"),
        avg_signal=("signal", "mean"),
        avg_amount20=("amount20", "mean"),
    ).reset_index()
    frame.insert(0, "preset", preset.name)
    frame["contribution_rank"] = frame["total_contribution"].rank(ascending=False, method="first").astype(int)
    frame["contribution_role"] = np.where(
        frame["total_contribution"] >= 0,
        "winner",
        "loser",
    )
    return frame.sort_values(["preset", "total_contribution"], ascending=[True, False])


def _style_exposure_summary(
    signal: pd.DataFrame,
    weights: pd.DataFrame,
    open_prices: pd.DataFrame,
    close_prices: pd.DataFrame,
    amount: pd.DataFrame,
    *,
    preset: EarningsForecastVersionPreset,
) -> pd.DataFrame:
    returns = open_to_close_returns(open_prices, close_prices)
    factors = {
        "amount20": amount.rolling(20, min_periods=5).mean().shift(1),
        "volatility20": returns.rolling(20, min_periods=5).std().shift(1),
        "momentum20": (close_prices / close_prices.shift(20) - 1.0).shift(1),
        "reversal5": -(close_prices / close_prices.shift(5) - 1.0).shift(1),
        "intraday_range20": returns.abs().rolling(20, min_periods=5).mean().shift(1),
    }
    rows: list[dict[str, object]] = []
    active_weights = weights.where(weights.abs() > 1e-12)
    for factor_name, factor in factors.items():
        aligned_factor = factor.reindex(index=weights.index, columns=weights.columns)
        percentile = aligned_factor.rank(axis=1, pct=True)
        weighted = _weighted_cross_sectional_mean(percentile, active_weights)
        active_unweighted = percentile.where(active_weights.notna()).mean(axis=1)
        signal_corr = _daily_signal_factor_corr(
            signal.reindex(index=weights.index, columns=weights.columns),
            aligned_factor,
        )
        rows.append(
            {
                "preset": preset.name,
                "factor": factor_name,
                "avg_weighted_percentile": float(weighted.mean()) if not weighted.dropna().empty else np.nan,
                "avg_active_percentile": float(active_unweighted.mean()) if not active_unweighted.dropna().empty else np.nan,
                "avg_signal_spearman_corr": float(signal_corr.mean()) if not signal_corr.dropna().empty else np.nan,
                "corr_positive_rate": float((signal_corr > 0).mean()) if not signal_corr.dropna().empty else np.nan,
                "valid_dates": int(weighted.dropna().shape[0]),
            }
        )
    return pd.DataFrame(rows)


def _weighted_cross_sectional_mean(factor_percentile: pd.DataFrame, weights: pd.DataFrame) -> pd.Series:
    abs_weights = weights.abs()
    denom = abs_weights.sum(axis=1).replace(0.0, np.nan)
    return (factor_percentile * abs_weights).sum(axis=1) / denom


def _stack_frame(frame: pd.DataFrame, name: str, *, dropna: bool) -> pd.DataFrame:
    stacked = frame.stack().rename(name).reset_index()
    stacked.columns = ["date", "symbol", name]
    if dropna:
        stacked = stacked.loc[stacked[name].notna()]
    return stacked


def _daily_signal_factor_corr(signal: pd.DataFrame, factor: pd.DataFrame) -> pd.Series:
    values: list[float] = []
    dates: list[object] = []
    for date in signal.index:
        pair = pd.concat([signal.loc[date].rename("signal"), factor.loc[date].rename("factor")], axis=1).dropna()
        if len(pair) < 3 or pair["signal"].nunique() < 2 or pair["factor"].nunique() < 2:
            continue
        dates.append(date)
        values.append(float(pair["signal"].corr(pair["factor"], method="spearman")))
    return pd.Series(values, index=pd.Index(dates, name="date"), dtype=float)


def _preset_selection_summary(
    preset: EarningsForecastVersionPreset,
    *,
    backtest_metrics: dict[str, object],
    signal_observations: pd.DataFrame,
    holding_profile: pd.DataFrame,
    style_exposure: pd.DataFrame,
    horizons: tuple[int, ...],
) -> dict[str, object]:
    top_bucket = signal_observations.loc[signal_observations["rank_bucket"] == "top001_030"]
    tail_bucket = signal_observations.loc[signal_observations["rank_bucket"].isin(["top111_160", "top161_200", "tail201_plus"])]
    horizon = horizons[0]
    top_forward = top_bucket[f"forward_return_{horizon}d"].mean() if not top_bucket.empty else np.nan
    tail_forward = tail_bucket[f"forward_return_{horizon}d"].mean() if not tail_bucket.empty else np.nan
    losers = holding_profile.loc[holding_profile["total_contribution"] < 0] if not holding_profile.empty else pd.DataFrame()
    return {
        "preset": preset.name,
        "top_n": preset.top_n,
        "candidate_pool_size": preset.candidate_pool_size,
        "backtest_metrics": backtest_metrics,
        "signal_observation_count": int(len(signal_observations)),
        "active_dates": int(signal_observations["date"].nunique()) if not signal_observations.empty else 0,
        f"top30_mean_forward_return_{horizon}d": None if pd.isna(top_forward) else float(top_forward),
        f"tail111_plus_mean_forward_return_{horizon}d": None if pd.isna(tail_forward) else float(tail_forward),
        "loser_symbol_count": int(len(losers)),
        "loser_contribution_sum": float(losers["total_contribution"].sum()) if not losers.empty else 0.0,
        "dominant_style_exposures": _dominant_style_exposures(style_exposure),
    }


def _dominant_style_exposures(style_exposure: pd.DataFrame) -> list[dict[str, object]]:
    if style_exposure.empty:
        return []
    frame = style_exposure.copy()
    frame["distance_from_neutral"] = (frame["avg_weighted_percentile"] - 0.5).abs()
    top = frame.sort_values("distance_from_neutral", ascending=False).head(3)
    return [
        {
            "factor": str(row["factor"]),
            "avg_weighted_percentile": float(row["avg_weighted_percentile"]),
            "avg_signal_spearman_corr": None
            if pd.isna(row["avg_signal_spearman_corr"])
            else float(row["avg_signal_spearman_corr"]),
        }
        for row in top.to_dict(orient="records")
    ]


def _selection_research_decision(preset_summaries: list[dict[str, object]]) -> dict[str, object]:
    if not preset_summaries:
        return {"decision": "blocked", "reason": "no preset summaries"}
    sorted_by_calmar = sorted(
        preset_summaries,
        key=lambda item: float(dict(item.get("backtest_metrics", {})).get("calmar", float("-inf"))),
        reverse=True,
    )
    leader = sorted_by_calmar[0]
    return {
        "decision": "continue_factor_research",
        "leader_by_theory_calmar": leader["preset"],
        "reason": (
            "This report is diagnostic only. Use winner/loser labels, rank-layer returns, "
            "and style exposures to seed CogAlpha factor-role tests before promoting any preset."
        ),
    }


def _selection_stability_markdown(
    label: str,
    summary: dict[str, object],
    event_bucket: pd.DataFrame,
    rank_bucket: pd.DataFrame,
    holding_profile: pd.DataFrame,
    style_exposure: pd.DataFrame,
) -> str:
    lines = [
        f"# {label}",
        "",
        "## Research decision",
        "",
        str(summary["research_decision"].get("reason", "")),
        "",
        "## Preset summary",
        "",
        "| Preset | Annual return | Max drawdown | Calmar | Signal observations | Loser contribution |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for item in summary["preset_summaries"]:
        metrics = dict(item.get("backtest_metrics", {}))
        lines.append(
            f"| {item['preset']} | "
            f"{float(metrics.get('annual_return', 0.0)) * 100:.2f}% | "
            f"{float(metrics.get('max_drawdown', 0.0)) * 100:.2f}% | "
            f"{float(metrics.get('calmar', 0.0)):.2f} | "
            f"{int(item['signal_observation_count'])} | "
            f"{float(item['loser_contribution_sum']) * 100:.2f}% |"
        )
    lines.extend(["", "## Rank-layer forward returns", ""])
    lines.extend(_markdown_table(rank_bucket.head(40)))
    lines.extend(["", "## Score-bucket forward returns", ""])
    lines.extend(_markdown_table(event_bucket.head(40)))
    lines.extend(["", "## Largest winners / losers", ""])
    if not holding_profile.empty:
        top_winners = holding_profile.sort_values("total_contribution", ascending=False).head(10)
        top_losers = holding_profile.sort_values("total_contribution", ascending=True).head(10)
        lines.extend(["### Winners", ""])
        lines.extend(_markdown_table(top_winners))
        lines.extend(["", "### Losers", ""])
        lines.extend(_markdown_table(top_losers))
    lines.extend(["", "## Style exposures", ""])
    lines.extend(_markdown_table(style_exposure))
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
        values = [str(row[column]).replace("|", "\\|") for column in printable.columns]
        lines.append("| " + " | ".join(values) + " |")
    return lines


def _concat_or_empty(frames: list[pd.DataFrame]) -> pd.DataFrame:
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _artifact_label(label: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in label.strip())
    return cleaned.strip("-") or DEFAULT_VERSION_REVIEW_LABEL
