"""CogAlpha role-cycle runners for earnings-forecast strategy research."""
from __future__ import annotations

from dataclasses import dataclass
import dataclasses
import json
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend
from vortex.research.alpha101_registry import DailyFactorInputs
from vortex.research.cogalpha.recipes import executable_recipe_by_template
from vortex.research.cogalpha.research_cycle import (
    CogAlphaResearchCycleConfig,
    CogAlphaResearchDirection,
    run_cogalpha_research_cycle,
)
from vortex.runtime.workspace import Workspace
from vortex.strategy.earnings_forecast_runner import _filter_date_frame, _jsonable


CogAlphaRoleCycle = Literal["bad_holder", "candidate_quality", "regime_execution"]

DEFAULT_COGALPHA_ROLE_LABEL = "业绩预告CogAlpha角色因子循环"


@dataclass(frozen=True)
class CogAlphaRoleCycleSpec:
    """Role-specific CogAlpha direction used by earnings-forecast research."""

    role: CogAlphaRoleCycle
    direction: CogAlphaResearchDirection
    output_slug: str


@dataclass(frozen=True)
class EarningsForecastCogAlphaRoleArtifacts:
    """CogAlpha role-cycle artifacts."""

    json_path: Path
    report_path: Path
    summary_path: Path
    cycle_path: Path
    summary: dict[str, object]


def run_earnings_forecast_cogalpha_role_cycle(
    root: str | Path,
    *,
    role: CogAlphaRoleCycle,
    start: str,
    end: str,
    output_dir: str | Path | None = None,
    label: str = DEFAULT_COGALPHA_ROLE_LABEL,
    min_periods: int = 30,
    groups: int = 5,
    top_n: int = 10,
) -> EarningsForecastCogAlphaRoleArtifacts:
    """Run a CogAlpha generation cycle targeted at one strategy role."""

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir) if output_dir is not None else workspace.research_dir / "cogalpha" / "earnings_forecast"
    output_root.mkdir(parents=True, exist_ok=True)

    spec = cogalpha_role_cycle_spec(role)
    cycle_dir = output_root / spec.output_slug
    recipes = tuple(executable_recipe_by_template(template_id) for template_id in spec.direction.recipe_templates)
    inputs = load_daily_factor_inputs_for_cogalpha(workspace, start=start, end=end)
    result = run_cogalpha_research_cycle(
        inputs,
        direction=spec.direction,
        config=CogAlphaResearchCycleConfig(
            output_dir=cycle_dir,
            min_periods=int(min_periods),
            groups=int(groups),
            top_n=int(top_n),
            input_type="workspace_bars_daily_factor_inputs",
            input_note="Loaded from workspace data/bars; high/low/volume are derived if absent.",
        ),
        recipes=recipes,
    )
    summary = {
        "label": label,
        "role": role,
        "start": start,
        "end": end,
        "direction": spec.direction.to_dict(),
        "config": {
            "min_periods": int(min_periods),
            "groups": int(groups),
            "top_n": int(top_n),
        },
        "input_shape": {
            "dates": len(inputs.close.index),
            "symbols": len(inputs.close.columns),
        },
        "report_path": result["report_path"],
        "summary_path": result["summary_path"],
        "cycle_path": result["cycle_path"],
        "generation_summary": result["summary"],
        "next_step": _role_next_step(role, result["summary"]),
    }
    json_path = output_root / f"{_artifact_label(label)}-{role}.json"
    summary["json_path"] = str(json_path)
    json_path.write_text(json.dumps(_jsonable(summary), ensure_ascii=False, indent=2), encoding="utf-8")
    return EarningsForecastCogAlphaRoleArtifacts(
        json_path=json_path,
        report_path=Path(str(result["report_path"])),
        summary_path=Path(str(result["summary_path"])),
        cycle_path=Path(str(result["cycle_path"])),
        summary=summary,
    )


def cogalpha_role_cycle_spec(role: CogAlphaRoleCycle) -> CogAlphaRoleCycleSpec:
    """Return the role-specific CogAlpha direction definition."""

    specs: dict[CogAlphaRoleCycle, CogAlphaRoleCycleSpec] = {
        "bad_holder": CogAlphaRoleCycleSpec(
            role="bad_holder",
            output_slug="bad_holder",
            direction=CogAlphaResearchDirection(
                direction_id="earnings_bad_holder_filter_evolution",
                name="业绩预告坏持仓过滤因子循环",
                hypothesis=(
                    "业绩预告漂移策略的主要实盘风险来自少数坏持仓；下行波动、脆弱性、"
                    "拥挤同步和回撤位置可作为过滤或风险预算因子。"
                ),
                target_horizons=(1, 5, 20),
                agents=("AgentTailRisk", "AgentCrashPredictor", "AgentDrawdown", "AgentHerding", "AgentRangeVol"),
                recipe_templates=(
                    "tail_risk_downside_vol_20d",
                    "crash_fragility_high_range_low_liquidity_20d",
                    "drawdown_recovery_position_60d",
                    "herding_amount_crowding_reversal_20d",
                    "range_vol_20d",
                ),
                archive_tags=("earnings_forecast", "bad_holder", "risk_filter", "cogalpha"),
                known_risks=(
                    "风险因子可能降低年化但改善左尾，需要进入 keep_shadow 判定而不是直接 reject",
                    "与现有高波动暴露可能方向冲突，必须在策略层验证",
                ),
            ),
        ),
        "candidate_quality": CogAlphaRoleCycleSpec(
            role="candidate_quality",
            output_slug="candidate_quality",
            direction=CogAlphaResearchDirection(
                direction_id="earnings_candidate_quality_rerank_evolution",
                name="业绩预告候选池质量重排因子循环",
                hypothesis=(
                    "主 alpha 来自 forecast surprise，但候选池内部可用量价一致性、反转、趋势和复合质量"
                    "改善 Top160/Top200 到 Top110 的排序。"
                ),
                target_horizons=(1, 5, 20),
                agents=("AgentPriceVolumeCoherence", "AgentReversal", "AgentDailyTrend", "AgentComposite"),
                recipe_templates=(
                    "price_volume_coherence_20d",
                    "short_reversal_5d",
                    "daily_trend_20d",
                    "composite_trend_reversal_liquidity",
                ),
                archive_tags=("earnings_forecast", "candidate_rerank", "quality", "cogalpha"),
                known_risks=(
                    "候选池重排可能重复 amount20/liquidity 暴露",
                    "不能仅凭 IC 晋级，必须对照 baseline_top110_large",
                ),
            ),
        ),
        "regime_execution": CogAlphaRoleCycleSpec(
            role="regime_execution",
            output_slug="regime_execution",
            direction=CogAlphaResearchDirection(
                direction_id="earnings_regime_execution_guard_evolution",
                name="业绩预告状态与执行门控因子循环",
                hypothesis=(
                    "弱市、波动压缩/扩张、流动性冲击和市场状态会放大实盘偏差；"
                    "这些因子更适合作为新开仓 gate、执行优先级或风险预算。"
                ),
                target_horizons=(1, 5, 20),
                agents=("AgentMarketCycle", "AgentVolatilityRegime", "AgentRegimeGating", "AgentLiquidity"),
                recipe_templates=(
                    "market_cycle_relative_trend_60d",
                    "volatility_regime_compression_20d",
                    "regime_gated_trend_lowvol_60d",
                    "liquidity_range_impact",
                ),
                archive_tags=("earnings_forecast", "regime", "execution_gate", "cogalpha"),
                known_risks=(
                    "状态门控容易过拟合市场区间，必须经过 walk-forward 和扰动测试",
                    "执行 gate 的价值可能体现在 paper-live gap 而非理论年化",
                ),
            ),
        ),
    }
    try:
        return specs[role]
    except KeyError as exc:
        allowed = ", ".join(sorted(specs))
        raise KeyError(f"unknown CogAlpha role cycle: {role}; allowed: {allowed}") from exc


def load_daily_factor_inputs_for_cogalpha(workspace: Workspace, *, start: str, end: str) -> DailyFactorInputs:
    """Load daily bars into the wide matrices required by CogAlpha recipes."""

    storage = ParquetDuckDBBackend(workspace.data_dir)
    bars = storage.read("bars", filters={"date": (">=", int(start))})
    bars = _filter_date_frame(bars, start=start, end=end)
    if bars.empty:
        raise ValueError("数据集为空或缺失: bars")
    required = {"date", "symbol", "open", "close", "amount"}
    missing = sorted(required - set(bars.columns))
    if missing:
        raise ValueError(f"bars 缺少必要字段: {', '.join(missing)}")

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


def _pivot_market_frame(frame: pd.DataFrame, value: str) -> pd.DataFrame:
    data = frame[["date", "symbol", value]].copy()
    data["date"] = data["date"].astype(str)
    data[value] = pd.to_numeric(data[value], errors="coerce")
    pivot = data.pivot_table(index="date", columns="symbol", values=value, aggfunc="last")
    return pivot.sort_index()


def _role_next_step(role: CogAlphaRoleCycle, generation_summary: dict[str, object]) -> dict[str, object]:
    decision_counts = dict(generation_summary.get("decision_counts", {}))
    qualified_count = int(decision_counts.get("qualified", 0)) + int(decision_counts.get("elite", 0))
    if qualified_count > 0:
        decision = "enter_factor_overlay_challenge"
    else:
        decision = "mutate_or_keep_shadow"
    return {
        "role": role,
        "decision": decision,
        "qualified_or_elite_count": qualified_count,
        "rationale": "Qualified/elite candidates must still prove their strategy role against version-review presets.",
    }


def _artifact_label(label: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in label.strip())
    return cleaned.strip("-") or DEFAULT_COGALPHA_ROLE_LABEL
