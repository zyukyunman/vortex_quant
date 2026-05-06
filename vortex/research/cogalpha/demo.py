"""Reproducible CogAlpha demo runner."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from vortex.research.alpha101_registry import DailyFactorInputs
from vortex.research.cogalpha.fitness import CogAlphaFitnessRule
from vortex.research.cogalpha.quality import QualityGateRule
from vortex.research.cogalpha.workflow import run_cogalpha_generation, summarize_generation_results


def build_demo_daily_inputs(days: int = 220, symbols: int = 60) -> DailyFactorInputs:
    """Build a deterministic OHLCV panel for proving the CogAlpha workflow."""

    dates = pd.date_range("2020-01-01", periods=days, freq="B")
    columns = [f"S{i:03d}" for i in range(symbols)]
    quality = np.linspace(0.0, 1.0, symbols)
    cyclic = np.sin(np.linspace(0.0, 6.0 * np.pi, days))
    close = pd.DataFrame(100.0, index=dates, columns=columns)
    for idx in range(1, days):
        cross_section = quality * 0.0018
        cycle_component = cyclic[idx] * (0.0004 + quality * 0.0003)
        reversal_component = (0.5 - quality) * 0.0003
        daily_ret = 0.0001 + cross_section + cycle_component + reversal_component
        close.iloc[idx] = close.iloc[idx - 1] * (1.0 + daily_ret)

    quality_row = pd.Series(quality, index=columns)
    open_ = close * (0.996 + quality_row * 0.001)
    high = close * (1.004 + quality_row * 0.004)
    low = close * (0.996 - quality_row * 0.002)
    volume_base = np.linspace(1200.0, 6000.0, symbols)
    volume_cycle = 1.0 + 0.10 * np.sin(np.linspace(0.0, 10.0 * np.pi, days))
    volume = pd.DataFrame(
        volume_cycle[:, None] * volume_base[None, :],
        index=dates,
        columns=columns,
    )
    amount = volume * close
    return DailyFactorInputs(open=open_, high=high, low=low, close=close, volume=volume, amount=amount)


def run_cogalpha_demo(output_dir: str | Path = "workspace/cogalpha/latest") -> dict[str, object]:
    """Run the full 21-agent CogAlpha demo and write JSON artifacts."""

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    inputs = build_demo_daily_inputs()
    report_path = output / "generation_report.json"
    results = run_cogalpha_generation(
        inputs,
        quality_rule=QualityGateRule(min_valid_dates=40, min_valid_symbols=40),
        fitness_rule=CogAlphaFitnessRule(min_rank_icir=0.0),
        report_path=report_path,
        min_periods=30,
    )
    summary = summarize_generation_results(results)
    summary_payload = {
        "schema": "vortex.cogalpha_generation_summary.v1",
        "demo": {
            "input_type": "deterministic_synthetic_ohlcv",
            "days": len(inputs.close),
            "symbols": len(inputs.close.columns),
            "note": "This proves the engineering workflow; it is not an A-share alpha conclusion.",
        },
        **summary,
    }
    summary_path = output / "generation_summary.json"
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "report_path": str(report_path),
        "summary_path": str(summary_path),
        "summary": summary_payload,
    }
