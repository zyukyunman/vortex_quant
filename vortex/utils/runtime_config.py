from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from vortex.strategy.dividend import DEFAULT_WEIGHTS


VALID_BACKTEST_FREQ = {"M", "Q", "SA"}


@dataclass
class BacktestRuntimeConfig:
    """Resolved runtime config for backtest scripts."""

    config_path: Optional[Path] = None
    weights_source: Optional[Path] = None
    run_config_source: Optional[Path] = None
    config_used: bool = False
    used_run_config: bool = False
    method: str = "default"
    weights: Dict[str, float] = field(default_factory=lambda: DEFAULT_WEIGHTS.copy())
    run_config: Dict[str, Any] = field(default_factory=dict)
    start: str = ""
    end: str = ""
    freq: str = "SA"
    top_n: int = 30


def _read_json_file(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    return {}


def _coerce_path(path_str: str | Path) -> Path:
    path = Path(path_str).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path


def resolve_backtest_runtime_config(
    cfg,
    *,
    default_start: str,
    default_end: str,
    default_freq: str,
    default_top_n: int,
    config_path: str | Path | None = None,
    use_config: bool = True,
    prefer_run_config: bool = False,
    start: str | None = None,
    end: str | None = None,
    freq: str | None = None,
    top_n: int | None = None,
) -> BacktestRuntimeConfig:
    """Resolve weights and optional run_config from research outputs."""

    runtime = BacktestRuntimeConfig(
        start=default_start,
        end=default_end,
        freq=default_freq,
        top_n=default_top_n,
    )

    if use_config:
        default_cfg = cfg.data_dir / "reports" / "strategy_weights_config.json"
        fallback_weights = cfg.data_dir / "reports" / "weights_optimal.json"

        candidate: Optional[Path] = None
        if config_path:
            candidate = _coerce_path(config_path)
        elif default_cfg.exists():
            candidate = default_cfg
        elif fallback_weights.exists():
            candidate = fallback_weights

        if candidate is not None and candidate.exists():
            payload = _read_json_file(candidate)
            runtime.config_used = True
            runtime.config_path = candidate
            runtime.weights_source = candidate
            runtime.method = str(payload.get("method", "configured"))
            runtime.weights = payload.get("weights", DEFAULT_WEIGHTS.copy())

            if candidate.name == "strategy_weights_config.json":
                weights_file = payload.get("weights_file")
                if weights_file:
                    linked_path = (candidate.parent / weights_file).resolve()
                    if linked_path.exists():
                        linked_payload = _read_json_file(linked_path)
                        runtime.run_config = linked_payload.get("run_config", {}) or {}
                        runtime.run_config_source = linked_path
                        if "weights" not in payload and linked_payload.get("weights"):
                            runtime.weights = linked_payload["weights"]
                            runtime.weights_source = linked_path
            elif payload.get("run_config"):
                runtime.run_config = payload.get("run_config", {}) or {}
                runtime.run_config_source = candidate

    if not isinstance(runtime.weights, dict) or not runtime.weights:
        runtime.weights = DEFAULT_WEIGHTS.copy()
        runtime.method = "default"

    if not isinstance(runtime.run_config, dict):
        runtime.run_config = {}

    runtime.used_run_config = prefer_run_config and bool(runtime.run_config)

    resolved_start = start
    if resolved_start is None and runtime.used_run_config:
        resolved_start = runtime.run_config.get("bt_start")
    runtime.start = str(resolved_start or default_start)

    resolved_end = end
    if resolved_end is None and runtime.used_run_config:
        resolved_end = runtime.run_config.get("bt_end")
    runtime.end = str(resolved_end or default_end)

    resolved_freq = freq
    if resolved_freq is None and runtime.used_run_config:
        resolved_freq = runtime.run_config.get("freq")
    runtime.freq = str(resolved_freq or default_freq).upper()
    if runtime.freq not in VALID_BACKTEST_FREQ:
        raise ValueError(f"invalid backtest freq: {runtime.freq}")

    resolved_top_n = top_n
    if resolved_top_n is None and runtime.used_run_config:
        resolved_top_n = runtime.run_config.get("top_n")
    runtime.top_n = int(resolved_top_n or default_top_n)

    return runtime