"""CogAlpha report writers."""
from __future__ import annotations

import json
from pathlib import Path

from vortex.research.cogalpha.schema import CogAlphaEvaluationResult


def write_generation_report_json(
    results: list[CogAlphaEvaluationResult],
    path: str | Path,
    *,
    metadata: dict[str, object] | None = None,
) -> Path:
    """Write a generation-level CogAlpha JSON report."""

    if not results:
        raise ValueError("results must be non-empty")
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": "vortex.cogalpha_generation_report.v1",
        "metadata": metadata or {},
        "results": [result.to_dict() for result in results],
    }
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output
