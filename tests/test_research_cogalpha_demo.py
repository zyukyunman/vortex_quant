from __future__ import annotations

import json

from vortex.research.cogalpha import build_demo_daily_inputs, run_cogalpha_demo


def test_build_demo_daily_inputs_is_wide_and_deterministic():
    first = build_demo_daily_inputs(days=80, symbols=20)
    second = build_demo_daily_inputs(days=80, symbols=20)

    assert first.close.shape == (80, 20)
    assert first.close.equals(second.close)
    assert first.amount.notna().all().all()


def test_cogalpha_demo_writes_report_and_summary(tmp_path):
    result = run_cogalpha_demo(tmp_path)
    report_path = tmp_path / "generation_report.json"
    summary_path = tmp_path / "generation_summary.json"

    assert result["report_path"] == str(report_path)
    assert result["summary_path"] == str(summary_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    assert report["schema"] == "vortex.cogalpha_generation_report.v1"
    assert summary["schema"] == "vortex.cogalpha_generation_summary.v1"
    assert summary["recipe_count"] == 21
    assert sum(summary["decision_counts"].values()) == 21
    assert sum(summary["semantic_status_counts"].values()) == 21
    assert summary["semantic_status_counts"]["mutation_proxy"] == 1
    assert len(summary["agent_results"]) == 21
