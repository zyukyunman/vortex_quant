from __future__ import annotations

import argparse
import json
from pathlib import Path

import vortex.cli as cli
import vortex.research.cogalpha.company_run as company_run
from vortex.research.cogalpha import RUN_MANIFEST_SCHEMA, run_cogalpha_company_demo_cycle


def test_company_demo_cycle_writes_manifest_and_artifacts(tmp_path):
    output_dir = tmp_path / "out"

    manifest = run_cogalpha_company_demo_cycle(
        tmp_path / "workspace",
        output_dir=output_dir,
        run_id="research-cogalpha-demo-test",
        days=180,
        symbols=50,
        min_periods=20,
        notify=False,
    )

    manifest_path = output_dir / "run_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["schema"] == RUN_MANIFEST_SCHEMA
    assert payload["schema"] == RUN_MANIFEST_SCHEMA
    assert payload["run"]["status"] == "success"
    assert payload["run"]["run_id"] == "research-cogalpha-demo-test"
    assert payload["input"]["input_type"] == "deterministic_synthetic_ohlcv"
    assert payload["quality_gate"]["status"] == "passed_with_candidates"
    assert payload["decision"]["approval_required"] is True
    assert "直接下单" in payload["decision"]["blocked_actions"]
    assert payload["next_generation_queue"]

    for artifact in payload["artifacts"].values():
        assert Path(artifact["path"]).exists()
        assert artifact["schema"]


def test_company_demo_cycle_writes_failed_manifest(monkeypatch, tmp_path):
    def _raise_runtime_error(*_args, **_kwargs):
        raise RuntimeError("research cycle failed")

    monkeypatch.setattr(company_run, "run_cogalpha_research_cycle", _raise_runtime_error)

    manifest = company_run.run_cogalpha_company_demo_cycle(
        tmp_path / "workspace",
        output_dir=tmp_path / "failed",
        run_id="research-cogalpha-demo-failed",
        days=60,
        symbols=40,
        notify=False,
    )

    manifest_path = tmp_path / "failed" / "run_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["run"]["status"] == "failed"
    assert payload["run"]["status"] == "failed"
    assert payload["quality_gate"]["status"] == "failed"
    assert payload["decision"]["default_action"] == "incident_review"
    assert "research cycle failed" in payload["error"]
    assert any(task["status"] == "failed" for task in payload["tasks"])


def test_cmd_research_cogalpha_cycle_prints_json(monkeypatch, capsys):
    payload = {
        "schema": RUN_MANIFEST_SCHEMA,
        "run": {
            "run_id": "research-cogalpha-demo-cli",
            "status": "success",
            "output_dir": "/tmp/out",
        },
        "quality_gate": {"status": "passed_with_candidates", "promoted_candidate_count": 1},
        "artifacts": {
            "run_manifest": {
                "path": "/tmp/out/run_manifest.json",
                "schema": RUN_MANIFEST_SCHEMA,
            }
        },
    }
    monkeypatch.setattr(cli, "_run_research_cogalpha_cycle", lambda _args: payload)

    cli.cmd_research(
        argparse.Namespace(
            research_action="cogalpha-cycle",
            demo=True,
            format="json",
        )
    )

    captured = capsys.readouterr()
    parsed = json.loads(captured.out)
    assert parsed["schema"] == RUN_MANIFEST_SCHEMA
    assert parsed["run"]["run_id"] == "research-cogalpha-demo-cli"
