from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import pandas as pd
import pytest

import vortex.cli as cli
from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend
from vortex.runtime.workspace import Workspace
from vortex.strategy.earnings_forecast_drift import EarningsForecastDriftConfig
from vortex.strategy.earnings_forecast_cogalpha import run_earnings_forecast_cogalpha_role_cycle
from vortex.strategy.earnings_forecast_overlay import (
    run_earnings_forecast_daily_mutation_grid,
    run_earnings_forecast_factor_overlay_challenge,
    run_earnings_forecast_overlay_execution_review,
    run_earnings_forecast_prv_target_pool_review,
    run_earnings_forecast_regime_budget_challenge,
    run_earnings_forecast_strategy_robustness_matrix,
)
from vortex.strategy.earnings_forecast_runner import (
    _build_version_signal_context,
    get_earnings_forecast_version_preset,
    load_earnings_forecast_inputs,
    run_opening_auction_execution_review,
    run_opening_liquidity_review,
    run_earnings_forecast_live_handoff,
    run_earnings_forecast_shadow_plan,
    run_earnings_forecast_version_review,
    run_precise_earnings_forecast_review,
)
from vortex.strategy.earnings_forecast_live import (
    execute_pending_qmt_task,
    get_earnings_forecast_auto_observability_paths,
    is_trade_day,
    prepare_earnings_forecast_next_session,
    resolve_next_trade_date,
    run_earnings_forecast_auto_once,
    run_earnings_forecast_auto_cycle_once,
)
from vortex.strategy.earnings_forecast_selection import run_earnings_forecast_selection_stability_review
from vortex.trade.market_rules import MarketPermissionConfig


def _clean_fake_bridge_transport(method, url, payload=None, headers=None):  # noqa: ARG001
    if url == "/api/meta/health":
        return {"status": "ok", "message": "healthy"}
    if url == "/api/meta/connection_status":
        return {"data": {"connected": True}}
    if url == "/api/trading/asset?account_id=99034443":
        return {"data": {"available_cash": 10_000_000.0, "total_asset": 10_000_000.0, "market_value": 0.0}}
    if url == "/api/trading/positions?account_id=99034443":
        return {"data": []}
    if url == "/api/trading/orders?account_id=99034443":
        return {"data": []}
    if url == "/api/trading/trades?account_id=99034443":
        return {"data": []}
    raise AssertionError(f"unexpected request: {method} {url}")


def test_load_earnings_forecast_inputs_filters_pre_start_events(tmp_path):
    root = _build_earnings_workspace(tmp_path)
    workspace = Workspace(root)

    inputs = load_earnings_forecast_inputs(
        workspace,
        start="20260101",
        end="20260310",
        require_precise_data=True,
    )

    assert set(inputs.forecast["ann_date"]) == {"20260205"}
    assert inputs.stk_limit is not None
    assert inputs.suspend_events is not None


def test_load_earnings_forecast_inputs_keeps_pre_start_financial_risk(tmp_path):
    root = _build_earnings_workspace(tmp_path)
    workspace = Workspace(root)

    inputs = load_earnings_forecast_inputs(
        workspace,
        start="20260101",
        end="20260310",
        require_precise_data=True,
    )

    assert inputs.st_risk_events is not None
    risk_rows = inputs.st_risk_events.loc[inputs.st_risk_events["symbol"] == "000002.SZ"]
    assert not risk_rows.empty
    assert risk_rows["date"].min() == "20260101"


def test_run_precise_earnings_forecast_review_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    artifacts = run_precise_earnings_forecast_review(
        root,
        start="20260101",
        end="20260310",
        output_dir=root / "strategy" / "reports",
        artifact_dir=root / "strategy" / "artifacts",
        label="test-review",
        config=EarningsForecastDriftConfig(hold_days=5, top_n=2),
        cost_grid=(20.0,),
        segments=(),
    )

    assert artifacts.json_path.exists()
    assert artifacts.html_path.exists()
    assert artifacts.holdings_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["strategy"] == "earnings_forecast_drift"
    assert payload["metadata"]["tradability_review"]["data_missing"] == []
    assert payload["metadata"]["safe_3pct_result"]["metrics"]["annual_return"] is not None
    assert "metrics" in artifacts.summary


def test_cmd_strategy_precise_review_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="precise-review",
            root=str(root),
            start="20260101",
            end="20260310",
            output_dir=str(root / "strategy" / "reports"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-review",
            costs="20",
            portfolio_notional=100_000_000,
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-review"
    assert payload["json_path"].endswith("cli-review报告.json")
    assert (root / "strategy" / "reports" / "cli-review报告.json").exists()


def test_get_earnings_forecast_version_preset_returns_named_preset():
    preset = get_earnings_forecast_version_preset("aggressive_100w")

    assert preset.top_n == 30
    assert preset.candidate_pool_size == 60
    assert preset.run_lot_execution is True
    assert preset.market_cap_top_pct == 0.50
    assert preset.market_cap_field == "total_mv"

    challenger = get_earnings_forecast_version_preset("baseline_top110_large")

    assert challenger.top_n == 110
    assert challenger.candidate_pool_size is None
    assert challenger.liquidity_rerank_weight == 0.0
    assert challenger.portfolio_notional == 100_000_000.0


def test_version_signal_context_filters_lower_half_market_cap(tmp_path):
    root = _build_earnings_workspace(tmp_path)
    workspace = Workspace(root)
    inputs = load_earnings_forecast_inputs(
        workspace,
        start="20260101",
        end="20260310",
        require_precise_data=True,
    )

    signal, _market_gate, _blocked_buy, _blocked_sell = _build_version_signal_context(
        inputs,
        preset=get_earnings_forecast_version_preset("aggressive_100w"),
    )

    assert signal["000001.SZ"].notna().any()
    assert signal["000003.SZ"].dropna().empty


def test_run_earnings_forecast_version_review_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    artifacts = run_earnings_forecast_version_review(
        root,
        preset_name="aggressive_100w",
        start="20260101",
        end="20260310",
        output_dir=root / "strategy" / "version-review",
        artifact_dir=root / "strategy" / "artifacts",
        label="version-test",
    )

    assert artifacts.json_path.exists()
    assert artifacts.metrics_path.exists()
    assert artifacts.weights_path.exists()
    assert artifacts.equity_path.exists()
    assert artifacts.annual_returns_path.exists()
    assert artifacts.monthly_returns_path.exists()
    assert artifacts.drawdowns_path.exists()
    assert artifacts.trades_path is not None and artifacts.trades_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["preset"]["name"] == "aggressive_100w"
    assert payload["lot_metrics"] is not None
    assert "sortino" in payload["theory_metrics"]
    assert "cvar_5pct" in payload["theory_metrics"]
    assert "worst_5d_return" in payload["theory_metrics"]
    assert payload["drawdowns_path"].endswith("version-test-aggressive_100w回撤区间.csv")


def test_cmd_strategy_version_review_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="version-review",
            root=str(root),
            preset="aggressive_100w",
            start="20260101",
            end="20260310",
            output_dir=str(root / "strategy" / "version-review"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-version",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-version"
    assert payload["preset"]["name"] == "aggressive_100w"
    assert payload["json_path"].endswith("cli-version-aggressive_100w.json")
    assert (root / "strategy" / "version-review" / "cli-version-aggressive_100w.json").exists()


def test_run_earnings_forecast_selection_stability_review_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    artifacts = run_earnings_forecast_selection_stability_review(
        root,
        start="20260101",
        end="20260310",
        presets=("stable_100w", "aggressive_100w"),
        horizons=(1, 5),
        output_dir=root / "strategy" / "selection",
        artifact_dir=root / "strategy" / "artifacts",
        label="selection-test",
    )

    assert artifacts.json_path.exists()
    assert artifacts.md_path.exists()
    assert artifacts.event_bucket_path.exists()
    assert artifacts.rank_bucket_path.exists()
    assert artifacts.holding_profile_path.exists()
    assert artifacts.style_exposure_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["label"] == "selection-test"
    assert len(payload["preset_summaries"]) == 2
    assert payload["research_decision"]["decision"] == "continue_factor_research"


def test_cmd_strategy_selection_stability_review_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="selection-stability-review",
            root=str(root),
            start="20260101",
            end="20260310",
            presets="stable_100w,aggressive_100w",
            horizons="1,5",
            output_dir=str(root / "strategy" / "selection"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-selection",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-selection"
    assert payload["json_path"].endswith("cli-selection.json")
    assert (root / "strategy" / "selection" / "cli-selection.json").exists()


def test_run_earnings_forecast_cogalpha_role_cycle_writes_artifacts(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    artifacts = run_earnings_forecast_cogalpha_role_cycle(
        root,
        role="bad_holder",
        start="20260101",
        end="20260310",
        output_dir=root / "research" / "cogalpha",
        label="cogalpha-test",
        min_periods=5,
        groups=3,
        top_n=3,
    )

    assert artifacts.json_path.exists()
    assert artifacts.report_path.exists()
    assert artifacts.summary_path.exists()
    assert artifacts.cycle_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["role"] == "bad_holder"
    assert payload["next_step"]["role"] == "bad_holder"
    assert payload["input_shape"]["symbols"] == 3


def test_cmd_strategy_cogalpha_role_cycle_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="cogalpha-role-cycle",
            root=str(root),
            role="candidate_quality",
            start="20260101",
            end="20260310",
            output_dir=str(root / "research" / "cogalpha"),
            label="cli-cogalpha",
            min_periods=5,
            groups=3,
            top_n=3,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-cogalpha"
    assert payload["role"] == "candidate_quality"
    assert payload["json_path"].endswith("cli-cogalpha-candidate_quality.json")


def test_run_earnings_forecast_factor_overlay_challenge_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    artifacts = run_earnings_forecast_factor_overlay_challenge(
        root,
        preset_name="stable_100w",
        start="20260101",
        end="20260310",
        output_dir=root / "strategy" / "overlay",
        artifact_dir=root / "strategy" / "artifacts",
        label="overlay-test",
    )

    assert artifacts.json_path.exists()
    assert artifacts.metrics_path.exists()
    assert artifacts.md_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["label"] == "overlay-test"
    assert payload["preset"] == "stable_100w"
    assert payload["variant_count"] > 1


def test_cmd_strategy_factor_overlay_challenge_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="factor-overlay-challenge",
            root=str(root),
            preset="stable_100w",
            start="20260101",
            end="20260310",
            output_dir=str(root / "strategy" / "overlay"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-overlay",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-overlay"
    assert payload["preset"] == "stable_100w"
    assert payload["json_path"].endswith("cli-overlay.json")


def test_run_earnings_forecast_strategy_robustness_matrix_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    artifacts = run_earnings_forecast_strategy_robustness_matrix(
        root,
        preset_name="stable_100w",
        challenger_name="rerank_tail_risk_w010",
        start="20260101",
        end="20260310",
        output_dir=root / "strategy" / "robustness",
        artifact_dir=root / "strategy" / "artifacts",
        label="robustness-test",
    )

    assert artifacts.json_path.exists()
    assert artifacts.matrix_path.exists()
    assert artifacts.md_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["label"] == "robustness-test"
    assert payload["challenger"] == "rerank_tail_risk_w010"
    assert payload["scenario_count"] >= 1


def test_cmd_strategy_robustness_matrix_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="robustness-matrix",
            root=str(root),
            preset="stable_100w",
            challenger="rerank_tail_risk_w010",
            start="20260101",
            end="20260310",
            output_dir=str(root / "strategy" / "robustness"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-robustness",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-robustness"
    assert payload["challenger"] == "rerank_tail_risk_w010"
    assert payload["json_path"].endswith("cli-robustness.json")


def test_run_earnings_forecast_daily_mutation_grid_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    artifacts = run_earnings_forecast_daily_mutation_grid(
        root,
        preset_name="stable_100w",
        start="20260101",
        end="20260310",
        output_dir=root / "strategy" / "mutation",
        artifact_dir=root / "strategy" / "artifacts",
        label="mutation-test",
    )

    assert artifacts.json_path.exists()
    assert artifacts.metrics_path.exists()
    assert artifacts.md_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["label"] == "mutation-test"
    assert payload["preset"] == "stable_100w"
    assert payload["variant_count"] > 10


def test_cmd_strategy_daily_mutation_grid_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="daily-mutation-grid",
            root=str(root),
            preset="stable_100w",
            start="20260101",
            end="20260310",
            output_dir=str(root / "strategy" / "mutation"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-mutation",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-mutation"
    assert payload["preset"] == "stable_100w"
    assert payload["json_path"].endswith("cli-mutation.json")


def test_run_earnings_forecast_overlay_execution_review_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)
    _build_minute_symbol_year_cache(root)

    artifacts = run_earnings_forecast_overlay_execution_review(
        root,
        preset_name="stable_100w",
        challenger_name="tail_risk_soft_q10_p25",
        start="20260101",
        end="20260310",
        output_dir=root / "strategy" / "execution",
        artifact_dir=root / "strategy" / "artifacts",
        label="execution-test",
        capital_tiers=(1_000_000.0,),
        participation_rates=(0.20,),
    )

    assert artifacts.json_path.exists()
    assert artifacts.metrics_path.exists()
    assert artifacts.md_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["label"] == "execution-test"
    assert payload["challenger"] == "tail_risk_soft_q10_p25"
    assert payload["decision"]["scenario_count"] >= 1
    assert payload["minute_coverage"][0]["minute_files"] > 0


def test_cmd_strategy_overlay_execution_review_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)
    _build_minute_symbol_year_cache(root)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="overlay-execution-review",
            root=str(root),
            preset="stable_100w",
            challenger="tail_risk_soft_q10_p25",
            start="20260101",
            end="20260310",
            capital_tiers="1000000",
            participation_rates="0.20",
            output_dir=str(root / "strategy" / "execution"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-execution",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-execution"
    assert payload["json_path"].endswith("cli-execution.json")
    assert payload["coverage_path"].endswith("cli-execution分钟覆盖.csv")


def test_run_earnings_forecast_regime_budget_challenge_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    artifacts = run_earnings_forecast_regime_budget_challenge(
        root,
        preset_name="stable_100w",
        challenger_name="tail_risk_soft_q10_p25",
        start="20260101",
        end="20260310",
        output_dir=root / "strategy" / "regime",
        artifact_dir=root / "strategy" / "artifacts",
        label="regime-test",
    )

    assert artifacts.json_path.exists()
    assert artifacts.metrics_path.exists()
    assert artifacts.md_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["label"] == "regime-test"
    assert payload["challenger"] == "tail_risk_soft_q10_p25"


def test_cmd_strategy_regime_budget_challenge_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="regime-budget-challenge",
            root=str(root),
            preset="stable_100w",
            challenger="tail_risk_soft_q10_p25",
            start="20260101",
            end="20260310",
            output_dir=str(root / "strategy" / "regime"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-regime",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-regime"
    assert payload["json_path"].endswith("cli-regime.json")


def test_run_earnings_forecast_prv_target_pool_review_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)
    _build_prv_panel(root)

    artifacts = run_earnings_forecast_prv_target_pool_review(
        root,
        preset_name="stable_100w",
        challenger_name="tail_risk_soft_q10_p25",
        start="20260101",
        end="20260310",
        output_dir=root / "strategy" / "prv",
        artifact_dir=root / "strategy" / "artifacts",
        label="prv-test",
    )

    assert artifacts.json_path.exists()
    assert artifacts.factor_metrics_path.exists()
    assert artifacts.strategy_metrics_path.exists()
    assert artifacts.md_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["label"] == "prv-test"
    assert payload["challenger"] == "tail_risk_soft_q10_p25"
    assert payload["panels"][0]["name"] == "all_a_2025_2026"


def test_cmd_strategy_prv_target_pool_review_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)
    _build_prv_panel(root)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="prv-target-pool-review",
            root=str(root),
            preset="stable_100w",
            challenger="tail_risk_soft_q10_p25",
            start="20260101",
            end="20260310",
            output_dir=str(root / "strategy" / "prv"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-prv",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-prv"
    assert payload["json_path"].endswith("cli-prv.json")


def test_run_earnings_forecast_shadow_plan_writes_target_files(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    artifacts = run_earnings_forecast_shadow_plan(
        root,
        start="20260101",
        as_of="20260310",
        output_dir=root / "strategy" / "shadow",
        artifact_dir=root / "strategy" / "artifacts",
        label="shadow-test",
        config=EarningsForecastDriftConfig(hold_days=5, top_n=2),
    )

    assert artifacts.json_path.exists()
    assert artifacts.html_path.exists()
    assert artifacts.target_path.exists()
    target = pd.read_csv(artifacts.target_path)
    assert {"date", "symbol", "weight", "prev_weight", "trade_delta", "action"} <= set(target.columns)
    assert artifacts.summary["requested_as_of"] == "20260310"


def test_run_earnings_forecast_shadow_plan_accepts_preset(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    artifacts = run_earnings_forecast_shadow_plan(
        root,
        start="20260101",
        as_of="20260310",
        output_dir=root / "strategy" / "shadow",
        artifact_dir=root / "strategy" / "artifacts",
        label="shadow-preset",
        preset_name="baseline_top110_large",
    )

    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["preset"]["name"] == "baseline_top110_large"
    assert payload["strategy_mode"] == "preset"
    assert payload["json_path"].endswith(f"shadow-preset-baseline_top110_large-{payload['as_of']}.json")
    assert Path(payload["target_path"]).name.endswith(f"shadow-preset-baseline_top110_large-{payload['as_of']}目标持仓.csv")


def test_run_earnings_forecast_live_handoff_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    def fake_transport(method, url, payload=None, headers=None):  # noqa: ARG001
        if url == "/api/meta/health":
            return {"status": "ok", "message": "healthy"}
        if url == "/api/meta/connection_status":
            return {"data": {"connected": True, "connected_at": "2026-05-01 09:30:00"}}
        if url == "/api/trading/asset?account_id=99034443":
            return {"data": {"available_cash": 10_000_000.0, "total_asset": 10_000_000.0, "market_value": 0.0}}
        if url == "/api/trading/positions?account_id=99034443":
            return {"data": [{"symbol": "000001.SZ", "volume": 10_000, "can_use_volume": 10_000, "avg_price": 10.0}]}
        if url == "/api/trading/orders?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/trades?account_id=99034443":
            return {"data": []}
        raise AssertionError(f"unexpected request: {method} {url}")

    artifacts = run_earnings_forecast_live_handoff(
        root,
        start="20260101",
        as_of="20260310",
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        output_dir=root / "strategy" / "handoff",
        artifact_dir=root / "strategy" / "artifacts",
        label="handoff-test",
        bridge_transport=fake_transport,
    )

    assert artifacts.json_path.exists()
    assert artifacts.html_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["qmt_ready"] is True
    assert payload["bridge_snapshot"]["cash"]["available_cash"] == 10_000_000.0
    assert payload["target_holding_count"] > 0


def test_run_earnings_forecast_live_handoff_accepts_preset(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    def fake_transport(method, url, payload=None, headers=None):  # noqa: ARG001
        if url == "/api/meta/health":
            return {"status": "ok", "message": "healthy"}
        if url == "/api/meta/connection_status":
            return {"data": {"connected": True}}
        if url == "/api/trading/asset?account_id=99034443":
            return {"data": {"available_cash": 10_000_000.0, "total_asset": 10_000_000.0, "market_value": 0.0}}
        if url == "/api/trading/positions?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/orders?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/trades?account_id=99034443":
            return {"data": []}
        raise AssertionError(f"unexpected request: {method} {url}")

    artifacts = run_earnings_forecast_live_handoff(
        root,
        start="20260101",
        as_of="20260310",
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        output_dir=root / "strategy" / "handoff",
        artifact_dir=root / "strategy" / "artifacts",
        label="handoff-preset",
        preset_name="baseline_top110_large",
        bridge_transport=fake_transport,
    )

    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["preset"]["name"] == "baseline_top110_large"
    assert payload["strategy_mode"] == "preset"
    assert payload["json_path"].endswith(f"handoff-preset-baseline_top110_large-{payload['as_of']}.json")
    assert Path(payload["target_path"]).name.endswith(f"handoff-preset-shadow-baseline_top110_large-{payload['as_of']}目标持仓.csv")


def test_run_earnings_forecast_live_handoff_tolerates_known_connection_status_bug(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    def fake_transport(method, url, payload=None, headers=None):  # noqa: ARG001
        if url == "/api/meta/health":
            return {"status": "ok", "message": "healthy"}
        if url == "/api/meta/connection_status":
            return {"data": {"connected": False, "error": "'xtquant.datacenter.IPythonApiClient' object has no attribute 'get_connect_status'"}}
        if url == "/api/trading/asset?account_id=99034443":
            return {"data": {"available_cash": 10_000_000.0, "total_asset": 10_000_000.0, "market_value": 0.0}}
        if url == "/api/trading/positions?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/orders?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/trades?account_id=99034443":
            return {"data": []}
        raise AssertionError(f"unexpected request: {method} {url}")

    artifacts = run_earnings_forecast_live_handoff(
        root,
        start="20260101",
        as_of="20260310",
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        output_dir=root / "strategy" / "handoff",
        artifact_dir=root / "strategy" / "artifacts",
        label="handoff-known-bug",
        bridge_transport=fake_transport,
    )

    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["qmt_ready"] is True
    assert "connection_status_warning" in payload["bridge_snapshot"]
    assert payload["blocking_reasons"] == []


def test_prepare_earnings_forecast_next_session_writes_target_portfolio_and_task(tmp_path):
    root = _build_earnings_workspace(tmp_path)

    def fake_transport(method, url, payload=None, headers=None):  # noqa: ARG001
        if url == "/api/meta/health":
            return {"status": "ok", "message": "healthy"}
        if url == "/api/meta/connection_status":
            return {"data": {"connected": True}}
        if url == "/api/trading/asset?account_id=99034443":
            return {"data": {"available_cash": 10_000_000.0, "total_asset": 10_000_000.0, "market_value": 0.0}}
        if url == "/api/trading/positions?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/orders?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/trades?account_id=99034443":
            return {"data": []}
        raise AssertionError(f"unexpected request: {method} {url}")

    artifacts = prepare_earnings_forecast_next_session(
        root,
        start="20260101",
        as_of="20260311",
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        output_dir=root / "strategy" / "handoff",
        artifact_dir=root / "strategy" / "artifacts",
        preset_name="baseline_top110_large",
        bridge_transport=fake_transport,
    )

    assert artifacts.handoff_json_path.exists()
    assert artifacts.target_portfolio_path.exists()
    assert artifacts.task_path.exists()
    payload = json.loads(artifacts.task_path.read_text(encoding="utf-8"))
    assert payload["status"] == "pending"
    assert payload["requested_as_of"] == "20260311"
    assert payload["trade_date"] == "20260311"
    assert Path(payload["quality_review_path"]).exists()
    assert payload["target_diagnostics"]["mode"] == "live_topn_replacement"
    assert payload["target_diagnostics"]["market_cap_top_pct"] == 0.50
    assert payload["target_diagnostics"]["market_cap_field"] == "total_mv"
    assert payload["target_diagnostics"]["selection_funnel"]["after_market_cap_top50_count"] <= payload["target_diagnostics"]["selection_funnel"]["after_st_filter_count"]
    assert payload["target_diagnostics"]["selection_funnel"]["selected_position_count"] == len(
        json.loads(artifacts.target_portfolio_path.read_text(encoding="utf-8"))["positions"]
    )
    assert payload["target_diagnostics"]["market_gate"]["benchmark"] == "000300.SH"
    assert "quality_summary" in payload
    portfolio = json.loads(artifacts.target_portfolio_path.read_text(encoding="utf-8"))
    assert portfolio["trade_date"] == "20260311"
    assert portfolio["strategy_version"] == "baseline_top110_large"
    assert artifacts.summary["quality_summary"]["holding_count"] == len(portfolio["positions"])


def test_prepare_next_session_fails_closed_when_stock_st_partition_missing(tmp_path):
    root = _build_earnings_workspace(tmp_path)
    missing_partition = sorted((root / "data" / "stock_st").glob("date=*"))[-1]
    shutil.rmtree(missing_partition)

    with pytest.raises(ValueError, match="stock_st"):
        prepare_earnings_forecast_next_session(
            root,
            start="20260101",
            as_of="20260311",
            qmt_bridge_url="http://bridge.local:8000",
            qmt_bridge_token="token",
            qmt_account_id="99034443",
            output_dir=root / "strategy" / "handoff",
            artifact_dir=root / "strategy" / "artifacts",
            preset_name="baseline_top110_large",
            bridge_transport=lambda *args, **kwargs: {"status": "ok"},
        )


def test_prepare_next_session_excludes_stock_st_candidate(tmp_path):
    root = _build_earnings_workspace(tmp_path)
    storage = ParquetDuckDBBackend(root / "data")
    storage.upsert(
        "stock_st",
        pd.DataFrame(
            [
                {
                    "date": "20260309",
                    "symbol": "000001.SZ",
                    "name": "ST平安",
                    "type": "ST",
                    "type_name": "风险警示板",
                }
            ]
        ),
        {"date": "20260309"},
    )

    artifacts = prepare_earnings_forecast_next_session(
        root,
        start="20260101",
        as_of="20260309",
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        output_dir=root / "strategy" / "handoff",
        artifact_dir=root / "strategy" / "artifacts",
        preset_name="baseline_top110_large",
        bridge_transport=_clean_fake_bridge_transport,
    )

    payload = json.loads(artifacts.task_path.read_text(encoding="utf-8"))
    portfolio = json.loads(artifacts.target_portfolio_path.read_text(encoding="utf-8"))
    held = {position["symbol"] for position in portfolio["positions"]}
    assert "000001.SZ" not in held
    assert "000001.SZ" in payload["target_diagnostics"]["skipped_st_symbols"]
    assert payload["target_diagnostics"]["skipped_counts"]["st"] >= 1


def test_prepare_next_session_respects_market_permission_diagnostics(tmp_path):
    root = _build_earnings_workspace(tmp_path)
    storage = ParquetDuckDBBackend(root / "data")
    bars = storage.read("bars")
    dates = sorted(bars["date"].astype(str).unique())
    star_rows = []
    for date in dates:
        star_rows.append({"date": date, "symbol": "688001.SH", "open": 10.0, "close": 10.2, "amount": 100_000.0})
    _upsert_by_date(storage, "bars", pd.DataFrame(star_rows))
    _upsert_by_date(
        storage,
        "valuation",
        pd.DataFrame(
            [{"date": date, "symbol": "688001.SH", "total_mv": 400_000.0, "circ_mv": 350_000.0} for date in dates]
        ),
    )
    _upsert_by_date(
        storage,
        "stk_limit",
        pd.DataFrame(
            [{"date": date, "symbol": "688001.SH", "up_limit": 99.0, "down_limit": 1.0} for date in dates]
        ),
    )
    existing_forecast = storage.read("forecast", filters={"report_date": "20260331"})
    storage.upsert(
        "forecast",
        pd.concat(
            [
                existing_forecast,
                pd.DataFrame(
                    [
                        {
                            "symbol": "688001.SH",
                            "ann_date": "20260205",
                            "type": "预增",
                            "p_change_min": 500.0,
                            "p_change_max": 600.0,
                            "report_date": 20260331,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        ),
        {"report_date": "20260331"},
    )

    artifacts = prepare_earnings_forecast_next_session(
        root,
        start="20260101",
        as_of="20260309",
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        output_dir=root / "strategy" / "handoff",
        artifact_dir=root / "strategy" / "artifacts",
        preset_name="baseline_top110_large",
        bridge_transport=_clean_fake_bridge_transport,
        market_permissions=MarketPermissionConfig(allow_star=False),
    )

    payload = json.loads(artifacts.task_path.read_text(encoding="utf-8"))
    portfolio = json.loads(artifacts.target_portfolio_path.read_text(encoding="utf-8"))
    held = {position["symbol"] for position in portfolio["positions"]}
    assert "688001.SH" not in held
    assert "688001.SH" in payload["target_diagnostics"]["skipped_permission_symbols"]
    assert payload["target_diagnostics"]["skipped_counts"]["market_permission"] == 1


def test_prepare_next_session_allows_empty_handoff_before_live_replacement(tmp_path, monkeypatch):
    from types import SimpleNamespace

    root = _build_earnings_workspace(tmp_path)
    handoff_target_path = root / "strategy" / "handoff" / "empty-target.csv"
    handoff_target_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=["symbol", "weight"]).to_csv(handoff_target_path, index=False)
    handoff_json_path = root / "strategy" / "handoff" / "empty-handoff.json"
    handoff_json_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live.run_earnings_forecast_live_handoff",
        lambda *args, **kwargs: SimpleNamespace(
            summary={"as_of": "20260205"},
            target_path=handoff_target_path,
            json_path=handoff_json_path,
        ),
    )

    def fake_transport(method, url, payload=None, headers=None):  # noqa: ARG001
        if url == "/api/meta/health":
            return {"status": "ok", "message": "healthy"}
        if url == "/api/meta/connection_status":
            return {"data": {"connected": True}}
        if url == "/api/trading/asset?account_id=99034443":
            return {"data": {"available_cash": 10_000_000.0, "total_asset": 10_000_000.0, "market_value": 0.0}}
        if url == "/api/trading/positions?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/orders?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/trades?account_id=99034443":
            return {"data": []}
        raise AssertionError(f"unexpected request: {method} {url}")

    artifacts = prepare_earnings_forecast_next_session(
        root,
        start="20260101",
        as_of="20260205",
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        output_dir=root / "strategy" / "handoff",
        artifact_dir=root / "strategy" / "artifacts",
        preset_name="baseline_top110_large",
        bridge_transport=fake_transport,
    )

    payload = json.loads(artifacts.task_path.read_text(encoding="utf-8"))
    assert payload["status"] == "pending"
    assert payload["target_diagnostics"]["shortfall_reason"] == "no_positive_signal_candidates"
    portfolio = json.loads(artifacts.target_portfolio_path.read_text(encoding="utf-8"))
    assert portfolio["positions"] == []
    assert portfolio["cash_target"] == 1_000_000.0


def test_execute_pending_qmt_task_uses_frozen_target_portfolio(tmp_path, monkeypatch):
    from types import SimpleNamespace

    root = _build_earnings_workspace(tmp_path)

    def fake_transport(method, url, payload=None, headers=None):  # noqa: ARG001
        if url == "/api/meta/health":
            return {"status": "ok", "message": "healthy"}
        if url == "/api/meta/connection_status":
            return {"data": {"connected": True}}
        if url == "/api/trading/asset?account_id=99034443":
            return {"data": {"available_cash": 10_000_000.0, "total_asset": 10_000_000.0, "market_value": 0.0}}
        if url == "/api/trading/positions?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/orders?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/trades?account_id=99034443":
            return {"data": []}
        raise AssertionError(f"unexpected request: {method} {url}")

    prepared = prepare_earnings_forecast_next_session(
        root,
        start="20260101",
        as_of="20260310",
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        output_dir=root / "strategy" / "handoff",
        artifact_dir=root / "strategy" / "artifacts",
        preset_name="baseline_top110_large",
        bridge_transport=fake_transport,
    )

    class FakeProbeAdapter:
        def __init__(self, config):
            self.config = config

        def get_positions(self):
            return []

    captured: dict[str, str] = {}

    def fake_run_qmt_rebalance(portfolio, **kwargs):  # noqa: ANN001
        exec_dir = root / "trade" / "executions" / "exec_20260311_test"
        exec_dir.mkdir(parents=True, exist_ok=True)
        report_path = exec_dir / "execution_report.json"
        report_md_path = exec_dir / "execution_report.md"
        report_path.write_text("{}", encoding="utf-8")
        report_md_path.write_text("# test", encoding="utf-8")
        captured["portfolio_id"] = portfolio.portfolio_id
        captured["trade_date"] = portfolio.trade_date
        captured["strategy_version"] = portfolio.strategy_version
        return SimpleNamespace(
            exec_id="exec_20260311_test",
            execution_report_path=report_path,
            execution_report_md_path=report_md_path,
            report=SimpleNamespace(
                risk_result=SimpleNamespace(
                    passed=True,
                    blocking_reasons=[],
                )
            ),
        )

    monkeypatch.setattr("vortex.trade.qmt_bridge.QmtBridgeAdapter", FakeProbeAdapter)
    monkeypatch.setattr("vortex.strategy.earnings_forecast_live.load_trade_st_flags", lambda *args, **kwargs: {})
    monkeypatch.setattr("vortex.strategy.earnings_forecast_live.run_qmt_rebalance", fake_run_qmt_rebalance)

    result = execute_pending_qmt_task(
        root,
        task_path=prepared.task_path,
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        allow_trading=False,
    )

    assert captured["portfolio_id"] == prepared.summary["portfolio_id"]
    assert captured["trade_date"] == prepared.summary["trade_date"]
    assert captured["strategy_version"] == "baseline_top110_large"
    assert result["task_status"] == "done"
    payload = json.loads(prepared.task_path.read_text(encoding="utf-8"))
    assert payload["status"] == "done"
    assert payload["target_portfolio_path"] == str(prepared.target_portfolio_path)


def test_run_earnings_forecast_auto_cycle_once_prepares_and_executes_due_task(tmp_path, monkeypatch):
    root = _build_earnings_workspace(tmp_path)

    def fake_data_update(*args, **kwargs):  # noqa: ARG001
        return {"status": "success"}

    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live._run_data_update_foreground",
        fake_data_update,
    )

    prepared = {
        "status": "prepared",
        "task_path": str(root / "state" / "trade" / "pending_qmt" / "20260310-auto.json"),
    }

    def fake_prepare(*args, **kwargs):  # noqa: ARG001
        task_path = Path(prepared["task_path"])
        task_path.parent.mkdir(parents=True, exist_ok=True)
        task_path.write_text(
            json.dumps(
                {
                    "task_type": "earnings_forecast_qmt_rebalance",
                    "status": "pending",
                    "as_of": "20260310",
                    "trade_date": "20260310",
                    "target_portfolio_path": str(root / "trade" / "targets" / "20260310" / "tp.json"),
                    "qmt_account_id": "99034443",
                }
            ),
            encoding="utf-8",
        )
        return type("Prepared", (), {"summary": prepared})()

    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live.prepare_earnings_forecast_next_session",
        fake_prepare,
    )

    executed = {
        "task_path": prepared["task_path"],
        "task_status": "done",
        "exec_id": "exec_20260310_test",
    }

    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live.execute_pending_qmt_task",
        lambda *args, **kwargs: executed,  # noqa: ARG005
    )
    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live.is_trade_day",
        lambda *args, **kwargs: True,  # noqa: ARG005
    )

    payload = run_earnings_forecast_auto_cycle_once(
        root,
        start="20260101",
        profile_name="default",
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        now=pd.Timestamp("2026-03-10 09:24:00").to_pydatetime(),
    )

    assert payload["prepared"] == prepared
    assert payload["executed"] == []
    assert "execute window not reached" in payload["skipped"]


def test_run_earnings_forecast_auto_cycle_once_records_nav_after_execution(tmp_path, monkeypatch):
    root = _build_earnings_workspace(tmp_path)
    task_path = root / "state" / "trade" / "pending_qmt" / "20260310-auto.json"

    def fake_prepare(*args, **kwargs):  # noqa: ARG001
        task_path.parent.mkdir(parents=True, exist_ok=True)
        task_path.write_text(
            json.dumps(
                {
                    "task_type": "earnings_forecast_qmt_rebalance",
                    "status": "pending",
                    "as_of": "20260310",
                    "trade_date": "20260310",
                    "target_portfolio_path": str(root / "trade" / "targets" / "20260310" / "tp.json"),
                    "qmt_account_id": "99034443",
                }
            ),
            encoding="utf-8",
        )
        return type("Prepared", (), {"summary": {"task_path": str(task_path)}})()

    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live.prepare_earnings_forecast_next_session",
        fake_prepare,
    )
    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live._run_data_update_foreground",
        lambda *args, **kwargs: {"status": "success"},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live.execute_pending_qmt_task",
        lambda *args, **kwargs: {  # noqa: ARG005
            "task_path": str(task_path),
            "task_status": "done",
            "exec_id": "exec_20260310_test",
            "risk_passed": True,
        },
    )
    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live.is_trade_day",
        lambda *args, **kwargs: True,  # noqa: ARG005
    )

    captured = {}

    def fake_nav_snapshot(*args, **kwargs):  # noqa: ARG001
        captured.update(kwargs)
        return {"run_id": "earnings_forecast_auto-baseline_top110_large-99034443", "net_value": 1.0}

    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live._record_auto_nav_snapshot",
        fake_nav_snapshot,
    )

    payload = run_earnings_forecast_auto_cycle_once(
        root,
        start="20260101",
        profile_name="default",
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        now=pd.Timestamp("2026-03-10 09:30:00").to_pydatetime(),
        nav_initial_equity=1_000_000.0,
        nav_benchmark="000852.SH",
    )

    assert payload["executed"][0]["exec_id"] == "exec_20260310_test"
    assert payload["nav_snapshot"]["net_value"] == 1.0
    assert captured["trade_date"] == "20260310"
    assert captured["preset_name"] == "baseline_top110_large"
    assert captured["initial_equity"] == 1_000_000.0
    assert captured["benchmark"] == "000852.SH"


def test_is_trade_day_reads_canonical_calendar_dataset(tmp_path):
    root = tmp_path / "workspace"
    Workspace(root).initialize()
    storage = ParquetDuckDBBackend(root / "data")
    storage.initialize()
    storage.upsert(
        "calendar",
        pd.DataFrame({"cal_date": ["20260310", "20260311"]}),
        {},
    )

    assert is_trade_day(root, "20260310") is True
    assert is_trade_day(root, "20260309") is False


def test_is_trade_day_extends_stale_calendar_dataset(tmp_path, monkeypatch):
    root = tmp_path / "workspace"
    Workspace(root).initialize()
    storage = ParquetDuckDBBackend(root / "data")
    storage.initialize()
    storage.upsert("calendar", pd.DataFrame({"cal_date": ["20260310"]}), {})

    class StubProvider:
        def fetch_calendar(self, market, start, end):
            assert market == "cn_stock"
            assert start.strftime("%Y%m%d") == "20260311"
            assert end.strftime("%Y%m%d") == "20260311"
            return [pd.Timestamp("2026-03-11").date()]

    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live.TushareProvider",
        lambda: StubProvider(),
    )

    assert is_trade_day(root, "20260311") is True
    refreshed = storage.read("calendar")
    assert "20260311" in set(refreshed["cal_date"].astype(str))


def test_is_trade_day_treats_uncovered_weekend_as_non_trade_day(tmp_path, monkeypatch):
    root = tmp_path / "workspace"
    Workspace(root).initialize()
    storage = ParquetDuckDBBackend(root / "data")
    storage.initialize()
    storage.upsert("calendar", pd.DataFrame({"cal_date": ["20260508"]}), {})

    class EmptyProvider:
        def fetch_calendar(self, market, start, end):  # noqa: ARG002
            return []

    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live.TushareProvider",
        lambda: EmptyProvider(),
    )

    assert is_trade_day(root, "20260509") is False


def test_resolve_next_trade_date_reads_canonical_calendar_dataset(tmp_path):
    root = tmp_path / "workspace"
    Workspace(root).initialize()
    storage = ParquetDuckDBBackend(root / "data")
    storage.initialize()
    storage.upsert(
        "calendar",
        pd.DataFrame({"cal_date": ["20260310", "20260311", "20260312"]}),
        {},
    )

    assert resolve_next_trade_date(root, "20260310") == "20260311"


def test_resolve_next_trade_date_extends_stale_calendar_dataset(tmp_path, monkeypatch):
    root = tmp_path / "workspace"
    Workspace(root).initialize()
    storage = ParquetDuckDBBackend(root / "data")
    storage.initialize()
    storage.upsert("calendar", pd.DataFrame({"cal_date": ["20260310"]}), {})

    class StubProvider:
        def fetch_calendar(self, market, start, end):
            assert market == "cn_stock"
            assert start.strftime("%Y%m%d") == "20260310"
            assert end.strftime("%Y%m%d") == "20260409"
            return [
                pd.Timestamp("2026-03-10").date(),
                pd.Timestamp("2026-03-11").date(),
                pd.Timestamp("2026-03-12").date(),
            ]

    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live.TushareProvider",
        lambda: StubProvider(),
    )

    assert resolve_next_trade_date(root, "20260310") == "20260311"


def test_run_earnings_forecast_auto_once_writes_status_and_log(tmp_path, monkeypatch):
    root = _build_earnings_workspace(tmp_path)

    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live.run_earnings_forecast_auto_cycle_once",
        lambda *args, **kwargs: {
            "today": "20260310",
            "prepared": {"status": "prepared"},
            "executed": [{"task_status": "done"}],
            "skipped": [],
        },
    )

    payload = run_earnings_forecast_auto_once(
        root,
        start="20260101",
        profile_name="default",
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        preset_name="baseline_top110_large",
        label="auto-observe-test",
        prepare_time="08:10",
        execute_time="09:25",
        allow_trading=False,
    )

    paths = get_earnings_forecast_auto_observability_paths(root)
    assert paths.status_path.exists()
    assert paths.log_path.exists()
    status = json.loads(paths.status_path.read_text(encoding="utf-8"))
    assert status["service_status"] == "stopped"
    assert status["last_tick_status"] == "success"
    assert status["last_tick"]["executed"] == [{"task_status": "done"}]
    assert status["log_path"] == str(paths.log_path)
    assert payload["prepared"] == {"status": "prepared"}
    log_text = paths.log_path.read_text(encoding="utf-8")
    assert '"event": "service.start"' in log_text
    assert '"event": "tick.success"' in log_text


def test_run_earnings_forecast_auto_cycle_once_skips_reprepare_when_today_plan_finished(
    tmp_path,
    monkeypatch,
):
    root = _build_earnings_workspace(tmp_path)
    task_path = root / "state" / "trade" / "pending_qmt" / "20260310-auto.json"
    task_path.parent.mkdir(parents=True, exist_ok=True)
    task_path.write_text(
        json.dumps(
            {
                "task_type": "earnings_forecast_qmt_rebalance",
                "status": "done",
                "as_of": "20260310",
                "trade_date": "20260310",
                "target_portfolio_path": str(root / "trade" / "targets" / "20260310" / "tp.json"),
                "qmt_account_id": "99034443",
            }
        ),
        encoding="utf-8",
    )

    prepare_called = {"value": False}
    execute_called = {"value": False}

    def fake_prepare(*args, **kwargs):  # noqa: ARG001
        prepare_called["value"] = True
        return None

    def fake_execute(*args, **kwargs):  # noqa: ARG001
        execute_called["value"] = True
        return {}

    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live.prepare_earnings_forecast_next_session",
        fake_prepare,
    )
    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live.execute_pending_qmt_task",
        fake_execute,
    )
    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live._run_data_update_foreground",
        lambda *args, **kwargs: {"status": "success"},  # noqa: ARG005
    )
    monkeypatch.setattr(
        "vortex.strategy.earnings_forecast_live.is_trade_day",
        lambda *args, **kwargs: True,  # noqa: ARG005
    )

    payload = run_earnings_forecast_auto_cycle_once(
        root,
        start="20260101",
        profile_name="default",
        qmt_bridge_url="http://bridge.local:8000",
        qmt_bridge_token="token",
        qmt_account_id="99034443",
        now=pd.Timestamp("2026-03-10 10:00:00").to_pydatetime(),
    )

    assert prepare_called["value"] is False
    assert execute_called["value"] is False
    assert payload["prepared"] is None
    assert payload["executed"] == []
    assert "trade-day plan already exists for today" in payload["skipped"]


def test_cmd_strategy_shadow_plan_with_preset_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="shadow-plan",
            root=str(root),
            start="20260101",
            as_of="20260310",
            preset="baseline_top110_large",
            output_dir=str(root / "strategy" / "shadow"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-shadow-preset",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["preset"]["name"] == "baseline_top110_large"
    assert payload["strategy_mode"] == "preset"
    assert payload["json_path"].endswith(f"cli-shadow-preset-baseline_top110_large-{payload['as_of']}.json")


def test_cmd_strategy_live_handoff_outputs_json(tmp_path, capsys, monkeypatch):
    from types import SimpleNamespace

    import vortex.strategy.earnings_forecast_runner as runner

    root = _build_earnings_workspace(tmp_path)

    class FakeQmtBridgeAdapter:
        def __init__(self, config, transport=None):  # noqa: ARG002
            self.config = config

        def health(self):
            return SimpleNamespace(ok=True, message="ok")

        def connection_status(self):
            return {"connected": True}

        def get_cash(self):
            return SimpleNamespace(available_cash=1_000_000.0, frozen_cash=0.0, total_asset=1_200_000.0, market_value=200_000.0)

        def get_positions(self):
            return []

        def get_orders(self):
            return []

        def get_fills(self):
            return []

    monkeypatch.setattr(runner, "QmtBridgeAdapter", FakeQmtBridgeAdapter)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="live-handoff",
            root=str(root),
            start="20260101",
            as_of="20260310",
            qmt_bridge_url="http://bridge.local:8000",
            qmt_bridge_token="token",
            qmt_account_id="99034443",
            output_dir=str(root / "strategy" / "handoff"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-handoff",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-handoff"
    assert payload["qmt_ready"] is True
    assert payload["json_path"].endswith(f"cli-handoff-{payload['as_of']}.json")


def test_cmd_strategy_live_handoff_with_preset_outputs_json(tmp_path, capsys, monkeypatch):
    from types import SimpleNamespace

    import vortex.strategy.earnings_forecast_runner as runner

    root = _build_earnings_workspace(tmp_path)

    class FakeQmtBridgeAdapter:
        def __init__(self, config, transport=None):  # noqa: ARG002
            self.config = config

        def health(self):
            return SimpleNamespace(ok=True, message="ok")

        def connection_status(self):
            return {"connected": True}

        def get_cash(self):
            return SimpleNamespace(available_cash=1_000_000.0, frozen_cash=0.0, total_asset=1_200_000.0, market_value=200_000.0)

        def get_positions(self):
            return []

        def get_orders(self):
            return []

        def get_fills(self):
            return []

    monkeypatch.setattr(runner, "QmtBridgeAdapter", FakeQmtBridgeAdapter)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="live-handoff",
            root=str(root),
            start="20260101",
            as_of="20260310",
            preset="baseline_top110_large",
            qmt_bridge_url="http://bridge.local:8000",
            qmt_bridge_token="token",
            qmt_account_id="99034443",
            output_dir=str(root / "strategy" / "handoff"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-handoff-preset",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["preset"]["name"] == "baseline_top110_large"
    assert payload["strategy_mode"] == "preset"
    assert payload["json_path"].endswith(f"cli-handoff-preset-baseline_top110_large-{payload['as_of']}.json")


def test_cmd_strategy_prepare_next_session_outputs_json(tmp_path, capsys, monkeypatch):
    root = _build_earnings_workspace(tmp_path)

    def fake_transport(method, url, payload=None, headers=None):  # noqa: ARG001
        if url == "/api/meta/health":
            return {"status": "ok", "message": "healthy"}
        if url == "/api/meta/connection_status":
            return {"data": {"connected": True}}
        if url == "/api/trading/asset?account_id=99034443":
            return {"data": {"available_cash": 10_000_000.0, "total_asset": 10_000_000.0, "market_value": 0.0}}
        if url == "/api/trading/positions?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/orders?account_id=99034443":
            return {"data": []}
        if url == "/api/trading/trades?account_id=99034443":
            return {"data": []}
        raise AssertionError(f"unexpected request: {method} {url}")

    import vortex.strategy.earnings_forecast_live as live_module

    original_prepare = live_module.prepare_earnings_forecast_next_session

    def prepare_with_fake_transport(*args, **kwargs):  # noqa: ANN002, ANN003
        kwargs["bridge_transport"] = fake_transport
        return original_prepare(*args, **kwargs)

    monkeypatch.setattr(live_module, "prepare_earnings_forecast_next_session", prepare_with_fake_transport)

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="prepare-next-session",
            root=str(root),
            start="20260101",
            as_of="20260311",
            preset="baseline_top110_large",
            qmt_bridge_url="http://bridge.local:8000",
            qmt_bridge_token="token",
            qmt_account_id="99034443",
            output_dir=str(root / "strategy" / "handoff"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-prepare",
            portfolio_notional=1_000_000.0,
            min_position_value=3_000.0,
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "prepared"
    assert payload["trade_date"] == "20260311"
    assert Path(payload["target_portfolio_path"]).exists()
    assert Path(payload["task_path"]).exists()
    assert Path(payload["target_portfolio_path"]).parent == root / "trade" / "targets" / "20260311"
    assert Path(payload["task_path"]).parent == root / "state" / "trade" / "pending_qmt"


def test_cmd_strategy_auto_status_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)
    paths = get_earnings_forecast_auto_observability_paths(root)
    paths.status_path.write_text(
        json.dumps(
            {
                "service": "earnings_forecast_auto_run",
                "service_status": "running",
                "pid": 43210,
                "updated_at": "2026-03-10T09:30:00",
                "log_path": str(paths.log_path),
                "status_path": str(paths.status_path),
                "last_tick_status": "error",
                "last_tick_at": "2026-03-10T09:29:00",
                "last_error": {
                    "type": "RuntimeError",
                    "message": "bridge timeout",
                },
                "last_tick": {
                    "status": "error",
                    "error": {
                        "type": "RuntimeError",
                        "message": "bridge timeout",
                    },
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="auto-status",
            root=str(root),
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["service_status"] == "running"
    assert payload["last_tick_status"] == "error"
    assert payload["last_error"]["message"] == "bridge timeout"
    assert payload["status_path"] == str(paths.status_path)


def test_cmd_strategy_auto_logs_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)
    paths = get_earnings_forecast_auto_observability_paths(root)
    paths.log_path.write_text("line-1\nline-2\nline-3\n", encoding="utf-8")
    paths.status_path.write_text(
        json.dumps(
            {
                "service": "earnings_forecast_auto_run",
                "service_status": "running",
                "pid": 43210,
                "log_path": str(paths.log_path),
                "status_path": str(paths.status_path),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="auto-logs",
            root=str(root),
            lines=2,
            follow=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["log_path"] == str(paths.log_path)
    assert payload["tail"] == "line-2\nline-3"


def test_run_opening_liquidity_review_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)
    opening_path = _build_opening_snapshot_csv(root / "strategy" / "opening_snapshots.csv")

    artifacts = run_opening_liquidity_review(
        root,
        opening_snapshot_path=opening_path,
        start="20260101",
        end="20260310",
        output_dir=root / "strategy" / "opening-review",
        label="opening-test",
        top_n_values=(2,),
        position_modes=("full_equal_selected",),
        portfolio_notional=100_000.0,
    )

    assert artifacts.json_path.exists()
    assert artifacts.csv_path.exists()
    assert artifacts.md_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["label"] == "opening-test"
    assert payload["summary"]


def test_cmd_strategy_opening_liquidity_review_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)
    opening_path = _build_opening_snapshot_csv(root / "strategy" / "opening_snapshots.csv")

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="opening-liquidity-review",
            root=str(root),
            start="20260101",
            end="20260310",
            opening_snapshots=str(opening_path),
            output_dir=str(root / "strategy" / "opening-review"),
            label="cli-opening",
            top_n_values="2",
            position_modes="full_equal_selected",
            portfolio_notional=100_000.0,
            capped_max_weight=0.05,
            volume_unit="shares",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-opening"
    assert payload["json_path"].endswith("cli-opening.json")
    assert (root / "strategy" / "opening-review" / "cli-opening.json").exists()



def test_run_opening_auction_execution_review_writes_reports(tmp_path):
    root = _build_earnings_workspace(tmp_path)
    opening_path = _build_opening_snapshot_csv(root / "strategy" / "auction_snapshots.csv")

    artifacts = run_opening_auction_execution_review(
        root,
        opening_snapshot_path=opening_path,
        start="20260101",
        end="20260310",
        output_dir=root / "strategy" / "auction-review",
        artifact_dir=root / "strategy" / "artifacts",
        label="auction-test",
        top_n=2,
        position_mode="full_equal_selected",
        portfolio_notional=100_000.0,
    )

    assert artifacts.json_path.exists()
    assert artifacts.html_path.exists()
    assert artifacts.holdings_path.exists()
    assert artifacts.trades_path.exists()
    assert artifacts.order_intents_path.exists()
    payload = json.loads(artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["strategy"] == "earnings_forecast_drift"
    assert payload["metadata"]["execution"] == "opening_auction_all_or_nothing"
    assert payload["metadata"]["auction_execution_summary"]["buy_order_count"] > 0



def test_cmd_strategy_auction_execution_review_outputs_json(tmp_path, capsys):
    root = _build_earnings_workspace(tmp_path)
    opening_path = _build_opening_snapshot_csv(root / "strategy" / "auction_snapshots.csv")

    cli.cmd_strategy(
        argparse.Namespace(
            strategy_action="earnings-forecast",
            earnings_action="auction-execution-review",
            root=str(root),
            start="20260101",
            end="20260310",
            opening_snapshots=str(opening_path),
            output_dir=str(root / "strategy" / "auction-review"),
            artifact_dir=str(root / "strategy" / "artifacts"),
            label="cli-auction",
            top_n=2,
            position_mode="full_equal_selected",
            portfolio_notional=100_000.0,
            capped_max_weight=0.05,
            volume_unit="shares",
            allow_missing_precise_data=False,
            format="json",
        )
    )

    payload = json.loads(capsys.readouterr().out)
    assert payload["label"] == "cli-auction"
    assert payload["json_path"].endswith("cli-auction报告.json")
    assert (root / "strategy" / "auction-review" / "cli-auction报告.json").exists()


def _build_earnings_workspace(tmp_path):
    root = tmp_path / "workspace"
    Workspace(root).initialize()
    storage = ParquetDuckDBBackend(root / "data")
    storage.initialize()
    dates = pd.bdate_range("2026-01-01", periods=48).strftime("%Y%m%d").tolist()
    symbols = ["000001.SZ", "000002.SZ", "000003.SZ"]
    bars_rows = []
    for idx, date in enumerate(dates):
        for symbol_idx, symbol in enumerate(symbols):
            open_price = 10.0 + symbol_idx
            daily_return = 0.002
            if symbol == "000001.SZ" and 26 <= idx <= 32:
                daily_return = 0.03
            bars_rows.append(
                {
                    "date": date,
                    "symbol": symbol,
                    "open": open_price,
                    "close": open_price * (1.0 + daily_return),
                    "amount": 100_000.0,
                }
            )
    _upsert_by_date(storage, "bars", pd.DataFrame(bars_rows))
    valuation_rows = [
        {
            "date": date,
            "symbol": symbol,
            "total_mv": {
                "000001.SZ": 300_000.0,
                "000002.SZ": 200_000.0,
                "000003.SZ": 100_000.0,
            }[symbol],
            "circ_mv": {
                "000001.SZ": 250_000.0,
                "000002.SZ": 150_000.0,
                "000003.SZ": 50_000.0,
            }[symbol],
        }
        for date in dates
        for symbol in symbols
    ]
    _upsert_by_date(storage, "valuation", pd.DataFrame(valuation_rows))
    index_rows = []
    for idx, date in enumerate(dates):
        for symbol in ["000300.SH", "000905.SH", "000852.SH"]:
            index_rows.append(
                {
                    "date": date,
                    "symbol": symbol,
                    "close": 1000.0 + idx * 5,
                }
            )
    _upsert_by_date(storage, "index_daily", pd.DataFrame(index_rows))
    forecast = pd.DataFrame(
        [
            {
                "symbol": "000003.SZ",
                "ann_date": "20251231",
                "type": "预增",
                "p_change_min": 300.0,
                "p_change_max": 400.0,
                "report_date": "20251231",
            },
            {
                "symbol": "000001.SZ",
                "ann_date": "20260205",
                "type": "预增",
                "p_change_min": 120.0,
                "p_change_max": 180.0,
                "report_date": "20260331",
            },
            {
                "symbol": "000003.SZ",
                "ann_date": "20260205",
                "type": "预增",
                "p_change_min": 200.0,
                "p_change_max": 260.0,
                "report_date": "20260331",
            },
        ]
    )
    for report_date, group in forecast.groupby("report_date"):
        storage.upsert("forecast", group, {"report_date": str(report_date)})
    limit_rows = [
        {
            "date": date,
            "symbol": symbol,
            "up_limit": 99.0,
            "down_limit": 1.0,
        }
        for date in dates
        for symbol in symbols
    ]
    _upsert_by_date(storage, "stk_limit", pd.DataFrame(limit_rows))
    suspend = pd.DataFrame(
        [
            {
                "date": dates[0],
                "symbol": "000002.SZ",
                "suspend_type": "R",
                "suspend_timing": None,
            }
        ]
    )
    _upsert_by_date(storage, "suspend_d", suspend)
    trade_cal = pd.DataFrame(
        [
            {"cal_date": date, "is_open": 1, "pretrade_date": dates[max(0, idx - 1)] if idx > 0 else ""}
            for idx, date in enumerate(dates)
        ]
        + [
            {"cal_date": "20260311", "is_open": 1, "pretrade_date": dates[-1]},
            {"cal_date": "20260312", "is_open": 1, "pretrade_date": "20260311"},
        ]
    )
    for cal_date, group in trade_cal.groupby("cal_date"):
        storage.upsert("trade_cal", group, {"cal_date": str(cal_date)})
    stock_st = pd.DataFrame(
        [
            {
                "date": date,
                "symbol": "000099.SZ",
                "name": "ST样本",
                "type": "ST",
                "type_name": "ST",
            }
            for date in dates
        ]
    )
    _upsert_by_date(storage, "stock_st", stock_st)
    fina_indicator = pd.DataFrame(
        [
            {
                "symbol": "000002.SZ",
                "ann_date": "20251231",
                "effective_from": "2025-12-31T09:30:00+08:00",
                "bps": -1.0,
                "roe": -5.0,
                "debt_to_assets": 80.0,
                "netprofit_yoy": -20.0,
                "report_date": "20251231",
            }
        ]
    )
    for report_date, group in fina_indicator.groupby("report_date"):
        storage.upsert("fina_indicator", group, {"report_date": str(report_date)})
    balancesheet = pd.DataFrame(
        [
            {
                "symbol": "000002.SZ",
                "ann_date": "20251231",
                "effective_from": "2025-12-31T09:30:00+08:00",
                "total_hldr_eqy_inc_min_int": -10.0,
                "total_hldr_eqy_exc_min_int": -10.0,
                "report_date": "20251231",
            }
        ]
    )
    for report_date, group in balancesheet.groupby("report_date"):
        storage.upsert("balancesheet", group, {"report_date": str(report_date)})
    cashflow = pd.DataFrame(
        [
            {
                "symbol": "000002.SZ",
                "ann_date": "20251231",
                "effective_from": "2025-12-31T09:30:00+08:00",
                "net_profit": -100.0,
                "n_cashflow_act": -120.0,
                "free_cashflow": -110.0,
                "report_date": "20251231",
            }
        ]
    )
    for report_date, group in cashflow.groupby("report_date"):
        storage.upsert("cashflow", group, {"report_date": str(report_date)})
    return root


def _upsert_by_date(storage: ParquetDuckDBBackend, dataset: str, frame: pd.DataFrame) -> None:
    for date, group in frame.groupby("date"):
        storage.upsert(dataset, group, {"date": str(date)})


def _build_minute_symbol_year_cache(root):
    dates = pd.bdate_range("2026-01-01", periods=48).strftime("%Y%m%d").tolist()
    prices = {"000001.SZ": 10.0, "000002.SZ": 11.0, "000003.SZ": 12.0}
    for symbol, price in prices.items():
        rows = []
        for date in dates:
            rows.extend(
                [
                    {
                        "symbol": symbol,
                        "trade_time": f"{date} 09:31:00",
                        "date": date,
                        "minute": "09:31",
                        "open": price,
                        "high": price * 1.01,
                        "low": price * 0.99,
                        "close": price,
                        "volume": 10_000.0,
                        "amount": 1_000_000.0,
                        "freq": "1min",
                    },
                    {
                        "symbol": symbol,
                        "trade_time": f"{date} 14:56:00",
                        "date": date,
                        "minute": "14:56",
                        "open": price,
                        "high": price * 1.01,
                        "low": price * 0.99,
                        "close": price,
                        "volume": 10_000.0,
                        "amount": 1_000_000.0,
                        "freq": "1min",
                    },
                ]
            )
        path = root / "data" / "stk_mins" / "year=2026" / "universe=all_active" / f"symbol={symbol}" / "data.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(path, index=False)


def _build_prv_panel(root):
    dates = pd.bdate_range("2026-01-01", periods=48).strftime("%Y%m%d").tolist()
    rows = []
    for idx, date in enumerate(dates):
        for symbol_idx, symbol in enumerate(["000001.SZ", "000002.SZ", "000003.SZ"]):
            rows.append(
                {
                    "date": date,
                    "symbol": symbol,
                    "ridge3_volume_ratio": 0.1 + symbol_idx * 0.05 + idx * 0.001,
                    "isolated_peak_volume_ratio": 0.2 + symbol_idx * 0.03,
                    "valley_relative_vwap": -0.01 + symbol_idx * 0.002,
                    "first30_volume_ratio": 0.3 + symbol_idx * 0.01,
                }
            )
    path = (
        root
        / "research"
        / "factor-reports"
        / "volume-peak-ridge-valley"
        / "all-a-2025-2026-prv"
        / "volume_prv_all_a_panel_2025_2026.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def _build_opening_snapshot_csv(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    dates = pd.bdate_range("2026-01-01", periods=48).strftime("%Y%m%d").tolist()
    for date in dates:
        for symbol, price in {"000001.SZ": 10.0, "000002.SZ": 11.0, "000003.SZ": 12.0}.items():
            rows.append(
                {
                    "date": date,
                    "symbol": symbol,
                    "open_price": price,
                    "ask1_price": price,
                    "ask1_volume": 5_000,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path
