"""业绩预告漂移策略的冻结组合、待执行任务与自动编排。"""

from __future__ import annotations

import dataclasses
import json
import os
import re
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from vortex.data.provider.tushare import TushareProvider
from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend
from vortex.runtime.workspace import Workspace
from vortex.strategy.earnings_forecast_drift import (
    EarningsForecastDriftConfig,
    build_financial_st_risk_events,
    build_liquidity_mask,
    build_persistent_st_risk_mask,
    build_stock_st_mask,
)
from vortex.strategy.earnings_forecast_runner import (
    DEFAULT_LIVE_HANDOFF_LABEL,
    _build_market_cap_top_pct_mask,
    _build_version_signal_context,
    get_earnings_forecast_version_preset,
    load_earnings_forecast_inputs,
    run_earnings_forecast_live_handoff,
)
from vortex.strategy.earnings_forecast_quality import (
    build_holding_quality_review,
    write_holding_quality_review,
)
from vortex.research.event_signals import build_forecast_surprise_signal
from vortex.research.market_state import build_market_state
from vortex.trade.market_rules import (
    MarketPermissionConfig,
    is_market_allowed,
    market_board,
    min_order_shares,
)
from vortex.trade.execution import run_qmt_rebalance
from vortex.trade.order_plan import OrderPlanConfig
from vortex.trade.qmt_bridge import QmtBridgeConfig, Transport
from vortex.trade.risk import PreTradeRiskConfig
from vortex.trade.serialization import read_json, target_portfolio_from_dict, write_json
from vortex.trade.target_portfolio import TargetPortfolioBuildConfig, build_target_portfolio

DEFAULT_AUTO_PREPARE_TIME = "08:10"
DEFAULT_AUTO_EXECUTE_TIME = "09:25"
DEFAULT_AUTO_LABEL = "业绩预告漂移策略自动编排"
DEFAULT_AUTO_PRESET = "baseline_top110_large"
DEFAULT_AUTO_LIVE_TOP_N = 30
DEFAULT_AUTO_NAV_INITIAL_EQUITY = 1_000_000.0
DEFAULT_AUTO_NAV_BENCHMARK = "000852.SH"
AUTO_RUN_STATUS_FILE = "status.json"
AUTO_RUN_LOG_FILE = "strategy-earnings-forecast-auto-run.log"
CRITICAL_LIVE_DATED_DATASETS = ("bars", "valuation", "stk_limit", "stock_st")
TARGETED_LIVE_REFRESH_DATASETS = (
    "forecast",
    "bars",
    "valuation",
    "stk_limit",
    "suspend_d",
    "stock_st",
    "fina_indicator",
    "express",
)


def _max_date_partition(storage: ParquetDuckDBBackend, dataset: str) -> str | None:
    dates: list[str] = []
    for partition in storage.list_partitions(dataset):
        match = re.search(r"(?:^|/)date=(\d{8})(?:/|$)", partition)
        if match:
            dates.append(match.group(1))
    return max(dates) if dates else None


def _date_partitions(storage: ParquetDuckDBBackend, dataset: str) -> set[str]:
    dates: set[str] = set()
    for partition in storage.list_partitions(dataset):
        match = re.search(r"(?:^|/)date=(\d{8})(?:/|$)", partition)
        if match:
            dates.add(match.group(1))
    return dates


def validate_live_required_data_freshness(root: str | Path, *, as_of: str) -> dict[str, Any]:
    """目标生成前的关键数据新鲜度门禁：缺风险数据时宁可停机。"""

    workspace = Workspace(Path(root).expanduser())
    storage = ParquetDuckDBBackend(workspace.data_dir)
    datasets: dict[str, dict[str, Any]] = {}
    missing: list[str] = []
    for dataset in CRITICAL_LIVE_DATED_DATASETS:
        partitions = _date_partitions(storage, dataset)
        max_date = max(partitions) if partitions else None
        ok = as_of in partitions
        datasets[dataset] = {
            "max_date": max_date,
            "required_as_of": as_of,
            "ok": ok,
        }
        if not ok:
            missing.append(dataset)

    stock_st_dates = datasets.get("stock_st", {})
    if missing:
        raise ValueError(
            "live target critical data is stale or missing: "
            + ", ".join(
                f"{dataset}(max_date={datasets[dataset]['max_date']}, required={as_of})"
                for dataset in missing
            )
        )
    return {
        "required_as_of": as_of,
        "datasets": datasets,
        "stock_st_as_of": stock_st_dates.get("max_date"),
        "status": "ok",
    }


@dataclass(frozen=True)
class EarningsForecastAutoObservabilityPaths:
    """auto-run 常驻服务的状态文件与日志文件路径。"""

    status_path: Path
    log_path: Path


class EarningsForecastAutoObserver:
    """把 auto-run 最近一轮 tick 状态、错误与日志落到 workspace。"""

    def __init__(
        self,
        root: str | Path,
        *,
        start: str,
        profile_name: str,
        qmt_bridge_url: str,
        qmt_account_id: str | None,
        preset_name: str,
        label: str,
        prepare_time: str,
        execute_time: str,
        poll_seconds: int,
        allow_trading: bool,
        loop_mode: str,
        nav_initial_equity: float = DEFAULT_AUTO_NAV_INITIAL_EQUITY,
        nav_benchmark: str = DEFAULT_AUTO_NAV_BENCHMARK,
    ) -> None:
        self.workspace = Workspace(Path(root).expanduser())
        self.workspace.ensure_initialized()
        self.paths = get_earnings_forecast_auto_observability_paths(self.workspace.root)
        self.loop_mode = loop_mode
        started_at = datetime.now().isoformat(timespec="seconds")
        self._status: dict[str, Any] = {
            "service": "earnings_forecast_auto_run",
            "service_status": "running",
            "loop_mode": loop_mode,
            "pid": os.getpid(),
            "started_at": started_at,
            "updated_at": started_at,
            "log_path": str(self.paths.log_path),
            "status_path": str(self.paths.status_path),
            "config": {
                "start": start,
                "profile_name": profile_name,
                "qmt_bridge_url": qmt_bridge_url,
                "qmt_account_id": qmt_account_id or "",
                "preset_name": preset_name,
                "label": label,
                "prepare_time": prepare_time,
                "execute_time": execute_time,
                "poll_seconds": int(poll_seconds),
                "allow_trading": bool(allow_trading),
                "nav_initial_equity": float(nav_initial_equity),
                "nav_benchmark": nav_benchmark,
            },
            "last_tick_status": "starting",
            "last_tick_at": None,
            "last_tick": None,
            "last_error": None,
        }
        self._append_log(
            "service.start",
            {
                "pid": os.getpid(),
                "loop_mode": loop_mode,
                "config": self._status["config"],
            },
        )
        self._persist()

    def record_tick_success(self, payload: dict[str, object], *, keep_running: bool) -> None:
        tick_at = datetime.now().isoformat(timespec="seconds")
        self._status.update(
            {
                "service_status": "running" if keep_running else "stopped",
                "updated_at": tick_at,
                "last_tick_at": tick_at,
                "last_tick_status": "success",
                "last_tick": payload,
                "last_error": None,
            }
        )
        self._append_log("tick.success", {"payload": payload})
        self._persist()

    def record_tick_error(self, exc: Exception, *, keep_running: bool) -> None:
        tick_at = datetime.now().isoformat(timespec="seconds")
        error_payload = {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc().strip(),
        }
        self._status.update(
            {
                "service_status": "running" if keep_running else "error",
                "updated_at": tick_at,
                "last_tick_at": tick_at,
                "last_tick_status": "error",
                "last_tick": {
                    "status": "error",
                    "error": error_payload,
                },
                "last_error": error_payload,
            }
        )
        self._append_log("tick.error", {"error": error_payload})
        self._persist()

    def record_shutdown(self, reason: str) -> None:
        stopped_at = datetime.now().isoformat(timespec="seconds")
        self._status.update(
            {
                "service_status": "stopped",
                "updated_at": stopped_at,
                "shutdown_reason": reason,
            }
        )
        self._append_log("service.stop", {"reason": reason})
        self._persist()

    def _append_log(self, event: str, payload: dict[str, Any]) -> None:
        line = {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "event": event,
            **payload,
        }
        with self.paths.log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(line, ensure_ascii=False, default=str) + "\n")

    def _persist(self) -> None:
        self.paths.status_path.parent.mkdir(parents=True, exist_ok=True)
        self.paths.status_path.write_text(
            json.dumps(self._status, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )


def get_earnings_forecast_auto_observability_paths(
    root: str | Path,
) -> EarningsForecastAutoObservabilityPaths:
    """返回 auto-run 的稳定状态文件与日志文件路径。"""

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    status_dir = workspace.state_dir / "strategy" / "earnings_forecast_auto"
    status_dir.mkdir(parents=True, exist_ok=True)
    workspace.logs_dir.mkdir(parents=True, exist_ok=True)
    return EarningsForecastAutoObservabilityPaths(
        status_path=status_dir / AUTO_RUN_STATUS_FILE,
        log_path=workspace.logs_dir / AUTO_RUN_LOG_FILE,
    )


def read_earnings_forecast_auto_status(root: str | Path) -> dict[str, Any]:
    """读取 auto-run 最近一次状态；未启动时返回默认骨架。"""

    paths = get_earnings_forecast_auto_observability_paths(root)
    if not paths.status_path.exists():
        return {
            "service": "earnings_forecast_auto_run",
            "service_status": "never_started",
            "status_path": str(paths.status_path),
            "log_path": str(paths.log_path),
            "last_tick_status": None,
            "last_tick_at": None,
            "last_tick": None,
            "last_error": None,
        }
    payload = read_json(paths.status_path)
    payload.setdefault("service", "earnings_forecast_auto_run")
    payload.setdefault("status_path", str(paths.status_path))
    payload.setdefault("log_path", str(paths.log_path))
    return payload


@dataclass(frozen=True)
class EarningsForecastPreparedArtifacts:
    """一次开盘前 prepare 生成的冻结产物与待执行任务。"""

    handoff_json_path: Path
    target_portfolio_path: Path
    task_path: Path
    summary: dict[str, object]


def prepare_earnings_forecast_next_session(
    root: str | Path,
    *,
    start: str,
    as_of: str,
    execution_trade_date: str | None = None,
    qmt_bridge_url: str,
    qmt_bridge_token: str | None = None,
    qmt_account_id: str | None = None,
    output_dir: str | Path | None = None,
    artifact_dir: str | Path | None = None,
    label: str = DEFAULT_LIVE_HANDOFF_LABEL,
    preset_name: str | None = DEFAULT_AUTO_PRESET,
    portfolio_notional: float = 1_000_000.0,
    live_top_n: int | None = DEFAULT_AUTO_LIVE_TOP_N,
    min_position_value: float = 3_000.0,
    require_precise_data: bool = True,
    bridge_transport: Transport | None = None,
    market_permissions: MarketPermissionConfig | None = None,
) -> EarningsForecastPreparedArtifacts:
    """基于最新可见数据生成冻结组合与待执行任务。

    orchestration 层不再把执行日硬编码成 T+1，而是优先把任务落到
    `as_of` 对应的交易日；如果 `as_of` 本身不是交易日，再顺延到下一个交易日。
    这里不会下单。prepare 的职责只有两件事：
    1. 产出带 QMT 只读快照的 live handoff；
    2. 把目标权重冻结成 `TargetPortfolio`，并写入待执行任务。
    """

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir).expanduser() if output_dir is not None else workspace.strategy_dir
    artifact_root = (
        Path(artifact_dir).expanduser()
        if artifact_dir is not None
        else workspace.strategy_dir / "artifacts"
    )
    output_root.mkdir(parents=True, exist_ok=True)
    artifact_root.mkdir(parents=True, exist_ok=True)

    handoff = run_earnings_forecast_live_handoff(
        workspace.root,
        start=start,
        as_of=as_of,
        qmt_bridge_url=qmt_bridge_url,
        qmt_bridge_token=qmt_bridge_token,
        qmt_account_id=qmt_account_id,
        output_dir=output_root,
        artifact_dir=artifact_root,
        label=label,
        preset_name=preset_name,
        require_precise_data=require_precise_data,
        bridge_transport=bridge_transport,
    )
    handoff_date = str(handoff.summary["as_of"])
    requested_trade_date = execution_trade_date or resolve_execution_trade_date(workspace.root, as_of)
    trade_date = requested_trade_date

    target = pd.read_csv(handoff.target_path)
    active = target.loc[target["weight"].astype(float) > 1e-12].copy()
    if active.empty and (live_top_n is None or int(live_top_n) <= 0 or preset_name is None):
        raise ValueError("handoff 目标持仓为空，无法生成冻结组合")

    target_candidates, target_diagnostics = _build_live_target_candidate_frame(
        workspace.root,
        start=start,
        as_of=handoff_date,
        preset_name=preset_name,
        fallback_active=active,
        portfolio_notional=float(portfolio_notional),
        live_top_n=live_top_n,
        min_position_value=float(min_position_value),
        market_permissions=market_permissions,
    )
    missing = target_candidates.loc[
        target_candidates["reference_price"].isna(),
        "symbol",
    ].astype(str).tolist()
    if missing:
        raise ValueError(f"冻结组合缺少参考价格: {sorted(missing)}")

    strategy_version = _strategy_version_from_handoff(handoff.summary)
    snapshot_id = Path(handoff.json_path).stem
    portfolio = build_target_portfolio(
        target_candidates.rename(columns={"weight": "target_weight"})[
            ["symbol", "target_weight", "reference_price", "action"]
        ].rename(columns={"action": "reason"}),
        trade_date=trade_date,
        strategy_version=strategy_version,
        run_id=f"handoff_{handoff_date}",
        snapshot_id=snapshot_id,
        config=TargetPortfolioBuildConfig(
            notional=float(portfolio_notional),
            min_position_value=float(min_position_value),
        ),
    )

    targets_dir = workspace.trade_dir / "targets" / trade_date
    targets_dir.mkdir(parents=True, exist_ok=True)
    target_portfolio_path = targets_dir / f"{portfolio.portfolio_id}.json"
    write_json(target_portfolio_path, portfolio)

    quality_review = _build_target_holding_quality_review(
        workspace.root,
        as_of=handoff_date,
        portfolio=portfolio,
    )
    quality_artifacts = write_holding_quality_review(
        quality_review,
        csv_path=artifact_root / f"{label}-holding-quality-{handoff_date}.csv",
        json_path=artifact_root / f"{label}-holding-quality-{handoff_date}.json",
        as_of=handoff_date,
    )

    task_payload = {
        "task_type": "earnings_forecast_qmt_rebalance",
        "status": "pending",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "as_of": handoff_date,
        "requested_as_of": execution_trade_date or as_of,
        "trade_date": trade_date,
        "label": label,
        "preset": handoff.summary.get("preset"),
        "strategy_version": strategy_version,
        "qmt_account_id": qmt_account_id or "",
        "target_portfolio_path": str(target_portfolio_path),
        "handoff_path": str(handoff.json_path),
        "quality_review_path": str(quality_artifacts.json_path),
        "target_diagnostics": target_diagnostics,
        "quality_summary": quality_artifacts.summary,
        "execution_report_path": "",
        "exec_id": "",
        "error": "",
    }
    task_path = _pending_task_dir(workspace.root) / f"{trade_date}-{portfolio.portfolio_id}.json"
    write_json(task_path, task_payload)

    summary = {
        "status": "prepared",
        "as_of": handoff_date,
        "requested_as_of": execution_trade_date or as_of,
        "trade_date": trade_date,
        "portfolio_id": portfolio.portfolio_id,
        "strategy_version": strategy_version,
        "holding_count": len(portfolio.positions),
        "cash_target": portfolio.cash_target,
        "quality_review_path": str(quality_artifacts.json_path),
        "quality_summary": quality_artifacts.summary,
        "target_diagnostics": target_diagnostics,
        "handoff_json_path": str(handoff.json_path),
        "target_portfolio_path": str(target_portfolio_path),
        "task_path": str(task_path),
    }
    return EarningsForecastPreparedArtifacts(
        handoff_json_path=handoff.json_path,
        target_portfolio_path=target_portfolio_path,
        task_path=task_path,
        summary=summary,
    )


def _count_signal_symbols_on_date(signal: pd.DataFrame, as_of: str) -> int:
    if as_of not in signal.index:
        return 0
    return int(signal.loc[as_of].dropna().shape[0])


def _market_gate_observation(inputs: Any, *, as_of: str, config: EarningsForecastDriftConfig) -> dict[str, Any]:
    state = build_market_state(inputs.index_close, config.market_state)
    if as_of not in state.index:
        return {
            "as_of": as_of,
            "risk_on": False,
            "reason": "market_state_missing_for_as_of",
            "config": dataclasses.asdict(config.market_state),
        }
    row = state.loc[as_of]
    return {
        "as_of": as_of,
        "benchmark": str(row.get("benchmark", config.market_state.benchmark)),
        "risk_on": bool(row.get("risk_on", False)),
        "benchmark_momentum": None
        if pd.isna(row.get("benchmark_momentum"))
        else float(row.get("benchmark_momentum")),
        "benchmark_above_support": bool(row.get("benchmark_above_support", False)),
        "risk_on_confirmations": int(row.get("risk_on_confirmations", 0)),
        "required_confirmations": int(config.market_state.min_risk_on_confirmations),
        "config": dataclasses.asdict(config.market_state),
    }


def _build_selection_funnel_base(
    inputs: Any,
    *,
    as_of: str,
    preset: Any,
    blocked_buy: pd.DataFrame | None,
) -> dict[str, Any]:
    """拆出 live 目标生成前的候选漏斗，方便解释为什么买不满 TopN。"""

    config = EarningsForecastDriftConfig(
        top_n=preset.top_n,
        position_mode=preset.position_mode,
        max_weight=preset.max_weight,
        transaction_cost_bps=preset.transaction_cost_bps,
    )
    raw_signal = build_forecast_surprise_signal(
        inputs.forecast,
        target_index=inputs.open_prices.index,
        target_columns=inputs.open_prices.columns,
        delay_days=config.delay_days,
        hold_days=config.hold_days,
    )
    liquidity = build_liquidity_mask(
        inputs.amount,
        window=config.liquidity_window,
        min_avg_amount=config.min_avg_amount,
    )
    liquidity_signal = build_forecast_surprise_signal(
        inputs.forecast,
        target_index=inputs.open_prices.index,
        target_columns=inputs.open_prices.columns,
        liquidity_mask=liquidity,
        delay_days=config.delay_days,
        hold_days=config.hold_days,
    )
    st_mask: pd.DataFrame | None = None
    if config.exclude_st and inputs.stock_st is not None:
        st_mask = build_stock_st_mask(inputs.stock_st, inputs.open_prices.index, inputs.open_prices.columns)
    if config.exclude_st_risk and inputs.st_risk_events is not None:
        risk_mask = build_persistent_st_risk_mask(
            inputs.st_risk_events,
            inputs.open_prices.index,
            inputs.open_prices.columns,
        )
        st_mask = risk_mask if st_mask is None else (st_mask | risk_mask.reindex_like(st_mask).fillna(False))
    st_signal = liquidity_signal
    if st_mask is not None:
        st_signal = st_signal.where(~st_mask.reindex_like(st_signal).fillna(False))
    market_cap_signal = st_signal
    if preset.market_cap_top_pct is not None:
        market_cap_mask = _build_market_cap_top_pct_mask(
            inputs.market_cap,
            target_index=st_signal.index,
            target_columns=st_signal.columns,
            top_pct=preset.market_cap_top_pct,
            value_column=preset.market_cap_field,
        )
        market_cap_signal = st_signal.where(market_cap_mask)
    open_block_signal = market_cap_signal
    if blocked_buy is not None:
        open_block_signal = open_block_signal.where(~blocked_buy.reindex_like(open_block_signal).fillna(False))
    return {
        "raw_signal_count": _count_signal_symbols_on_date(raw_signal, as_of),
        "positive_signal_count": _count_signal_symbols_on_date(raw_signal, as_of),
        "after_liquidity_count": _count_signal_symbols_on_date(liquidity_signal, as_of),
        "after_st_filter_count": _count_signal_symbols_on_date(st_signal, as_of),
        "after_market_cap_top50_count": _count_signal_symbols_on_date(market_cap_signal, as_of),
        "after_open_block_count": _count_signal_symbols_on_date(open_block_signal, as_of),
    }


def _build_live_target_candidate_frame(
    root: str | Path,
    *,
    start: str,
    as_of: str,
    preset_name: str | None,
    fallback_active: pd.DataFrame,
    portfolio_notional: float,
    live_top_n: int | None,
    min_position_value: float,
    market_permissions: MarketPermissionConfig | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """按 100 万执行口径从信号候选里补位生成冻结目标输入。"""

    desired_top_n = int(live_top_n or 0)
    if desired_top_n <= 0 or preset_name is None:
        frame = _fallback_target_candidates(root, as_of, fallback_active)
        return frame, {
            "mode": "handoff_target",
            "desired_top_n": None,
            "market_cap_top_pct": None,
            "market_cap_field": None,
            "eligible_signal_count": int(len(frame)),
            "final_position_count": int(len(frame)),
            "skipped_counts": {},
            "skipped_unaffordable_symbols": [],
            "skipped_market_rule_symbols": [],
            "skipped_quality_blocked_symbols": [],
            "skipped_st_symbols": [],
            "skipped_permission_symbols": [],
            "shortfall_reason": "" if len(frame) else "empty_handoff_target",
        }

    workspace = Workspace(Path(root).expanduser())
    freshness = validate_live_required_data_freshness(workspace.root, as_of=as_of)
    inputs = load_earnings_forecast_inputs(
        workspace,
        start=start,
        end=as_of,
        require_precise_data=True,
    )
    preset = get_earnings_forecast_version_preset(preset_name)
    strategy_config = EarningsForecastDriftConfig(
        top_n=preset.top_n,
        position_mode=preset.position_mode,
        max_weight=preset.max_weight,
        transaction_cost_bps=preset.transaction_cost_bps,
    )
    signal, market_gate, blocked_buy, _blocked_sell = _build_version_signal_context(inputs, preset=preset)
    selection_funnel = _build_selection_funnel_base(
        inputs,
        as_of=as_of,
        preset=preset,
        blocked_buy=blocked_buy,
    )
    market_observation = _market_gate_observation(inputs, as_of=as_of, config=strategy_config)
    if as_of not in signal.index:
        raise ValueError(f"信号矩阵缺少 {as_of}")
    all_signal_symbols = [str(symbol) for symbol in signal.columns.astype(str)]
    all_st_flags = load_trade_st_flags(root, as_of=as_of, symbols=all_signal_symbols)
    known_st_symbols = sorted(symbol for symbol, flagged in all_st_flags.items() if flagged)
    permission_config = market_permissions or MarketPermissionConfig()
    known_permission_symbols = sorted(
        symbol for symbol in all_signal_symbols if not is_market_allowed(symbol, permission_config)
    )
    if as_of in market_gate.index and not bool(market_gate.loc[as_of]):
        return _empty_live_target_candidates(), {
            "mode": "live_topn_replacement",
            "desired_top_n": desired_top_n,
            "market_cap_top_pct": preset.market_cap_top_pct,
            "market_cap_field": preset.market_cap_field,
            "eligible_signal_count": 0,
            "selection_funnel": selection_funnel,
            "market_gate": market_observation,
            "final_position_count": 0,
            "skipped_counts": {"st": len(known_st_symbols), "market_permission": len(known_permission_symbols)},
            "skipped_unaffordable_symbols": [],
            "skipped_market_rule_symbols": [],
            "skipped_quality_blocked_symbols": [],
            "skipped_st_symbols": known_st_symbols,
            "skipped_permission_symbols": known_permission_symbols,
            "shortfall_reason": "market_gate_off",
        }

    day_signal = signal.loc[as_of].dropna().sort_values(ascending=False)
    if blocked_buy is not None and as_of in blocked_buy.index:
        blocked = blocked_buy.loc[as_of].reindex(day_signal.index).fillna(False).astype(bool)
        day_signal = day_signal.loc[~blocked]
    eligible_count = int(len(day_signal))
    if day_signal.empty:
        return _empty_live_target_candidates(), {
            "mode": "live_topn_replacement",
            "desired_top_n": desired_top_n,
            "market_cap_top_pct": preset.market_cap_top_pct,
            "market_cap_field": preset.market_cap_field,
            "eligible_signal_count": 0,
            "selection_funnel": selection_funnel,
            "market_gate": market_observation,
            "final_position_count": 0,
            "skipped_counts": {"st": len(known_st_symbols), "market_permission": len(known_permission_symbols)},
            "skipped_unaffordable_symbols": [],
            "skipped_market_rule_symbols": [],
            "skipped_quality_blocked_symbols": [],
            "skipped_st_symbols": known_st_symbols,
            "skipped_permission_symbols": known_permission_symbols,
            "shortfall_reason": "no_positive_signal_candidates",
        }

    close_prices = _read_close_prices_for_symbols(root, as_of, day_signal.index.astype(str).tolist())
    quality_labels = _candidate_quality_labels(root, as_of=as_of, symbols=day_signal.index.astype(str).tolist())
    after_quality_symbols = [
        str(symbol)
        for symbol in day_signal.index.astype(str)
        if not all_st_flags.get(str(symbol), False) and quality_labels.get(str(symbol)) != "blocked"
    ]
    after_permission_symbols = [
        symbol for symbol in after_quality_symbols if is_market_allowed(symbol, permission_config)
    ]
    selection_funnel.update(
        {
            "after_quality_block_count": len(after_quality_symbols),
            "after_permission_count": len(after_permission_symbols),
        }
    )
    candidate_limit = min(desired_top_n, eligible_count)
    target_weight = min(float(preset.max_weight), 1.0 / float(candidate_limit))
    selected: list[dict[str, Any]] = []
    skipped_unaffordable: list[str] = []
    skipped_market_rule: list[str] = []
    skipped_quality_blocked: list[str] = []
    skipped_st_symbols: set[str] = set(known_st_symbols)
    skipped_permission_symbols: set[str] = set(known_permission_symbols)
    for symbol, signal_value in day_signal.items():
        symbol_text = str(symbol)
        if all_st_flags.get(symbol_text, False):
            skipped_st_symbols.add(symbol_text)
            continue
        if quality_labels.get(symbol_text) == "blocked":
            skipped_quality_blocked.append(symbol_text)
            continue
        if not is_market_allowed(symbol_text, permission_config):
            skipped_permission_symbols.add(symbol_text)
            continue
        price = close_prices.get(symbol_text)
        if price is None or price <= 0:
            skipped_unaffordable.append(symbol_text)
            continue
        target_value = portfolio_notional * target_weight
        shares = int(target_value / float(price) // 100) * 100
        min_shares = min_order_shares(symbol_text, "buy")
        if shares < min_shares:
            skipped_market_rule.append(symbol_text)
            continue
        rounded_value = shares * float(price)
        if rounded_value < min_position_value:
            skipped_unaffordable.append(symbol_text)
            continue
        selected.append(
            {
                "symbol": symbol_text,
                "weight": target_weight,
                "reference_price": float(price),
                "action": "buy_or_increase",
                "signal_value": float(signal_value),
                "market_board": market_board(symbol_text),
                "min_order_shares": min_shares,
            }
        )
        if len(selected) >= candidate_limit:
            break

    selection_funnel["executable_candidate_count"] = len(selected)
    selection_funnel["selected_position_count"] = len(selected)
    shortfall_reason = ""
    if len(selected) < desired_top_n:
        if selection_funnel.get("after_market_cap_top50_count", eligible_count) < desired_top_n:
            shortfall_reason = "market_cap_filter_shortfall"
        elif selection_funnel.get("after_open_block_count", eligible_count) < desired_top_n:
            shortfall_reason = "open_block_shortfall"
        elif selection_funnel.get("after_quality_block_count", eligible_count) < desired_top_n:
            shortfall_reason = "quality_filter_shortfall"
        elif selection_funnel.get("after_permission_count", eligible_count) < desired_top_n:
            shortfall_reason = "market_permission_shortfall"
        elif len(selected) < desired_top_n:
            shortfall_reason = "execution_rule_shortfall"
    diagnostics = {
        "mode": "live_topn_replacement",
        "desired_top_n": desired_top_n,
        "market_cap_top_pct": preset.market_cap_top_pct,
        "market_cap_field": preset.market_cap_field,
        "eligible_signal_count": eligible_count,
        "selection_funnel": selection_funnel,
        "market_gate": market_observation,
        "candidate_limit": candidate_limit,
        "final_position_count": int(len(selected)),
        "target_weight": target_weight,
        "data_freshness": freshness,
        "skipped_counts": {
            "unaffordable": len(skipped_unaffordable),
            "market_rule": len(skipped_market_rule),
            "quality_blocked": len(skipped_quality_blocked),
            "st": len(skipped_st_symbols),
            "market_permission": len(skipped_permission_symbols),
        },
        "skipped_unaffordable_symbols": skipped_unaffordable,
        "skipped_market_rule_symbols": skipped_market_rule,
        "skipped_quality_blocked_symbols": skipped_quality_blocked,
        "skipped_st_symbols": sorted(skipped_st_symbols),
        "skipped_permission_symbols": sorted(skipped_permission_symbols),
        "shortfall_reason": shortfall_reason,
    }
    return pd.DataFrame(
        selected,
        columns=["symbol", "weight", "reference_price", "action", "signal_value", "market_board", "min_order_shares"],
    ), diagnostics


def _fallback_target_candidates(root: str | Path, as_of: str, active: pd.DataFrame) -> pd.DataFrame:
    close_prices = _read_close_prices_for_symbols(
        root,
        as_of,
        active["symbol"].astype(str).tolist(),
    )
    frame = active.copy()
    frame["reference_price"] = frame["symbol"].astype(str).map(close_prices)
    return frame


def _empty_live_target_candidates() -> pd.DataFrame:
    return pd.DataFrame(columns=["symbol", "weight", "reference_price", "action"])


def _build_target_holding_quality_review(
    root: str | Path,
    *,
    as_of: str,
    portfolio: Any,
) -> pd.DataFrame:
    storage = ParquetDuckDBBackend(Workspace(Path(root).expanduser()).data_dir)
    holdings = pd.DataFrame(
        [
            {
                "symbol": position.symbol,
                "target_weight": position.target_weight,
                "target_value": position.target_value,
                "target_shares": position.target_shares,
                "reference_price": position.reference_price,
            }
            for position in portfolio.positions
        ]
    )
    forecast = storage.read("forecast")
    fina_indicator = storage.read("fina_indicator")
    return build_holding_quality_review(
        holdings,
        forecast=forecast,
        fina_indicator=fina_indicator,
        as_of=as_of,
    )


def _candidate_quality_labels(root: str | Path, *, as_of: str, symbols: list[str]) -> dict[str, str]:
    if not symbols:
        return {}
    storage = ParquetDuckDBBackend(Workspace(Path(root).expanduser()).data_dir)
    holdings = pd.DataFrame({"symbol": [str(symbol) for symbol in symbols]})
    review = build_holding_quality_review(
        holdings,
        forecast=storage.read("forecast"),
        fina_indicator=storage.read("fina_indicator"),
        as_of=as_of,
    )
    return dict(zip(review["symbol"].astype(str), review["quality_label"].astype(str), strict=False))


def list_pending_qmt_tasks(
    root: str | Path,
    *,
    trade_date: str | None = None,
    statuses: set[str] | None = None,
) -> list[dict[str, Any]]:
    """列出指定状态下的 QMT 待执行任务。"""

    task_dir = _pending_task_dir(root)
    if not task_dir.exists():
        return []
    allowed = statuses or {"pending", "submitted"}
    rows: list[dict[str, Any]] = []
    for path in sorted(task_dir.glob("*.json")):
        payload = read_json(path)
        payload["_task_path"] = str(path)
        if trade_date and str(payload.get("trade_date", "")) != trade_date:
            continue
        if str(payload.get("status", "")) not in allowed:
            continue
        rows.append(payload)
    return rows


def execute_pending_qmt_task(
    root: str | Path,
    *,
    task_path: str | Path,
    qmt_bridge_url: str,
    qmt_bridge_token: str | None = None,
    qmt_account_id: str | None = None,
    allow_missing_st_data: bool = False,
    allow_trading: bool = True,
    buy_limit_bps: float = 30.0,
    sell_limit_bps: float = 30.0,
    min_order_value: float = 3_000.0,
    max_order_count: int = 80,
    max_single_order_value: float = 100_000.0,
    max_daily_order_value: float = 1_000_000.0,
) -> dict[str, object]:
    """执行一份 prepare 阶段写入的待执行任务。

    这里消费 prepare 阶段冻结好的组合，只做执行差分，不重新计算策略信号。
    """

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    task_file = Path(task_path).expanduser()
    task_payload = read_json(task_file)
    portfolio = target_portfolio_from_dict(
        read_json(Path(task_payload["target_portfolio_path"]).expanduser())
    )

    bridge_probe = QmtBridgeConfig(
        base_url=qmt_bridge_url,
        token=qmt_bridge_token,
        account_id=qmt_account_id or task_payload.get("qmt_account_id") or None,
        allow_trading=False,
    )
    # 先读当前持仓，用它和冻结目标一起构建执行时的 ST 风险标记。
    from vortex.trade.qmt_bridge import QmtBridgeAdapter

    adapter = QmtBridgeAdapter(bridge_probe)
    positions = adapter.get_positions()
    symbols = sorted(
        {
            *(position.symbol for position in positions),
            *(item.symbol for item in portfolio.positions),
        }
    )
    st_flags = load_trade_st_flags(
        workspace.root,
        as_of=str(task_payload.get("as_of") or portfolio.trade_date),
        symbols=symbols,
    )

    artifacts = run_qmt_rebalance(
        portfolio,
        bridge_config=QmtBridgeConfig(
            base_url=qmt_bridge_url,
            token=qmt_bridge_token,
            account_id=qmt_account_id or task_payload.get("qmt_account_id") or None,
            allow_trading=allow_trading,
        ),
        output_root=workspace.root,
        st_flags=st_flags,
        order_config=OrderPlanConfig(
            buy_limit_bps=float(buy_limit_bps),
            sell_limit_bps=float(sell_limit_bps),
            min_order_value=float(min_order_value),
        ),
        risk_config=PreTradeRiskConfig(
            mode="qmt_sim",
            allow_live=bool(allow_trading),
            require_st_data=not bool(allow_missing_st_data),
            max_order_count=int(max_order_count),
            max_single_order_value=float(max_single_order_value),
            max_daily_order_value=float(max_daily_order_value),
        ),
    )
    task_payload.update(
        {
            "status": "done" if artifacts.report.risk_result.passed else "blocked",
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "exec_id": artifacts.exec_id,
            "execution_report_path": str(artifacts.execution_report_path),
            "error": "" if artifacts.report.risk_result.passed else "; ".join(artifacts.report.risk_result.blocking_reasons),
        }
    )
    write_json(task_file, task_payload)
    return {
        "task_path": str(task_file),
        "task_status": task_payload["status"],
        "exec_id": artifacts.exec_id,
        "risk_passed": artifacts.report.risk_result.passed,
        "blocking_reasons": artifacts.report.risk_result.blocking_reasons,
        "execution_report_path": str(artifacts.execution_report_path),
        "execution_report_md_path": str(artifacts.execution_report_md_path),
    }


def _record_auto_nav_snapshot(
    root: str | Path,
    *,
    trade_date: str,
    qmt_bridge_url: str,
    qmt_bridge_token: str | None,
    qmt_account_id: str | None,
    preset_name: str,
    initial_equity: float,
    benchmark: str,
) -> dict[str, Any]:
    if not qmt_account_id:
        raise ValueError("缺少 qmt_account_id，无法记录账户级策略净值")

    from vortex.trade.nav import ensure_nav_binding, latest_benchmark_close, record_nav_snapshot
    from vortex.trade.qmt_bridge import QmtBridgeAdapter

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    binding = ensure_nav_binding(
        workspace.root,
        strategy_name="earnings_forecast_auto",
        strategy_version=preset_name,
        account_id=qmt_account_id,
        initial_equity=float(initial_equity),
        benchmark=benchmark,
        start_date=trade_date,
    )
    adapter = QmtBridgeAdapter(
        QmtBridgeConfig(
            base_url=qmt_bridge_url,
            token=qmt_bridge_token,
            account_id=qmt_account_id,
            allow_trading=False,
        )
    )
    cash = adapter.get_cash()
    benchmark_close = latest_benchmark_close(workspace.root, benchmark=binding.benchmark, trade_date=trade_date)
    return record_nav_snapshot(
        workspace.root,
        binding=binding,
        trade_date=trade_date,
        cash=cash,
        benchmark_close=benchmark_close,
    )


def run_earnings_forecast_auto_cycle_once(
    root: str | Path,
    *,
    start: str,
    profile_name: str,
    qmt_bridge_url: str,
    qmt_bridge_token: str | None = None,
    qmt_account_id: str | None = None,
    preset_name: str = DEFAULT_AUTO_PRESET,
    label: str = DEFAULT_AUTO_LABEL,
    prepare_time: str = DEFAULT_AUTO_PREPARE_TIME,
    execute_time: str = DEFAULT_AUTO_EXECUTE_TIME,
    now: datetime | None = None,
    allow_trading: bool = True,
    nav_initial_equity: float = DEFAULT_AUTO_NAV_INITIAL_EQUITY,
    nav_benchmark: str = DEFAULT_AUTO_NAV_BENCHMARK,
) -> dict[str, object]:
    """执行一次自动编排 tick。

    第一版采用“单进程顺序编排”：
    1. 到 prepare 时间后先检查关键数据新鲜度，缺分区只定向补数；
    2. 然后按最新可见数据生成 `trade_date=today` 的冻结组合；
    3. 到 execute 时间后执行今天到期的 pending 任务。
    """

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    current = now or datetime.now()
    today = current.strftime("%Y%m%d")
    summary: dict[str, object] = {
        "now": current.isoformat(timespec="seconds"),
        "today": today,
        "prepared": None,
        "executed": [],
        "nav_snapshot": None,
        "skipped": [],
    }
    if not is_trade_day(workspace.root, today):
        summary["skipped"] = ["today is not a trade day"]
        return summary

    if current.time() >= _parse_hhmm(prepare_time):
        existing = list_pending_qmt_tasks(
            workspace.root,
            trade_date=today,
            statuses={"pending", "submitted", "done", "blocked"},
        )
        if not existing:
            strategy_as_of = _resolve_latest_strategy_as_of(workspace.root, today)
            try:
                validate_live_required_data_freshness(workspace.root, as_of=strategy_as_of)
            except ValueError as freshness_error:
                summary["skipped"].append(str(freshness_error))
                _run_data_update_foreground(
                    workspace.root,
                    profile_name,
                    datasets=TARGETED_LIVE_REFRESH_DATASETS,
                )
                after_update = datetime.now() if now is None else current
                if after_update.time() >= _parse_hhmm(execute_time):
                    summary["skipped"].append("targeted data refresh finished after execute window")
                    return summary
                validate_live_required_data_freshness(workspace.root, as_of=strategy_as_of)
            if strategy_as_of != today:
                summary["skipped"].append(f"strategy as_of resolved to latest visible date {strategy_as_of}")
            prepared = prepare_earnings_forecast_next_session(
                workspace.root,
                start=start,
                as_of=strategy_as_of,
                execution_trade_date=today,
                qmt_bridge_url=qmt_bridge_url,
                qmt_bridge_token=qmt_bridge_token,
                qmt_account_id=qmt_account_id,
                label=label,
                preset_name=preset_name,
            )
            summary["prepared"] = prepared.summary
        else:
            summary["skipped"].append("trade-day plan already exists for today")
    else:
        summary["skipped"].append("prepare window not reached")

    execute_current = datetime.now() if now is None else current
    if execute_current.time() >= _parse_hhmm(execute_time):
        for task in list_pending_qmt_tasks(workspace.root, trade_date=today, statuses={"pending"}):
            result = execute_pending_qmt_task(
                workspace.root,
                task_path=str(task["_task_path"]),
                qmt_bridge_url=qmt_bridge_url,
                qmt_bridge_token=qmt_bridge_token,
                qmt_account_id=qmt_account_id,
                allow_trading=allow_trading,
            )
            cast_list = summary["executed"]
            assert isinstance(cast_list, list)
            cast_list.append(result)
        executed_results = summary["executed"]
        if executed_results and qmt_account_id:
            assert isinstance(executed_results, list)
            if any(bool(item.get("risk_passed")) for item in executed_results if isinstance(item, dict)):
                try:
                    summary["nav_snapshot"] = _record_auto_nav_snapshot(
                        workspace.root,
                        trade_date=today,
                        qmt_bridge_url=qmt_bridge_url,
                        qmt_bridge_token=qmt_bridge_token,
                        qmt_account_id=qmt_account_id,
                        preset_name=preset_name,
                        initial_equity=nav_initial_equity,
                        benchmark=nav_benchmark,
                    )
                except Exception as exc:  # noqa: BLE001 - surfaced in tick status for manual follow-up.
                    summary["nav_snapshot"] = {
                        "status": "error",
                        "type": type(exc).__name__,
                        "message": str(exc),
                    }
                    summary["skipped"].append(f"nav snapshot failed: {exc}")
    else:
        summary["skipped"].append("execute window not reached")
    return summary


def run_earnings_forecast_auto_once(
    root: str | Path,
    *,
    start: str,
    profile_name: str,
    qmt_bridge_url: str,
    qmt_bridge_token: str | None = None,
    qmt_account_id: str | None = None,
    preset_name: str = DEFAULT_AUTO_PRESET,
    label: str = DEFAULT_AUTO_LABEL,
    prepare_time: str = DEFAULT_AUTO_PREPARE_TIME,
    execute_time: str = DEFAULT_AUTO_EXECUTE_TIME,
    allow_trading: bool = True,
    nav_initial_equity: float = DEFAULT_AUTO_NAV_INITIAL_EQUITY,
    nav_benchmark: str = DEFAULT_AUTO_NAV_BENCHMARK,
) -> dict[str, object]:
    """执行一轮 auto-run，并同步刷新状态文件与稳定日志。"""

    observer = EarningsForecastAutoObserver(
        root,
        start=start,
        profile_name=profile_name,
        qmt_bridge_url=qmt_bridge_url,
        qmt_account_id=qmt_account_id,
        preset_name=preset_name,
        label=label,
        prepare_time=prepare_time,
        execute_time=execute_time,
        poll_seconds=0,
        allow_trading=allow_trading,
        loop_mode="once",
        nav_initial_equity=nav_initial_equity,
        nav_benchmark=nav_benchmark,
    )
    try:
        payload = run_earnings_forecast_auto_cycle_once(
            root,
            start=start,
            profile_name=profile_name,
            qmt_bridge_url=qmt_bridge_url,
            qmt_bridge_token=qmt_bridge_token,
            qmt_account_id=qmt_account_id,
            preset_name=preset_name,
            label=label,
            prepare_time=prepare_time,
            execute_time=execute_time,
            allow_trading=allow_trading,
            nav_initial_equity=nav_initial_equity,
            nav_benchmark=nav_benchmark,
        )
    except Exception as exc:
        observer.record_tick_error(exc, keep_running=False)
        raise
    observer.record_tick_success(payload, keep_running=False)
    return payload


def run_earnings_forecast_auto_loop(
    root: str | Path,
    *,
    start: str,
    profile_name: str,
    qmt_bridge_url: str,
    qmt_bridge_token: str | None = None,
    qmt_account_id: str | None = None,
    preset_name: str = DEFAULT_AUTO_PRESET,
    label: str = DEFAULT_AUTO_LABEL,
    prepare_time: str = DEFAULT_AUTO_PREPARE_TIME,
    execute_time: str = DEFAULT_AUTO_EXECUTE_TIME,
    poll_seconds: int = 60,
    allow_trading: bool = True,
    nav_initial_equity: float = DEFAULT_AUTO_NAV_INITIAL_EQUITY,
    nav_benchmark: str = DEFAULT_AUTO_NAV_BENCHMARK,
) -> None:
    """常驻循环执行自动编排。"""

    observer = EarningsForecastAutoObserver(
        root,
        start=start,
        profile_name=profile_name,
        qmt_bridge_url=qmt_bridge_url,
        qmt_account_id=qmt_account_id,
        preset_name=preset_name,
        label=label,
        prepare_time=prepare_time,
        execute_time=execute_time,
        poll_seconds=poll_seconds,
        allow_trading=allow_trading,
        loop_mode="loop",
        nav_initial_equity=nav_initial_equity,
        nav_benchmark=nav_benchmark,
    )
    try:
        while True:
            try:
                payload = run_earnings_forecast_auto_cycle_once(
                    root,
                    start=start,
                    profile_name=profile_name,
                    qmt_bridge_url=qmt_bridge_url,
                    qmt_bridge_token=qmt_bridge_token,
                    qmt_account_id=qmt_account_id,
                    preset_name=preset_name,
                    label=label,
                    prepare_time=prepare_time,
                    execute_time=execute_time,
                    allow_trading=allow_trading,
                    nav_initial_equity=nav_initial_equity,
                    nav_benchmark=nav_benchmark,
                )
            except Exception as exc:
                observer.record_tick_error(exc, keep_running=True)
                print(
                    json.dumps(
                        {
                            "status": "error",
                            "error": {
                                "type": type(exc).__name__,
                                "message": str(exc),
                            },
                        },
                        ensure_ascii=False,
                        indent=2,
                    ),
                    file=sys.stderr,
                )
            else:
                observer.record_tick_success(payload, keep_running=True)
                print(json.dumps(payload, ensure_ascii=False, indent=2))
            time.sleep(max(int(poll_seconds), 5))
    finally:
        observer.record_shutdown("loop exited")


def _normalize_open_trade_calendar_rows(rows: pd.DataFrame) -> pd.DataFrame:
    """把开市日历统一归一到单列 `cal_date`。"""

    if rows.empty or "cal_date" not in rows.columns:
        return pd.DataFrame(columns=["cal_date"])
    normalized = rows.loc[:, ["cal_date"]].copy()
    normalized["cal_date"] = normalized["cal_date"].astype(str)
    normalized = normalized.loc[normalized["cal_date"].str.fullmatch(r"\d{8}")]
    return normalized.drop_duplicates(subset=["cal_date"]).sort_values("cal_date").reset_index(drop=True)


def _extend_open_trade_calendar_if_needed(
    storage: ParquetDuckDBBackend,
    rows: pd.DataFrame,
    *,
    ensure_start: str | None,
    ensure_end: str | None,
    require_full_range: bool = False,
) -> pd.DataFrame:
    """当本地 calendar 未覆盖目标日期时，仅补拉交易日历。

    盘前我们会把日线类 dataset 的可见截止日回退到最近已完成交易日，但
    auto-run 仍然需要知道“今天是不是交易日”。因此这里单独对 calendar
    做轻量补拉：只在 workspace 现有日历覆盖不到查询范围时触发，不影响
    其它 dataset 的可见性口径。
    """

    start_key = ensure_start or ensure_end
    end_key = ensure_end or ensure_start
    if start_key is None or end_key is None:
        return rows
    if not rows.empty:
        max_key = str(rows["cal_date"].max())
        if require_full_range and max_key >= end_key:
            return rows
        if not require_full_range and max_key >= start_key:
            return rows

    provider = TushareProvider()
    fetched_days = provider.fetch_calendar(
        "cn_stock",
        datetime.strptime(start_key, "%Y%m%d").date(),
        datetime.strptime(end_key, "%Y%m%d").date(),
    )
    fetched = _normalize_open_trade_calendar_rows(
        pd.DataFrame({"cal_date": [day.strftime("%Y%m%d") for day in fetched_days]})
    )
    if fetched.empty:
        raise ValueError(f"calendar 数据未覆盖 {start_key}~{end_key}，且 Tushare 补拉为空")
    storage.upsert("calendar", fetched, {})
    return _normalize_open_trade_calendar_rows(pd.concat([rows, fetched], ignore_index=True))


def _load_open_trade_calendar(
    root: str | Path,
    *,
    ensure_start: str | None = None,
    ensure_end: str | None = None,
    require_full_range: bool = False,
) -> pd.DataFrame:
    """加载开市日历，兼容新 `calendar` 与旧 `trade_cal` 两种落盘口径。"""

    workspace = Workspace(Path(root).expanduser())
    storage = ParquetDuckDBBackend(workspace.data_dir)

    try:
        calendar = storage.read("calendar", columns=["cal_date"])
    except Exception:
        calendar = pd.DataFrame()
    rows = _normalize_open_trade_calendar_rows(calendar)
    if not rows.empty:
        return _extend_open_trade_calendar_if_needed(
            storage,
            rows,
            ensure_start=ensure_start,
            ensure_end=ensure_end,
            require_full_range=require_full_range,
        )

    try:
        legacy = storage.read("trade_cal", columns=["cal_date", "is_open"])
    except Exception:
        legacy = pd.DataFrame()
    if legacy.empty:
        raise ValueError("calendar/trade_cal 数据集为空，无法判断交易日；请先运行 data update")

    rows = legacy.copy()
    rows["cal_date"] = rows["cal_date"].astype(str)
    rows["is_open"] = pd.to_numeric(rows["is_open"], errors="coerce").fillna(0).astype(int)
    rows = rows.loc[(rows["cal_date"].str.fullmatch(r"\d{8}")) & (rows["is_open"] == 1), ["cal_date"]]
    rows = _normalize_open_trade_calendar_rows(rows)
    return _extend_open_trade_calendar_if_needed(
        storage,
        rows,
        ensure_start=ensure_start,
        ensure_end=ensure_end,
        require_full_range=require_full_range,
    )


def resolve_next_trade_date(root: str | Path, as_of: str) -> str:
    """从 workspace 的开市日历里解析下一个交易日。"""

    current = datetime.strptime(as_of, "%Y%m%d")
    end_date = (current + timedelta(days=30)).strftime("%Y%m%d")
    rows = _load_open_trade_calendar(root, ensure_start=as_of, ensure_end=end_date)
    future = rows.loc[(rows["cal_date"] > as_of) & (rows["cal_date"] <= end_date)].sort_values("cal_date")
    if future.empty:
        rows = _load_open_trade_calendar(
            root,
            ensure_start=as_of,
            ensure_end=end_date,
            require_full_range=True,
        )
        future = rows.loc[(rows["cal_date"] > as_of) & (rows["cal_date"] <= end_date)].sort_values("cal_date")
    if future.empty:
        raise ValueError(f"无法在 calendar/trade_cal 中找到 {as_of} 之后的交易日")
    return str(future.iloc[0]["cal_date"])


def resolve_execution_trade_date(root: str | Path, as_of: str) -> str:
    """解析 prepare 对应的执行日。

    交易日开盘前 prepare 默认生成当天执行计划；只有当 `as_of` 不是交易日时，
    才顺延到下一个交易日，避免 orchestration 层再额外叠加一层硬编码 T+1。
    """

    if is_trade_day(root, as_of):
        return as_of
    return resolve_next_trade_date(root, as_of)


def _resolve_latest_strategy_as_of(root: str | Path, trade_date: str) -> str:
    """自动编排用执行日前最新已有日线交易日生成信号，避免盘前要求当天日线。"""

    storage = ParquetDuckDBBackend(Workspace(Path(root).expanduser()).data_dir)
    bars = storage.read("bars", filters={"date": ("<", int(trade_date))}, columns=["date"])
    if bars.empty:
        raise ValueError(f"无法找到 {trade_date} 之前的 bars 数据，不能生成盘前策略信号")
    dates = pd.to_numeric(bars["date"], errors="coerce").dropna()
    if dates.empty:
        raise ValueError(f"bars 日期无效，不能为 {trade_date} 生成盘前策略信号")
    return str(int(dates.max()))


def is_trade_day(root: str | Path, date_str: str) -> bool:
    """判断某个自然日是否为交易日。"""

    try:
        rows = _load_open_trade_calendar(root, ensure_start=date_str, ensure_end=date_str)
    except ValueError:
        if datetime.strptime(date_str, "%Y%m%d").weekday() >= 5:
            return False
        raise
    matched = rows.loc[rows["cal_date"] == date_str]
    return not matched.empty


def _looks_like_st_name(name: object) -> bool:
    text = str(name or "").strip().upper().replace("＊", "*")
    if not text:
        return False
    return text.startswith("ST") or text.startswith("*ST") or text.startswith("SST") or " ST" in text


def load_trade_st_flags(
    root: str | Path,
    *,
    as_of: str,
    symbols: list[str],
) -> dict[str, bool]:
    """按执行日口径为一组 symbol 构建 ST 风险标记。"""

    workspace = Workspace(Path(root).expanduser())
    storage = ParquetDuckDBBackend(workspace.data_dir)
    if not symbols:
        return {}

    symbol_index = pd.Index([str(symbol) for symbol in symbols], dtype="object")
    target_index = pd.Index([as_of], dtype="object")
    flags = {str(symbol): False for symbol in symbols}

    stock_st_partitions = _date_partitions(storage, "stock_st")
    stock_st_as_of = max(stock_st_partitions) if stock_st_partitions else None
    if as_of not in stock_st_partitions:
        raise ValueError(f"stock_st 缺少 {as_of} 分区（最新 {stock_st_as_of}），不能生成 ST 风险标记")

    stock_st = storage.read(
        "stock_st",
        columns=["date", "symbol", "name", "type", "type_name"],
    )
    stock_st = _filter_daily_frame(stock_st, start=as_of, end=as_of)
    if not stock_st.empty:
        stock_mask = build_stock_st_mask(stock_st, target_index, symbol_index)
        for symbol in symbols:
            flags[str(symbol)] = flags[str(symbol)] or bool(stock_mask.loc[as_of, str(symbol)])
        name_rows = stock_st.loc[stock_st["symbol"].astype(str).isin(symbol_index), ["symbol", "name"]].copy()
        for _, row in name_rows.iterrows():
            symbol = str(row["symbol"])
            flags[symbol] = flags.get(symbol, False) or _looks_like_st_name(row.get("name"))

    fina_indicator = _read_effective_dataset_as_of(
        storage,
        "fina_indicator",
        as_of=as_of,
        columns=["symbol", "ann_date", "effective_from", "bps", "roe", "debt_to_assets", "netprofit_yoy"],
    )
    balancesheet = _read_effective_dataset_as_of(
        storage,
        "balancesheet",
        as_of=as_of,
        columns=["symbol", "ann_date", "effective_from", "total_hldr_eqy_inc_min_int", "total_hldr_eqy_exc_min_int"],
    )
    cashflow = _read_effective_dataset_as_of(
        storage,
        "cashflow",
        as_of=as_of,
        columns=["symbol", "ann_date", "effective_from", "net_profit", "n_cashflow_act", "free_cashflow"],
    )
    st_risk_events = build_financial_st_risk_events(
        fina_indicator=fina_indicator if not fina_indicator.empty else None,
        balancesheet=balancesheet if not balancesheet.empty else None,
        cashflow=cashflow if not cashflow.empty else None,
        target_index=target_index,
    )
    if not st_risk_events.empty:
        risk_mask = build_persistent_st_risk_mask(st_risk_events, target_index, symbol_index)
        for symbol in symbols:
            flags[str(symbol)] = flags[str(symbol)] or bool(risk_mask.loc[as_of, str(symbol)])
    return flags


def _strategy_version_from_handoff(summary: dict[str, Any]) -> str:
    preset = summary.get("preset")
    if isinstance(preset, dict) and preset.get("name"):
        return str(preset["name"])
    return "earnings_forecast_drift"


def _pending_task_dir(root: str | Path) -> Path:
    workspace = Workspace(Path(root).expanduser())
    task_dir = workspace.state_dir / "trade" / "pending_qmt"
    task_dir.mkdir(parents=True, exist_ok=True)
    return task_dir


def _read_close_prices_for_symbols(root: str | Path, date_str: str, symbols: list[str]) -> dict[str, float]:
    workspace = Workspace(Path(root).expanduser())
    storage = ParquetDuckDBBackend(workspace.data_dir)
    bars = storage.read("bars", columns=["date", "symbol", "close"])
    bars = _filter_daily_frame(bars, start=date_str, end=date_str)
    if bars.empty:
        raise ValueError(f"bars 在 {date_str} 没有收盘价数据")
    rows = bars.loc[bars["symbol"].astype(str).isin([str(symbol) for symbol in symbols])].copy()
    if rows.empty:
        raise ValueError(f"bars 在 {date_str} 没有目标股票收盘价数据: {symbols}")
    rows["symbol"] = rows["symbol"].astype(str)
    rows["close"] = pd.to_numeric(rows["close"], errors="coerce")
    return {
        str(symbol): float(price)
        for symbol, price in rows.dropna(subset=["close"]).drop_duplicates("symbol", keep="last")[["symbol", "close"]].itertuples(index=False)
    }


def _read_effective_dataset_as_of(
    storage: ParquetDuckDBBackend,
    dataset: str,
    *,
    as_of: str,
    columns: list[str],
) -> pd.DataFrame:
    frame = storage.read(dataset, columns=columns)
    if frame.empty:
        return frame
    if "effective_from" in frame.columns:
        digits = frame["effective_from"].astype(str).str.replace(r"\D", "", regex=True).str.slice(0, 8)
    elif "ann_date" in frame.columns:
        digits = frame["ann_date"].astype(str).str.replace(r"\D", "", regex=True).str.slice(0, 8)
    else:
        return frame.iloc[0:0].copy()
    dates = pd.to_numeric(digits, errors="coerce")
    return frame.loc[dates.le(int(as_of))].copy()


def _filter_daily_frame(frame: pd.DataFrame, *, start: str, end: str) -> pd.DataFrame:
    if frame.empty or "date" not in frame.columns:
        return frame
    dates = pd.to_numeric(frame["date"], errors="coerce")
    return frame.loc[dates.between(int(start), int(end), inclusive="both")].copy()


def _parse_hhmm(raw: str) -> datetime.time:
    text = raw.strip()
    return datetime.strptime(text, "%H:%M").time()


def _run_data_update_foreground(
    root: str | Path,
    profile_name: str,
    *,
    datasets: tuple[str, ...] | list[str] | None = None,
) -> dict[str, Any]:
    """通过 CLI 子进程顺序运行 data update，保证自动服务口径与人工一致。"""

    command = [
        sys.executable,
        "-m",
        "vortex",
        "data",
        "update",
        "--root",
        str(Path(root).expanduser()),
        "--profile",
        profile_name,
        "--foreground",
        "--format",
        "json",
    ]
    if datasets:
        command.extend(["--datasets", ",".join(str(dataset) for dataset in datasets)])
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        error = completed.stderr.strip() or completed.stdout.strip() or "unknown data update failure"
        raise RuntimeError(f"data update 失败: {error}")
    text = completed.stdout.strip()
    if not text:
        return {}
    return json.loads(text)
