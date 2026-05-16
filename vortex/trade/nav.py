"""账户级策略子账本净值台账。

默认约定是一个 QMT 账户只跑一个自动策略，但账户里可能有超过策略名义本金的闲置现金。
因此首条快照会锁定“外部资金偏移”，后续用账户总资产扣除该偏移来计算策略子账本权益。
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend
from vortex.runtime.workspace import Workspace
from vortex.trade.broker import CashSnapshot
from vortex.trade.serialization import read_json, write_json

NAV_WINDOWS: dict[str, int | None] = {
    "since_inception": None,
    "1w": 5,
    "1m": 21,
    "3m": 63,
    "6m": 126,
    "1y": 252,
}


@dataclass(frozen=True)
class StrategyAccountBinding:
    run_id: str
    strategy_name: str
    strategy_version: str
    account_id: str
    initial_equity: float
    start_date: str
    benchmark: str
    nav_mode: str = "account_subledger"
    external_cash_offset: float | None = None
    status: str = "active"


def default_nav_run_id(strategy_name: str, strategy_version: str, account_id: str) -> str:
    raw = f"{strategy_name}-{strategy_version}-{account_id}"
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", raw).strip("-").lower()


def nav_binding_path(root: str | Path, run_id: str) -> Path:
    workspace = Workspace(Path(root).expanduser())
    return workspace.state_dir / "nav" / f"{run_id}.json"


def nav_ledger_path(root: str | Path, run_id: str) -> Path:
    workspace = Workspace(Path(root).expanduser())
    return workspace.trade_dir / "nav" / f"{run_id}.csv"


def load_nav_binding(root: str | Path, run_id: str) -> StrategyAccountBinding:
    payload = read_json(nav_binding_path(root, run_id))
    return StrategyAccountBinding(**payload)


def ensure_nav_binding(
    root: str | Path,
    *,
    strategy_name: str,
    strategy_version: str,
    account_id: str,
    initial_equity: float,
    benchmark: str,
    start_date: str,
    run_id: str | None = None,
    reset: bool = False,
) -> StrategyAccountBinding:
    """创建或读取策略净值绑定；默认重启服务时延续原子账本曲线。"""

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    resolved_run_id = run_id or default_nav_run_id(strategy_name, strategy_version, account_id)
    path = nav_binding_path(workspace.root, resolved_run_id)
    if path.exists() and not reset:
        return load_nav_binding(workspace.root, resolved_run_id)
    binding = StrategyAccountBinding(
        run_id=resolved_run_id,
        strategy_name=strategy_name,
        strategy_version=strategy_version,
        account_id=account_id,
        initial_equity=float(initial_equity),
        start_date=str(start_date),
        benchmark=str(benchmark),
        nav_mode="account_subledger",
        external_cash_offset=None,
    )
    write_json(path, binding)
    if reset:
        ledger = nav_ledger_path(workspace.root, resolved_run_id)
        if ledger.exists():
            ledger.unlink()
    return binding


def load_nav_ledger(root: str | Path, run_id: str) -> pd.DataFrame:
    path = nav_ledger_path(root, run_id)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype={"trade_date": str})


def latest_benchmark_close(root: str | Path, *, benchmark: str, trade_date: str) -> float | None:
    storage = ParquetDuckDBBackend(Workspace(Path(root).expanduser()).data_dir)
    frame = storage.read("index_daily")
    if frame.empty:
        return None
    symbol_col = "symbol" if "symbol" in frame.columns else "ts_code"
    if symbol_col not in frame.columns or "date" not in frame.columns or "close" not in frame.columns:
        return None
    rows = frame.loc[
        (frame[symbol_col].astype(str) == str(benchmark))
        & (frame["date"].astype(str) <= str(trade_date))
    ].copy()
    if rows.empty:
        return None
    rows = rows.sort_values("date")
    return float(rows.iloc[-1]["close"])


def record_nav_snapshot(
    root: str | Path,
    *,
    binding: StrategyAccountBinding,
    trade_date: str,
    cash: CashSnapshot,
    benchmark_close: float | None = None,
    external_position_drift: bool = False,
    cash_flow_suspected: bool = False,
) -> dict[str, Any]:
    """追加或覆盖某交易日净值快照。

    子账本口径下，第一条快照会将 `账户总资产 - initial_equity` 锁定为外部资金偏移。
    之后默认这些外部资金无人触碰，策略净值只跟踪 `账户总资产 - 外部资金偏移`。
    """

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    ledger_path = nav_ledger_path(workspace.root, binding.run_id)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    existing = load_nav_ledger(workspace.root, binding.run_id)
    binding = _ensure_external_cash_offset(
        workspace.root,
        binding,
        account_total_asset=float(cash.total_asset),
        existing=existing,
    )
    existing = _apply_subledger_columns(existing, binding)
    benchmark_value = (
        benchmark_close
        if benchmark_close is not None
        else latest_benchmark_close(workspace.root, benchmark=binding.benchmark, trade_date=trade_date)
    )
    account_total_asset = float(cash.total_asset)
    account_available_cash = float(cash.available_cash)
    account_frozen_cash = float(cash.frozen_cash)
    account_market_value = float(cash.market_value)
    external_offset = float(binding.external_cash_offset or 0.0)
    strategy_equity = account_total_asset - external_offset
    strategy_available_cash = account_available_cash - external_offset
    row = {
        "trade_date": str(trade_date),
        "run_id": binding.run_id,
        "strategy_name": binding.strategy_name,
        "strategy_version": binding.strategy_version,
        "account_id": binding.account_id,
        "nav_mode": binding.nav_mode,
        "initial_equity": float(binding.initial_equity),
        "external_cash_offset": external_offset,
        "account_total_asset": account_total_asset,
        "account_available_cash": account_available_cash,
        "account_frozen_cash": account_frozen_cash,
        "account_market_value": account_market_value,
        "total_asset": strategy_equity,
        "available_cash": strategy_available_cash,
        "frozen_cash": account_frozen_cash,
        "market_value": account_market_value,
        "net_value": strategy_equity / float(binding.initial_equity),
        "benchmark": binding.benchmark,
        "benchmark_close": benchmark_value,
        "external_position_drift": bool(external_position_drift),
        "cash_flow_suspected": bool(cash_flow_suspected),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }
    if existing.empty:
        ledger = pd.DataFrame([row])
    else:
        ledger = existing.loc[existing["trade_date"].astype(str) != str(trade_date)].copy()
        ledger = pd.concat([ledger, pd.DataFrame([row])], ignore_index=True)
    ledger = _enrich_nav_returns(ledger, binding)
    ledger.to_csv(ledger_path, index=False)
    return {
        "run_id": binding.run_id,
        "trade_date": str(trade_date),
        "net_value": row["net_value"],
        "strategy_equity": strategy_equity,
        "account_total_asset": account_total_asset,
        "external_cash_offset": external_offset,
        "benchmark_close": benchmark_value,
        "ledger_path": str(ledger_path),
        "summary": summarize_nav_ledger(ledger, binding),
    }


def _ensure_external_cash_offset(
    root: str | Path,
    binding: StrategyAccountBinding,
    *,
    account_total_asset: float,
    existing: pd.DataFrame,
) -> StrategyAccountBinding:
    if binding.external_cash_offset is not None:
        return binding

    first_account_total = _first_account_total(existing)
    if first_account_total is None:
        first_account_total = float(account_total_asset)
    updated = replace(
        binding,
        nav_mode="account_subledger",
        external_cash_offset=float(first_account_total) - float(binding.initial_equity),
    )
    write_json(nav_binding_path(root, updated.run_id), updated)
    return updated


def _first_account_total(ledger: pd.DataFrame) -> float | None:
    if ledger.empty:
        return None
    first = ledger.sort_values("trade_date").iloc[0]
    if "account_total_asset" in ledger.columns and pd.notna(first.get("account_total_asset")):
        return float(first["account_total_asset"])
    if "total_asset" in ledger.columns and pd.notna(first.get("total_asset")):
        return float(first["total_asset"])
    return None


def _apply_subledger_columns(ledger: pd.DataFrame, binding: StrategyAccountBinding | None) -> pd.DataFrame:
    if ledger.empty or binding is None:
        return ledger

    frame = ledger.copy()
    offset = float(binding.external_cash_offset or 0.0)
    if "account_total_asset" not in frame.columns:
        frame["account_total_asset"] = frame["total_asset"].astype(float)
    if "account_available_cash" not in frame.columns:
        frame["account_available_cash"] = frame["available_cash"].astype(float)
    if "account_frozen_cash" not in frame.columns:
        frame["account_frozen_cash"] = frame["frozen_cash"].astype(float)
    if "account_market_value" not in frame.columns:
        frame["account_market_value"] = frame["market_value"].astype(float)
    frame["nav_mode"] = frame.get("nav_mode", binding.nav_mode)
    frame["initial_equity"] = float(binding.initial_equity)
    frame["external_cash_offset"] = offset
    frame["total_asset"] = frame["account_total_asset"].astype(float) - offset
    frame["available_cash"] = frame["account_available_cash"].astype(float) - offset
    frame["frozen_cash"] = frame["account_frozen_cash"].astype(float)
    frame["market_value"] = frame["account_market_value"].astype(float)
    frame["net_value"] = frame["total_asset"].astype(float) / float(binding.initial_equity)
    return frame


def _enrich_nav_returns(ledger: pd.DataFrame, binding: StrategyAccountBinding | None = None) -> pd.DataFrame:
    if ledger.empty:
        return ledger
    frame = _apply_subledger_columns(ledger, binding)
    frame["trade_date"] = frame["trade_date"].astype(str)
    frame = frame.sort_values("trade_date").reset_index(drop=True)
    frame["daily_return"] = frame["net_value"].astype(float).pct_change().fillna(0.0)
    if frame["benchmark_close"].notna().any():
        first_benchmark = float(frame["benchmark_close"].dropna().iloc[0])
        frame["benchmark_net_value"] = frame["benchmark_close"].astype(float) / first_benchmark
        frame["benchmark_return"] = frame["benchmark_net_value"].pct_change().fillna(0.0)
        frame["excess_return"] = frame["daily_return"] - frame["benchmark_return"]
    else:
        frame["benchmark_net_value"] = pd.NA
        frame["benchmark_return"] = pd.NA
        frame["excess_return"] = pd.NA
    return frame


def summarize_nav(root: str | Path, run_id: str) -> dict[str, Any]:
    binding = load_nav_binding(root, run_id)
    ledger = load_nav_ledger(root, run_id)
    return {
        "binding": binding.__dict__,
        "ledger_path": str(nav_ledger_path(root, run_id)),
        "summary": summarize_nav_ledger(ledger, binding),
    }


def summarize_nav_ledger(ledger: pd.DataFrame, binding: StrategyAccountBinding | None = None) -> dict[str, Any]:
    if ledger.empty:
        return {"snapshot_count": 0, "windows": {}}
    frame = _enrich_nav_returns(ledger, binding)
    latest = frame.iloc[-1]
    windows = {
        name: _window_summary(frame, rows)
        for name, rows in NAV_WINDOWS.items()
    }
    return {
        "snapshot_count": int(len(frame)),
        "latest_trade_date": str(latest["trade_date"]),
        "latest_net_value": float(latest["net_value"]),
        "latest_total_asset": float(latest["total_asset"]),
        "latest_account_total_asset": None
        if pd.isna(latest.get("account_total_asset"))
        else float(latest["account_total_asset"]),
        "external_cash_offset": None
        if pd.isna(latest.get("external_cash_offset"))
        else float(latest["external_cash_offset"]),
        "latest_benchmark_net_value": None
        if pd.isna(latest.get("benchmark_net_value"))
        else float(latest["benchmark_net_value"]),
        "windows": windows,
    }


def _window_summary(frame: pd.DataFrame, rows: int | None) -> dict[str, float | None]:
    window = frame if rows is None else frame.tail(rows + 1)
    if window.empty:
        return {"strategy_return": None, "benchmark_return": None, "excess_return": None, "max_drawdown": None}
    start = window.iloc[0]
    end = window.iloc[-1]
    strategy_return = float(end["net_value"]) / float(start["net_value"]) - 1.0
    benchmark_return = None
    if pd.notna(start.get("benchmark_net_value")) and pd.notna(end.get("benchmark_net_value")):
        benchmark_return = float(end["benchmark_net_value"]) / float(start["benchmark_net_value"]) - 1.0
    running_peak = window["net_value"].astype(float).cummax()
    drawdown = window["net_value"].astype(float) / running_peak - 1.0
    return {
        "strategy_return": strategy_return,
        "benchmark_return": benchmark_return,
        "excess_return": None if benchmark_return is None else strategy_return - benchmark_return,
        "max_drawdown": float(drawdown.min()),
    }
