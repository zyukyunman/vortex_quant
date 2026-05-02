"""业绩预告漂移策略的可复用运行入口。

该模块把研究阶段验证过的“精确可交易复核”流程沉淀为函数入口，
供 CLI、自动化任务和后续 shadow trading 复用，避免继续依赖
一次性脚本。它只读取本地 workspace 数据，不主动联网补数。
"""
from __future__ import annotations

import json
import dataclasses
from dataclasses import dataclass
from html import escape
from pathlib import Path
from typing import Iterable

import pandas as pd

from vortex.data.storage.parquet_duckdb import ParquetDuckDBBackend
from vortex.research.event_signals import build_forecast_surprise_signal
from vortex.research.goal_review import (
    ExperimentQuality,
    StrategyCandidateInput,
    review_strategy_candidate,
)
from vortex.research.market_state import build_market_state, market_gate_from_state
from vortex.runtime.workspace import Workspace
from vortex.strategy.backtest import BacktestResult, review_backtest_metrics
from vortex.strategy.earnings_forecast_drift import (
    DEFAULT_SEGMENTS,
    EarningsForecastDriftConfig,
    EarningsForecastDriftResult,
    SegmentSpec,
    build_financial_st_risk_events,
    build_forecast_surprise_signal,
    build_liquidity_mask,
    build_persistent_st_risk_mask,
    build_stock_st_mask,
    capacity_report,
    exposure_diagnostics,
    holdings_diagnostics,
    holdings_to_long_frame,
    open_to_close_returns,
    period_returns,
    run_earnings_forecast_drift,
    segment_report,
    write_earnings_forecast_report_html,
    write_earnings_forecast_report_json,
)
from vortex.strategy.opening_liquidity import (
    OpeningLiquidityConfig,
    analyze_opening_ask1_capacity,
    load_opening_snapshots,
)
from vortex.strategy.small_capital import SmallCapitalExecutionConfig, run_lot_constrained_backtest
from vortex.trade.qmt_bridge import QmtBridgeAdapter, QmtBridgeConfig, Transport, is_known_connection_status_bug


DEFAULT_COST_GRID: tuple[float, ...] = (0.0, 10.0, 20.0, 30.0, 50.0, 80.0, 100.0)
DEFAULT_REVIEW_LABEL = "业绩预告漂移策略v3精确可交易复核"
DEFAULT_SHADOW_LABEL = "业绩预告漂移策略shadow跟踪"
DEFAULT_LIVE_HANDOFF_LABEL = "业绩预告漂移策略实盘交接"
DEFAULT_OPENING_LIQUIDITY_LABEL = "业绩预告漂移策略开盘卖一容量复核"
DEFAULT_AUCTION_EXECUTION_LABEL = "业绩预告漂移策略开盘竞价可靠性回测"


@dataclass(frozen=True)
class EarningsForecastInputFrames:
    """业绩预告策略运行所需的本地数据矩阵。"""

    forecast: pd.DataFrame
    open_prices: pd.DataFrame
    close_prices: pd.DataFrame
    amount: pd.DataFrame
    index_close: pd.DataFrame
    raw_open_prices: pd.DataFrame
    stk_limit: pd.DataFrame | None
    suspend_events: pd.DataFrame | None
    stock_st: pd.DataFrame | None
    st_risk_events: pd.DataFrame | None


@dataclass(frozen=True)
class EarningsForecastReviewArtifacts:
    """精确复核输出文件与核心指标。"""

    json_path: Path
    html_path: Path
    holdings_path: Path
    summary: dict[str, object]


@dataclass(frozen=True)
class EarningsForecastShadowArtifacts:
    """影子跟踪输出文件与当日目标持仓摘要。"""

    json_path: Path
    html_path: Path
    target_path: Path
    summary: dict[str, object]


@dataclass(frozen=True)
class EarningsForecastOpeningLiquidityArtifacts:
    """开盘卖一容量复核输出文件与摘要。"""

    json_path: Path
    csv_path: Path
    md_path: Path
    summary: dict[str, object]


@dataclass(frozen=True)
class EarningsForecastAuctionExecutionArtifacts:
    """开盘竞价 all-or-nothing 回测输出文件与摘要。"""

    json_path: Path
    html_path: Path
    holdings_path: Path
    trades_path: Path
    order_intents_path: Path
    diagnostics_path: Path
    summary: dict[str, object]


@dataclass(frozen=True)
class EarningsForecastLiveHandoffArtifacts:
    """影子目标 + qmt-bridge 实盘交接输出。"""

    json_path: Path
    html_path: Path
    target_path: Path
    summary: dict[str, object]


def run_precise_earnings_forecast_review(
    root: str | Path,
    *,
    start: str,
    end: str,
    output_dir: str | Path | None = None,
    artifact_dir: str | Path | None = None,
    label: str = DEFAULT_REVIEW_LABEL,
    config: EarningsForecastDriftConfig | None = None,
    safe_config: EarningsForecastDriftConfig | None = None,
    cost_grid: Iterable[float] = DEFAULT_COST_GRID,
    portfolio_notional: float = 100_000_000.0,
    amount_unit_multiplier: float = 1000.0,
    segments: Iterable[SegmentSpec] = DEFAULT_SEGMENTS,
    require_precise_data: bool = True,
) -> EarningsForecastReviewArtifacts:
    """运行全流程精确可交易复核并写出 JSON、HTML 与持仓 CSV。

    `bars` 是 Tushare daily 未复权日线；同一交易日的 open→close 收益在
    复权和未复权口径下比例一致，因此可以直接用于日内收益。`stk_limit`
    则必须和未复权 open 比较，不能和复权价格混用。
    """

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir) if output_dir is not None else workspace.strategy_dir
    artifact_root = Path(artifact_dir) if artifact_dir is not None else workspace.strategy_dir / "artifacts"
    output_root.mkdir(parents=True, exist_ok=True)
    artifact_root.mkdir(parents=True, exist_ok=True)

    strategy_config = config or EarningsForecastDriftConfig()
    safe_variant = safe_config or EarningsForecastDriftConfig(
        position_mode="capped_with_cash",
        max_weight=0.03,
    )
    inputs = load_earnings_forecast_inputs(
        workspace,
        start=start,
        end=end,
        require_precise_data=require_precise_data,
    )
    quality = ExperimentQuality(
        pit_safe=True,
        adjusted_prices=True,
        cost_included=True,
        no_future_leakage=True,
        out_of_sample_checked=True,
    )
    result = run_earnings_forecast_drift(
        inputs.forecast,
        inputs.open_prices,
        inputs.close_prices,
        inputs.amount,
        inputs.index_close,
        strategy_config,
        quality=quality,
        segments=_overlapping_segments(segments, start=start, end=end),
        stk_limit=inputs.stk_limit,
        limit_open_prices=inputs.raw_open_prices,
        suspend_events=inputs.suspend_events,
        stock_st=inputs.stock_st,
        st_risk_events=inputs.st_risk_events,
    )
    returns = open_to_close_returns(inputs.open_prices, inputs.close_prices)
    holdings = holdings_to_long_frame(result, returns=returns, amount=inputs.amount)
    holdings_path = artifact_root / f"{label}每日持仓.csv"
    holdings.to_csv(holdings_path, index=False)

    cost_pressure = _run_cost_pressure(
        inputs,
        base_config=strategy_config,
        quality=quality,
        cost_grid=cost_grid,
    )
    safe_result = run_earnings_forecast_drift(
        inputs.forecast,
        inputs.open_prices,
        inputs.close_prices,
        inputs.amount,
        inputs.index_close,
        safe_variant,
        quality=quality,
        segments=(),
        stk_limit=inputs.stk_limit,
        limit_open_prices=inputs.raw_open_prices,
        suspend_events=inputs.suspend_events,
        stock_st=inputs.stock_st,
        st_risk_events=inputs.st_risk_events,
    )
    metadata = _build_metadata(
        start=start,
        end=end,
        label=label,
        inputs=inputs,
        result=result,
        safe_result=safe_result,
        safe_config=safe_variant,
        cost_pressure=cost_pressure,
        portfolio_notional=portfolio_notional,
        amount_unit_multiplier=amount_unit_multiplier,
        holdings_path=holdings_path,
        config=strategy_config,
    )
    diagnostics = holdings_diagnostics(holdings)
    json_path = output_root / f"{label}报告.json"
    html_path = output_root / f"{label}报告.html"
    write_earnings_forecast_report_json(
        result,
        json_path,
        config=strategy_config,
        metadata=metadata,
    )
    write_earnings_forecast_report_html(
        result,
        html_path,
        title=f"{label}报告",
        metadata=metadata,
        holdings_path=holdings_path,
        diagnostics=diagnostics,
    )
    return EarningsForecastReviewArtifacts(
        json_path=json_path,
        html_path=html_path,
        holdings_path=holdings_path,
        summary={
            "label": label,
            "json_path": str(json_path),
            "html_path": str(html_path),
            "holdings_path": str(holdings_path),
            "metrics": result.backtest.metrics.__dict__,
            "candidate_review": result.candidate_review.to_dict(),
            "safe_3pct_metrics": safe_result.backtest.metrics.__dict__,
            "safe_3pct_candidate_review": safe_result.candidate_review.to_dict(),
            "capacity_100m": metadata["capacity_100m"],
            "safe_3pct_capacity_100m": metadata["safe_3pct_result"]["capacity_100m"],
        },
    )


def run_earnings_forecast_shadow_plan(
    root: str | Path,
    *,
    start: str,
    as_of: str,
    output_dir: str | Path | None = None,
    artifact_dir: str | Path | None = None,
    label: str = DEFAULT_SHADOW_LABEL,
    config: EarningsForecastDriftConfig | None = None,
    require_precise_data: bool = True,
) -> EarningsForecastShadowArtifacts:
    """生成某个交易日的影子跟踪目标持仓与交易变化。

    影子跟踪不下单，只回答“如果今天运行策略，目标仓位和调仓变化是什么”。
    它复用正式策略 runner，所以可交易约束、ST 过滤、市场门控和成本口径一致。
    """

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir) if output_dir is not None else workspace.strategy_dir
    artifact_root = Path(artifact_dir) if artifact_dir is not None else workspace.strategy_dir / "artifacts"
    output_root.mkdir(parents=True, exist_ok=True)
    artifact_root.mkdir(parents=True, exist_ok=True)

    strategy_config = config or EarningsForecastDriftConfig()
    inputs = load_earnings_forecast_inputs(
        workspace,
        start=start,
        end=as_of,
        require_precise_data=require_precise_data,
    )
    quality = ExperimentQuality(
        pit_safe=True,
        adjusted_prices=True,
        cost_included=True,
        no_future_leakage=True,
        out_of_sample_checked=True,
    )
    result = run_earnings_forecast_drift(
        inputs.forecast,
        inputs.open_prices,
        inputs.close_prices,
        inputs.amount,
        inputs.index_close,
        strategy_config,
        quality=quality,
        segments=(),
        stk_limit=inputs.stk_limit,
        limit_open_prices=inputs.raw_open_prices,
        suspend_events=inputs.suspend_events,
        stock_st=inputs.stock_st,
        st_risk_events=inputs.st_risk_events,
    )
    target = _latest_target_frame(result.weights)
    target_path = artifact_root / f"{label}-{target['date'].iloc[0]}目标持仓.csv"
    target.to_csv(target_path, index=False)
    summary = {
        "label": label,
        "as_of": str(target["date"].iloc[0]),
        "requested_as_of": as_of,
        "json_path": str(output_root / f"{label}-{target['date'].iloc[0]}.json"),
        "html_path": str(output_root / f"{label}-{target['date'].iloc[0]}.html"),
        "target_path": str(target_path),
        "holding_count": int((target["weight"] > 0).sum()),
        "trade_count": int((target["trade_delta"].abs() > 1e-12).sum()),
        "exposure": float(target["weight"].sum()),
        "turnover": float(target["trade_delta"].abs().sum()),
        "metrics_to_date": result.backtest.metrics.__dict__,
        "candidate_review": result.candidate_review.to_dict(),
    }
    json_path = output_root / f"{label}-{target['date'].iloc[0]}.json"
    html_path = output_root / f"{label}-{target['date'].iloc[0]}.html"
    json_path.write_text(
        json.dumps(_jsonable(summary), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    html_path.write_text(_shadow_html(summary, target), encoding="utf-8")
    return EarningsForecastShadowArtifacts(
        json_path=json_path,
        html_path=html_path,
        target_path=target_path,
        summary=summary,
    )


def run_earnings_forecast_live_handoff(
    root: str | Path,
    *,
    start: str,
    as_of: str,
    qmt_bridge_url: str,
    qmt_bridge_token: str | None = None,
    qmt_account_id: str | None = None,
    output_dir: str | Path | None = None,
    artifact_dir: str | Path | None = None,
    label: str = DEFAULT_LIVE_HANDOFF_LABEL,
    config: EarningsForecastDriftConfig | None = None,
    require_precise_data: bool = True,
    bridge_transport: Transport | None = None,
) -> EarningsForecastLiveHandoffArtifacts:
    """生成“影子目标 + QMT 账户快照”的交接包。

    该入口不提交任何交易委托。它用于把策略目标持仓和执行通道状态放到同一份
    JSON/HTML 中，供模拟盘/实盘前人工确认与自动化编排消费。
    """

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir) if output_dir is not None else workspace.strategy_dir
    artifact_root = Path(artifact_dir) if artifact_dir is not None else workspace.strategy_dir / "artifacts"
    output_root.mkdir(parents=True, exist_ok=True)
    artifact_root.mkdir(parents=True, exist_ok=True)

    shadow = run_earnings_forecast_shadow_plan(
        workspace.root,
        start=start,
        as_of=as_of,
        output_dir=output_root,
        artifact_dir=artifact_root,
        label=f"{label}-shadow",
        config=config,
        require_precise_data=require_precise_data,
    )
    target = pd.read_csv(shadow.target_path)
    target_active = target.loc[target["weight"].abs() > 1e-12].copy()
    target_trade = target.loc[target["trade_delta"].abs() > 1e-12].copy()

    adapter = QmtBridgeAdapter(
        QmtBridgeConfig(
            base_url=qmt_bridge_url,
            token=qmt_bridge_token,
            account_id=qmt_account_id or None,
            allow_trading=False,
        ),
        transport=bridge_transport,
    )
    health = adapter.health()
    blocking_reasons: list[str] = []
    bridge_snapshot: dict[str, object] = {
        "health": {"ok": health.ok, "message": health.message},
    }
    if not health.ok:
        blocking_reasons.append(f"bridge health failed: {health.message}")
    else:
        try:
            connection = adapter.connection_status()
            cash = adapter.get_cash()
            positions = adapter.get_positions()
            orders = adapter.get_orders()
            fills = adapter.get_fills()
        except Exception as exc:  # noqa: BLE001 - handoff report should retain failure evidence.
            blocking_reasons.append(f"bridge read failed: {exc}")
        else:
            bridge_snapshot.update(
                {
                    "connection_status": connection,
                    "cash": {
                        "available_cash": cash.available_cash,
                        "frozen_cash": cash.frozen_cash,
                        "total_asset": cash.total_asset,
                        "market_value": cash.market_value,
                    },
                    "position_count": len(positions),
                    "order_count": len(orders),
                    "fill_count": len(fills),
                    "positions": [position.__dict__ for position in positions[:80]],
                }
            )
            if isinstance(connection, dict) and connection.get("connected") is False:
                if is_known_connection_status_bug(connection):
                    bridge_snapshot["connection_status_warning"] = (
                        "bridge connection_status endpoint uses incompatible xtdata API; "
                        "treated as non-blocking because cash/positions/orders/fills are readable"
                    )
                else:
                    blocking_reasons.append(f"bridge connected=false: {connection}")

    qmt_ready = len(blocking_reasons) == 0
    handoff_date = str(shadow.summary["as_of"])
    json_path = output_root / f"{label}-{handoff_date}.json"
    html_path = output_root / f"{label}-{handoff_date}.html"
    summary = {
        "label": label,
        "as_of": handoff_date,
        "requested_as_of": as_of,
        "qmt_bridge_url": qmt_bridge_url,
        "qmt_account_id": qmt_account_id or "",
        "qmt_ready": qmt_ready,
        "blocking_reasons": blocking_reasons,
        "target_path": str(shadow.target_path),
        "target_holding_count": int(len(target_active)),
        "target_trade_count": int(len(target_trade)),
        "target_exposure": float(target["weight"].sum()),
        "target_turnover": float(target["trade_delta"].abs().sum()),
        "shadow_summary": shadow.summary,
        "bridge_snapshot": bridge_snapshot,
        "json_path": str(json_path),
        "html_path": str(html_path),
    }
    json_path.write_text(
        json.dumps(_jsonable(summary), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    html_path.write_text(_live_handoff_html(summary, target), encoding="utf-8")
    return EarningsForecastLiveHandoffArtifacts(
        json_path=json_path,
        html_path=html_path,
        target_path=shadow.target_path,
        summary=summary,
    )


def run_opening_liquidity_review(
    root: str | Path,
    *,
    opening_snapshot_path: str | Path,
    start: str,
    end: str,
    output_dir: str | Path | None = None,
    label: str = DEFAULT_OPENING_LIQUIDITY_LABEL,
    top_n_values: Iterable[int] = (30, 40, 50),
    position_modes: Iterable[str] = ("full_equal_selected", "capped_with_cash"),
    portfolio_notional: float = 1_000_000.0,
    capped_max_weight: float = 0.05,
    volume_unit: str = "shares",
    require_precise_data: bool = True,
) -> EarningsForecastOpeningLiquidityArtifacts:
    """Run conservative ask1 opening capacity review for small-capital variants."""

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir) if output_dir is not None else workspace.strategy_dir
    output_root.mkdir(parents=True, exist_ok=True)
    inputs = load_earnings_forecast_inputs(
        workspace,
        start=start,
        end=end,
        require_precise_data=require_precise_data,
    )
    opening_snapshots = load_opening_snapshots(
        opening_snapshot_path,
        config=OpeningLiquidityConfig(volume_unit="lots" if volume_unit == "lots" else "shares"),
    )
    quality = ExperimentQuality(
        pit_safe=True,
        adjusted_prices=True,
        cost_included=True,
        no_future_leakage=True,
        out_of_sample_checked=True,
    )
    variant_rows: list[dict[str, object]] = []
    order_rows: list[pd.DataFrame] = []
    daily_rows: list[pd.DataFrame] = []

    for top_n in top_n_values:
        for position_mode in position_modes:
            strategy_config = EarningsForecastDriftConfig(
                top_n=int(top_n),
                position_mode=str(position_mode),
                max_weight=float(capped_max_weight) if position_mode == "capped_with_cash" else None,
            )
            result = run_earnings_forecast_drift(
                inputs.forecast,
                inputs.open_prices,
                inputs.close_prices,
                inputs.amount,
                inputs.index_close,
                strategy_config,
                quality=quality,
                segments=(),
                stk_limit=inputs.stk_limit,
                limit_open_prices=inputs.raw_open_prices,
                suspend_events=inputs.suspend_events,
                stock_st=inputs.stock_st,
                st_risk_events=inputs.st_risk_events,
            )
            small_result = run_lot_constrained_backtest(
                result.weights,
                inputs.open_prices,
                inputs.close_prices,
                SmallCapitalExecutionConfig(initial_cash=float(portfolio_notional)),
            )
            variant = _opening_liquidity_variant_name(top_n=int(top_n), position_mode=str(position_mode))
            report = analyze_opening_ask1_capacity(small_result.order_intents, opening_snapshots)
            variant_rows.append(
                {
                    "variant": variant,
                    "top_n": int(top_n),
                    "position_mode": str(position_mode),
                    "annual_return": small_result.metrics.annual_return,
                    "max_drawdown": small_result.metrics.max_drawdown,
                    "avg_holding_count": small_result.summary["avg_holding_count"],
                    "avg_cash_ratio": small_result.summary["avg_cash_ratio"],
                    **report.overall_summary,
                }
            )
            if not report.order_level.empty:
                order_rows.append(report.order_level.assign(variant=variant))
            if not report.daily_summary.empty:
                daily_rows.append(report.daily_summary.assign(variant=variant))

    summary_table = pd.DataFrame(variant_rows).sort_values(
        ["one_lot_feasible_rate", "covered_shares_ratio", "annual_return"],
        ascending=False,
    )
    orders_table = pd.concat(order_rows, ignore_index=True) if order_rows else pd.DataFrame()
    daily_table = pd.concat(daily_rows, ignore_index=True) if daily_rows else pd.DataFrame()
    csv_path = output_root / f"{label}.csv"
    json_path = output_root / f"{label}.json"
    md_path = output_root / f"{label}.md"
    summary_table.to_csv(csv_path, index=False)
    json_path.write_text(
        json.dumps(
            {
                "label": label,
                "opening_snapshot_path": str(Path(opening_snapshot_path).expanduser()),
                "summary": _jsonable(summary_table.to_dict(orient="records")),
                "daily_summary": _jsonable(daily_table.to_dict(orient="records")),
                "order_sample": _jsonable(orders_table.head(200).to_dict(orient="records")),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    md_path.write_text(_opening_liquidity_markdown(label, summary_table), encoding="utf-8")
    return EarningsForecastOpeningLiquidityArtifacts(
        json_path=json_path,
        csv_path=csv_path,
        md_path=md_path,
        summary={
            "label": label,
            "json_path": str(json_path),
            "csv_path": str(csv_path),
            "md_path": str(md_path),
            "best_variant": summary_table.iloc[0].to_dict() if not summary_table.empty else {},
            "variant_count": int(len(summary_table)),
        },
    )


def run_opening_auction_execution_review(
    root: str | Path,
    *,
    opening_snapshot_path: str | Path,
    start: str,
    end: str,
    output_dir: str | Path | None = None,
    artifact_dir: str | Path | None = None,
    label: str = DEFAULT_AUCTION_EXECUTION_LABEL,
    top_n: int = 30,
    position_mode: str = "capped_with_cash",
    portfolio_notional: float = 1_000_000.0,
    capped_max_weight: float = 0.05,
    volume_unit: str = "shares",
    require_precise_data: bool = True,
    segments: Iterable[SegmentSpec] = DEFAULT_SEGMENTS,
) -> EarningsForecastAuctionExecutionArtifacts:
    """Run an opening auction all-or-nothing reliability backtest and write a formal report."""

    workspace = Workspace(Path(root).expanduser())
    workspace.ensure_initialized()
    output_root = Path(output_dir) if output_dir is not None else workspace.strategy_dir
    artifact_root = Path(artifact_dir) if artifact_dir is not None else workspace.strategy_dir / "artifacts"
    output_root.mkdir(parents=True, exist_ok=True)
    artifact_root.mkdir(parents=True, exist_ok=True)

    strategy_config = EarningsForecastDriftConfig(
        top_n=int(top_n),
        position_mode=str(position_mode),
        max_weight=float(capped_max_weight) if position_mode == "capped_with_cash" else None,
    )
    inputs = load_earnings_forecast_inputs(
        workspace,
        start=start,
        end=end,
        require_precise_data=require_precise_data,
    )
    quality = ExperimentQuality(
        pit_safe=True,
        adjusted_prices=True,
        cost_included=True,
        no_future_leakage=True,
        out_of_sample_checked=True,
    )
    base_result = run_earnings_forecast_drift(
        inputs.forecast,
        inputs.open_prices,
        inputs.close_prices,
        inputs.amount,
        inputs.index_close,
        strategy_config,
        quality=quality,
        segments=_overlapping_segments(segments, start=start, end=end),
        stk_limit=inputs.stk_limit,
        limit_open_prices=inputs.raw_open_prices,
        suspend_events=inputs.suspend_events,
        stock_st=inputs.stock_st,
        st_risk_events=inputs.st_risk_events,
    )
    liquidity = build_liquidity_mask(
        inputs.amount,
        window=strategy_config.liquidity_window,
        min_avg_amount=strategy_config.min_avg_amount,
    )
    if strategy_config.exclude_st and inputs.stock_st is not None:
        liquidity = liquidity & ~build_stock_st_mask(
            inputs.stock_st,
            inputs.open_prices.index,
            inputs.open_prices.columns,
        )
    if strategy_config.exclude_st_risk and inputs.st_risk_events is not None:
        liquidity = liquidity & ~build_persistent_st_risk_mask(
            inputs.st_risk_events,
            inputs.open_prices.index,
            inputs.open_prices.columns,
        )
    signal = build_forecast_surprise_signal(
        inputs.forecast,
        target_index=inputs.open_prices.index,
        target_columns=inputs.open_prices.columns,
        liquidity_mask=liquidity,
        delay_days=strategy_config.delay_days,
        hold_days=strategy_config.hold_days,
    )
    market_gate = market_gate_from_state(build_market_state(inputs.index_close, strategy_config.market_state))
    opening_snapshots = load_opening_snapshots(
        opening_snapshot_path,
        config=OpeningLiquidityConfig(volume_unit="lots" if volume_unit == "lots" else "shares"),
    )
    opening_share_limits = _build_opening_share_limits(
        opening_snapshots,
        target_index=inputs.open_prices.index,
        target_columns=inputs.open_prices.columns,
    )
    unconstrained_result = run_lot_constrained_backtest(
        base_result.weights,
        inputs.open_prices,
        inputs.close_prices,
        SmallCapitalExecutionConfig(initial_cash=float(portfolio_notional)),
        market_gate=market_gate,
        signal=signal,
    )
    constrained_result = run_lot_constrained_backtest(
        base_result.weights,
        inputs.open_prices,
        inputs.close_prices,
        SmallCapitalExecutionConfig(
            initial_cash=float(portfolio_notional),
            allow_partial_buy_fills=False,
        ),
        market_gate=market_gate,
        signal=signal,
        buy_share_limits=opening_share_limits,
    )
    drift_like_result = _small_capital_result_to_drift_result(
        constrained_result,
        quality=quality,
        segments=_overlapping_segments(segments, start=start, end=end),
    )
    returns = open_to_close_returns(inputs.open_prices, inputs.close_prices)
    holdings = holdings_to_long_frame(drift_like_result, returns=returns, amount=inputs.amount)
    holdings_path = artifact_root / f"{label}每日持仓.csv"
    trades_path = artifact_root / f"{label}成交明细.csv"
    order_intents_path = artifact_root / f"{label}买单意图.csv"
    diagnostics_path = artifact_root / f"{label}执行诊断.csv"
    holdings.to_csv(holdings_path, index=False)
    constrained_result.trades.to_csv(trades_path, index=False)
    constrained_result.order_intents.to_csv(order_intents_path, index=False)
    constrained_result.diagnostics.to_csv(diagnostics_path, index=False)

    capacity_report_result = analyze_opening_ask1_capacity(
        constrained_result.order_intents,
        opening_snapshots,
    )
    metadata = {
        "label": label,
        "data_start": start,
        "data_end": end,
        "event_table": "forecast",
        "execution": "opening_auction_all_or_nothing",
        "opening_snapshot_path": str(Path(opening_snapshot_path).expanduser()),
        "opening_snapshot_volume_unit": volume_unit,
        "tradability_review": {
            "data_missing": [],
            "precision": "opening_auction_all_or_nothing",
            "special_rule": "买单若无法被开盘竞价成交量完整覆盖，则整笔视为未成交",
        },
        "auction_execution_summary": _auction_execution_summary(constrained_result.order_intents),
        "opening_capacity_summary": capacity_report_result.overall_summary,
        "unconstrained_small_cap_metrics": unconstrained_result.metrics.__dict__,
        "unconstrained_small_cap_summary": unconstrained_result.summary,
        "base_target_strategy_metrics": base_result.backtest.metrics.__dict__,
        "base_target_candidate_review": base_result.candidate_review.to_dict(),
        "holdings_path": str(holdings_path),
        "trades_path": str(trades_path),
        "order_intents_path": str(order_intents_path),
        "diagnostics_path": str(diagnostics_path),
    }
    diagnostics = holdings_diagnostics(holdings)
    json_path = output_root / f"{label}报告.json"
    html_path = output_root / f"{label}报告.html"
    write_earnings_forecast_report_json(
        drift_like_result,
        json_path,
        config=strategy_config,
        metadata=metadata,
    )
    write_earnings_forecast_report_html(
        drift_like_result,
        html_path,
        title=f"{label}报告",
        metadata=metadata,
        holdings_path=holdings_path,
        diagnostics=diagnostics,
    )
    return EarningsForecastAuctionExecutionArtifacts(
        json_path=json_path,
        html_path=html_path,
        holdings_path=holdings_path,
        trades_path=trades_path,
        order_intents_path=order_intents_path,
        diagnostics_path=diagnostics_path,
        summary={
            "label": label,
            "json_path": str(json_path),
            "html_path": str(html_path),
            "holdings_path": str(holdings_path),
            "trades_path": str(trades_path),
            "order_intents_path": str(order_intents_path),
            "diagnostics_path": str(diagnostics_path),
            "metrics": drift_like_result.backtest.metrics.__dict__,
            "candidate_review": drift_like_result.candidate_review.to_dict(),
            "auction_execution_summary": metadata["auction_execution_summary"],
            "opening_capacity_summary": metadata["opening_capacity_summary"],
        },
    )


def _build_opening_share_limits(
    opening_snapshots: pd.DataFrame,
    *,
    target_index: pd.Index,
    target_columns: pd.Index,
) -> pd.DataFrame:
    limits = opening_snapshots.pivot_table(
        index="date",
        columns="symbol",
        values="ask1_shares",
        aggfunc="last",
    )
    return limits.reindex(index=target_index, columns=target_columns).fillna(0.0)



def _small_capital_result_to_drift_result(
    result: SmallCapitalBacktestResult,
    *,
    quality: ExperimentQuality,
    segments: Iterable[SegmentSpec],
) -> EarningsForecastDriftResult:
    goal_review = review_backtest_metrics(result.metrics, quality=quality)
    annual = period_returns(result.returns, "Y")
    monthly = period_returns(result.returns, "M")
    positive_year_rate = float((annual > 0).mean()) if not annual.empty else None
    candidate_review = review_strategy_candidate(
        StrategyCandidateInput(
            annual_return=result.metrics.annual_return,
            max_drawdown=result.metrics.max_drawdown,
            sharpe=result.metrics.sharpe,
            calmar=result.metrics.calmar,
            positive_year_rate=positive_year_rate,
            quality=quality,
        )
    )
    backtest = BacktestResult(
        metrics=result.metrics,
        goal_review=goal_review,
        equity_curve=result.equity_curve,
        weights=result.weights,
        returns=result.returns,
    )
    segment_reports = tuple(segment_report(result.returns, spec) for spec in segments)
    return EarningsForecastDriftResult(
        backtest=backtest,
        candidate_review=candidate_review,
        annual_returns=annual,
        monthly_returns=monthly,
        segments=segment_reports,
    )



def _auction_execution_summary(order_intents: pd.DataFrame) -> dict[str, object]:
    buys = order_intents.loc[(order_intents["side"] == "buy") & (order_intents["requested_shares"] > 0)].copy()
    if buys.empty:
        return {
            "buy_order_count": 0,
            "filled_order_count": 0,
            "skipped_order_count": 0,
            "filled_order_rate": 0.0,
            "requested_shares_total": 0,
            "executed_shares_total": 0,
            "executed_shares_ratio": 0.0,
            "reason_counts": {},
        }
    filled_mask = buys["status"] == "filled"
    skipped_mask = buys["status"] == "skipped"
    requested_total = int(buys["requested_shares"].sum())
    executed_total = int(buys["executed_shares"].sum())
    return {
        "buy_order_count": int(len(buys)),
        "filled_order_count": int(filled_mask.sum()),
        "skipped_order_count": int(skipped_mask.sum()),
        "filled_order_rate": float(filled_mask.mean()),
        "requested_shares_total": requested_total,
        "executed_shares_total": executed_total,
        "executed_shares_ratio": float(executed_total / requested_total) if requested_total > 0 else 0.0,
        "reason_counts": buys["reason"].fillna("").value_counts().to_dict(),
    }


def load_earnings_forecast_inputs(
    workspace: Workspace,
    *,
    start: str,
    end: str,
    require_precise_data: bool = True,
) -> EarningsForecastInputFrames:
    """从 workspace 读取并整理业绩预告策略输入。

    回测起点前的公告不会被纳入。否则 `_align_financial_event_dates` 会把
    起点前事件映射到首个交易日，形成启动日事件堆积，影响复核可信度。
    """

    _validate_date_range(start, end)
    storage = ParquetDuckDBBackend(workspace.data_dir)
    bars = _read_dated_dataset(
        storage,
        "bars",
        start=start,
        end=end,
        columns=["date", "symbol", "open", "close", "amount"],
    )
    forecast = storage.read(
        "forecast",
        columns=["symbol", "ann_date", "type", "p_change_min", "p_change_max"],
    )
    forecast = _filter_announcements(forecast, start=start, end=end)
    index_daily = _read_dated_dataset(
        storage,
        "index_daily",
        start=start,
        end=end,
        columns=["date", "symbol", "close"],
    )
    stock_st = _optional_dated_dataset(
        storage,
        "stock_st",
        start=start,
        end=end,
        columns=["date", "symbol", "type", "type_name"],
    )
    stk_limit = _optional_dated_dataset(
        storage,
        "stk_limit",
        start=start,
        end=end,
        columns=["date", "symbol", "up_limit", "down_limit"],
    )
    suspend_events = _optional_dated_dataset(
        storage,
        "suspend_d",
        start=start,
        end=end,
        columns=["date", "symbol", "suspend_type", "suspend_timing"],
    )
    if require_precise_data and (stk_limit is None or suspend_events is None):
        missing = []
        if stk_limit is None:
            missing.append("stk_limit")
        if suspend_events is None:
            missing.append("suspend_d")
        raise ValueError(f"精确可交易复核缺少数据集: {', '.join(missing)}")

    open_prices = _pivot_market_frame(bars, "open")
    close_prices = _pivot_market_frame(bars, "close")
    amount = _pivot_market_frame(bars, "amount")
    index_close = _pivot_market_frame(index_daily, "close")
    fina_indicator = _optional_effective_dataset(
        storage,
        "fina_indicator",
        start=start,
        end=end,
        columns=["symbol", "ann_date", "effective_from", "bps", "roe", "debt_to_assets", "netprofit_yoy"],
        include_pre_start=True,
    )
    balancesheet = _optional_effective_dataset(
        storage,
        "balancesheet",
        start=start,
        end=end,
        columns=["symbol", "ann_date", "effective_from", "total_hldr_eqy_inc_min_int", "total_hldr_eqy_exc_min_int"],
        include_pre_start=True,
    )
    cashflow = _optional_effective_dataset(
        storage,
        "cashflow",
        start=start,
        end=end,
        columns=["symbol", "ann_date", "effective_from", "net_profit", "n_cashflow_act", "free_cashflow"],
        include_pre_start=True,
    )
    st_risk_events = build_financial_st_risk_events(
        fina_indicator=fina_indicator,
        balancesheet=balancesheet,
        cashflow=cashflow,
        target_index=open_prices.index,
    )
    return EarningsForecastInputFrames(
        forecast=forecast,
        open_prices=open_prices,
        close_prices=close_prices,
        amount=amount,
        index_close=index_close,
        raw_open_prices=open_prices,
        stk_limit=stk_limit,
        suspend_events=suspend_events,
        stock_st=stock_st,
        st_risk_events=None if st_risk_events.empty else st_risk_events,
    )


def _opening_liquidity_variant_name(*, top_n: int, position_mode: str) -> str:
    mode = "eq" if position_mode == "full_equal_selected" else "cap"
    return f"top{top_n}_{mode}"


def _opening_liquidity_markdown(label: str, summary_table: pd.DataFrame) -> str:
    lines = [
        f"# {label}",
        "",
        "## Variant summary",
        "",
        "| Variant | TopN | Position mode | One-lot feasible | Target feasible | Covered shares ratio | Annual return | Max drawdown |",
        "|---|---:|---|---:|---:|---:|---:|---:|",
    ]
    for row in summary_table.to_dict(orient="records"):
        lines.append(
            f"| {row['variant']} | {row['top_n']} | {row['position_mode']} | "
            f"{float(row['one_lot_feasible_rate']) * 100:.2f}% | {float(row['target_feasible_rate']) * 100:.2f}% | "
            f"{float(row['covered_shares_ratio']) * 100:.2f}% | {float(row['annual_return']) * 100:.2f}% | "
            f"{float(row['max_drawdown']) * 100:.2f}% |"
        )
    return "\n".join(lines) + "\n"


def _run_cost_pressure(
    inputs: EarningsForecastInputFrames,
    *,
    base_config: EarningsForecastDriftConfig,
    quality: ExperimentQuality,
    cost_grid: Iterable[float],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for cost in cost_grid:
        config = dataclasses.replace(base_config, transaction_cost_bps=float(cost))
        result = run_earnings_forecast_drift(
            inputs.forecast,
            inputs.open_prices,
            inputs.close_prices,
            inputs.amount,
            inputs.index_close,
            config,
            quality=quality,
            segments=(),
            stk_limit=inputs.stk_limit,
            limit_open_prices=inputs.raw_open_prices,
            suspend_events=inputs.suspend_events,
            stock_st=inputs.stock_st,
            st_risk_events=inputs.st_risk_events,
        )
        metrics = result.backtest.metrics
        rows.append(
            {
                "cost_bps": float(cost),
                "annual_return": metrics.annual_return,
                "max_drawdown": metrics.max_drawdown,
                "sharpe": round(metrics.sharpe, 4),
                "calmar": round(metrics.calmar, 4),
                "grade": result.candidate_review.grade,
            }
        )
    return rows


def _build_metadata(
    *,
    start: str,
    end: str,
    label: str,
    inputs: EarningsForecastInputFrames,
    result,
    safe_result,
    safe_config: EarningsForecastDriftConfig,
    cost_pressure: list[dict[str, object]],
    portfolio_notional: float,
    amount_unit_multiplier: float,
    holdings_path: Path,
    config: EarningsForecastDriftConfig,
) -> dict[str, object]:
    liquidity = build_liquidity_mask(
        inputs.amount,
        window=config.liquidity_window,
        min_avg_amount=config.min_avg_amount,
    )
    if config.exclude_st and inputs.stock_st is not None:
        liquidity = liquidity & ~build_stock_st_mask(
            inputs.stock_st,
            inputs.open_prices.index,
            inputs.open_prices.columns,
        )
    if config.exclude_st_risk and inputs.st_risk_events is not None:
        liquidity = liquidity & ~build_persistent_st_risk_mask(
            inputs.st_risk_events,
            inputs.open_prices.index,
            inputs.open_prices.columns,
        )
    signal = build_forecast_surprise_signal(
        inputs.forecast,
        target_index=inputs.open_prices.index,
        target_columns=inputs.open_prices.columns,
        liquidity_mask=liquidity,
        delay_days=config.delay_days,
        hold_days=config.hold_days,
    )
    market_gate = market_gate_from_state(build_market_state(inputs.index_close, config.market_state))
    data_missing = []
    if inputs.stk_limit is None:
        data_missing.append("stk_limit")
    if inputs.suspend_events is None:
        data_missing.append("suspend_d")
    return {
        "label": label,
        "data_start": start,
        "data_end": end,
        "event_table": "forecast",
        "execution": "open_to_close",
        "amount_unit": "thousand_yuan",
        "price_note": "bars 为 Tushare daily 未复权价格；open→close 日内收益同日复权因子抵消，stk_limit 精确比较使用未复权 open。",
        "tradability_review": {
            "data_missing": data_missing,
            "precision": "full_history_stk_limit_suspend_d" if not data_missing else "partial_precise_data",
            "stk_limit_rows": int(len(inputs.stk_limit)) if inputs.stk_limit is not None else 0,
            "suspend_d_rows": int(len(inputs.suspend_events)) if inputs.suspend_events is not None else 0,
            "st_risk_event_rows": int(len(inputs.st_risk_events)) if inputs.st_risk_events is not None else 0,
            "strict_result": {
                "annual_return": result.backtest.metrics.annual_return,
                "max_drawdown": result.backtest.metrics.max_drawdown,
                "sharpe": result.backtest.metrics.sharpe,
                "calmar": result.backtest.metrics.calmar,
                "grade": result.candidate_review.grade,
            },
        },
        "capacity_100m": capacity_report(
            result.weights,
            inputs.amount,
            portfolio_notional=portfolio_notional,
            amount_unit_multiplier=amount_unit_multiplier,
        ),
        "safe_3pct_result": {
            "config": {
                "position_mode": safe_config.position_mode,
                "max_weight": safe_config.max_weight,
            },
            "metrics": safe_result.backtest.metrics.__dict__,
            "candidate_review": safe_result.candidate_review.to_dict(),
            "capacity_100m": capacity_report(
                safe_result.weights,
                inputs.amount,
                portfolio_notional=portfolio_notional,
                amount_unit_multiplier=amount_unit_multiplier,
            ),
        },
        "cost_pressure": cost_pressure,
        "exposure_diagnostics": exposure_diagnostics(
            result,
            signal=signal,
            market_gate=market_gate,
        ),
        "holdings_path": str(holdings_path),
    }


def _read_dated_dataset(
    storage: ParquetDuckDBBackend,
    dataset: str,
    *,
    start: str,
    end: str,
    columns: list[str],
) -> pd.DataFrame:
    frame = storage.read(dataset, filters={"date": (">=", int(start))}, columns=columns)
    frame = _filter_date_frame(frame, start=start, end=end)
    if frame.empty:
        raise ValueError(f"数据集为空或缺失: {dataset}")
    return frame


def _optional_dated_dataset(
    storage: ParquetDuckDBBackend,
    dataset: str,
    *,
    start: str,
    end: str,
    columns: list[str],
) -> pd.DataFrame | None:
    frame = storage.read(dataset, filters={"date": (">=", int(start))}, columns=columns)
    frame = _filter_date_frame(frame, start=start, end=end)
    return None if frame.empty else frame


def _optional_effective_dataset(
    storage: ParquetDuckDBBackend,
    dataset: str,
    *,
    start: str,
    end: str,
    columns: list[str],
    include_pre_start: bool = False,
) -> pd.DataFrame | None:
    frame = storage.read(dataset, columns=columns)
    frame = _filter_effective_frame(frame, start=start, end=end, include_pre_start=include_pre_start)
    return None if frame.empty else frame


def _filter_date_frame(frame: pd.DataFrame, *, start: str, end: str) -> pd.DataFrame:
    if frame.empty or "date" not in frame.columns:
        return frame
    dates = pd.to_numeric(frame["date"], errors="coerce")
    mask = dates.between(int(start), int(end), inclusive="both")
    return frame.loc[mask].copy()


def _filter_effective_frame(
    frame: pd.DataFrame,
    *,
    start: str,
    end: str,
    include_pre_start: bool = False,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    if "effective_from" in frame.columns:
        digits = frame["effective_from"].astype(str).str.replace(r"\D", "", regex=True).str.slice(0, 8)
        dates = pd.to_numeric(digits, errors="coerce")
    elif "ann_date" in frame.columns:
        dates = pd.to_numeric(frame["ann_date"], errors="coerce")
    else:
        return frame
    if include_pre_start:
        mask = dates.le(int(end))
    else:
        mask = dates.between(int(start), int(end), inclusive="both")
    return frame.loc[mask].copy()


def _filter_announcements(frame: pd.DataFrame, *, start: str, end: str) -> pd.DataFrame:
    if frame.empty:
        raise ValueError("数据集为空或缺失: forecast")
    dates = pd.to_numeric(frame["ann_date"], errors="coerce")
    clean = frame.loc[dates.between(int(start), int(end), inclusive="both")].copy()
    if clean.empty:
        raise ValueError("指定区间内没有 forecast 公告")
    clean["ann_date"] = clean["ann_date"].astype(str)
    return clean


def _pivot_market_frame(frame: pd.DataFrame, value_column: str) -> pd.DataFrame:
    required = {"date", "symbol", value_column}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"数据缺少字段: {sorted(missing)}")
    clean = frame[["date", "symbol", value_column]].dropna(subset=["date", "symbol"]).copy()
    clean["date"] = clean["date"].astype(str)
    clean["symbol"] = clean["symbol"].astype(str)
    result = clean.pivot_table(
        index="date",
        columns="symbol",
        values=value_column,
        aggfunc="last",
    ).sort_index()
    result.columns.name = None
    if result.empty:
        raise ValueError(f"无法构建矩阵: {value_column}")
    return result


def _validate_date_range(start: str, end: str) -> None:
    if not (start.isdigit() and end.isdigit() and len(start) == 8 and len(end) == 8):
        raise ValueError("start/end 必须是 YYYYMMDD")
    if int(start) > int(end):
        raise ValueError("start 不能晚于 end")


def _overlapping_segments(
    segments: Iterable[SegmentSpec],
    *,
    start: str,
    end: str,
) -> tuple[SegmentSpec, ...]:
    start_value = int(start)
    end_value = int(end)
    return tuple(
        segment
        for segment in segments
        if int(segment.start) <= end_value and int(segment.end) >= start_value
    )


def _latest_target_frame(weights: pd.DataFrame) -> pd.DataFrame:
    if weights.empty:
        raise ValueError("策略没有生成任何目标持仓")
    latest_date = weights.index[-1]
    current = weights.iloc[-1].fillna(0.0)
    previous = weights.iloc[-2].fillna(0.0) if len(weights) > 1 else current * 0.0
    frame = pd.DataFrame(
        {
            "date": latest_date,
            "symbol": current.index.astype(str),
            "weight": current.to_numpy(dtype=float),
            "prev_weight": previous.reindex(current.index).to_numpy(dtype=float),
        }
    )
    frame["trade_delta"] = frame["weight"] - frame["prev_weight"]
    frame["action"] = "hold"
    frame.loc[frame["trade_delta"] > 1e-12, "action"] = "buy_or_increase"
    frame.loc[frame["trade_delta"] < -1e-12, "action"] = "sell_or_reduce"
    active = (frame["weight"].abs() > 1e-12) | (frame["trade_delta"].abs() > 1e-12)
    if not active.any():
        return pd.DataFrame(
            [
                {
                    "date": latest_date,
                    "symbol": "CASH",
                    "weight": 0.0,
                    "prev_weight": 0.0,
                    "trade_delta": 0.0,
                    "action": "cash",
                }
            ]
        )
    return frame.loc[active].sort_values(["weight", "trade_delta"], ascending=[False, False]).reset_index(drop=True)


def _shadow_html(summary: dict[str, object], target: pd.DataFrame) -> str:
    rows = []
    for _, row in target.head(80).iterrows():
        rows.append(
            "<tr>"
            f"<td>{escape(str(row['symbol']))}</td>"
            f"<td>{float(row['weight']) * 100:.2f}%</td>"
            f"<td>{float(row['prev_weight']) * 100:.2f}%</td>"
            f"<td>{float(row['trade_delta']) * 100:.2f}%</td>"
            f"<td>{escape(str(row['action']))}</td>"
            "</tr>"
        )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{escape(str(summary["label"]))}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #172033; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; }}
    .card {{ border: 1px solid #dfe4ee; border-radius: 14px; padding: 16px; background: #fff; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 16px; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #dfe4ee; padding: 8px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
  </style>
</head>
<body>
  <h1>{escape(str(summary["label"]))}</h1>
  <p>影子跟踪只生成目标持仓和调仓变化，不代表已经下单或成交。</p>
  <section class="grid">
    <div class="card"><strong>目标日期</strong><br>{escape(str(summary["as_of"]))}</div>
    <div class="card"><strong>目标仓位</strong><br>{float(summary["exposure"]) * 100:.2f}%</div>
    <div class="card"><strong>持仓数</strong><br>{summary["holding_count"]}</div>
    <div class="card"><strong>调仓数</strong><br>{summary["trade_count"]}</div>
    <div class="card"><strong>换手</strong><br>{float(summary["turnover"]) * 100:.2f}%</div>
  </section>
  <h2>目标持仓与交易变化（前 80 行）</h2>
  <table>
    <thead><tr><th>股票</th><th>目标权重</th><th>前一日权重</th><th>交易变化</th><th>动作</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</body>
</html>
"""


def _live_handoff_html(summary: dict[str, object], target: pd.DataFrame) -> str:
    rows = []
    for _, row in target.head(80).iterrows():
        rows.append(
            "<tr>"
            f"<td>{escape(str(row['symbol']))}</td>"
            f"<td>{float(row['weight']) * 100:.2f}%</td>"
            f"<td>{float(row['trade_delta']) * 100:.2f}%</td>"
            f"<td>{escape(str(row['action']))}</td>"
            "</tr>"
        )
    health = summary.get("bridge_snapshot", {}).get("health", {}) if isinstance(summary.get("bridge_snapshot"), dict) else {}
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{escape(str(summary["label"]))}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #172033; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; }}
    .card {{ border: 1px solid #dfe4ee; border-radius: 14px; padding: 16px; background: #fff; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 16px; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #dfe4ee; padding: 8px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
  </style>
</head>
<body>
  <h1>{escape(str(summary["label"]))}</h1>
  <p>该报告用于模拟盘/实盘交接，不会自动提交委托。</p>
  <section class="grid">
    <div class="card"><strong>目标日期</strong><br>{escape(str(summary["as_of"]))}</div>
    <div class="card"><strong>QMT 就绪</strong><br>{"yes" if bool(summary["qmt_ready"]) else "no"}</div>
    <div class="card"><strong>Bridge Health</strong><br>{escape(str(health.get("message", "-")))}</div>
    <div class="card"><strong>目标仓位</strong><br>{float(summary["target_exposure"]) * 100:.2f}%</div>
    <div class="card"><strong>持仓数</strong><br>{summary["target_holding_count"]}</div>
    <div class="card"><strong>调仓数</strong><br>{summary["target_trade_count"]}</div>
  </section>
  <h2>阻断原因</h2>
  <ul>
    {''.join(f"<li>{escape(str(item))}</li>" for item in (summary.get("blocking_reasons") or ["无"]))}
  </ul>
  <h2>目标持仓与交易变化（前 80 行）</h2>
  <table>
    <thead><tr><th>股票</th><th>目标权重</th><th>交易变化</th><th>动作</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
</body>
</html>
"""


def _jsonable(value: object) -> object:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except ValueError:
            pass
    if pd.isna(value) if not isinstance(value, (dict, list, tuple)) else False:
        return None
    return value
