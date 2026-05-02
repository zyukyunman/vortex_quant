"""业绩预告公告后漂移策略。

该模块把当前最有效的 A 股日频事件 alpha 固化为可复用策略组件：
用业绩预告 surprise 构建事件信号，用公告后可交易日的 open→close 收益验证，
并输出满仓 alpha 候选评级、年度/月度收益和分段稳健性指标。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from html import escape
from pathlib import Path
from typing import Iterable, Literal

import pandas as pd

from vortex.research.event_signals import build_forecast_surprise_signal
from vortex.research.goal_review import (
    ExperimentQuality,
    StrategyCandidateCriteria,
    StrategyCandidateInput,
    StrategyCandidateReviewResult,
    review_strategy_candidate,
)
from vortex.research.market_state import MarketStateConfig, build_market_state, market_gate_from_state
from vortex.strategy.backtest import BacktestMetrics, BacktestResult, _compute_metrics
from vortex.strategy.event_backtest import EventBacktestConfig, run_event_signal_backtest


@dataclass(frozen=True)
class EarningsForecastDriftConfig:
    """业绩预告漂移策略配置。"""

    delay_days: int = 1
    hold_days: int = 40
    top_n: int = 80
    target_exposure: float = 1.0
    max_weight: float | None = None
    position_mode: Literal["full_equal_selected", "capped_with_cash"] = "full_equal_selected"
    transaction_cost_bps: float = 20.0
    liquidity_window: int = 20
    min_avg_amount: float = 30000.0
    exclude_st: bool = True
    exclude_st_risk: bool = True
    block_limit_up_buys: bool = True
    block_limit_down_sells: bool = True
    block_suspended_trades: bool = True
    market_state: MarketStateConfig = field(
        default_factory=lambda: MarketStateConfig(momentum_window=5, support_window=20)
    )


@dataclass(frozen=True)
class SegmentSpec:
    """报告分段配置。"""

    name: str
    start: str
    end: str


@dataclass(frozen=True)
class SegmentReport:
    """单个分段收益风险报告。"""

    name: str
    start: str
    end: str
    days: int
    metrics: BacktestMetrics

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "start": self.start,
            "end": self.end,
            "days": self.days,
            "metrics": self.metrics.__dict__,
        }


@dataclass(frozen=True)
class EarningsForecastDriftResult:
    """业绩预告漂移策略完整结果。"""

    backtest: BacktestResult
    candidate_review: StrategyCandidateReviewResult
    annual_returns: pd.Series = field(repr=False)
    monthly_returns: pd.Series = field(repr=False)
    segments: tuple[SegmentReport, ...] = field(default_factory=tuple)

    @property
    def weights(self) -> pd.DataFrame:
        """每日目标权重，用于容量、换手和可交易性复核。"""

        return self.backtest.weights

    def to_dict(self) -> dict[str, object]:
        exposure = self.weights.sum(axis=1) if not self.weights.empty else pd.Series(dtype=float)
        holding_count = self.weights.gt(0).sum(axis=1) if not self.weights.empty else pd.Series(dtype=float)
        return {
            "metrics": self.backtest.metrics.__dict__,
            "goal_review": self.backtest.goal_review.to_dict(),
            "candidate_review": self.candidate_review.to_dict(),
            "equity_curve": self.backtest.equity_curve.to_dict(),
            "daily_returns": self.backtest.returns.to_dict(),
            "exposure": exposure.to_dict(),
            "holding_count": holding_count.to_dict(),
            "annual_returns": self.annual_returns.to_dict(),
            "monthly_returns": self.monthly_returns.to_dict(),
            "segments": [segment.to_dict() for segment in self.segments],
        }


DEFAULT_SEGMENTS: tuple[SegmentSpec, ...] = (
    SegmentSpec("train_like_2017_2019", "20170101", "20191231"),
    SegmentSpec("stress_2020_2022", "20200101", "20221231"),
    SegmentSpec("recent_2023_2026", "20230101", "20260424"),
)


def run_earnings_forecast_drift(
    forecast_events: pd.DataFrame,
    open_prices: pd.DataFrame,
    close_prices: pd.DataFrame,
    amount: pd.DataFrame,
    index_close: pd.DataFrame,
    config: EarningsForecastDriftConfig | None = None,
    *,
    quality: ExperimentQuality | None = None,
    candidate_criteria: StrategyCandidateCriteria | None = None,
    segments: Iterable[SegmentSpec] = DEFAULT_SEGMENTS,
    limit_events: pd.DataFrame | None = None,
    stk_limit: pd.DataFrame | None = None,
    limit_open_prices: pd.DataFrame | None = None,
    suspend_events: pd.DataFrame | None = None,
    stock_st: pd.DataFrame | None = None,
    st_risk_events: pd.DataFrame | None = None,
) -> EarningsForecastDriftResult:
    """运行业绩预告漂移策略并生成候选评级。"""

    config = config or EarningsForecastDriftConfig()
    _validate_inputs(forecast_events, open_prices, close_prices, amount, index_close, config)
    open_prices, close_prices, amount, index_close = _align_market_frames(
        open_prices,
        close_prices,
        amount,
        index_close,
    )
    liquidity = build_liquidity_mask(
        amount,
        window=config.liquidity_window,
        min_avg_amount=config.min_avg_amount,
    )
    st_mask: pd.DataFrame | None = None
    if config.exclude_st and stock_st is not None:
        st_mask = build_stock_st_mask(stock_st, open_prices.index, open_prices.columns)
        liquidity = liquidity & ~st_mask
    if config.exclude_st_risk and st_risk_events is not None:
        risk_mask = build_persistent_st_risk_mask(st_risk_events, open_prices.index, open_prices.columns)
        st_mask = _combine_masks(st_mask, risk_mask)
        liquidity = liquidity & ~risk_mask
    limit_up_mask: pd.DataFrame | None = None
    limit_down_mask: pd.DataFrame | None = None
    if limit_events is not None and (config.block_limit_up_buys or config.block_limit_down_sells):
        limit_up_mask, limit_down_mask = build_limit_event_masks(
            limit_events,
            open_prices.index,
            open_prices.columns,
        )
    if stk_limit is not None and (config.block_limit_up_buys or config.block_limit_down_sells):
        if limit_open_prices is None:
            raise ValueError("使用 stk_limit 精确涨跌停约束时必须传入未复权 limit_open_prices")
        price_limit_up_mask, price_limit_down_mask = build_open_limit_price_masks(
            stk_limit,
            limit_open_prices.reindex(index=open_prices.index, columns=open_prices.columns),
        )
        limit_up_mask = _combine_masks(limit_up_mask, price_limit_up_mask)
        limit_down_mask = _combine_masks(limit_down_mask, price_limit_down_mask)
    if suspend_events is not None and config.block_suspended_trades:
        suspend_mask = build_suspend_trade_mask(suspend_events, open_prices.index, open_prices.columns)
        limit_up_mask = _combine_masks(limit_up_mask, suspend_mask)
        limit_down_mask = _combine_masks(limit_down_mask, suspend_mask)
    signal = build_forecast_surprise_signal(
        forecast_events,
        target_index=open_prices.index,
        target_columns=open_prices.columns,
        liquidity_mask=liquidity,
        delay_days=config.delay_days,
        hold_days=config.hold_days,
    )
    if st_mask is not None:
        signal = signal.where(~st_mask.reindex_like(signal).fillna(False))
    returns = open_to_close_returns(open_prices, close_prices)
    market_gate = market_gate_from_state(build_market_state(index_close, config.market_state))
    event_config = EventBacktestConfig(
        top_n=config.top_n,
        max_weight=config.max_weight or min(0.2, 1.0 / config.top_n),
        target_exposure=config.target_exposure,
        transaction_cost_bps=config.transaction_cost_bps,
        position_mode=config.position_mode,
    )
    backtest = run_event_signal_backtest(
        signal,
        returns,
        event_config,
        market_gate=market_gate,
        blocked_buy_mask=limit_up_mask if config.block_limit_up_buys else None,
        blocked_sell_mask=limit_down_mask if config.block_limit_down_sells else None,
        quality=quality,
        goal_criteria=None,
    )
    annual = period_returns(backtest.returns, "Y")
    monthly = period_returns(backtest.returns, "M")
    positive_year_rate = float((annual > 0).mean()) if not annual.empty else None
    candidate_review = review_strategy_candidate(
        StrategyCandidateInput(
            annual_return=backtest.metrics.annual_return,
            max_drawdown=backtest.metrics.max_drawdown,
            sharpe=backtest.metrics.sharpe,
            calmar=backtest.metrics.calmar,
            positive_year_rate=positive_year_rate,
            quality=quality,
        ),
        criteria=candidate_criteria,
    )
    segment_reports = tuple(segment_report(backtest.returns, spec) for spec in segments)
    return EarningsForecastDriftResult(
        backtest=backtest,
        candidate_review=candidate_review,
        annual_returns=annual,
        monthly_returns=monthly,
        segments=segment_reports,
    )


def run_earnings_forecast_grid(
    forecast_events: pd.DataFrame,
    open_prices: pd.DataFrame,
    close_prices: pd.DataFrame,
    amount: pd.DataFrame,
    index_close: pd.DataFrame,
    *,
    hold_days: Iterable[int],
    top_n: Iterable[int],
    transaction_cost_bps: Iterable[float] = (20.0,),
    target_exposure: Iterable[float] = (1.0,),
    base_config: EarningsForecastDriftConfig | None = None,
    quality: ExperimentQuality | None = None,
) -> pd.DataFrame:
    """运行参数/成本压力网格，返回可排序的指标表。"""

    base = base_config or EarningsForecastDriftConfig()
    rows: list[dict[str, object]] = []
    for hold in hold_days:
        for top in top_n:
            for cost in transaction_cost_bps:
                for exposure in target_exposure:
                    config = EarningsForecastDriftConfig(
                        delay_days=base.delay_days,
                        hold_days=hold,
                        top_n=top,
                        target_exposure=exposure,
                        max_weight=base.max_weight,
                        position_mode=base.position_mode,
                        transaction_cost_bps=cost,
                        liquidity_window=base.liquidity_window,
                        min_avg_amount=base.min_avg_amount,
                        exclude_st=base.exclude_st,
                        exclude_st_risk=base.exclude_st_risk,
                        block_limit_up_buys=base.block_limit_up_buys,
                        block_limit_down_sells=base.block_limit_down_sells,
                        block_suspended_trades=base.block_suspended_trades,
                        market_state=base.market_state,
                    )
                    result = run_earnings_forecast_drift(
                        forecast_events,
                        open_prices,
                        close_prices,
                        amount,
                        index_close,
                        config,
                        quality=quality,
                        segments=(),
                    )
                    rows.append(
                        {
                            "hold_days": hold,
                            "top_n": top,
                            "target_exposure": exposure,
                            "transaction_cost_bps": cost,
                            "annual_return": result.backtest.metrics.annual_return,
                            "max_drawdown": result.backtest.metrics.max_drawdown,
                            "sharpe": result.backtest.metrics.sharpe,
                            "calmar": result.backtest.metrics.calmar,
                            "grade": result.candidate_review.grade,
                            "worth_owning": result.candidate_review.worth_owning,
                        }
                    )
    return pd.DataFrame(rows)


def write_earnings_forecast_report_json(
    result: EarningsForecastDriftResult,
    path: str | Path,
    *,
    config: EarningsForecastDriftConfig | None = None,
    metadata: dict[str, object] | None = None,
) -> Path:
    """写出标准 JSON 策略报告。"""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "strategy": "earnings_forecast_drift",
        "config": _config_to_dict(config) if config is not None else None,
        "metadata": metadata or {},
        "result": result.to_dict(),
    }
    output_path.write_text(
        json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def holdings_to_long_frame(
    result: EarningsForecastDriftResult,
    returns: pd.DataFrame | None = None,
    amount: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """导出每日非零持仓明细，便于复盘真实持仓和交易来源。"""

    weights = result.weights.copy()
    if weights.empty:
        return pd.DataFrame(columns=["date", "symbol", "weight", "prev_weight", "trade_delta"])
    prev_weights = weights.shift(1).fillna(0.0)
    rows = weights.stack().rename("weight").reset_index()
    rows.columns = ["date", "symbol", "weight"]
    rows = rows.loc[rows["weight"].abs() > 1e-12].copy()
    if rows.empty:
        return rows

    prev_long = prev_weights.stack().rename("prev_weight").reset_index()
    prev_long.columns = ["date", "symbol", "prev_weight"]
    rows = rows.merge(prev_long, on=["date", "symbol"], how="left")
    rows["trade_delta"] = rows["weight"] - rows["prev_weight"].fillna(0.0)

    if returns is not None:
        aligned_returns = returns.reindex(index=weights.index, columns=weights.columns).fillna(0.0)
        ret_long = aligned_returns.stack().rename("return").reset_index()
        ret_long.columns = ["date", "symbol", "return"]
        rows = rows.merge(ret_long, on=["date", "symbol"], how="left")
        rows["contribution"] = rows["weight"] * rows["return"].fillna(0.0)

    if amount is not None:
        aligned_amount = amount.reindex(index=weights.index, columns=weights.columns)
        amount_long = aligned_amount.stack().rename("amount").reset_index()
        amount_long.columns = ["date", "symbol", "amount"]
        rows = rows.merge(amount_long, on=["date", "symbol"], how="left")
    return rows.sort_values(["date", "weight"], ascending=[True, False]).reset_index(drop=True)


def write_holdings_csv(
    result: EarningsForecastDriftResult,
    path: str | Path,
    *,
    returns: pd.DataFrame | None = None,
    amount: pd.DataFrame | None = None,
) -> Path:
    """写出每日非零持仓 CSV。"""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    holdings_to_long_frame(result, returns=returns, amount=amount).to_csv(output_path, index=False)
    return output_path


def holdings_diagnostics(holdings: pd.DataFrame) -> dict[str, object]:
    """基于持仓明细提炼贡献和交易复盘指标。"""

    if holdings.empty:
        return {
            "holding_rows": 0,
            "top_contributors": [],
            "bottom_contributors": [],
            "top_traded_symbols": [],
        }
    summary: dict[str, object] = {"holding_rows": int(len(holdings))}
    if "contribution" in holdings.columns:
        contribution = holdings.groupby("symbol")["contribution"].sum().sort_values(ascending=False)
        summary["top_contributors"] = _series_to_records(contribution.head(15), "symbol", "contribution")
        summary["bottom_contributors"] = _series_to_records(contribution.tail(15).sort_values(), "symbol", "contribution")
    trade_count = holdings.loc[holdings["trade_delta"].abs() > 1e-12].groupby("symbol")["trade_delta"].count()
    trade_count = trade_count.sort_values(ascending=False)
    summary["top_traded_symbols"] = _series_to_records(trade_count.head(15), "symbol", "trade_count")
    return summary


def exposure_diagnostics(
    result: EarningsForecastDriftResult,
    *,
    signal: pd.DataFrame | None = None,
    market_gate: pd.Series | None = None,
) -> dict[str, object]:
    """解释策略为什么满仓、半仓或空仓。"""

    weights = result.weights
    if weights.empty:
        return {"days": 0}
    exposure = weights.sum(axis=1)
    full_cash = exposure <= 1e-12
    full_exposure = exposure >= result.backtest.weights.sum(axis=1).max() - 1e-12
    partial = (exposure > 1e-12) & ~full_exposure
    diagnostics: dict[str, object] = {
        "days": int(len(weights)),
        "avg_exposure": float(exposure.mean()),
        "full_cash_days": int(full_cash.sum()),
        "partial_exposure_days": int(partial.sum()),
        "full_exposure_days": int(full_exposure.sum()),
        "first_trade_date": str(exposure[exposure > 0].index.min()) if (exposure > 0).any() else None,
        "last_trade_date": str(exposure[exposure > 0].index.max()) if (exposure > 0).any() else None,
    }
    if signal is not None:
        signal_count = signal.reindex(weights.index).notna().sum(axis=1)
        diagnostics.update(
            {
                "signal_empty_days": int((signal_count == 0).sum()),
                "signal_count_mean": float(signal_count.mean()),
                "signal_count_p50": float(signal_count.quantile(0.50)),
                "signal_count_p90": float(signal_count.quantile(0.90)),
            }
        )
    else:
        signal_count = None
    if market_gate is not None:
        gate = market_gate.reindex(weights.index).fillna(False).astype(bool)
        diagnostics["market_risk_off_days"] = int((~gate).sum())
        diagnostics["cash_because_risk_off"] = int((full_cash & ~gate).sum())
        if signal_count is not None:
            diagnostics["cash_because_no_signal_while_risk_on"] = int((full_cash & gate & (signal_count == 0)).sum())
            diagnostics["cash_other"] = int((full_cash & gate & (signal_count > 0)).sum())
            diagnostics["risk_on_with_signal_days"] = int((gate & (signal_count > 0)).sum())
            diagnostics["risk_on_without_signal_days"] = int((gate & (signal_count == 0)).sum())
            diagnostics["risk_off_with_signal_days"] = int((~gate & (signal_count > 0)).sum())
    return diagnostics


def write_earnings_forecast_report_html(
    result: EarningsForecastDriftResult,
    path: str | Path,
    *,
    title: str = "业绩预告漂移策略回测报告",
    metadata: dict[str, object] | None = None,
    holdings_path: str | Path | None = None,
    diagnostics: dict[str, object] | None = None,
) -> Path:
    """写出无外部依赖的 HTML 可视化报告。"""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    metrics = result.backtest.metrics
    equity = result.backtest.equity_curve
    drawdown = equity / equity.cummax() - 1.0
    exposure = result.weights.sum(axis=1) if not result.weights.empty else pd.Series(dtype=float)
    holding_count = result.weights.gt(0).sum(axis=1) if not result.weights.empty else pd.Series(dtype=float)
    metadata = metadata or {}
    diagnostics = diagnostics or {}
    launch_suggestion = _launch_suggestion(metadata)

    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{escape(title)}</title>
  <style>
    :root {{ color-scheme: light; --bg:#f7f8fb; --card:#fff; --text:#172033; --muted:#687386; --line:#dfe4ee; --good:#0f8f5f; --warn:#b7791f; --bad:#c2410c; --accent:#2563eb; }}
    body {{ margin:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; background:var(--bg); color:var(--text); }}
    header {{ padding:32px 40px 12px; }}
    main {{ padding:0 40px 48px; display:grid; gap:18px; }}
    h1 {{ margin:6px 0 8px; font-size:30px; }}
    h2 {{ margin:0 0 14px; font-size:20px; }}
    .eyebrow {{ color:var(--accent); font-weight:700; letter-spacing:.08em; text-transform:uppercase; margin:0; }}
    .muted {{ color:var(--muted); }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:14px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:18px; padding:20px; box-shadow:0 8px 24px rgba(15,23,42,.04); }}
    .metric {{ border-left:4px solid var(--accent); }}
    .metric strong {{ display:block; font-size:26px; }}
    .grade {{ display:inline-flex; align-items:center; padding:5px 12px; border-radius:999px; background:#e0f2fe; color:#075985; font-weight:700; }}
    table {{ border-collapse:collapse; width:100%; font-size:14px; }}
    th,td {{ border-bottom:1px solid var(--line); padding:9px 8px; text-align:right; }}
    th:first-child,td:first-child {{ text-align:left; }}
    th {{ color:var(--muted); font-weight:600; }}
    .charts {{ display:grid; grid-template-columns:2fr 1fr; gap:18px; }}
    svg {{ width:100%; height:auto; background:#fbfcff; border:1px solid var(--line); border-radius:14px; }}
    .bar-pos {{ fill:var(--good); }}
    .bar-neg {{ fill:var(--bad); }}
    .note {{ border-left:4px solid var(--warn); background:#fffbeb; padding:12px 14px; border-radius:12px; }}
    a {{ color:var(--accent); }}
    @media (max-width:900px) {{ header, main {{ padding-left:18px; padding-right:18px; }} .charts {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <header>
    <p class="eyebrow">Vortex Quant Strategy Report</p>
    <h1>{escape(title)}</h1>
    <p class="muted">报告口径：公告后延迟交易、复权 open→close、计成本、阻断涨停买入/跌停卖出、剔除 ST。生成时间由本地回测数据决定，不依赖外部图表库。</p>
  </header>
  <main>
    <section class="grid">
      {_metric_card("评级", result.candidate_review.grade, "grade")}
      {_metric_card("年化收益", _format_pct(metrics.annual_return))}
      {_metric_card("最大回撤", _format_pct(metrics.max_drawdown))}
      {_metric_card("Sharpe", f"{metrics.sharpe:.2f}")}
      {_metric_card("Calmar", f"{metrics.calmar:.2f}")}
      {_metric_card("总收益", _format_pct(metrics.total_return))}
    </section>
    <section class="charts">
      <div class="card"><h2>权益曲线</h2>{_svg_line_chart(equity, color="#2563eb")}</div>
      <div class="card"><h2>回撤曲线</h2>{_svg_line_chart(drawdown, color="#c2410c", pct=True)}</div>
    </section>
    <section class="charts">
      <div class="card"><h2>年度收益</h2>{_svg_bar_chart(result.annual_returns)}</div>
      <div class="card"><h2>仓位与持仓数量</h2>{_summary_table({"平均仓位": _format_pct(exposure.mean()), "最大仓位": _format_pct(exposure.max()), "平均持仓数": f"{holding_count.mean():.1f}", "最大持仓数": f"{holding_count.max():.0f}"})}</div>
    </section>
    <section class="card"><h2>月度收益</h2>{_returns_table(result.monthly_returns)}</section>
    <section class="card"><h2>分段稳健性</h2>{_segments_table(result.segments)}</section>
    <section class="card"><h2>可交易性与容量复核</h2>{_metadata_tables(metadata)}</section>
    <section class="card"><h2>空仓原因</h2>{_exposure_tables(metadata.get("exposure_diagnostics") if isinstance(metadata, dict) else None)}</section>
    <section class="card"><h2>持仓诊断</h2>{_diagnostics_tables(diagnostics, holdings_path)}</section>
    <section class="card"><h2>上线建议</h2>
      <div class="note">
        {launch_suggestion}
      </div>
    </section>
  </main>
</body>
</html>
"""
    output_path.write_text(html, encoding="utf-8")
    return output_path


def capacity_report(
    weights: pd.DataFrame,
    amount: pd.DataFrame,
    *,
    portfolio_notional: float,
    amount_unit_multiplier: float = 1.0,
    participation_thresholds: Iterable[float] = (0.01, 0.03, 0.05, 0.10),
) -> dict[str, object]:
    """用换手金额占成交额比例估算策略容量。

    `weights` 是每日目标权重，容量约束应看权重变化带来的交易金额，而不是持仓市值。
    `amount` 必须与 `weights` 同频同标的；若数据源成交额单位为千元，可传
    `amount_unit_multiplier=1000` 转为元。
    """

    if portfolio_notional <= 0:
        raise ValueError("portfolio_notional 必须为正数")
    if amount_unit_multiplier <= 0:
        raise ValueError("amount_unit_multiplier 必须为正数")

    aligned_amount = amount.reindex_like(weights).astype(float) * amount_unit_multiplier
    trade_value = weights.diff().abs().fillna(weights.abs()) * portfolio_notional
    participation = trade_value.where(aligned_amount > 0).div(aligned_amount.where(aligned_amount > 0))
    active = participation.where(trade_value > 0).stack().dropna()
    daily_max = participation.max(axis=1).dropna()
    traded_names = trade_value.gt(0).sum(axis=1)
    summary: dict[str, object] = {
        "portfolio_notional": portfolio_notional,
        "amount_unit_multiplier": amount_unit_multiplier,
        "trade_observations": int(active.size),
        "active_days": int(daily_max.size),
        "mean_traded_names": float(traded_names[traded_names > 0].mean()) if (traded_names > 0).any() else 0.0,
        "max_traded_names": int(traded_names.max()) if not traded_names.empty else 0,
    }
    if active.empty:
        summary.update(
            {
                "participation_median": 0.0,
                "participation_p90": 0.0,
                "participation_p95": 0.0,
                "participation_p99": 0.0,
                "participation_max": 0.0,
                "daily_max_p95": 0.0,
                "daily_max_max": 0.0,
            }
        )
    else:
        summary.update(
            {
                "participation_median": float(active.quantile(0.50)),
                "participation_p90": float(active.quantile(0.90)),
                "participation_p95": float(active.quantile(0.95)),
                "participation_p99": float(active.quantile(0.99)),
                "participation_max": float(active.max()),
                "daily_max_p95": float(daily_max.quantile(0.95)),
                "daily_max_max": float(daily_max.max()),
            }
        )
    for threshold in participation_thresholds:
        key = f"trades_over_{int(threshold * 10000)}bp"
        summary[key] = int((active > threshold).sum()) if not active.empty else 0
    return summary


def build_liquidity_mask(
    amount: pd.DataFrame,
    *,
    window: int,
    min_avg_amount: float,
) -> pd.DataFrame:
    """按滚动平均成交额构建可交易过滤。"""

    if window <= 0:
        raise ValueError("window 必须为正整数")
    if min_avg_amount < 0:
        raise ValueError("min_avg_amount 不能为负")
    return amount.rolling(window, min_periods=max(1, window // 2)).mean() >= min_avg_amount


def build_limit_event_masks(
    limit_events: pd.DataFrame,
    target_index: pd.Index,
    target_columns: pd.Index,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """从 `limit_list_d` 构建涨停买入阻断和跌停卖出阻断矩阵。"""

    required = {"date", "symbol", "limit"}
    missing = required - set(limit_events.columns)
    if missing:
        raise ValueError(f"limit_events 缺少字段: {sorted(missing)}")
    upper = _event_mask(
        limit_events.loc[limit_events["limit"].astype(str).str.upper() == "U"],
        target_index,
        target_columns,
    )
    lower = _event_mask(
        limit_events.loc[limit_events["limit"].astype(str).str.upper() == "D"],
        target_index,
        target_columns,
    )
    return upper, lower


def build_open_limit_price_masks(
    stk_limit: pd.DataFrame,
    raw_open_prices: pd.DataFrame,
    *,
    tolerance: float = 1e-6,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """用 `stk_limit` 与未复权开盘价识别开盘涨停不可买、开盘跌停不可卖。"""

    required = {"date", "symbol", "up_limit", "down_limit"}
    missing = required - set(stk_limit.columns)
    if missing:
        raise ValueError(f"stk_limit 缺少字段: {sorted(missing)}")
    rows = stk_limit[list(required)].dropna(subset=["date", "symbol"]).copy()
    rows["date"] = rows["date"].astype(str)
    rows["symbol"] = rows["symbol"].astype(str)
    rows["up_limit"] = pd.to_numeric(rows["up_limit"], errors="coerce")
    rows["down_limit"] = pd.to_numeric(rows["down_limit"], errors="coerce")
    rows = rows.dropna(subset=["up_limit", "down_limit"])
    aligned_open = raw_open_prices.copy()
    aligned_open.index = aligned_open.index.astype(str)
    aligned_open.columns = aligned_open.columns.astype(str)
    up = pd.DataFrame(False, index=aligned_open.index, columns=aligned_open.columns)
    down = up.copy()
    for date, group in rows.groupby("date"):
        if date not in aligned_open.index:
            continue
        valid_symbols = [symbol for symbol in group["symbol"] if symbol in aligned_open.columns]
        if not valid_symbols:
            continue
        group = group.set_index("symbol").loc[valid_symbols]
        opens = aligned_open.loc[date, valid_symbols].astype(float)
        up.loc[date, valid_symbols] = opens >= group["up_limit"] - tolerance
        down.loc[date, valid_symbols] = opens <= group["down_limit"] + tolerance
    up.index = raw_open_prices.index
    up.columns = raw_open_prices.columns
    down.index = raw_open_prices.index
    down.columns = raw_open_prices.columns
    return up, down


def build_suspend_trade_mask(
    suspend_events: pd.DataFrame,
    target_index: pd.Index,
    target_columns: pd.Index,
) -> pd.DataFrame:
    """从 `suspend_d` 构建停牌不可买卖矩阵，只阻断 `suspend_type == S`。"""

    required = {"date", "symbol"}
    missing = required - set(suspend_events.columns)
    if missing:
        raise ValueError(f"suspend_events 缺少字段: {sorted(missing)}")
    events = suspend_events.copy()
    if "suspend_type" in events.columns:
        events = events.loc[events["suspend_type"].astype(str).str.upper() == "S"]
    return _event_mask(events, target_index, target_columns)


def build_stock_st_mask(
    stock_st: pd.DataFrame,
    target_index: pd.Index,
    target_columns: pd.Index,
) -> pd.DataFrame:
    """从 `stock_st` 构建 ST 风险警示过滤矩阵。"""

    required = {"date", "symbol"}
    missing = required - set(stock_st.columns)
    if missing:
        raise ValueError(f"stock_st 缺少字段: {sorted(missing)}")
    return _event_mask(stock_st, target_index, target_columns)


def build_persistent_st_risk_mask(
    st_risk_events: pd.DataFrame,
    target_index: pd.Index,
    target_columns: pd.Index,
) -> pd.DataFrame:
    """从风险事件日起持续剔除标的，适用于财务 ST 预警类过滤。"""

    required = {"date", "symbol"}
    missing = required - set(st_risk_events.columns)
    if missing:
        raise ValueError(f"st_risk_events 缺少字段: {sorted(missing)}")
    mask = _event_mask(st_risk_events, target_index, target_columns)
    return mask.where(mask).ffill().fillna(False).astype(bool)


def build_financial_st_risk_events(
    *,
    fina_indicator: pd.DataFrame | None = None,
    balancesheet: pd.DataFrame | None = None,
    cashflow: pd.DataFrame | None = None,
    target_index: pd.Index | None = None,
) -> pd.DataFrame:
    """把财务退市/ST 风险转成可用于策略过滤的日频事件。

    第一版采用保守、可解释的风险规则：净资产或每股净资产为负、资产负债率
    极高且盈利恶化、ROE 极低、净利润与经营现金流同时为负。事件日期优先使用
    PIT 可见时间 `effective_from`，否则使用公告日 `ann_date`。
    """

    frames = []
    if fina_indicator is not None and not fina_indicator.empty:
        frame = fina_indicator.copy()
        symbol = frame.get("symbol", pd.Series(index=frame.index, dtype=object)).astype(str)
        bps = pd.to_numeric(frame.get("bps", pd.Series(pd.NA, index=frame.index)), errors="coerce")
        roe = pd.to_numeric(frame.get("roe", pd.Series(pd.NA, index=frame.index)), errors="coerce")
        debt_to_assets = pd.to_numeric(frame.get("debt_to_assets", pd.Series(pd.NA, index=frame.index)), errors="coerce")
        netprofit_yoy = pd.to_numeric(frame.get("netprofit_yoy", pd.Series(pd.NA, index=frame.index)), errors="coerce")
        risk = (
            bps.le(0)
            | roe.le(-50)
            | (debt_to_assets.ge(95) & netprofit_yoy.le(-50))
        ).fillna(False)
        frames.append(_financial_risk_rows(frame.loc[risk], symbol.loc[risk], "fina_indicator_st_risk", target_index))
    if balancesheet is not None and not balancesheet.empty:
        frame = balancesheet.copy()
        symbol = frame.get("symbol", pd.Series(index=frame.index, dtype=object)).astype(str)
        equity_inc = pd.to_numeric(frame.get("total_hldr_eqy_inc_min_int", pd.Series(pd.NA, index=frame.index)), errors="coerce")
        equity_exc = pd.to_numeric(frame.get("total_hldr_eqy_exc_min_int", pd.Series(pd.NA, index=frame.index)), errors="coerce")
        risk = (equity_inc.le(0) | equity_exc.le(0)).fillna(False)
        frames.append(_financial_risk_rows(frame.loc[risk], symbol.loc[risk], "negative_equity_st_risk", target_index))
    if cashflow is not None and not cashflow.empty:
        frame = cashflow.copy()
        symbol = frame.get("symbol", pd.Series(index=frame.index, dtype=object)).astype(str)
        net_profit = pd.to_numeric(frame.get("net_profit", pd.Series(pd.NA, index=frame.index)), errors="coerce")
        operating_cashflow = pd.to_numeric(frame.get("n_cashflow_act", pd.Series(pd.NA, index=frame.index)), errors="coerce")
        free_cashflow = pd.to_numeric(frame.get("free_cashflow", pd.Series(pd.NA, index=frame.index)), errors="coerce")
        risk = (net_profit.lt(0) & operating_cashflow.lt(0) & free_cashflow.lt(0)).fillna(False)
        frames.append(_financial_risk_rows(frame.loc[risk], symbol.loc[risk], "profit_cashflow_st_risk", target_index))
    non_empty = [frame for frame in frames if frame is not None and not frame.empty]
    if not non_empty:
        return pd.DataFrame(columns=["date", "symbol", "risk_reason"])
    result = pd.concat(non_empty, ignore_index=True)
    return result.drop_duplicates(["date", "symbol", "risk_reason"]).sort_values(["date", "symbol"]).reset_index(drop=True)


def _combine_masks(left: pd.DataFrame | None, right: pd.DataFrame | None) -> pd.DataFrame | None:
    if left is None:
        return right
    if right is None:
        return left
    right = right.reindex(index=left.index, columns=left.columns).fillna(False).astype(bool)
    return left.fillna(False).astype(bool) | right


def _financial_risk_rows(
    frame: pd.DataFrame,
    symbol: pd.Series,
    reason: str,
    target_index: pd.Index | None,
) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["date", "symbol", "risk_reason"])
    dates = _financial_effective_dates(frame)
    if target_index is not None:
        dates = _align_risk_dates_to_trading_index(dates, target_index)
    rows = pd.DataFrame({"date": dates, "symbol": symbol.astype(str).to_numpy(), "risk_reason": reason})
    return rows.dropna(subset=["date", "symbol"])


def _financial_effective_dates(frame: pd.DataFrame) -> pd.Series:
    raw = frame.get("effective_from")
    if raw is not None:
        extracted = _extract_yyyymmdd(raw)
    else:
        extracted = pd.Series(pd.NA, index=frame.index, dtype=object)
    fallback = _extract_yyyymmdd(frame.get("ann_date", pd.Series(pd.NA, index=frame.index, dtype=object)))
    dates = extracted.fillna(fallback)
    return dates.where(dates.str.len() == 8)


def _extract_yyyymmdd(values: pd.Series) -> pd.Series:
    digits = values.astype(str).str.replace(r"\D", "", regex=True)
    return digits.str.slice(0, 8).where(digits.str.len() >= 8)


def _align_risk_dates_to_trading_index(dates: pd.Series, target_index: pd.Index) -> pd.Series:
    trading = pd.Series(target_index.astype(str), index=target_index.astype(str))
    trading_int = pd.to_numeric(trading, errors="coerce").dropna().astype(int).to_numpy()
    raw = pd.to_numeric(dates, errors="coerce")
    aligned: list[str | None] = []
    for value in raw:
        if pd.isna(value):
            aligned.append(None)
            continue
        pos = int(pd.Index(trading_int).searchsorted(int(value), side="left"))
        aligned.append(str(trading_int[pos]) if pos < len(trading_int) else None)
    return pd.Series(aligned, index=dates.index, dtype=object)


def open_to_close_returns(open_prices: pd.DataFrame, close_prices: pd.DataFrame) -> pd.DataFrame:
    """计算 open→close 收益矩阵。"""

    returns = close_prices / open_prices - 1.0
    return returns.replace([float("inf"), float("-inf")], pd.NA).fillna(0.0)


def period_returns(returns: pd.Series, freq: str) -> pd.Series:
    """按年或月聚合收益。"""

    if returns.empty:
        return pd.Series(dtype=float)
    periods = _to_datetime_index(returns.index).to_period(freq)
    grouped = (1.0 + returns.fillna(0.0)).groupby(periods).prod() - 1.0
    grouped.index = grouped.index.astype(str)
    return grouped


def segment_report(returns: pd.Series, spec: SegmentSpec) -> SegmentReport:
    """生成单个时间段的收益风险报告。"""

    dates = _to_datetime_index(returns.index)
    start = pd.to_datetime(spec.start, format="%Y%m%d")
    end = pd.to_datetime(spec.end, format="%Y%m%d")
    mask = (dates >= start) & (dates <= end)
    segment_returns = returns.loc[mask]
    if segment_returns.empty:
        raise ValueError(f"分段没有收益数据: {spec.name}")
    equity = (1.0 + segment_returns.fillna(0.0)).cumprod()
    equity = pd.concat([pd.Series([1.0], index=[segment_returns.index[0]]), equity])
    metrics = _compute_metrics(
        equity,
        segment_returns,
        turnover_sum=0.0,
        rebalance_count=max(len(segment_returns), 1),
    )
    return SegmentReport(
        name=spec.name,
        start=spec.start,
        end=spec.end,
        days=len(segment_returns),
        metrics=metrics,
    )


def _event_mask(events: pd.DataFrame, target_index: pd.Index, target_columns: pd.Index) -> pd.DataFrame:
    mask = pd.DataFrame(False, index=target_index, columns=target_columns)
    if events.empty:
        return mask
    date_lookup = {str(value): value for value in target_index}
    symbol_lookup = {str(value): value for value in target_columns}
    rows = events[["date", "symbol"]].dropna().copy()
    rows["date"] = rows["date"].astype(str)
    rows["symbol"] = rows["symbol"].astype(str)
    valid = rows["date"].isin(date_lookup) & rows["symbol"].isin(symbol_lookup)
    rows = rows.loc[valid].drop_duplicates()
    if rows.empty:
        return mask
    for date, group in rows.groupby("date"):
        symbols = [symbol_lookup[symbol] for symbol in group["symbol"]]
        mask.loc[date_lookup[date], symbols] = True
    return mask


def _align_market_frames(
    open_prices: pd.DataFrame,
    close_prices: pd.DataFrame,
    amount: pd.DataFrame,
    index_close: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    common_dates = open_prices.index.intersection(close_prices.index).intersection(amount.index)
    common_symbols = open_prices.columns.intersection(close_prices.columns).intersection(amount.columns)
    open_prices = open_prices.loc[common_dates, common_symbols].sort_index()
    close_prices = close_prices.loc[common_dates, common_symbols].sort_index()
    amount = amount.loc[common_dates, common_symbols].sort_index()
    index_close = index_close.reindex(common_dates).ffill()
    return open_prices, close_prices, amount, index_close


def _to_datetime_index(index: pd.Index) -> pd.DatetimeIndex:
    if isinstance(index, pd.DatetimeIndex):
        return index
    values = pd.Series(index.astype(str))
    dates = pd.to_datetime(values, format="%Y%m%d", errors="coerce")
    if dates.isna().any():
        dates = pd.to_datetime(values, errors="coerce")
    if dates.isna().any():
        raise ValueError("日期索引无法转换为 DatetimeIndex")
    return pd.DatetimeIndex(dates)


def _validate_inputs(
    forecast_events: pd.DataFrame,
    open_prices: pd.DataFrame,
    close_prices: pd.DataFrame,
    amount: pd.DataFrame,
    index_close: pd.DataFrame,
    config: EarningsForecastDriftConfig,
) -> None:
    if forecast_events.empty:
        raise ValueError("forecast_events 不能为空")
    if open_prices.empty:
        raise ValueError("open_prices 不能为空")
    if close_prices.empty:
        raise ValueError("close_prices 不能为空")
    if amount.empty:
        raise ValueError("amount 不能为空")
    if index_close.empty:
        raise ValueError("index_close 不能为空")
    if config.hold_days <= 0:
        raise ValueError("hold_days 必须为正整数")
    if config.top_n <= 0:
        raise ValueError("top_n 必须为正整数")
    if not 0 < config.target_exposure <= 1:
        raise ValueError("target_exposure 必须在 (0, 1] 内")
    if config.max_weight is not None and not 0 < config.max_weight <= 1:
        raise ValueError("max_weight 必须在 (0, 1] 内")
    if config.position_mode not in {"full_equal_selected", "capped_with_cash"}:
        raise ValueError("position_mode 必须是 full_equal_selected 或 capped_with_cash")


def _config_to_dict(config: EarningsForecastDriftConfig) -> dict[str, object]:
    return {
        "delay_days": config.delay_days,
        "hold_days": config.hold_days,
        "top_n": config.top_n,
        "target_exposure": config.target_exposure,
        "max_weight": config.max_weight,
        "position_mode": config.position_mode,
        "transaction_cost_bps": config.transaction_cost_bps,
        "liquidity_window": config.liquidity_window,
        "min_avg_amount": config.min_avg_amount,
        "exclude_st": config.exclude_st,
        "exclude_st_risk": config.exclude_st_risk,
        "block_limit_up_buys": config.block_limit_up_buys,
        "block_limit_down_sells": config.block_limit_down_sells,
        "block_suspended_trades": config.block_suspended_trades,
        "market_state": config.market_state.__dict__,
    }


def _series_to_records(series: pd.Series, name_key: str, value_key: str) -> list[dict[str, object]]:
    return [{name_key: str(index), value_key: _jsonable(value)} for index, value in series.items()]


def _format_pct(value: object) -> str:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return "-"
    return f"{number * 100:.2f}%"


def _metric_card(label: str, value: str, css_class: str = "") -> str:
    return (
        f'<div class="card metric {escape(css_class)}">'
        f"<span class=\"muted\">{escape(label)}</span><strong>{escape(value)}</strong>"
        "</div>"
    )


def _summary_table(items: dict[str, str]) -> str:
    rows = "".join(f"<tr><td>{escape(str(key))}</td><td>{escape(str(value))}</td></tr>" for key, value in items.items())
    return f"<table><tbody>{rows}</tbody></table>"


def _svg_line_chart(series: pd.Series, *, color: str, pct: bool = False) -> str:
    clean = series.dropna().astype(float)
    width, height, pad = 720, 260, 28
    if clean.empty:
        return f'<svg viewBox="0 0 {width} {height}" role="img"><text x="24" y="40">无数据</text></svg>'
    values = clean.to_list()
    min_v = min(values)
    max_v = max(values)
    if min_v == max_v:
        min_v -= 1.0
        max_v += 1.0
    points: list[str] = []
    for idx, value in enumerate(values):
        x = pad + idx * (width - pad * 2) / max(len(values) - 1, 1)
        y = height - pad - (value - min_v) * (height - pad * 2) / (max_v - min_v)
        points.append(f"{x:.2f},{y:.2f}")
    label_left = _format_pct(min_v) if pct else f"{min_v:.2f}"
    label_right = _format_pct(max_v) if pct else f"{max_v:.2f}"
    return (
        f'<svg viewBox="0 0 {width} {height}" role="img">'
        f'<polyline fill="none" stroke="{escape(color)}" stroke-width="2.5" points="{" ".join(points)}" />'
        f'<text x="{pad}" y="{height - 8}" fill="#687386" font-size="12">{escape(label_left)}</text>'
        f'<text x="{pad}" y="18" fill="#687386" font-size="12">{escape(label_right)}</text>'
        "</svg>"
    )


def _svg_bar_chart(series: pd.Series) -> str:
    clean = series.dropna().astype(float)
    width, height, pad = 720, 280, 34
    if clean.empty:
        return f'<svg viewBox="0 0 {width} {height}" role="img"><text x="24" y="40">无数据</text></svg>'
    labels = [str(index) for index in clean.index]
    values = clean.to_list()
    min_v = min(min(values), 0.0)
    max_v = max(max(values), 0.0)
    if min_v == max_v:
        max_v += 1.0
    zero_y = height - pad - (0.0 - min_v) * (height - pad * 2) / (max_v - min_v)
    bar_w = (width - pad * 2) / len(values) * 0.68
    parts = [
        f'<line x1="{pad}" y1="{zero_y:.2f}" x2="{width - pad}" y2="{zero_y:.2f}" stroke="#dfe4ee" />'
    ]
    for idx, value in enumerate(values):
        center = pad + (idx + 0.5) * (width - pad * 2) / len(values)
        y = height - pad - (value - min_v) * (height - pad * 2) / (max_v - min_v)
        top = min(y, zero_y)
        bar_h = abs(zero_y - y)
        css = "bar-pos" if value >= 0 else "bar-neg"
        parts.append(
            f'<rect class="{css}" x="{center - bar_w / 2:.2f}" y="{top:.2f}" width="{bar_w:.2f}" height="{bar_h:.2f}">'
            f"<title>{escape(labels[idx])}: {_format_pct(value)}</title></rect>"
        )
        parts.append(f'<text x="{center:.2f}" y="{height - 8}" text-anchor="middle" font-size="11" fill="#687386">{escape(labels[idx][-4:])}</text>')
    return f'<svg viewBox="0 0 {width} {height}" role="img">{"".join(parts)}</svg>'


def _returns_table(monthly_returns: pd.Series) -> str:
    if monthly_returns.empty:
        return "<p class=\"muted\">无月度收益数据</p>"
    table = monthly_returns.copy()
    table.index = table.index.astype(str)
    years = sorted({item.split("-")[0] for item in table.index})
    rows = []
    for year in years:
        cells = [f"<td>{escape(year)}</td>"]
        for month in range(1, 13):
            key = f"{year}-{month:02d}"
            value = table.get(key)
            css = "color:var(--good)" if value is not None and float(value) >= 0 else "color:var(--bad)"
            cells.append(f'<td style="{css}">{_format_pct(value) if value is not None else ""}</td>')
        rows.append(f"<tr>{''.join(cells)}</tr>")
    header = "<tr><th>年份</th>" + "".join(f"<th>{month}</th>" for month in range(1, 13)) + "</tr>"
    return f"<table><thead>{header}</thead><tbody>{''.join(rows)}</tbody></table>"


def _segments_table(segments: tuple[SegmentReport, ...]) -> str:
    if not segments:
        return "<p class=\"muted\">无分段数据</p>"
    rows = []
    for segment in segments:
        metrics = segment.metrics
        rows.append(
            "<tr>"
            f"<td>{escape(segment.name)}</td>"
            f"<td>{escape(segment.start)} - {escape(segment.end)}</td>"
            f"<td>{segment.days}</td>"
            f"<td>{_format_pct(metrics.annual_return)}</td>"
            f"<td>{_format_pct(metrics.max_drawdown)}</td>"
            f"<td>{metrics.sharpe:.2f}</td>"
            f"<td>{metrics.calmar:.2f}</td>"
            "</tr>"
        )
    header = "<tr><th>分段</th><th>区间</th><th>交易日</th><th>年化</th><th>最大回撤</th><th>Sharpe</th><th>Calmar</th></tr>"
    return f"<table><thead>{header}</thead><tbody>{''.join(rows)}</tbody></table>"


def _metadata_tables(metadata: dict[str, object]) -> str:
    tradability = metadata.get("tradability_review") if isinstance(metadata, dict) else None
    cost_pressure = metadata.get("cost_pressure") if isinstance(metadata, dict) else None
    safe_result = metadata.get("safe_3pct_result") if isinstance(metadata, dict) else None
    parts: list[str] = []
    if isinstance(tradability, dict):
        strict = tradability.get("strict_result")
        if isinstance(strict, dict):
            parts.append(
                "<h3>严格可交易口径</h3>"
                + _summary_table(
                    {
                        "年化": _format_pct(strict.get("annual_return")),
                        "最大回撤": _format_pct(strict.get("max_drawdown")),
                        "Sharpe": f"{float(strict.get('sharpe', 0.0)):.2f}",
                        "Calmar": f"{float(strict.get('calmar', 0.0)):.2f}",
                        "评级": str(strict.get("grade", "-")),
                    }
                )
            )
        parts.append(
            "<h3>不可交易暴露</h3>"
            + _summary_table(
                {
                    "涨停买入/加仓笔数": str(tradability.get("base_buy_on_limit_up_trades", "-")),
                    "总买入/加仓笔数": str(tradability.get("base_buy_trades", "-")),
                    "跌停卖出/减仓笔数": str(tradability.get("base_sell_on_limit_down_trades", "-")),
                    "缺失数据": ", ".join(str(item) for item in tradability.get("data_missing", [])),
                }
            )
        )
    if isinstance(safe_result, dict):
        metrics = safe_result.get("metrics")
        capacity = safe_result.get("capacity_100m")
        if isinstance(metrics, dict):
            parts.append(
                "<h3>单票 3% 安全版</h3>"
                + _summary_table(
                    {
                        "年化": _format_pct(metrics.get("annual_return")),
                        "最大回撤": _format_pct(metrics.get("max_drawdown")),
                        "Sharpe": f"{float(metrics.get('sharpe', 0.0)):.2f}",
                        "Calmar": f"{float(metrics.get('calmar', 0.0)):.2f}",
                    }
                )
            )
        if isinstance(capacity, dict):
            parts.append(
                "<h3>单票 3% 安全版容量</h3>"
                + _summary_table(
                    {
                        "1 亿本金 P95 参与率": _format_pct(capacity.get("participation_p95")),
                        "1 亿本金 P99 参与率": _format_pct(capacity.get("participation_p99")),
                        "单日最大参与率 P95": _format_pct(capacity.get("daily_max_p95")),
                    }
                )
            )
    if isinstance(cost_pressure, list):
        rows = []
        for item in cost_pressure:
            if not isinstance(item, dict):
                continue
            rows.append(
                "<tr>"
                f"<td>{item.get('cost_bps')}</td>"
                f"<td>{_format_pct(item.get('annual_return'))}</td>"
                f"<td>{_format_pct(item.get('max_drawdown'))}</td>"
                f"<td>{item.get('sharpe')}</td>"
                f"<td>{item.get('calmar')}</td>"
                f"<td>{escape(str(item.get('grade')))}</td>"
                "</tr>"
            )
        parts.append("<h3>成本压力</h3><table><thead><tr><th>成本 bps</th><th>年化</th><th>回撤</th><th>Sharpe</th><th>Calmar</th><th>评级</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table>")
    return "".join(parts) if parts else "<p class=\"muted\">无可交易性元数据</p>"


def _launch_suggestion(metadata: dict[str, object]) -> str:
    tradability = metadata.get("tradability_review") if isinstance(metadata, dict) else None
    data_missing: list[object] = []
    if isinstance(tradability, dict):
        raw_missing = tradability.get("data_missing", [])
        data_missing = raw_missing if isinstance(raw_missing, list) else []
    elif isinstance(metadata.get("data_missing"), list):
        data_missing = metadata["data_missing"]  # type: ignore[assignment]

    if data_missing:
        missing = ", ".join(f"<code>{escape(str(item))}</code>" for item in data_missing)
        return (
            "当前建议先进入 shadow trading / paper trading（影子跟踪），不要直接满额上线。"
            f"原因是策略和业绩预告披露季强相关，且本地仍缺 {missing} 精确约束。"
            "若补齐一字板和停牌约束后结果仍保持 A 级以上，最早适合在下一轮业绩预告密集窗口前小规模上线；"
            "按当前日期，更合理的窗口是 2026 年半年度业绩预告前后，而不是在事件密度下降时盲目上线。"
        )
    return (
        "全历史 <code>stk_limit</code> 与 <code>suspend_d</code> 已进入本报告的精确可交易复核，"
        "但策略仍建议先做 shadow trading / paper trading（影子跟踪），不要直接满额上线。"
        "原因是收益来自业绩预告披露季，真实执行还要继续观察公告延迟、开盘成交、冲击成本和当期事件密度；"
        "如果影子跟踪和 1 亿元容量约束继续保持 A 级以上，更合理的小规模上线窗口仍是 2026 年半年度业绩预告前后。"
    )


def _exposure_tables(diagnostics: object) -> str:
    if not isinstance(diagnostics, dict):
        return "<p class=\"muted\">无空仓诊断数据</p>"
    return _summary_table(
        {
            "回测天数": str(diagnostics.get("days", "-")),
            "平均仓位": _format_pct(diagnostics.get("avg_exposure")),
            "空仓天数": str(diagnostics.get("full_cash_days", "-")),
            "部分仓位天数": str(diagnostics.get("partial_exposure_days", "-")),
            "满仓天数": str(diagnostics.get("full_exposure_days", "-")),
            "市场 risk-off 天数": str(diagnostics.get("market_risk_off_days", "-")),
            "因市场 risk-off 空仓": str(diagnostics.get("cash_because_risk_off", "-")),
            "risk-on 但无合格信号空仓": str(diagnostics.get("cash_because_no_signal_while_risk_on", "-")),
            "信号池为空天数": str(diagnostics.get("signal_empty_days", "-")),
        }
    )


def _diagnostics_tables(diagnostics: dict[str, object], holdings_path: str | Path | None) -> str:
    parts: list[str] = []
    if holdings_path is not None:
        parts.append(f'<p>每日非零持仓明细：<code>{escape(str(holdings_path))}</code></p>')
    parts.append(_summary_table({"持仓明细行数": str(diagnostics.get("holding_rows", "-"))}))
    for title, key, value_label in [
        ("贡献最高股票", "top_contributors", "贡献"),
        ("贡献最低股票", "bottom_contributors", "贡献"),
        ("交易最频繁股票", "top_traded_symbols", "交易次数"),
    ]:
        records = diagnostics.get(key)
        if not isinstance(records, list) or not records:
            continue
        rows = []
        for record in records[:10]:
            if not isinstance(record, dict):
                continue
            symbol = str(record.get("symbol", "-"))
            value = record.get("contribution", record.get("trade_count", "-"))
            formatted = _format_pct(value) if key != "top_traded_symbols" else str(value)
            rows.append(f"<tr><td>{escape(symbol)}</td><td>{escape(formatted)}</td></tr>")
        parts.append(f"<h3>{escape(title)}</h3><table><thead><tr><th>股票</th><th>{escape(value_label)}</th></tr></thead><tbody>{''.join(rows)}</tbody></table>")
    return "".join(parts)


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
