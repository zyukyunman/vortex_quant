"""小资金账户的整手撮合回测工具。

标准权重回测假设可以按任意小数股和目标权重成交；30 万、50 万、100 万账户
会明显受到 100 股整手、单笔金额过低和最低佣金影响。本模块把这些执行约束
显式纳入，用于把机构展示版策略改造成小资金可执行版本。
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from vortex.strategy.backtest import BacktestMetrics, _compute_metrics


@dataclass(frozen=True)
class SmallCapitalExecutionConfig:
    """小资金整手执行配置。"""

    initial_cash: float = 1_000_000.0
    lot_size: int = 100
    min_trade_value: float = 3_000.0
    commission_bps: float = 2.5
    min_commission: float = 5.0
    stamp_duty_sell_bps: float = 5.0
    allow_partial_buy_fills: bool = True


@dataclass(frozen=True)
class SmallCapitalBacktestResult:
    """小资金整手撮合结果。"""

    metrics: BacktestMetrics
    equity_curve: pd.Series = field(repr=False)
    returns: pd.Series = field(repr=False)
    weights: pd.DataFrame = field(repr=False)
    trades: pd.DataFrame = field(repr=False)
    order_intents: pd.DataFrame = field(repr=False)
    diagnostics: pd.DataFrame = field(repr=False)
    summary: dict[str, object]


def run_lot_constrained_backtest(
    target_weights: pd.DataFrame,
    open_prices: pd.DataFrame,
    close_prices: pd.DataFrame,
    config: SmallCapitalExecutionConfig | None = None,
    *,
    market_gate: pd.Series | None = None,
    signal: pd.DataFrame | None = None,
    buy_share_limits: pd.DataFrame | None = None,
) -> SmallCapitalBacktestResult:
    """按目标权重进行 100 股整手撮合，并返回真实资金曲线。

    每个交易日开盘先按目标权重调仓，收盘按 close 估值。卖出先执行，买入再按
    可用现金执行；低于最小交易金额或不足一手的订单会跳过。

    `market_gate` 和 `signal` 只用于诊断现金闲置原因，不改变撮合结果。
    `buy_share_limits` 若提供，则表示每个 date/symbol 在买入侧可成交的最大股数；
    当 `allow_partial_buy_fills=False` 时，任何买不满目标股数的订单都会整笔跳过。
    """

    config = config or SmallCapitalExecutionConfig()
    _validate_small_capital_inputs(target_weights, open_prices, close_prices, config)
    common_dates = target_weights.index.intersection(open_prices.index).intersection(close_prices.index)
    common_symbols = target_weights.columns.intersection(open_prices.columns).intersection(close_prices.columns)
    active_symbols = target_weights.loc[common_dates, common_symbols].fillna(0.0).abs().gt(1e-12).any(axis=0)
    if active_symbols.any():
        common_symbols = common_symbols[active_symbols.to_numpy()]
    target_weights = target_weights.loc[common_dates, common_symbols].sort_index().fillna(0.0)
    open_prices = open_prices.loc[common_dates, common_symbols].sort_index()
    close_prices = close_prices.loc[common_dates, common_symbols].sort_index()
    aligned_buy_limits = None
    if buy_share_limits is not None:
        aligned_buy_limits = (
            buy_share_limits.reindex(index=common_dates, columns=common_symbols)
            .apply(pd.to_numeric, errors="coerce")
            .fillna(0.0)
        )

    cash = float(config.initial_cash)
    shares = pd.Series(0, index=common_symbols, dtype=int)
    mark_prices = (
        pd.to_numeric(close_prices.iloc[0], errors="coerce").where(lambda s: s.gt(0)).combine_first(
            pd.to_numeric(open_prices.iloc[0], errors="coerce").where(lambda s: s.gt(0))
        )
    ).fillna(0.0)
    previous_equity = cash
    equity_rows = [{"date": common_dates[0], "equity": previous_equity}]
    return_rows: list[dict[str, object]] = []
    weight_rows: list[pd.Series] = []
    trade_rows: list[dict[str, object]] = []
    order_intent_rows: list[dict[str, object]] = []
    diagnostic_rows: list[dict[str, object]] = []
    turnover_sum = 0.0
    traded_days = 0
    aligned_gate = market_gate.reindex(common_dates).astype("boolean") if market_gate is not None else None
    aligned_signal = signal.reindex(index=common_dates, columns=common_symbols) if signal is not None else None

    for idx in range(1, len(common_dates)):
        date = common_dates[idx]
        opens = pd.to_numeric(open_prices.loc[date], errors="coerce")
        closes = pd.to_numeric(close_prices.loc[date], errors="coerce")
        tradable = opens.gt(0) & closes.gt(0)
        open_mark = opens.where(opens.gt(0)).combine_first(mark_prices).fillna(0.0)
        open_equity = cash + float((shares * open_mark).sum())
        day_target_weights = target_weights.loc[date].astype(float)
        target_value = day_target_weights * open_equity
        target_shares = (target_value / opens).where(tradable, 0).fillna(0.0).map(
            lambda value: int(value // config.lot_size) * config.lot_size
        )

        day_turnover = 0.0
        day_trade_count = 0
        sell_symbols = shares.loc[(shares > target_shares) & shares.gt(0)].index
        for symbol in sell_symbols:
            diff = int(target_shares.loc[symbol] - shares.loc[symbol])
            if diff >= 0 or not tradable.loc[symbol]:
                continue
            sell_shares = abs(diff)
            gross = sell_shares * float(opens.loc[symbol])
            if gross < config.min_trade_value:
                continue
            fee = _trade_fee(gross, config, sell=True)
            cash += gross - fee
            shares.loc[symbol] -= sell_shares
            day_turnover += gross
            day_trade_count += 1
            trade_rows.append(_trade_row(date, symbol, "sell", sell_shares, opens.loc[symbol], gross, fee, cash))

        buy_candidates = (target_shares - shares)
        buy_candidates = buy_candidates.loc[buy_candidates > 0].sort_values(ascending=False)
        for symbol, diff_value in buy_candidates.items():
            diff = int(diff_value)
            if diff <= 0:
                continue
            requested_shares = diff
            current_shares_before = int(shares.loc[symbol])
            target_shares_before = int(target_shares.loc[symbol])
            cash_before = float(cash)
            if not tradable.loc[symbol]:
                order_intent_rows.append(
                    _order_intent_row(
                        date=date,
                        symbol=symbol,
                        side="buy",
                        target_weight=float(day_target_weights.loc[symbol]),
                        target_shares=target_shares_before,
                        current_shares=current_shares_before,
                        requested_shares=requested_shares,
                        price=float(opens.loc[symbol]) if pd.notna(opens.loc[symbol]) else 0.0,
                        cash_before=cash_before,
                        executed_shares=0,
                        status="skipped",
                        reason="non_tradable",
                    )
                )
                continue
            price = float(opens.loc[symbol])
            limit_shares = requested_shares
            if aligned_buy_limits is not None:
                raw_limit = float(aligned_buy_limits.loc[date, symbol])
                limit_shares = max(0, int(raw_limit // config.lot_size) * config.lot_size)
            max_affordable_lots = int(cash // (price * config.lot_size))
            affordable_shares = max_affordable_lots * config.lot_size
            buy_shares = min(requested_shares, affordable_shares, limit_shares)
            status = "filled"
            reason = ""
            if buy_shares <= 0:
                if aligned_buy_limits is not None and limit_shares <= 0:
                    reason = "opening_volume_insufficient"
                else:
                    reason = "insufficient_cash"
                order_intent_rows.append(
                    _order_intent_row(
                        date=date,
                        symbol=symbol,
                        side="buy",
                        target_weight=float(day_target_weights.loc[symbol]),
                        target_shares=target_shares_before,
                        current_shares=current_shares_before,
                        requested_shares=requested_shares,
                        price=price,
                        cash_before=cash_before,
                        executed_shares=0,
                        status="skipped",
                        reason=reason,
                    )
                )
                continue
            gross = buy_shares * price
            fee = _trade_fee(gross, config, sell=False)
            while buy_shares > 0 and gross + fee > cash:
                buy_shares -= config.lot_size
                gross = buy_shares * price
                fee = _trade_fee(gross, config, sell=False) if buy_shares > 0 else 0.0
            if buy_shares <= 0:
                order_intent_rows.append(
                    _order_intent_row(
                        date=date,
                        symbol=symbol,
                        side="buy",
                        target_weight=float(day_target_weights.loc[symbol]),
                        target_shares=target_shares_before,
                        current_shares=current_shares_before,
                        requested_shares=requested_shares,
                        price=price,
                        cash_before=cash_before,
                        executed_shares=0,
                        status="skipped",
                        reason="insufficient_cash_after_fee",
                    )
                )
                continue
            if gross < config.min_trade_value:
                order_intent_rows.append(
                    _order_intent_row(
                        date=date,
                        symbol=symbol,
                        side="buy",
                        target_weight=float(day_target_weights.loc[symbol]),
                        target_shares=target_shares_before,
                        current_shares=current_shares_before,
                        requested_shares=requested_shares,
                        price=price,
                        cash_before=cash_before,
                        executed_shares=0,
                        status="skipped",
                        reason="below_min_trade_value",
                    )
                )
                continue
            if buy_shares < requested_shares:
                if not config.allow_partial_buy_fills:
                    if aligned_buy_limits is not None and limit_shares < requested_shares:
                        reason = "opening_volume_insufficient"
                    else:
                        reason = "partial_fill_not_allowed"
                    order_intent_rows.append(
                        _order_intent_row(
                            date=date,
                            symbol=symbol,
                            side="buy",
                            target_weight=float(day_target_weights.loc[symbol]),
                            target_shares=target_shares_before,
                            current_shares=current_shares_before,
                            requested_shares=requested_shares,
                            price=price,
                            cash_before=cash_before,
                            executed_shares=0,
                            status="skipped",
                            reason=reason,
                        )
                    )
                    continue
                status = "partial"
                reason = "cash_constrained"
                if aligned_buy_limits is not None and limit_shares < requested_shares:
                    reason = "opening_volume_constrained"
            cash -= gross + fee
            shares.loc[symbol] += buy_shares
            day_turnover += gross
            day_trade_count += 1
            trade_rows.append(_trade_row(date, symbol, "buy", buy_shares, price, gross, fee, cash))
            order_intent_rows.append(
                _order_intent_row(
                    date=date,
                    symbol=symbol,
                    side="buy",
                    target_weight=float(day_target_weights.loc[symbol]),
                    target_shares=target_shares_before,
                    current_shares=current_shares_before,
                    requested_shares=requested_shares,
                    price=price,
                    cash_before=cash_before,
                    executed_shares=buy_shares,
                    status=status,
                    reason=reason,
                )
            )

        close_mark = closes.where(closes.gt(0)).combine_first(open_mark).fillna(0.0)
        close_equity = cash + float((shares * close_mark).sum())
        mark_prices = close_mark.combine_first(mark_prices).fillna(0.0)
        daily_return = close_equity / previous_equity - 1.0 if previous_equity > 0 else 0.0
        previous_equity = close_equity
        if day_turnover > 0:
            traded_days += 1
            turnover_sum += day_turnover / max(open_equity, 1.0)
        weights = (shares * close_mark).div(close_equity).fillna(0.0) if close_equity > 0 else shares.astype(float)
        weights.name = date
        weight_rows.append(weights)
        return_rows.append({"date": date, "return": daily_return})
        equity_rows.append({"date": date, "equity": close_equity})
        target_exposure = float(day_target_weights.sum())
        actual_exposure = float(weights.sum())
        target_count = int(day_target_weights.gt(1e-12).sum())
        actual_count = int(weights.gt(1e-12).sum())
        signal_count = (
            int(aligned_signal.loc[date].notna().sum())
            if aligned_signal is not None and date in aligned_signal.index
            else None
        )
        gate_value = (
            bool(aligned_gate.loc[date])
            if aligned_gate is not None and date in aligned_gate.index and pd.notna(aligned_gate.loc[date])
            else None
        )
        diagnostic_rows.append(
            {
                "date": date,
                "market_gate_on": gate_value,
                "signal_count": signal_count,
                "target_holding_count": target_count,
                "actual_holding_count": actual_count,
                "target_exposure": target_exposure,
                "actual_exposure": actual_exposure,
                "cash_ratio": float(cash / close_equity) if close_equity > 0 else 0.0,
                "target_actual_exposure_gap": max(target_exposure - actual_exposure, 0.0),
                "target_actual_holding_gap": max(target_count - actual_count, 0),
                "idle_reason": _cash_idle_reason(
                    market_gate_on=gate_value,
                    signal_count=signal_count,
                    target_holding_count=target_count,
                    actual_holding_count=actual_count,
                    target_exposure=target_exposure,
                    actual_exposure=actual_exposure,
                ),
                "open_equity": float(open_equity),
                "close_equity": float(close_equity),
                "day_turnover": float(day_turnover),
                "day_trade_count": int(day_trade_count),
            }
        )

    equity_curve = pd.DataFrame(equity_rows).set_index("date")["equity"]
    returns = pd.DataFrame(return_rows).set_index("date")["return"]
    weights = pd.DataFrame(weight_rows).fillna(0.0)
    trades = pd.DataFrame(trade_rows)
    order_intents = pd.DataFrame(order_intent_rows)
    diagnostics = pd.DataFrame(diagnostic_rows)
    metrics = _compute_metrics(equity_curve, returns, turnover_sum=turnover_sum, rebalance_count=max(traded_days, 1))
    summary = {
        "initial_cash": config.initial_cash,
        "ending_equity": float(equity_curve.iloc[-1]),
        "trade_rows": int(len(trades)),
        "traded_days": int(traded_days),
        "avg_trade_value": float(trades["gross_value"].mean()) if not trades.empty else 0.0,
        "max_holding_count": int(weights.gt(0).sum(axis=1).max()) if not weights.empty else 0,
        "avg_holding_count": float(weights.gt(0).sum(axis=1).mean()) if not weights.empty else 0.0,
        "avg_actual_exposure": float(diagnostics["actual_exposure"].mean()) if not diagnostics.empty else 0.0,
        "avg_target_exposure": float(diagnostics["target_exposure"].mean()) if not diagnostics.empty else 0.0,
        "avg_cash_ratio": float(diagnostics["cash_ratio"].mean()) if not diagnostics.empty else 0.0,
        "idle_reason_days": diagnostics["idle_reason"].value_counts().to_dict() if not diagnostics.empty else {},
        "idle_reason_avg_cash_ratio": (
            diagnostics.groupby("idle_reason")["cash_ratio"].mean().to_dict() if not diagnostics.empty else {}
        ),
    }
    return SmallCapitalBacktestResult(
        metrics=metrics,
        equity_curve=equity_curve,
        returns=returns,
        weights=weights,
        trades=trades,
        order_intents=order_intents,
        diagnostics=diagnostics,
        summary=summary,
    )


def _cash_idle_reason(
    *,
    market_gate_on: bool | None,
    signal_count: int | None,
    target_holding_count: int,
    actual_holding_count: int,
    target_exposure: float,
    actual_exposure: float,
) -> str:
    """给每日现金闲置归因，用于区分主动风控和执行买不满。"""

    eps = 1e-6
    if target_exposure <= eps:
        if market_gate_on is False:
            return "market_risk_off"
        if signal_count == 0 or target_holding_count == 0:
            return "no_signal"
        return "no_target_other"
    if actual_exposure >= target_exposure - 0.01:
        return "near_target"
    if target_exposure < 1.0 - eps:
        return "position_cap_cash"
    if actual_holding_count < target_holding_count:
        return "lot_cash_price_constraint"
    return "lot_rounding_cash"


def _trade_fee(gross_value: float, config: SmallCapitalExecutionConfig, *, sell: bool) -> float:
    commission = max(config.min_commission, gross_value * config.commission_bps / 10000.0)
    stamp_duty = gross_value * config.stamp_duty_sell_bps / 10000.0 if sell else 0.0
    return round(commission + stamp_duty, 2)


def _trade_row(date: object, symbol: object, side: str, shares: int, price: float, gross: float, fee: float, cash_after: float) -> dict[str, object]:
    return {
        "date": date,
        "symbol": str(symbol),
        "side": side,
        "shares": shares,
        "price": float(price),
        "gross_value": float(gross),
        "fee": float(fee),
        "cash_after": float(cash_after),
    }


def _order_intent_row(
    *,
    date: object,
    symbol: object,
    side: str,
    target_weight: float,
    target_shares: int,
    current_shares: int,
    requested_shares: int,
    price: float,
    cash_before: float,
    executed_shares: int,
    status: str,
    reason: str,
) -> dict[str, object]:
    return {
        "date": date,
        "symbol": str(symbol),
        "side": side,
        "target_weight": float(target_weight),
        "target_shares": int(target_shares),
        "current_shares": int(current_shares),
        "requested_shares": int(requested_shares),
        "requested_notional": float(requested_shares * price),
        "open_price": float(price),
        "cash_before": float(cash_before),
        "executed_shares": int(executed_shares),
        "executed_notional": float(executed_shares * price),
        "at_least_one_lot_required": bool(requested_shares >= 100),
        "status": status,
        "reason": reason,
    }


def _validate_small_capital_inputs(
    target_weights: pd.DataFrame,
    open_prices: pd.DataFrame,
    close_prices: pd.DataFrame,
    config: SmallCapitalExecutionConfig,
) -> None:
    if target_weights.empty:
        raise ValueError("target_weights 不能为空")
    if open_prices.empty or close_prices.empty:
        raise ValueError("open_prices/close_prices 不能为空")
    if config.initial_cash <= 0:
        raise ValueError("initial_cash 必须为正数")
    if config.lot_size <= 0:
        raise ValueError("lot_size 必须为正整数")
    if config.min_trade_value < 0:
        raise ValueError("min_trade_value 不能为负")
    if config.commission_bps < 0 or config.stamp_duty_sell_bps < 0:
        raise ValueError("交易费率不能为负")
