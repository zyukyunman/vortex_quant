"""Minute-level target-price execution capacity analysis."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class TargetPriceCapacityConfig:
    """Configuration for target-price minute capacity analysis."""

    participation_rates: tuple[float, ...] = (0.10, 0.20, 0.30)
    target_price_column: str = "open_price"
    lot_size: int = 100


@dataclass(frozen=True)
class TargetPriceCapacityReport:
    """Order-level and summary capacity report."""

    order_level: pd.DataFrame = field(repr=False)
    summary: pd.DataFrame = field(repr=False)
    daily_summary: pd.DataFrame = field(repr=False)
    overall_summary: dict[str, object]


def analyze_target_price_minute_capacity(
    order_intents: pd.DataFrame,
    minute_bars: pd.DataFrame,
    *,
    config: TargetPriceCapacityConfig | None = None,
    group_fields: Iterable[str] = (),
) -> TargetPriceCapacityReport:
    """Evaluate whether buy orders can be filled during minutes touching target price.

    A minute is considered target-price executable when ``low <= target_price <= high``.
    Available notional is the sum of matching minute ``amount`` multiplied by the
    configured participation rate.
    """

    config = config or TargetPriceCapacityConfig()
    group_fields = tuple(group_fields)
    buys = _normalize_buy_orders(order_intents, target_price_column=config.target_price_column)
    minutes = _normalize_minute_bars(minute_bars)
    if buys.empty:
        empty = pd.DataFrame()
        return TargetPriceCapacityReport(
            order_level=empty,
            summary=empty,
            daily_summary=empty,
            overall_summary={"buy_order_count": 0},
        )

    capacity = _target_price_capacity_by_order(
        buys,
        minutes,
        passthrough_columns=group_fields,
    )
    rows: list[pd.DataFrame] = []
    for participation_rate in config.participation_rates:
        frame = capacity.copy()
        frame["participation_rate"] = float(participation_rate)
        frame["available_notional"] = frame["target_price_matched_amount"] * float(participation_rate)
        frame["available_shares"] = (
            (frame["available_notional"] / frame["target_price"]).fillna(0.0).clip(lower=0.0)
        )
        frame["available_lot_shares"] = (
            (frame["available_shares"] // config.lot_size) * config.lot_size
        ).astype(int)
        frame["filled_notional"] = frame[["requested_notional", "available_notional"]].min(axis=1)
        frame["filled_shares"] = frame[["requested_shares", "available_lot_shares"]].min(axis=1).astype(int)
        frame["notional_fill_ratio"] = (
            frame["filled_notional"].div(frame["requested_notional"].where(frame["requested_notional"] > 0)).fillna(0.0)
        )
        frame["share_fill_ratio"] = (
            frame["filled_shares"].div(frame["requested_shares"].where(frame["requested_shares"] > 0)).fillna(0.0)
        )
        frame["target_notional_feasible"] = frame["available_notional"] >= frame["requested_notional"]
        frame["target_share_feasible"] = frame["available_lot_shares"] >= frame["requested_shares"]
        rows.append(frame)
    order_level = pd.concat(rows, ignore_index=True)
    summary = _summarize_capacity(order_level, group_fields=group_fields)
    daily_summary = _summarize_capacity(order_level, group_fields=(*group_fields, "date"))
    overall_summary = _overall_summary(order_level)
    return TargetPriceCapacityReport(
        order_level=order_level,
        summary=summary,
        daily_summary=daily_summary,
        overall_summary=overall_summary,
    )


def write_target_price_capacity_report(
    report: TargetPriceCapacityReport,
    *,
    output_dir: str | Path,
    stem: str,
) -> dict[str, Path]:
    """Write target-price capacity artifacts."""

    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    order_path = root / f"{stem}_orders.csv"
    summary_path = root / f"{stem}_summary.csv"
    daily_path = root / f"{stem}_daily.csv"
    json_path = root / f"{stem}.json"
    report.order_level.to_csv(order_path, index=False)
    report.summary.to_csv(summary_path, index=False)
    report.daily_summary.to_csv(daily_path, index=False)
    json_path.write_text(
        json.dumps(
            {
                "overall_summary": _jsonable(report.overall_summary),
                "summary_path": str(summary_path),
                "daily_summary_path": str(daily_path),
                "order_level_path": str(order_path),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {
        "json_path": json_path,
        "summary_path": summary_path,
        "daily_summary_path": daily_path,
        "order_level_path": order_path,
    }


def build_target_price_buy_share_limits(
    target_weights: pd.DataFrame,
    target_prices: pd.DataFrame,
    minute_bars: pd.DataFrame,
    *,
    participation_rate: float,
    lot_size: int = 100,
) -> pd.DataFrame:
    """Build date x symbol buy limits from target-price reachable minute amount.

    Unlike order-intent-only capacity reports, this matrix can be supplied to
    ``run_lot_constrained_backtest`` for every active target holding day. If a
    previous buy only partially fills and the symbol remains in the target
    portfolio, the backtest can naturally keep trying to buy on later days.
    """

    if participation_rate < 0:
        raise ValueError("participation_rate must be non-negative")
    if lot_size <= 0:
        raise ValueError("lot_size must be positive")
    common_dates = target_weights.index.intersection(target_prices.index)
    common_symbols = target_weights.columns.intersection(target_prices.columns)
    if len(common_dates) == 0 or len(common_symbols) == 0:
        return pd.DataFrame(index=target_weights.index, columns=target_weights.columns).fillna(0.0)

    weights = target_weights.loc[common_dates, common_symbols].fillna(0.0)
    prices = target_prices.loc[common_dates, common_symbols]
    active = weights.gt(0)
    active_rows = active.stack()
    active_rows = active_rows[active_rows]
    if active_rows.empty:
        return pd.DataFrame(0.0, index=target_weights.index, columns=target_weights.columns)

    requests = active_rows.rename("active").reset_index()
    requests.columns = ["date", "symbol", "active"]
    price_values = prices.stack().rename("target_price").reset_index()
    price_values.columns = ["date", "symbol", "target_price"]
    requests = requests.merge(price_values, on=["date", "symbol"], how="left")
    requests = requests.dropna(subset=["target_price"])
    requests["target_price"] = pd.to_numeric(requests["target_price"], errors="raise").astype(float)
    requests = requests.loc[requests["target_price"] > 0, ["date", "symbol", "target_price"]].copy()
    requests["_order_id"] = range(len(requests))

    minutes = _normalize_minute_bars(minute_bars)
    capacity = _target_price_capacity_by_order(
        requests.assign(
            requested_shares=lot_size,
            requested_notional=requests["target_price"] * lot_size,
        ),
        minutes,
    )
    capacity["available_lot_shares"] = (
        (
            capacity["target_price_matched_amount"] * float(participation_rate)
            / capacity["target_price"]
        ).fillna(0.0)
        // lot_size
        * lot_size
    ).astype(int)
    limits = capacity.pivot_table(
        index="date",
        columns="symbol",
        values="available_lot_shares",
        aggfunc="max",
    )
    return limits.reindex(index=target_weights.index, columns=target_weights.columns).fillna(0.0)


def _normalize_buy_orders(order_intents: pd.DataFrame, *, target_price_column: str) -> pd.DataFrame:
    required = {"date", "symbol", "side", "requested_shares", "requested_notional", target_price_column}
    missing = required.difference(order_intents.columns)
    if missing:
        raise ValueError(f"order_intents missing columns: {sorted(missing)}")
    frame = order_intents.copy()
    frame["date"] = frame["date"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["requested_shares"] = pd.to_numeric(frame["requested_shares"], errors="raise").astype(int)
    frame["requested_notional"] = pd.to_numeric(frame["requested_notional"], errors="raise").astype(float)
    frame["target_price"] = pd.to_numeric(frame[target_price_column], errors="raise").astype(float)
    frame = frame.loc[
        (frame["side"] == "buy")
        & (frame["requested_shares"] > 0)
        & (frame["requested_notional"] > 0)
        & (frame["target_price"] > 0)
    ].copy()
    frame["_order_id"] = range(len(frame))
    return frame


def _normalize_minute_bars(minute_bars: pd.DataFrame) -> pd.DataFrame:
    required = {"date", "symbol", "open", "high", "low", "close", "amount"}
    missing = required.difference(minute_bars.columns)
    if missing:
        raise ValueError(f"minute_bars missing columns: {sorted(missing)}")
    frame = minute_bars.copy()
    frame["date"] = frame["date"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    for column in ["open", "high", "low", "close", "amount"]:
        frame[column] = pd.to_numeric(frame[column], errors="raise").astype(float)
    return frame.loc[(frame["high"] > 0) & (frame["low"] > 0) & (frame["amount"] > 0)].copy()


def _target_price_capacity_by_order(
    buys: pd.DataFrame,
    minutes: pd.DataFrame,
    *,
    passthrough_columns: tuple[str, ...] = (),
) -> pd.DataFrame:
    joined = buys.merge(
        minutes,
        on=["date", "symbol"],
        how="left",
        suffixes=("", "_minute"),
    )
    joined["target_price_touched"] = (
        joined["low"].le(joined["target_price"])
        & joined["high"].ge(joined["target_price"])
    ).fillna(False)
    joined["matched_amount"] = joined["amount"].where(joined["target_price_touched"], 0.0).fillna(0.0)
    joined["matched_minutes"] = joined["target_price_touched"].astype(int)
    grouped = (
        joined.groupby("_order_id", as_index=False)
        .agg(
            target_price_matched_amount=("matched_amount", "sum"),
            target_price_matched_minutes=("matched_minutes", "sum"),
            minute_rows=("amount", "count"),
        )
    )
    base_cols = [
        "_order_id",
        "date",
        "symbol",
        "target_price",
        "requested_shares",
        "requested_notional",
    ]
    optional_cols = [
        column
        for column in ["target_weight", "target_shares", "current_shares", "cash_before", "status", "reason"]
        if column in buys.columns
    ]
    for column in passthrough_columns:
        if column in buys.columns and column not in optional_cols and column not in base_cols:
            optional_cols.append(column)
    return buys[base_cols + optional_cols].merge(grouped, on="_order_id", how="left").fillna(
        {
            "target_price_matched_amount": 0.0,
            "target_price_matched_minutes": 0,
            "minute_rows": 0,
        }
    )


def _summarize_capacity(order_level: pd.DataFrame, *, group_fields: tuple[str, ...]) -> pd.DataFrame:
    keys = [*group_fields, "participation_rate"]
    return (
        order_level.groupby(keys, dropna=False)
        .apply(_summary_row)
        .reset_index()
    )


def _summary_row(frame: pd.DataFrame) -> pd.Series:
    requested = float(frame["requested_notional"].sum())
    filled = float(frame["filled_notional"].sum())
    trading_days = int(frame["date"].nunique()) if "date" in frame.columns else 1
    return pd.Series(
        {
            "buy_order_count": int(len(frame)),
            "trading_days": trading_days,
            "target_price_touch_rate": float((frame["target_price_matched_minutes"] > 0).mean()) if len(frame) else 0.0,
            "target_notional_feasible_rate": float(frame["target_notional_feasible"].mean()) if len(frame) else 0.0,
            "target_share_feasible_rate": float(frame["target_share_feasible"].mean()) if len(frame) else 0.0,
            "requested_notional_total": requested,
            "matched_amount_total": float(frame["target_price_matched_amount"].sum()),
            "available_notional_total": float(frame["available_notional"].sum()),
            "filled_notional_total": filled,
            "filled_notional_ratio": filled / requested if requested else 0.0,
            "p50_required_to_matched_amount": _safe_quantile(
                frame["requested_notional"].div(
                    frame["target_price_matched_amount"].where(frame["target_price_matched_amount"] > 0)
                ),
                0.50,
            ),
            "p90_required_to_matched_amount": _safe_quantile(
                frame["requested_notional"].div(
                    frame["target_price_matched_amount"].where(frame["target_price_matched_amount"] > 0)
                ),
                0.90,
            ),
        }
    )


def _overall_summary(order_level: pd.DataFrame) -> dict[str, object]:
    if order_level.empty:
        return {"buy_order_count": 0}
    return {
        "buy_order_count": int(order_level["_order_id"].nunique()),
        "participation_rates": sorted(float(value) for value in order_level["participation_rate"].unique()),
        "trading_days": int(order_level["date"].nunique()),
        "symbols": int(order_level["symbol"].nunique()),
    }


def _safe_quantile(series: pd.Series, q: float) -> float:
    clean = pd.to_numeric(series, errors="coerce")
    clean = clean[clean.notna() & (clean != float("inf")) & (clean != float("-inf"))]
    return float(clean.quantile(q)) if not clean.empty else 0.0


def _jsonable(value):
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if hasattr(value, "item"):
        return value.item()
    return value
