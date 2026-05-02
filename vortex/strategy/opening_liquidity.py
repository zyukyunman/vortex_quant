"""Opening ask1 capacity analysis for small-capital strategy reliability."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import pandas as pd


@dataclass(frozen=True)
class OpeningLiquidityConfig:
    """Configuration for conservative ask1-based opening capacity analysis."""

    lot_size: int = 100
    volume_unit: Literal["shares", "lots"] = "shares"


@dataclass(frozen=True)
class OpeningLiquidityReport:
    """Order-level and aggregated opening capacity review."""

    order_level: pd.DataFrame = field(repr=False)
    daily_summary: pd.DataFrame = field(repr=False)
    overall_summary: dict[str, object]


def load_opening_snapshots(
    path: str | Path,
    *,
    config: OpeningLiquidityConfig | None = None,
) -> pd.DataFrame:
    """Load opening execution snapshots from a file or partitioned dataset directory."""

    config = config or OpeningLiquidityConfig()
    snapshot_path = Path(path).expanduser()
    if snapshot_path.is_dir():
        parquet_files = sorted(snapshot_path.rglob("*.parquet"))
        if parquet_files:
            frame = pd.concat(
                [pd.read_parquet(file_path) for file_path in parquet_files],
                ignore_index=True,
            )
        else:
            csv_files = sorted(snapshot_path.rglob("*.csv"))
            if csv_files:
                frame = pd.concat(
                    [pd.read_csv(file_path) for file_path in csv_files],
                    ignore_index=True,
                )
            else:
                raise ValueError(f"opening snapshot directory has no readable files: {snapshot_path}")
    else:
        suffix = snapshot_path.suffix.lower()
        if suffix == ".csv":
            frame = pd.read_csv(snapshot_path)
        elif suffix == ".json":
            raw = json.loads(snapshot_path.read_text(encoding="utf-8"))
            rows = raw.get("items", raw) if isinstance(raw, dict) else raw
            frame = pd.DataFrame(rows)
        elif suffix in {".parquet", ".pq"}:
            frame = pd.read_parquet(snapshot_path)
        else:
            raise ValueError(f"unsupported snapshot format: {snapshot_path.suffix}")
    return normalize_opening_snapshots(frame, config=config)


def normalize_opening_snapshots(
    snapshots: pd.DataFrame,
    *,
    config: OpeningLiquidityConfig | None = None,
) -> pd.DataFrame:
    """Normalize direct ask1 snapshots and auction datasets to a strict internal schema."""

    config = config or OpeningLiquidityConfig()
    frame = snapshots.copy()
    required = {"date", "symbol", "open_price", "ask1_price", "ask1_volume"}
    missing = required.difference(frame.columns)
    if missing:
        if {"date", "symbol", "close", "volume"} <= set(frame.columns):
            # stk_auction_o stores the final opening auction match in ``close``.
            frame = frame.assign(
                open_price=frame["close"],
                ask1_price=frame["close"],
                ask1_volume=frame["volume"],
            )
        elif {"date", "symbol", "price", "volume"} <= set(frame.columns):
            frame = frame.assign(
                open_price=frame["price"],
                ask1_price=frame["price"],
                ask1_volume=frame["volume"],
            )
        elif {"date", "symbol", "open", "volume"} <= set(frame.columns):
            frame = frame.assign(
                open_price=frame["open"],
                ask1_price=frame["open"],
                ask1_volume=frame["volume"],
            )
        else:
            raise ValueError(
                "opening snapshots missing columns: "
                f"{sorted(missing)}; expected direct snapshot columns "
                "[date, symbol, open_price, ask1_price, ask1_volume] or auction columns "
                "[date, symbol, close, volume] / [date, symbol, price, volume]"
            )
    frame["date"] = frame["date"].astype(str)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["open_price"] = pd.to_numeric(frame["open_price"], errors="raise").astype(float)
    frame["ask1_price"] = pd.to_numeric(frame["ask1_price"], errors="raise").astype(float)
    frame["ask1_volume"] = pd.to_numeric(frame["ask1_volume"], errors="raise").astype(float)
    if (frame["open_price"] <= 0).any() or (frame["ask1_price"] <= 0).any():
        raise ValueError("open_price and ask1_price must be positive")
    if (frame["ask1_volume"] < 0).any():
        raise ValueError("ask1_volume cannot be negative")
    if config.volume_unit == "lots":
        frame["ask1_shares"] = (frame["ask1_volume"] * config.lot_size).astype(int)
    else:
        frame["ask1_shares"] = frame["ask1_volume"].astype(int)
    frame["ask1_notional"] = frame["ask1_shares"] * frame["ask1_price"]
    return frame[["date", "symbol", "open_price", "ask1_price", "ask1_volume", "ask1_shares", "ask1_notional"]]


def analyze_opening_ask1_capacity(
    order_intents: pd.DataFrame,
    opening_snapshots: pd.DataFrame,
    *,
    config: OpeningLiquidityConfig | None = None,
    group_fields: list[str] | None = None,
) -> OpeningLiquidityReport:
    """Compare planned opening buy orders against ask1 volume as a conservative lower bound."""

    config = config or OpeningLiquidityConfig()
    _validate_order_intents(order_intents)
    snapshots = normalize_opening_snapshots(opening_snapshots, config=config)
    group_fields = list(group_fields or [])

    buys = order_intents.copy()
    buys["date"] = buys["date"].astype(str)
    buys["symbol"] = buys["symbol"].astype(str)
    buys = buys.loc[(buys["side"] == "buy") & (buys["requested_shares"] > 0)].copy()
    if buys.empty:
        empty = pd.DataFrame()
        return OpeningLiquidityReport(
            order_level=empty,
            daily_summary=empty,
            overall_summary={
                "buy_order_count": 0,
                "one_lot_feasible_rate": 0.0,
                "target_feasible_rate": 0.0,
                "requested_shares_total": 0,
                "covered_shares_total": 0,
                "covered_shares_ratio": 0.0,
            },
        )

    join_cols = ["date", "symbol"]
    merged = buys.merge(snapshots, on=join_cols, how="left", validate="m:1")
    merged["snapshot_available"] = merged["ask1_price"].notna()
    merged["one_lot_shares"] = config.lot_size
    merged["one_lot_feasible"] = merged["snapshot_available"] & (merged["ask1_shares"] >= config.lot_size)
    merged["target_feasible"] = merged["snapshot_available"] & (merged["ask1_shares"] >= merged["requested_shares"])
    merged["covered_shares"] = (
        merged[["requested_shares", "ask1_shares"]].min(axis=1).where(merged["snapshot_available"], 0).fillna(0).astype(int)
    )
    merged["covered_notional"] = merged["covered_shares"] * merged["ask1_price"].fillna(0.0)
    merged["coverage_ratio"] = (
        merged["covered_shares"].div(merged["requested_shares"].where(merged["requested_shares"] > 0)).fillna(0.0)
    )
    merged["required_vs_ask1_notional_ratio"] = (
        merged["requested_notional"].div(merged["ask1_notional"].where(merged["ask1_notional"] > 0)).fillna(pd.NA)
    )

    daily_group = group_fields + ["date"]
    daily_summary = (
        merged.groupby(daily_group, dropna=False)
        .apply(_daily_capacity_summary)
        .reset_index()
    )
    overall_summary = _overall_capacity_summary(merged)
    return OpeningLiquidityReport(order_level=merged, daily_summary=daily_summary, overall_summary=overall_summary)


def write_opening_liquidity_report(
    report: OpeningLiquidityReport,
    *,
    output_dir: str | Path,
    stem: str = "opening-liquidity-review",
) -> dict[str, Path]:
    """Write JSON / CSV / Markdown artifacts for opening capacity review."""

    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    csv_path = root / f"{stem}.csv"
    json_path = root / f"{stem}.json"
    md_path = root / f"{stem}.md"
    report.order_level.to_csv(csv_path, index=False)
    json_path.write_text(
        json.dumps(
            {
                "overall_summary": _jsonable(report.overall_summary),
                "daily_summary": _jsonable(report.daily_summary.to_dict(orient="records")),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    md_path.write_text(_markdown_report(report), encoding="utf-8")
    return {"csv_path": csv_path, "json_path": json_path, "md_path": md_path}


def _validate_order_intents(order_intents: pd.DataFrame) -> None:
    required = {"date", "symbol", "side", "requested_shares", "requested_notional"}
    missing = required.difference(order_intents.columns)
    if missing:
        raise ValueError(f"order_intents missing columns: {sorted(missing)}")


def _daily_capacity_summary(frame: pd.DataFrame) -> pd.Series:
    buy_order_count = int(len(frame))
    requested_shares_total = int(frame["requested_shares"].sum())
    covered_shares_total = int(frame["covered_shares"].sum())
    return pd.Series(
        {
            "buy_order_count": buy_order_count,
            "snapshot_coverage_count": int(frame["snapshot_available"].sum()),
            "one_lot_feasible_count": int(frame["one_lot_feasible"].sum()),
            "target_feasible_count": int(frame["target_feasible"].sum()),
            "all_one_lot_feasible": bool(frame["one_lot_feasible"].all()) if buy_order_count else False,
            "all_target_feasible": bool(frame["target_feasible"].all()) if buy_order_count else False,
            "requested_shares_total": requested_shares_total,
            "covered_shares_total": covered_shares_total,
            "covered_shares_ratio": float(covered_shares_total / requested_shares_total) if requested_shares_total else 0.0,
            "requested_notional_total": float(frame["requested_notional"].sum()),
            "covered_notional_total": float(frame["covered_notional"].sum()),
        }
    )


def _overall_capacity_summary(frame: pd.DataFrame) -> dict[str, object]:
    requested_shares_total = int(frame["requested_shares"].sum())
    covered_shares_total = int(frame["covered_shares"].sum())
    return {
        "buy_order_count": int(len(frame)),
        "trading_days": int(frame["date"].nunique()),
        "snapshot_coverage_rate": float(frame["snapshot_available"].mean()) if len(frame) else 0.0,
        "one_lot_feasible_rate": float(frame["one_lot_feasible"].mean()) if len(frame) else 0.0,
        "target_feasible_rate": float(frame["target_feasible"].mean()) if len(frame) else 0.0,
        "requested_shares_total": requested_shares_total,
        "covered_shares_total": covered_shares_total,
        "covered_shares_ratio": float(covered_shares_total / requested_shares_total) if requested_shares_total else 0.0,
        "requested_notional_total": float(frame["requested_notional"].sum()),
        "covered_notional_total": float(frame["covered_notional"].sum()),
        "all_one_lot_feasible_days": int(frame.groupby("date")["one_lot_feasible"].all().sum()),
        "all_target_feasible_days": int(frame.groupby("date")["target_feasible"].all().sum()),
    }


def _markdown_report(report: OpeningLiquidityReport) -> str:
    summary = report.overall_summary
    lines = [
        "# Opening Liquidity Review",
        "",
        "## Summary",
        "",
        f"- Buy orders: {summary['buy_order_count']}",
        f"- Trading days: {summary['trading_days']}",
        f"- Snapshot coverage rate: {float(summary['snapshot_coverage_rate']) * 100:.2f}%",
        f"- One-lot feasible rate: {float(summary['one_lot_feasible_rate']) * 100:.2f}%",
        f"- Target feasible rate: {float(summary['target_feasible_rate']) * 100:.2f}%",
        f"- Covered shares ratio: {float(summary['covered_shares_ratio']) * 100:.2f}%",
        "",
        "## Daily summary",
        "",
        "| Date | Buy orders | One lot feasible | Target feasible | Covered shares ratio |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in report.daily_summary.to_dict(orient="records"):
        lines.append(
            f"| {row['date']} | {row['buy_order_count']} | {row['one_lot_feasible_count']} | "
            f"{row['target_feasible_count']} | {float(row['covered_shares_ratio']) * 100:.2f}% |"
        )
    return "\n".join(lines) + "\n"


def _jsonable(value: Any) -> Any:
    if isinstance(value, pd.DataFrame):
        return value.to_dict(orient="records")
    if isinstance(value, pd.Series):
        return value.to_dict()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if pd.isna(value):
        return None
    return value
