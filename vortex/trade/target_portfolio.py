"""Build frozen target portfolios for the Trade domain."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

import pandas as pd

from vortex.trade.models import Lineage, TargetPortfolio, TargetPosition


@dataclass(frozen=True)
class TargetPortfolioBuildConfig:
    notional: float = 1_000_000.0
    lot_size: int = 100
    min_position_value: float = 0.0


def build_target_portfolio(
    targets: pd.DataFrame,
    *,
    trade_date: str,
    strategy_version: str,
    run_id: str,
    snapshot_id: str,
    config: TargetPortfolioBuildConfig | None = None,
) -> TargetPortfolio:
    """Convert target weights and prices into a frozen lot-rounded portfolio."""

    config = config or TargetPortfolioBuildConfig()
    _validate_config(config)
    required = {"symbol", "target_weight", "reference_price"}
    missing = required.difference(targets.columns)
    if missing:
        raise ValueError(f"target frame missing columns: {sorted(missing)}")
    rows = targets.copy()
    rows["symbol"] = rows["symbol"].astype(str)
    rows["target_weight"] = pd.to_numeric(rows["target_weight"], errors="raise").astype(float)
    rows["reference_price"] = pd.to_numeric(rows["reference_price"], errors="raise").astype(float)
    if (rows["target_weight"] < 0).any():
        raise ValueError("target_weight must be non-negative")
    if (rows["reference_price"] <= 0).any():
        raise ValueError("reference_price must be positive")
    if rows["target_weight"].sum() > 1.000001:
        raise ValueError("target_weight sum cannot exceed 1")

    positions: list[TargetPosition] = []
    invested = 0.0
    reason_series = rows["reason"] if "reason" in rows.columns else pd.Series("", index=rows.index)
    for row, reason in zip(rows.itertuples(index=False), reason_series, strict=False):
        target_value = float(config.notional * row.target_weight)
        shares = int(target_value / float(row.reference_price) // config.lot_size) * config.lot_size
        rounded_value = shares * float(row.reference_price)
        if shares <= 0 or rounded_value < config.min_position_value:
            continue
        invested += rounded_value
        positions.append(
            TargetPosition(
                symbol=str(row.symbol),
                target_weight=float(row.target_weight),
                target_value=float(rounded_value),
                target_shares=int(shares),
                reference_price=float(row.reference_price),
                reason=str(reason) if pd.notna(reason) else "",
            )
        )

    portfolio_id = _portfolio_id(trade_date, strategy_version, run_id, snapshot_id, positions)
    lineage = Lineage(
        portfolio_id=portfolio_id,
        strategy_version=strategy_version,
        strategy_run_id=run_id,
        snapshot_id=snapshot_id,
    )
    return TargetPortfolio(
        portfolio_id=portfolio_id,
        trade_date=trade_date,
        strategy_version=strategy_version,
        run_id=run_id,
        snapshot_id=snapshot_id,
        cash_target=float(config.notional - invested),
        positions=positions,
        lineage=lineage,
    )


def _validate_config(config: TargetPortfolioBuildConfig) -> None:
    if config.notional <= 0:
        raise ValueError("notional must be positive")
    if config.lot_size <= 0:
        raise ValueError("lot_size must be positive")
    if config.min_position_value < 0:
        raise ValueError("min_position_value must be non-negative")


def _portfolio_id(
    trade_date: str,
    strategy_version: str,
    run_id: str,
    snapshot_id: str,
    positions: list[TargetPosition],
) -> str:
    payload = "|".join(
        [trade_date, strategy_version, run_id, snapshot_id]
        + [f"{item.symbol}:{item.target_shares}:{item.reference_price:.4f}" for item in positions]
    )
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:10]
    return f"tp_{trade_date}_{digest}"
