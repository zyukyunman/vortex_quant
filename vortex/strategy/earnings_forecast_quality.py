"""业绩预告策略持仓质量审查。

该模块不重新定义 alpha，只在每日目标生成后补一层可解释性审查：
信号来自哪条 forecast 事件，最新已披露财报是否支持继续持有，以及
是否存在连续营收/利润负增长等需要人工复核的风险。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from vortex.trade.serialization import write_json


FORECAST_TYPE_SCORE: dict[str, float] = {
    "预增": 1.0,
    "略增": 0.5,
    "续盈": 0.3,
    "扭亏": 0.8,
    "减亏": 0.2,
    "预减": -0.8,
    "略减": -0.5,
    "首亏": -1.0,
    "续亏": -0.7,
}


@dataclass(frozen=True)
class HoldingQualityArtifacts:
    csv_path: Path
    json_path: Path
    summary: dict[str, Any]


def forecast_event_score(events: pd.DataFrame) -> pd.Series:
    """按业绩预告策略现有口径计算原始事件分数。"""

    if events.empty:
        return pd.Series(dtype=float)
    p_change_min = pd.to_numeric(events.get("p_change_min"), errors="coerce")
    p_change_max = pd.to_numeric(events.get("p_change_max"), errors="coerce")
    avg_growth = (p_change_min.fillna(p_change_max) + p_change_max.fillna(p_change_min)) / 2
    type_score = events.get("type", pd.Series("", index=events.index)).astype(str).map(
        FORECAST_TYPE_SCORE
    )
    return avg_growth.clip(-200, 500).fillna(0.0) / 100 + type_score.fillna(0.0)


def build_holding_quality_review(
    holdings: pd.DataFrame,
    *,
    forecast: pd.DataFrame,
    fina_indicator: pd.DataFrame,
    as_of: str,
    recent_report_count: int = 4,
) -> pd.DataFrame:
    """生成目标/实际持仓的基本面质量审查表。

    `holdings` 至少包含 `symbol`；其他列会原样保留。财报和预告均只使用
    `ann_date <= as_of` 的记录，避免把未来披露结果用于当日判断。
    """

    if holdings.empty:
        return pd.DataFrame()
    if "symbol" not in holdings.columns:
        raise ValueError("holdings must contain symbol")
    rows = holdings.copy()
    rows["symbol"] = rows["symbol"].astype(str)
    rows = rows.drop_duplicates("symbol", keep="first").reset_index(drop=True)

    forecast_snapshot = _latest_forecast_snapshot(forecast, rows["symbol"].tolist(), as_of)
    financial_snapshot = _latest_financial_snapshot(
        fina_indicator,
        rows["symbol"].tolist(),
        as_of,
        recent_report_count=recent_report_count,
    )
    review = rows.merge(forecast_snapshot, on="symbol", how="left").merge(
        financial_snapshot,
        on="symbol",
        how="left",
    )
    labels: list[str] = []
    reasons: list[str] = []
    for item in review.to_dict("records"):
        label, reason = _classify_holding(item)
        labels.append(label)
        reasons.append(reason)
    review["quality_label"] = labels
    review["quality_reason"] = reasons
    ordered = [
        "symbol",
        "quality_label",
        "quality_reason",
        "forecast_ann_date",
        "forecast_type",
        "forecast_p_change_min",
        "forecast_p_change_max",
        "raw_event_score",
        "financial_ann_date",
        "financial_report_date",
        "q_sales_yoy",
        "netprofit_yoy",
        "dt_netprofit_yoy",
        "revenue_negative_streak",
        "profit_negative_streak",
    ]
    passthrough = [col for col in review.columns if col not in ordered]
    return review[[col for col in ordered if col in review.columns] + passthrough]


def write_holding_quality_review(
    review: pd.DataFrame,
    *,
    csv_path: Path,
    json_path: Path,
    as_of: str,
) -> HoldingQualityArtifacts:
    """写出持仓质量审查 CSV/JSON，并返回摘要。"""

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    review.to_csv(csv_path, index=False)
    counts = review["quality_label"].value_counts().to_dict() if "quality_label" in review else {}
    summary = {
        "as_of": as_of,
        "holding_count": int(len(review)),
        "label_counts": {str(key): int(value) for key, value in counts.items()},
        "blocked_symbols": _symbols_by_label(review, "blocked"),
        "review_symbols": _symbols_by_label(review, "review"),
        "watch_symbols": _symbols_by_label(review, "watch"),
        "csv_path": str(csv_path),
        "json_path": str(json_path),
    }
    write_json(json_path, {"summary": summary, "rows": review.to_dict("records")})
    return HoldingQualityArtifacts(csv_path=csv_path, json_path=json_path, summary=summary)


def _latest_forecast_snapshot(forecast: pd.DataFrame, symbols: list[str], as_of: str) -> pd.DataFrame:
    base = pd.DataFrame({"symbol": symbols})
    columns = {
        "forecast_ann_date": "",
        "forecast_type": "",
        "forecast_p_change_min": pd.NA,
        "forecast_p_change_max": pd.NA,
        "raw_event_score": pd.NA,
    }
    if forecast.empty or not {"symbol", "ann_date"}.issubset(forecast.columns):
        return base.assign(**columns)
    frame = forecast.loc[
        forecast["symbol"].astype(str).isin(symbols) & (forecast["ann_date"].astype(str) <= as_of)
    ].copy()
    if frame.empty:
        return base.assign(**columns)
    frame["symbol"] = frame["symbol"].astype(str)
    frame["raw_event_score"] = forecast_event_score(frame)
    frame = frame.sort_values(["symbol", "ann_date"]).groupby("symbol", as_index=False).tail(1)
    frame = frame.rename(
        columns={
            "ann_date": "forecast_ann_date",
            "type": "forecast_type",
            "p_change_min": "forecast_p_change_min",
            "p_change_max": "forecast_p_change_max",
        }
    )
    keep = ["symbol", *columns.keys()]
    return base.merge(frame[[col for col in keep if col in frame.columns]], on="symbol", how="left")


def _latest_financial_snapshot(
    fina_indicator: pd.DataFrame,
    symbols: list[str],
    as_of: str,
    *,
    recent_report_count: int,
) -> pd.DataFrame:
    base = pd.DataFrame({"symbol": symbols})
    defaults = {
        "financial_ann_date": "",
        "financial_report_date": "",
        "q_sales_yoy": pd.NA,
        "netprofit_yoy": pd.NA,
        "dt_netprofit_yoy": pd.NA,
        "revenue_negative_streak": 0,
        "profit_negative_streak": 0,
    }
    if fina_indicator.empty or not {"symbol", "ann_date"}.issubset(fina_indicator.columns):
        return base.assign(**defaults)
    frame = fina_indicator.loc[
        fina_indicator["symbol"].astype(str).isin(symbols)
        & (fina_indicator["ann_date"].astype(str) <= as_of)
    ].copy()
    if frame.empty:
        return base.assign(**defaults)
    frame["symbol"] = frame["symbol"].astype(str)
    rows: list[dict[str, Any]] = []
    for symbol, group in frame.sort_values(["ann_date", "report_date"]).groupby("symbol"):
        recent = group.tail(recent_report_count)
        latest = recent.iloc[-1]
        profit_series = _profit_series(recent)
        rows.append(
            {
                "symbol": symbol,
                "financial_ann_date": str(latest.get("ann_date", "")),
                "financial_report_date": str(latest.get("report_date", "")),
                "q_sales_yoy": _number_or_na(latest.get("q_sales_yoy")),
                "netprofit_yoy": _number_or_na(latest.get("netprofit_yoy")),
                "dt_netprofit_yoy": _number_or_na(latest.get("dt_netprofit_yoy")),
                "revenue_negative_streak": _negative_streak(
                    pd.to_numeric(recent.get("q_sales_yoy"), errors="coerce")
                ),
                "profit_negative_streak": _negative_streak(profit_series),
            }
        )
    return base.merge(pd.DataFrame(rows), on="symbol", how="left").fillna(defaults)


def _classify_holding(item: dict[str, Any]) -> tuple[str, str]:
    reasons: list[str] = []
    raw_event_score = _to_float(item.get("raw_event_score"))
    q_sales_yoy = _to_float(item.get("q_sales_yoy"))
    netprofit_yoy = _to_float(item.get("netprofit_yoy"))
    dt_netprofit_yoy = _to_float(item.get("dt_netprofit_yoy"))
    revenue_streak = int(_to_float(item.get("revenue_negative_streak")) or 0)
    profit_streak = int(_to_float(item.get("profit_negative_streak")) or 0)
    if raw_event_score is not None and raw_event_score <= 0:
        reasons.append(f"非正业绩预告分数 {raw_event_score:.4f}")
        return "blocked", "; ".join(reasons)
    if not item.get("forecast_ann_date"):
        reasons.append("缺少可见业绩预告事件")
    if revenue_streak >= 2:
        reasons.append(f"营收同比连续 {revenue_streak} 期为负")
    if profit_streak >= 2:
        reasons.append(f"利润同比连续 {profit_streak} 期为负")
    if q_sales_yoy is not None and q_sales_yoy < 0 and (
        (netprofit_yoy is not None and netprofit_yoy < 0)
        or (dt_netprofit_yoy is not None and dt_netprofit_yoy < 0)
    ):
        reasons.append("最新收入与利润同比同时为负")
    if reasons:
        return "review", "; ".join(reasons)
    watch_reasons: list[str] = []
    if q_sales_yoy is not None and q_sales_yoy < 0:
        watch_reasons.append("最新收入同比为负")
    if (netprofit_yoy is not None and netprofit_yoy < 0) or (
        dt_netprofit_yoy is not None and dt_netprofit_yoy < 0
    ):
        watch_reasons.append("最新利润同比为负")
    if watch_reasons:
        return "watch", "; ".join(watch_reasons)
    return "pass", "forecast 与最新财报质量未见硬冲突"


def _profit_series(frame: pd.DataFrame) -> pd.Series:
    if "dt_netprofit_yoy" in frame.columns:
        primary = pd.to_numeric(frame["dt_netprofit_yoy"], errors="coerce")
    else:
        primary = pd.Series(pd.NA, index=frame.index, dtype="float64")
    if "netprofit_yoy" in frame.columns:
        fallback = pd.to_numeric(frame["netprofit_yoy"], errors="coerce")
        primary = primary.fillna(fallback)
    return primary


def _negative_streak(values: pd.Series) -> int:
    streak = 0
    for value in values.iloc[::-1]:
        if pd.isna(value) or float(value) >= 0:
            break
        streak += 1
    return streak


def _number_or_na(value: Any) -> float | Any:
    number = _to_float(value)
    return pd.NA if number is None else number


def _to_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _symbols_by_label(review: pd.DataFrame, label: str) -> list[str]:
    if review.empty or "quality_label" not in review.columns:
        return []
    return review.loc[review["quality_label"] == label, "symbol"].astype(str).tolist()
