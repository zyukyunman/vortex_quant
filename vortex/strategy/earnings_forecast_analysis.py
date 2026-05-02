"""业绩预告策略复盘分析工具。

这些函数不重新定义策略本身，而是基于已生成的日收益、持仓和基础数据，
回答“什么时候启动更合适”“持仓是否安全”“收益和容量如何取舍”等产品化问题。
"""
from __future__ import annotations

import json
from html import escape
from pathlib import Path
from typing import Iterable

import pandas as pd

from vortex.research.evaluation import FactorEvaluationResult, evaluate_factor
from vortex.research.event_signals import build_forecast_surprise_signal


def load_series_from_report(report_path: str | Path, key: str) -> pd.Series:
    """从标准 JSON 报告读取日期序列字段。"""

    payload = json.loads(Path(report_path).read_text(encoding="utf-8"))
    data = payload["result"][key]
    series = pd.Series(data, dtype=float)
    series.index = pd.Index(series.index.astype(str), name="date")
    return series.sort_index()


def analyze_launch_months(
    daily_returns: pd.Series,
    *,
    exposure: pd.Series | None = None,
    holding_count: pd.Series | None = None,
    horizons: Iterable[int] = (21, 42, 63, 126),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """按“每年每个月第一个交易日启动”统计后续收益和胜率。

    返回值：
    - summary：按启动月份聚合的胜率、平均收益、最差年份等。
    - detail：每一年、每一个启动月的明细，便于 HTML 展示。
    """

    returns = _normalise_date_series(daily_returns, "daily_returns")
    exposure = _normalise_date_series(exposure, "exposure").reindex(returns.index).fillna(0.0) if exposure is not None else None
    holding_count = (
        _normalise_date_series(holding_count, "holding_count").reindex(returns.index).fillna(0.0)
        if holding_count is not None
        else None
    )
    horizon_values = tuple(int(item) for item in horizons)
    if any(item <= 0 for item in horizon_values):
        raise ValueError("horizons 必须为正整数")

    dates = _to_datetime_index(returns.index)
    frame = pd.DataFrame({"date": returns.index, "return": returns.to_numpy()}, index=dates)
    if exposure is not None:
        frame["exposure"] = exposure.to_numpy()
    if holding_count is not None:
        frame["holding_count"] = holding_count.to_numpy()
    rows: list[dict[str, object]] = []

    for year, year_frame in frame.groupby(frame.index.year):
        for month in range(1, 13):
            month_frame = year_frame.loc[year_frame.index.month == month]
            if month_frame.empty:
                continue
            start_date = month_frame.index[0]
            tail = year_frame.loc[year_frame.index >= start_date]
            row: dict[str, object] = {
                "year": int(year),
                "start_month": int(month),
                "start_date": str(month_frame.iloc[0]["date"]),
                "trading_days_to_year_end": int(len(tail)),
                "return_to_year_end": compound_return(tail["return"]),
                "max_drawdown_to_year_end": max_drawdown(tail["return"]),
            }
            if "exposure" in tail.columns:
                row["avg_exposure_to_year_end"] = float(tail["exposure"].mean())
                row["cash_days_to_year_end"] = int((tail["exposure"] <= 1e-12).sum())
            if "holding_count" in tail.columns:
                row["avg_holding_count_to_year_end"] = float(tail["holding_count"].mean())
            for horizon in horizon_values:
                window = frame.loc[frame.index >= start_date].head(horizon)
                if len(window) < horizon:
                    row[f"return_{horizon}d"] = None
                    row[f"max_drawdown_{horizon}d"] = None
                else:
                    row[f"return_{horizon}d"] = compound_return(window["return"])
                    row[f"max_drawdown_{horizon}d"] = max_drawdown(window["return"])
            rows.append(row)

    detail = pd.DataFrame(rows)
    if detail.empty:
        return pd.DataFrame(), detail

    aggregations: dict[str, tuple[str, str]] = {
        "observations": ("return_to_year_end", "count"),
        "win_rate_to_year_end": ("return_to_year_end", _positive_rate),
        "avg_return_to_year_end": ("return_to_year_end", "mean"),
        "median_return_to_year_end": ("return_to_year_end", "median"),
        "worst_return_to_year_end": ("return_to_year_end", "min"),
        "avg_max_drawdown_to_year_end": ("max_drawdown_to_year_end", "mean"),
        "worst_max_drawdown_to_year_end": ("max_drawdown_to_year_end", "min"),
    }
    for horizon in horizon_values:
        aggregations[f"win_rate_{horizon}d"] = (f"return_{horizon}d", _positive_rate)
        aggregations[f"avg_return_{horizon}d"] = (f"return_{horizon}d", "mean")
        aggregations[f"worst_return_{horizon}d"] = (f"return_{horizon}d", "min")
    if "avg_exposure_to_year_end" in detail.columns:
        aggregations["avg_exposure_to_year_end"] = ("avg_exposure_to_year_end", "mean")
    if "avg_holding_count_to_year_end" in detail.columns:
        aggregations["avg_holding_count_to_year_end"] = ("avg_holding_count_to_year_end", "mean")
    summary = detail.groupby("start_month", as_index=False).agg(**aggregations)
    return summary, detail


def compound_return(returns: pd.Series) -> float:
    """复利收益。"""

    clean = pd.to_numeric(returns, errors="coerce").dropna()
    if clean.empty:
        return 0.0
    return float((1.0 + clean).prod() - 1.0)


def max_drawdown(returns: pd.Series) -> float:
    """由日收益计算最大回撤。"""

    clean = pd.to_numeric(returns, errors="coerce").fillna(0.0)
    if clean.empty:
        return 0.0
    equity = (1.0 + clean).cumprod()
    return float((equity / equity.cummax() - 1.0).min())


def write_launch_month_report_json(
    summary: pd.DataFrame,
    detail: pd.DataFrame,
    path: str | Path,
    *,
    metadata: dict[str, object] | None = None,
) -> Path:
    """写出启动月份研究 JSON。"""

    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "metadata": metadata or {},
        "summary": _records(summary),
        "detail": _records(detail),
    }
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output


def write_launch_month_report_html(
    summary: pd.DataFrame,
    detail: pd.DataFrame,
    path: str | Path,
    *,
    title: str = "业绩预告策略启动月份研究",
) -> Path:
    """写出可直接打开的启动月份 HTML 报告。"""

    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    best = summary.sort_values(["win_rate_to_year_end", "avg_return_to_year_end"], ascending=False).head(3)
    summary_percent = set(summary.columns) - {"start_month", "observations", "avg_holding_count_to_year_end"}
    best_percent = set(best.columns) - {"start_month", "observations", "avg_holding_count_to_year_end"}
    detail_percent = {col for col in detail.columns if "return" in col or "drawdown" in col or "exposure" in col}
    best_display = _label_launch_columns(best)
    summary_display = _label_launch_columns(summary)
    detail_display = _label_launch_columns(detail)
    html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>{escape(title)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #17202a; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0 28px; font-size: 13px; }}
    th, td {{ border: 1px solid #d6dde5; padding: 6px 8px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ background: #f3f6f9; }}
    .card {{ border: 1px solid #d6dde5; border-radius: 10px; padding: 16px; margin-bottom: 18px; }}
  </style>
</head>
<body>
  <h1>{escape(title)}</h1>
  <div class="card">
    <h2>怎么读</h2>
    <p>这张表回答的是：如果每年从某个月第一个交易日才开始运行策略，后面到年底的历史胜率和收益大概如何。它是上线节奏参考，不是硬性开关。</p>
  </div>
  <div class="card">
    <h2>历史上更友好的启动月份</h2>
    {_html_table(best_display, percent_columns=_label_set(best_percent))}
  </div>
  <div class="card">
    <h2>按启动月份汇总</h2>
    {_html_table(summary_display, percent_columns=_label_set(summary_percent))}
  </div>
  <div class="card">
    <h2>逐年明细</h2>
    {_html_table(detail_display, percent_columns=_label_set(detail_percent))}
  </div>
</body>
</html>
"""
    output.write_text(html, encoding="utf-8")
    return output


def evaluate_forecast_surprise_factor(
    forecast_events: pd.DataFrame,
    close_prices: pd.DataFrame,
    *,
    amount: pd.DataFrame | None = None,
    liquidity_window: int = 20,
    min_avg_amount: float = 30000.0,
    delay_days: int = 1,
    horizons: Iterable[int] = (1, 5, 20, 40, 60),
    long_short_horizon: int = 20,
    groups: int = 5,
    min_periods: int = 30,
) -> FactorEvaluationResult:
    """对 forecast surprise 做标准因子评测。

    这一步回答“业绩预告信号本身是否有预测力”，区别于完整策略回测。
    默认只在公告后可交易日产生因子值，不做持仓期前向填充。
    """

    if close_prices.empty:
        raise ValueError("close_prices 不能为空")
    close = close_prices.sort_index()
    liquidity = None
    if amount is not None:
        liquidity = _build_liquidity_mask(
            amount.reindex(index=close.index, columns=close.columns),
            window=liquidity_window,
            min_avg_amount=min_avg_amount,
        )
    factor = build_forecast_surprise_signal(
        forecast_events,
        target_index=close.index,
        target_columns=close.columns,
        liquidity_mask=liquidity,
        delay_days=delay_days,
        hold_days=1,
    )
    return evaluate_factor(
        factor,
        close,
        horizons=tuple(int(item) for item in horizons),
        long_short_horizon=long_short_horizon,
        groups=groups,
        min_periods=min_periods,
    )


def write_factor_evaluation_report_json(
    result: FactorEvaluationResult,
    path: str | Path,
    *,
    factor_name: str,
    metadata: dict[str, object] | None = None,
) -> Path:
    """写出轻量因子评测 JSON，供策略测试报告、审计和对比流程引用。"""

    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "factor": factor_name,
        "metadata": metadata or {},
        "result": result.to_dict(),
        "ic_series": {
            str(horizon): series.to_dict()
            for horizon, series in result.ic_series.items()
        },
        "long_short_series": result.long_short_series.to_dict(orient="index"),
    }
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output


def _normalise_date_series(series: pd.Series | None, name: str) -> pd.Series:
    if series is None:
        raise ValueError(f"{name} 不能为空")
    if series.empty:
        raise ValueError(f"{name} 不能为空")
    normalised = pd.Series(series.copy(), dtype=float)
    normalised.index = pd.Index(normalised.index.astype(str), name="date")
    return normalised.sort_index()


def _build_liquidity_mask(
    amount: pd.DataFrame,
    *,
    window: int,
    min_avg_amount: float,
) -> pd.DataFrame:
    if window <= 0:
        raise ValueError("liquidity_window 必须为正整数")
    if min_avg_amount < 0:
        raise ValueError("min_avg_amount 不能为负")
    return amount.sort_index().rolling(window, min_periods=1).mean() >= min_avg_amount


def _to_datetime_index(index: pd.Index) -> pd.DatetimeIndex:
    dates = pd.to_datetime(pd.Series(index.astype(str), index=index), format="%Y%m%d", errors="coerce")
    if dates.isna().any():
        dates = pd.to_datetime(pd.Series(index.astype(str), index=index), errors="coerce")
    if dates.isna().any():
        raise ValueError("日期索引无法解析")
    return pd.DatetimeIndex(dates)


def _positive_rate(values: pd.Series) -> float:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return 0.0
    return float((clean > 0).mean())


def _records(frame: pd.DataFrame) -> list[dict[str, object]]:
    return json.loads(frame.to_json(orient="records", force_ascii=False))


def _html_table(frame: pd.DataFrame, *, percent_columns: set[str]) -> str:
    if frame.empty:
        return "<p>无数据</p>"
    head = "".join(f"<th>{escape(str(col))}</th>" for col in frame.columns)
    body = []
    for _, row in frame.iterrows():
        cells = []
        for col in frame.columns:
            value = row[col]
            if pd.isna(value):
                text = ""
            elif col in percent_columns and isinstance(value, (int, float)):
                text = f"{value:.2%}"
            elif isinstance(value, float):
                text = f"{value:.4f}"
            else:
                text = str(value)
            cells.append(f"<td>{escape(text)}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return "<table><thead><tr>" + head + "</tr></thead><tbody>" + "".join(body) + "</tbody></table>"


_LAUNCH_COLUMN_LABELS = {
    "year": "年份",
    "start_month": "启动月份",
    "start_date": "启动日期",
    "observations": "样本年数",
    "trading_days_to_year_end": "到年底交易日",
    "return_to_year_end": "到年底收益",
    "max_drawdown_to_year_end": "到年底最大回撤",
    "win_rate_to_year_end": "到年底胜率",
    "avg_return_to_year_end": "到年底平均收益",
    "median_return_to_year_end": "到年底中位收益",
    "worst_return_to_year_end": "到年底最差收益",
    "avg_max_drawdown_to_year_end": "平均最大回撤",
    "worst_max_drawdown_to_year_end": "最差最大回撤",
    "avg_exposure_to_year_end": "到年底平均仓位",
    "cash_days_to_year_end": "到年底空仓天数",
    "avg_holding_count_to_year_end": "到年底平均持仓数",
}


def _label_launch_columns(frame: pd.DataFrame) -> pd.DataFrame:
    labels = dict(_LAUNCH_COLUMN_LABELS)
    for col in frame.columns:
        if col.startswith("return_") and col.endswith("d"):
            days = col.removeprefix("return_").removesuffix("d")
            labels[col] = f"未来约{days}日收益"
        elif col.startswith("max_drawdown_") and col.endswith("d"):
            days = col.removeprefix("max_drawdown_").removesuffix("d")
            labels[col] = f"未来约{days}日最大回撤"
        elif col.startswith("win_rate_") and col.endswith("d"):
            days = col.removeprefix("win_rate_").removesuffix("d")
            labels[col] = f"未来约{days}日胜率"
        elif col.startswith("avg_return_") and col.endswith("d"):
            days = col.removeprefix("avg_return_").removesuffix("d")
            labels[col] = f"未来约{days}日平均收益"
        elif col.startswith("worst_return_") and col.endswith("d"):
            days = col.removeprefix("worst_return_").removesuffix("d")
            labels[col] = f"未来约{days}日最差收益"
    return frame.rename(columns=labels)


def _label_set(columns: set[str]) -> set[str]:
    labels = _label_launch_columns(pd.DataFrame(columns=list(columns))).columns
    return set(labels)
