"""事件型日频/盘中候选信号。

这里的信号只负责把事件表转换成 date × symbol 的排序分数。
事件可交易时点由调用方通过 `delay_days` 明确声明：

- `delay_days=1`：事件收盘后才可得，次日再进入回测，适合保守验证。
- `delay_days=0`：事件在盘中或临近收盘可观测，必须有分钟/实时事件流支撑。
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def build_limit_strength_signal(
    limit_events: pd.DataFrame,
    *,
    target_index: pd.Index,
    target_columns: pd.Index,
    liquidity_mask: pd.DataFrame | None = None,
    delay_days: int = 1,
) -> pd.DataFrame:
    """构建涨停强度信号。"""

    _validate_event_frame(limit_events)
    _validate_delay(delay_days)
    events = limit_events.copy()
    fd_amount = _numeric(events, "fd_amount")
    float_mv = _numeric(events, "float_mv").replace(0, np.nan)
    open_times = _numeric(events, "open_times")
    limit_times = _numeric(events, "limit_times")
    turnover = _numeric(events, "turnover_ratio")
    raw = (fd_amount / float_mv).replace([np.inf, -np.inf], np.nan)
    events["event_score"] = (
        raw
        + 0.3 * (-open_times).groupby(events["date"]).rank(pct=True)
        + 0.2 * limit_times.groupby(events["date"]).rank(pct=True)
        - 0.1 * turnover.groupby(events["date"]).rank(pct=True)
    )
    signal = _event_score_matrix(events, target_index=target_index, target_columns=target_columns)
    signal = signal.rank(axis=1, pct=True)
    if liquidity_mask is not None:
        signal = signal.where(liquidity_mask.reindex_like(signal).fillna(False))
    if delay_days:
        signal = signal.shift(delay_days)
    return signal


def build_top_list_signal(
    top_list: pd.DataFrame,
    *,
    target_index: pd.Index,
    target_columns: pd.Index,
    liquidity_mask: pd.DataFrame | None = None,
    delay_days: int = 1,
) -> pd.DataFrame:
    """构建龙虎榜净买入强度信号。"""

    _validate_event_frame(top_list)
    _validate_delay(delay_days)
    events = top_list.copy()
    net_amount = _numeric(events, "net_amount")
    amount = _numeric(events, "amount").replace(0, np.nan)
    net_rate = _numeric(events, "net_rate")
    pct_change = _numeric(events, "pct_change")
    events["event_score"] = (
        (net_amount / amount).replace([np.inf, -np.inf], np.nan)
        + 0.01 * net_rate
        + 0.2 * pct_change.groupby(events["date"]).rank(pct=True)
    )
    signal = _event_score_matrix(events, target_index=target_index, target_columns=target_columns)
    signal = signal.rank(axis=1, pct=True)
    if liquidity_mask is not None:
        signal = signal.where(liquidity_mask.reindex_like(signal).fillna(False))
    if delay_days:
        signal = signal.shift(delay_days)
    return signal


def build_forecast_surprise_signal(
    forecast_events: pd.DataFrame,
    *,
    target_index: pd.Index,
    target_columns: pd.Index,
    liquidity_mask: pd.DataFrame | None = None,
    delay_days: int = 1,
    hold_days: int = 1,
) -> pd.DataFrame:
    """构建业绩预告 surprise 信号。

    `forecast.ann_date` 是公告日期，不一定是交易日；这里会把事件映射到公告后
    第 `delay_days` 个可交易日，避免周末/节假日公告被 `reindex` 直接丢失。
    """

    _validate_financial_event_frame(forecast_events, date_column="ann_date")
    _validate_delay(delay_days)
    _validate_hold(hold_days)
    events = _align_financial_event_dates(
        forecast_events,
        target_index=target_index,
        date_column="ann_date",
        delay_days=delay_days,
    )
    if events.empty:
        return pd.DataFrame(index=target_index, columns=target_columns, dtype=float)
    p_change_min = _numeric(events, "p_change_min")
    p_change_max = _numeric(events, "p_change_max")
    avg_growth = (p_change_min.fillna(p_change_max) + p_change_max.fillna(p_change_min)) / 2
    type_score = events.get("type", pd.Series("", index=events.index)).astype(str).map(
        {
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
    )
    events["event_score"] = avg_growth.clip(-200, 500).fillna(0.0) / 100 + type_score.fillna(0.0)
    return _ranked_event_signal(
        events,
        target_index=target_index,
        target_columns=target_columns,
        liquidity_mask=liquidity_mask,
        hold_days=hold_days,
    )


def build_express_quality_signal(
    express_events: pd.DataFrame,
    *,
    target_index: pd.Index,
    target_columns: pd.Index,
    liquidity_mask: pd.DataFrame | None = None,
    delay_days: int = 1,
    hold_days: int = 1,
) -> pd.DataFrame:
    """构建业绩快报质量信号。

    信号偏向“利润同比增长 + ROE + EPS”的公告后漂移假设。它只使用快报
    `ann_date` 之后可见的信息，适合日频安全口径验证。
    """

    _validate_financial_event_frame(express_events, date_column="ann_date")
    _validate_delay(delay_days)
    _validate_hold(hold_days)
    events = _align_financial_event_dates(
        express_events,
        target_index=target_index,
        date_column="ann_date",
        delay_days=delay_days,
    )
    if events.empty:
        return pd.DataFrame(index=target_index, columns=target_columns, dtype=float)
    yoy_profit = _numeric(events, "yoy_net_profit").clip(-200, 500) / 100
    diluted_roe = _numeric(events, "diluted_roe").clip(-50, 80) / 20
    diluted_eps = _numeric(events, "diluted_eps").clip(-5, 10) / 2
    events["event_score"] = yoy_profit.fillna(0.0) + 0.5 * diluted_roe.fillna(0.0) + 0.2 * diluted_eps.fillna(0.0)
    return _ranked_event_signal(
        events,
        target_index=target_index,
        target_columns=target_columns,
        liquidity_mask=liquidity_mask,
        hold_days=hold_days,
    )


def build_hot_rank_signal(
    hot_events: pd.DataFrame,
    *,
    target_index: pd.Index,
    target_columns: pd.Index,
    liquidity_mask: pd.DataFrame | None = None,
    delay_days: int = 1,
    hold_days: int = 1,
) -> pd.DataFrame:
    """构建热榜排名事件信号。

    东方财富/同花顺热榜通常在收盘后或盘后更新；默认 `delay_days=1`，
    只在下一交易日进入策略，避免把盘后热度当成当日可交易信号。
    """

    _validate_event_frame(hot_events)
    _validate_delay(delay_days)
    _validate_hold(hold_days)
    events = hot_events.copy()
    rank = _numeric(events, "rank")
    hot = _numeric(events, "hot")
    pct_change = _numeric(events, "pct_change")
    events["event_score"] = (
        -rank.groupby(events["date"]).rank(pct=True)
        + 0.3 * hot.groupby(events["date"]).rank(pct=True).fillna(0.0)
        - 0.05 * pct_change.groupby(events["date"]).rank(pct=True).fillna(0.0)
    )
    signal = _ranked_event_signal(
        events,
        target_index=target_index,
        target_columns=target_columns,
        liquidity_mask=liquidity_mask,
        hold_days=hold_days,
    )
    if delay_days:
        signal = signal.shift(delay_days)
    return signal


def _event_score_matrix(
    events: pd.DataFrame,
    *,
    target_index: pd.Index,
    target_columns: pd.Index,
) -> pd.DataFrame:
    frame = events[["date", "symbol", "event_score"]].dropna()
    if frame.empty:
        return pd.DataFrame(index=target_index, columns=target_columns, dtype=float)
    matrix = frame.pivot_table(
        index="date",
        columns="symbol",
        values="event_score",
        aggfunc="max",
    )
    return matrix.reindex(index=target_index, columns=target_columns)


def _ranked_event_signal(
    events: pd.DataFrame,
    *,
    target_index: pd.Index,
    target_columns: pd.Index,
    liquidity_mask: pd.DataFrame | None,
    hold_days: int,
) -> pd.DataFrame:
    signal = _event_score_matrix(events, target_index=target_index, target_columns=target_columns)
    signal = signal.rank(axis=1, pct=True)
    if liquidity_mask is not None:
        signal = signal.where(liquidity_mask.reindex_like(signal).fillna(False))
    if hold_days > 1:
        signal = signal.ffill(limit=hold_days - 1)
    return signal


def _align_financial_event_dates(
    events: pd.DataFrame,
    *,
    target_index: pd.Index,
    date_column: str,
    delay_days: int,
) -> pd.DataFrame:
    target_dates = pd.to_numeric(pd.Series(target_index, index=target_index), errors="coerce")
    if target_dates.isna().any():
        raise ValueError("target_index 必须是 YYYYMMDD 形式的日期")
    sorted_dates = target_dates.astype(int).to_numpy()
    raw_dates = pd.to_numeric(events[date_column], errors="coerce")
    valid = raw_dates.notna()
    if not valid.any():
        return events.iloc[0:0].copy()
    base_positions = np.searchsorted(
        sorted_dates,
        raw_dates[valid].astype(int).to_numpy(),
        side="left" if delay_days == 0 else "right",
    )
    positions = base_positions + max(delay_days - 1, 0)
    aligned = events.loc[valid].copy()
    in_range = positions < len(target_index)
    aligned = aligned.loc[in_range]
    if aligned.empty:
        return aligned
    aligned["date"] = target_index.take(positions[in_range])
    return aligned


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(0.0, index=frame.index)
    return pd.to_numeric(frame[column], errors="coerce")


def _validate_event_frame(frame: pd.DataFrame) -> None:
    if frame.empty:
        raise ValueError("event frame 不能为空")
    missing = {"date", "symbol"} - set(frame.columns)
    if missing:
        raise ValueError(f"event frame 缺少字段: {sorted(missing)}")


def _validate_financial_event_frame(frame: pd.DataFrame, *, date_column: str) -> None:
    if frame.empty:
        raise ValueError("event frame 不能为空")
    missing = {"symbol", date_column} - set(frame.columns)
    if missing:
        raise ValueError(f"event frame 缺少字段: {sorted(missing)}")


def _validate_delay(delay_days: int) -> None:
    if delay_days < 0:
        raise ValueError("delay_days 不能为负")


def _validate_hold(hold_days: int) -> None:
    if hold_days <= 0:
        raise ValueError("hold_days 必须为正整数")
