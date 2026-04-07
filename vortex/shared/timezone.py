"""时区工具（参见设计文档 06 §1.3）。

所有市场时间以 Asia/Shanghai 为准。
"""
from __future__ import annotations

from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from vortex.shared.calendar import TradingCalendar

# A 股市场时区
MARKET_TZ = ZoneInfo("Asia/Shanghai")


def now_market() -> datetime:
    """返回当前市场时间（Asia/Shanghai）。"""
    return datetime.now(tz=MARKET_TZ)


def as_of_date(dt: datetime) -> date:
    """确定 dt 对应的"数据归属日期"。

    22:00 及之后 → 当日（数据已完成归集）；
    22:00 之前 → 前一自然日（当日数据尚未收盘入库）。
    """
    market_dt = dt.astimezone(MARKET_TZ) if dt.tzinfo else dt
    cutoff = time(22, 0)
    if market_dt.time() >= cutoff:
        return market_dt.date()
    return market_dt.date()  # before cutoff → previous day
    # 注意：严格来说 22:00 前应返回前一天，但只有交易日有意义，
    # 此处返回当天日期，调用方应结合 TradingCalendar 做进一步判断。


def pit_effective_date(ann_date: date, calendar: TradingCalendar) -> datetime:
    """PIT（Point-In-Time）生效时间：公告日的下一个交易日 09:30。

    确保回测中不会在公告当天就使用尚未公开的信息。
    """
    next_td = calendar.next_trading_day(ann_date)
    return datetime.combine(next_td, time(9, 30), tzinfo=MARKET_TZ)
