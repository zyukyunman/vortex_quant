"""A 股交易日历服务。

默认日历为空，需通过 load_from_csv / load_from_dataframe 加载实际交易日数据。
"""
from __future__ import annotations

import csv
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


class TradingCalendar:
    """交易日历。内部维护一个排序的交易日集合。"""

    def __init__(self) -> None:
        self._trading_days: set[date] = set()
        self._sorted_days: list[date] = []

    # ------------------------------------------------------------------
    # 数据加载
    # ------------------------------------------------------------------

    def load_from_csv(self, path: str | Path) -> None:
        """从 CSV 加载交易日（需包含 cal_date 列，格式 YYYYMMDD）。"""
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            days: list[date] = []
            for row in reader:
                raw = row.get("cal_date", "").strip()
                if raw:
                    days.append(date(int(raw[:4]), int(raw[4:6]), int(raw[6:8])))
        self._trading_days = set(days)
        self._sorted_days = sorted(days)

    def load_from_dataframe(self, df: pd.DataFrame) -> None:
        """从 DataFrame 加载交易日（需包含 cal_date 列，格式 YYYYMMDD str 或 date 对象）。"""
        days: list[date] = []
        for val in df["cal_date"]:
            if isinstance(val, date):
                days.append(val)
            else:
                raw = str(val).strip()
                days.append(date(int(raw[:4]), int(raw[4:6]), int(raw[6:8])))
        self._trading_days = set(days)
        self._sorted_days = sorted(days)

    # ------------------------------------------------------------------
    # 查询接口
    # ------------------------------------------------------------------

    def is_trading_day(self, d: date) -> bool:
        return d in self._trading_days

    def next_trading_day(self, d: date) -> date:
        """返回 d 之后（不含 d）的最近交易日。"""
        if not self._sorted_days:
            raise ValueError("交易日历尚未加载")
        # 二分查找
        import bisect

        idx = bisect.bisect_right(self._sorted_days, d)
        if idx < len(self._sorted_days):
            return self._sorted_days[idx]
        # 超出范围时逐日探测（上限 30 天）
        for offset in range(1, 31):
            candidate = d + timedelta(days=offset)
            if candidate in self._trading_days:
                return candidate
        raise ValueError(f"无法找到 {d} 之后的交易日（日历可能不完整）")

    def prev_trading_day(self, d: date) -> date:
        """返回 d 之前（不含 d）的最近交易日。"""
        if not self._sorted_days:
            raise ValueError("交易日历尚未加载")
        import bisect

        idx = bisect.bisect_left(self._sorted_days, d) - 1
        if idx >= 0:
            return self._sorted_days[idx]
        for offset in range(1, 31):
            candidate = d - timedelta(days=offset)
            if candidate in self._trading_days:
                return candidate
        raise ValueError(f"无法找到 {d} 之前的交易日（日历可能不完整）")

    def trading_days_between(self, start: date, end: date) -> list[date]:
        """返回 [start, end] 闭区间内的所有交易日。"""
        import bisect

        lo = bisect.bisect_left(self._sorted_days, start)
        hi = bisect.bisect_right(self._sorted_days, end)
        return self._sorted_days[lo:hi]
