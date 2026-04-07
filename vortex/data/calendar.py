"""DataCalendar — Data 域交易日历服务（06 §3.1）。

从 Provider 获取并缓存在本地 Parquet。
与 shared/calendar.py 的 TradingCalendar 不同：此版本集成了 Provider 拉取和存储缓存。
"""
from __future__ import annotations

import bisect
from datetime import date, timedelta
from typing import TYPE_CHECKING

import pandas as pd

from vortex.shared.logging import get_logger

if TYPE_CHECKING:
    from vortex.data.provider.base import DataProvider
    from vortex.data.storage.base import StorageBackend

logger = get_logger(__name__)


class DataCalendar:
    """Data 域交易日历服务。

    首次调用时从 Provider 拉取交易日历，写入存储后端缓存；
    后续调用优先从缓存读取，避免重复网络请求。
    """

    def __init__(
        self,
        storage: StorageBackend,
        provider: DataProvider | None = None,
    ) -> None:
        self._storage = storage
        self._provider = provider
        self._cache: list[date] = []
        self._cache_set: set[date] = set()

    def load_or_fetch(
        self, market: str, start: date, end: date,
    ) -> list[date]:
        """加载或拉取交易日历。

        优先从存储缓存读取；缓存未命中或覆盖不完整则从 Provider 拉取并合并。
        返回 [start, end] 闭区间内排序的交易日列表。
        """
        # 尝试从内存缓存读取（验证完整覆盖）
        if self._cache and self._covers_range(start, end):
            lo = bisect.bisect_left(self._cache, start)
            hi = bisect.bisect_right(self._cache, end)
            return self._cache[lo:hi]

        # 尝试从存储后端读取
        try:
            cached_df = self._storage.read("calendar")
            if not cached_df.empty and "cal_date" in cached_df.columns:
                days = self._parse_dates(cached_df["cal_date"])
                self._update_cache(days)
                if self._covers_range(start, end):
                    lo = bisect.bisect_left(self._cache, start)
                    hi = bisect.bisect_right(self._cache, end)
                    result = self._cache[lo:hi]
                    logger.debug("从存储缓存加载交易日历: %d 天", len(result))
                    return result
        except Exception:
            logger.debug("存储缓存读取失败，将从 Provider 拉取")

        # 从 Provider 拉取（缓存不完整或不存在）
        if self._provider is None:
            logger.warning("无 Provider 且缓存覆盖不完整，返回已有数据")
            lo = bisect.bisect_left(self._cache, start)
            hi = bisect.bisect_right(self._cache, end)
            return self._cache[lo:hi]

        days = self._provider.fetch_calendar(market, start, end)
        if days:
            self._update_cache(days)
            # 写入存储缓存
            cal_df = pd.DataFrame({
                "cal_date": [d.strftime("%Y%m%d") for d in self._cache],
            })
            try:
                self._storage.upsert("calendar", cal_df, {})
            except Exception:
                logger.warning("交易日历写入缓存失败", exc_info=True)

        return sorted(d for d in days if start <= d <= end)

    def is_trading_day(self, d: date) -> bool:
        """判断是否为交易日。需先调用 load_or_fetch 加载数据。"""
        return d in self._cache_set

    def next_trading_day(self, d: date) -> date:
        """返回 d 之后（不含 d）的最近交易日。"""
        if not self._cache:
            raise ValueError("交易日历尚未加载")
        idx = bisect.bisect_right(self._cache, d)
        if idx < len(self._cache):
            return self._cache[idx]
        # 超出范围逐日探测
        for offset in range(1, 31):
            candidate = d + timedelta(days=offset)
            if candidate in self._cache_set:
                return candidate
        raise ValueError(f"无法找到 {d} 之后的交易日（日历可能不完整）")

    def prev_trading_day(self, d: date) -> date:
        """返回 d 之前（不含 d）的最近交易日。"""
        if not self._cache:
            raise ValueError("交易日历尚未加载")
        idx = bisect.bisect_left(self._cache, d) - 1
        if idx >= 0:
            return self._cache[idx]
        for offset in range(1, 31):
            candidate = d - timedelta(days=offset)
            if candidate in self._cache_set:
                return candidate
        raise ValueError(f"无法找到 {d} 之前的交易日（日历可能不完整）")

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    def _update_cache(self, days: list[date]) -> None:
        merged = self._cache_set | set(days)
        self._cache = sorted(merged)
        self._cache_set = merged

    def _covers_range(self, start: date, end: date) -> bool:
        """检查缓存是否完整覆盖 [start, end] 范围。

        判断标准：缓存的最小日期 <= start 且最大日期 >= end。
        """
        if not self._cache:
            return False
        return self._cache[0] <= start and self._cache[-1] >= end

    @staticmethod
    def _parse_dates(series: pd.Series) -> list[date]:
        result: list[date] = []
        for val in series:
            if isinstance(val, date):
                result.append(val)
            else:
                s = str(val).strip().replace("-", "")[:8]
                if len(s) == 8 and s.isdigit():
                    result.append(date(int(s[:4]), int(s[4:6]), int(s[6:8])))
        return result
