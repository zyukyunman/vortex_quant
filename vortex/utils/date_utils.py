"""
date_utils.py
交易日历与日期工具

依赖 DataStore 中缓存的交易日历 Parquet 文件。
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# 模块级缓存
_trade_dates: Optional[pd.DatetimeIndex] = None


def load_trade_cal(data_dir: Path) -> pd.DatetimeIndex:
    """从 Parquet 加载交易日历并缓存"""
    global _trade_dates
    if _trade_dates is not None:
        return _trade_dates

    cal_path = data_dir / "meta" / "trade_cal.parquet"
    if not cal_path.exists():
        raise FileNotFoundError(
            f"交易日历文件不存在: {cal_path}\n请先运行 scripts/init_data.py 下载数据"
        )
    df = pd.read_parquet(cal_path)
    # trade_cal 表有 cal_date / is_open 两列
    open_dates = df.loc[df["is_open"] == 1, "cal_date"]
    _trade_dates = pd.DatetimeIndex(pd.to_datetime(open_dates, format="%Y%m%d"))
    _trade_dates = _trade_dates.sort_values()
    logger.debug("交易日历已加载, %d 个交易日", len(_trade_dates))
    return _trade_dates


def is_trade_day(date_str: str, data_dir: Path) -> bool:
    """判断是否为交易日"""
    cal = load_trade_cal(data_dir)
    dt = pd.Timestamp(date_str)
    return dt in cal


def get_prev_trade_date(date_str: str, data_dir: Path, n: int = 1) -> str:
    """获取前第 n 个交易日 (YYYYMMDD)"""
    cal = load_trade_cal(data_dir)
    dt = pd.Timestamp(date_str)
    past = cal[cal < dt]
    if len(past) < n:
        raise ValueError(f"交易日历中找不到 {date_str} 之前第 {n} 个交易日")
    return past[-n].strftime("%Y%m%d")


def get_recent_trade_dates(date_str: str, data_dir: Path, n: int = 1) -> List[str]:
    """获取截至 date_str(含)的最近 n 个交易日"""
    cal = load_trade_cal(data_dir)
    dt = pd.Timestamp(date_str)
    past = cal[cal <= dt]
    return [d.strftime("%Y%m%d") for d in past[-n:]]


def today_str() -> str:
    """当前日期 YYYYMMDD"""
    return datetime.now().strftime("%Y%m%d")


def get_fiscal_periods(n_years: int = 3) -> List[str]:
    """
    获取最近 n_years 年的年报期末日期。
    例: 当前 2026 年 → ['20251231', '20241231', '20231231']
    """
    current_year = datetime.now().year
    return [f"{current_year - i}1231" for i in range(1, n_years + 1)]


def get_quarterly_periods(n_quarters: int = 12) -> List[str]:
    """
    获取最近 n_quarters 个季度的期末日期。
    例: 当前 2026-03 → ['20251231', '20250930', '20250630', ...]
    """
    now = datetime.now()
    periods = []
    # 从当前往回推
    year, month = now.year, now.month
    # 找到最近已结束的季度
    quarter_ends = [3, 6, 9, 12]
    # 上一个季末
    for _ in range(n_quarters + 4):  # 多留余量
        for qm in reversed(quarter_ends):
            end_date = datetime(year, qm, 31 if qm == 12 else (30 if qm in (6, 9) else 31))
            if end_date < now and len(periods) < n_quarters:
                periods.append(end_date.strftime("%Y%m%d"))
        year -= 1
    return sorted(periods)[:n_quarters]
