"""
base.py
因子基类 + 通用工具函数

所有因子继承 BaseFactor，统一注册/计算/检验接口。
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class BaseFactor(ABC):
    """因子抽象基类"""

    name: str = ""              # 因子名，如 "dividend_yield"
    category: str = ""          # 类别: value / quality / cashflow / momentum / technical
    direction: int = 1          # +1 = 越大越好, -1 = 越小越好
    description: str = ""

    @abstractmethod
    def compute(self, ds, date: str) -> pd.Series:
        """
        计算截面因子值

        Parameters
        ----------
        ds : DataStore
            数据源（只读）
        date : str
            计算日期 YYYYMMDD

        Returns
        -------
        pd.Series
            index=ts_code, value=因子原始值
        """
        ...

    def __repr__(self):
        return f"<Factor:{self.name} dir={self.direction:+d}>"


# ================================================================
#  通用工具函数
# ================================================================

def zscore(s: pd.Series, winsorize_std: float = 3.0) -> pd.Series:
    """
    截面标准化 (z-score)

    步骤:
      1. 去极值 (MAD 法或标准差缩尾)
      2. 减均值除以标准差

    Parameters
    ----------
    s : pd.Series
        因子原始值, index=ts_code
    winsorize_std : float
        缩尾标准差倍数
    """
    s = s.copy().astype(float)
    if s.dropna().empty or s.std() == 0:
        return pd.Series(0.0, index=s.index)

    # MAD 去极值
    median = s.median()
    mad = (s - median).abs().median()
    mad_e = 1.4826 * mad  # MAD → 标准差等价
    if mad_e > 0:
        lower = median - winsorize_std * mad_e
        upper = median + winsorize_std * mad_e
        s = s.clip(lower, upper)

    # z-score
    mean = s.mean()
    std = s.std()
    if std == 0:
        return pd.Series(0.0, index=s.index)
    return (s - mean) / std


def rank_pct(s: pd.Series) -> pd.Series:
    """截面百分比排名，0~1"""
    return s.rank(pct=True, na_option="keep")


def get_latest_annual_period(date: str) -> str:
    """
    根据当前日期推断最新可用的年报期末。
    4月30日前用前前年年报，之后用前一年年报。

    例: date='20260328' → '20241231' (2025年报可能还没出完)
         date='20260501' → '20251231'
    """
    year = int(date[:4])
    month = int(date[4:6])
    if month <= 4:
        return f"{year - 2}1231"
    return f"{year - 1}1231"


def get_latest_n_annual_periods(date: str, n: int = 3) -> list:
    """获取最近 n 个年报期末"""
    latest = get_latest_annual_period(date)
    latest_year = int(latest[:4])
    return [f"{latest_year - i}1231" for i in range(n)]
