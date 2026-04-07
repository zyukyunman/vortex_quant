"""精度工具函数（参见设计文档 06 §1.2）。

不同量纲使用不同精度以避免浮点累积误差：
- 价格 2 位、权重 4 位、IC 6 位、Sharpe 4 位
- 股数向下取整、佣金向上取整到分
"""
from __future__ import annotations

import math


def round_price(value: float) -> float:
    """价格保留 2 位小数。"""
    return round(value, 2)


def round_weight(value: float) -> float:
    """权重保留 4 位小数。"""
    return round(value, 4)


def round_ic(value: float) -> float:
    """IC 值保留 6 位小数。"""
    return round(value, 6)


def round_sharpe(value: float) -> float:
    """Sharpe 值保留 4 位小数。"""
    return round(value, 4)


def floor_shares(value: float) -> int:
    """股数向下取整到整数（不买零碎股）。"""
    return math.floor(value)


def ceil_commission(value: float) -> float:
    """佣金向上取整到分（2 位小数，确保不少收）。"""
    return math.ceil(value * 100) / 100
