"""指数日线市场状态因子。

在分钟数据权限不可用时，先用核心指数日线构建风险门控，避免策略在
明显下行阶段保持满仓暴露。
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class MarketStateConfig:
    """市场状态门控配置。"""

    benchmark: str = "000300.SH"
    confirmation_indices: tuple[str, ...] = ("000300.SH", "000905.SH", "000852.SH")
    momentum_window: int = 60
    support_window: int = 120
    min_momentum: float = 0.0
    support_break_pct: float = 0.03
    min_risk_on_confirmations: int = 2


def build_market_state(
    close: pd.DataFrame,
    config: MarketStateConfig | None = None,
) -> pd.DataFrame:
    """用指数收盘价构建市场 risk-on/risk-off 状态。"""

    config = config or MarketStateConfig()
    _validate_market_state_inputs(close, config)
    close = close.sort_index()
    available = [code for code in config.confirmation_indices if code in close.columns]

    momentum = close[available] / close[available].shift(config.momentum_window) - 1.0
    support = close[available].rolling(config.support_window, min_periods=config.support_window).min().shift(1)
    above_support = close[available] >= support * (1.0 - config.support_break_pct)
    momentum_ok = momentum >= config.min_momentum
    confirmations = (momentum_ok & above_support).sum(axis=1)

    benchmark_momentum = close[config.benchmark] / close[config.benchmark].shift(config.momentum_window) - 1.0
    benchmark_support = close[config.benchmark].rolling(
        config.support_window,
        min_periods=config.support_window,
    ).min().shift(1)
    benchmark_above_support = close[config.benchmark] >= benchmark_support * (1.0 - config.support_break_pct)
    risk_on = (confirmations >= config.min_risk_on_confirmations) & benchmark_above_support

    return pd.DataFrame(
        {
            "benchmark": config.benchmark,
            "benchmark_momentum": benchmark_momentum,
            "benchmark_above_support": benchmark_above_support.fillna(False),
            "risk_on_confirmations": confirmations.astype(int),
            "risk_on": risk_on.fillna(False).astype(bool),
        },
        index=close.index,
    )


def market_gate_from_state(state: pd.DataFrame) -> pd.Series:
    """提取可直接传入策略回测的 market_gate。"""

    if "risk_on" not in state.columns:
        raise ValueError("state 必须包含 risk_on 列")
    return state["risk_on"].fillna(False).astype(bool)


def _validate_market_state_inputs(close: pd.DataFrame, config: MarketStateConfig) -> None:
    if close.empty:
        raise ValueError("close 不能为空")
    if not close.index.is_monotonic_increasing:
        raise ValueError("close index 必须按日期升序排列")
    if config.benchmark not in close.columns:
        raise ValueError(f"benchmark 不存在: {config.benchmark}")
    if config.momentum_window <= 0:
        raise ValueError("momentum_window 必须为正整数")
    if config.support_window <= 0:
        raise ValueError("support_window 必须为正整数")
    if config.min_risk_on_confirmations <= 0:
        raise ValueError("min_risk_on_confirmations 必须为正整数")
    available = [code for code in config.confirmation_indices if code in close.columns]
    if len(available) < config.min_risk_on_confirmations:
        raise ValueError("可用确认指数数量少于 min_risk_on_confirmations")
