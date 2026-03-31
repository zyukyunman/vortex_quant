"""
engine.py
L6 PortfolioEngine — 组合构建 & 再平衡

职责:
  - 多策略信号合并
  - 目标持仓计算 (等权 / 优化)
  - 行业/个股约束
  - 再平衡指令生成
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from vortex.models import Signal

logger = logging.getLogger(__name__)


@dataclass
class Position:
    """持仓头寸"""
    ts_code: str
    name: str
    weight: float          # 目标权重
    shares: int = 0        # 目标股数
    cost: float = 0.0      # 成本价
    market_value: float = 0.0


@dataclass
class RebalanceOrder:
    """再平衡指令"""
    ts_code: str
    name: str
    action: str            # buy / sell / hold
    target_weight: float
    current_weight: float
    delta_weight: float    # 正=加仓, 负=减仓
    reason: str = ""


class PortfolioEngine:
    """
    组合管理引擎

    接收 Signal → 输出 target_positions + rebalance_orders
    """

    def __init__(
        self,
        max_position_pct: float = 0.10,
        max_industry_pct: float = 0.30,
        min_trade_weight: float = 0.005,
    ):
        self.max_position_pct = max_position_pct
        self.max_industry_pct = max_industry_pct
        self.min_trade_weight = min_trade_weight

        self._current_positions: Dict[str, Position] = {}
        self._history: List[Dict] = []

    @property
    def current_positions(self) -> Dict[str, Position]:
        return self._current_positions

    def merge_signals(
        self, signals: List[Signal], method: str = "score_weighted"
    ) -> pd.Series:
        """
        多策略信号合并为目标权重

        Parameters
        ----------
        signals : List[Signal]
            来自一个或多个策略的信号
        method : str
            合并方式: equal / score_weighted

        Returns
        -------
        pd.Series
            index=ts_code, values=target_weight
        """
        if not signals:
            return pd.Series(dtype=float)

        df = pd.DataFrame([
            {"ts_code": s.ts_code, "name": s.name, "weight": s.weight,
             "score": s.score, "strategy": s.strategy}
            for s in signals if s.action == "buy"
        ])

        if df.empty:
            return pd.Series(dtype=float)

        if method == "equal":
            grouped = df.groupby("ts_code").first()
            n = len(grouped)
            return pd.Series(1.0 / n, index=grouped.index)
        else:
            # 按 score 加权平均
            grouped = df.groupby("ts_code").agg({
                "weight": "mean", "score": "sum"
            })
            weights = grouped["score"] / grouped["score"].sum()
            return weights

    def apply_constraints(
        self,
        target_weights: pd.Series,
        industry_map: Optional[pd.Series] = None,
    ) -> pd.Series:
        """应用个股和行业权重约束"""
        w = target_weights.copy()

        # 个股上限
        w = w.clip(upper=self.max_position_pct)

        # 行业上限
        if industry_map is not None:
            industries = industry_map.reindex(w.index).fillna("未知")
            for industry in industries.unique():
                mask = industries == industry
                ind_weight = w[mask].sum()
                if ind_weight > self.max_industry_pct:
                    scale = self.max_industry_pct / ind_weight
                    w[mask] *= scale

        # 归一化
        if w.sum() > 0:
            w = w / w.sum()

        return w

    def generate_rebalance(
        self, target_weights: pd.Series, name_map: Optional[Dict] = None,
    ) -> List[RebalanceOrder]:
        """
        对比当前持仓 → 生成再平衡指令

        Parameters
        ----------
        target_weights : pd.Series
            目标权重 (index=ts_code)
        name_map : Dict
            ts_code → 股票名称

        Returns
        -------
        List[RebalanceOrder]
            需要执行的交易指令
        """
        name_map = name_map or {}
        orders: List[RebalanceOrder] = []

        current_weights = pd.Series({
            code: pos.weight for code, pos in self._current_positions.items()
        }, dtype=float)

        all_codes = set(target_weights.index) | set(current_weights.index)

        for code in sorted(all_codes):
            target_w = target_weights.get(code, 0.0)
            current_w = current_weights.get(code, 0.0)
            delta = target_w - current_w

            if abs(delta) < self.min_trade_weight:
                continue

            if delta > 0:
                action = "buy"
                reason = f"加仓 {delta:.2%}"
            else:
                action = "sell"
                reason = f"减仓 {abs(delta):.2%}"

            orders.append(RebalanceOrder(
                ts_code=code,
                name=name_map.get(code, code),
                action=action,
                target_weight=target_w,
                current_weight=current_w,
                delta_weight=delta,
                reason=reason,
            ))

        logger.info("再平衡指令: %d 条 (买入 %d, 卖出 %d)",
                     len(orders),
                     sum(1 for o in orders if o.action == "buy"),
                     sum(1 for o in orders if o.action == "sell"))
        return orders

    def update_positions(self, target_weights: pd.Series, name_map: Optional[Dict] = None):
        """更新当前持仓快照"""
        name_map = name_map or {}
        self._current_positions.clear()
        for code, weight in target_weights.items():
            if weight > 1e-6:
                self._current_positions[code] = Position(
                    ts_code=code,
                    name=name_map.get(code, code),
                    weight=weight,
                )
        self._history.append({
            "n_positions": len(self._current_positions),
            "weights": target_weights.to_dict(),
        })

    def summary(self) -> str:
        """当前持仓摘要"""
        if not self._current_positions:
            return "当前无持仓"
        lines = [f"持仓数量: {len(self._current_positions)}"]
        for code, pos in sorted(self._current_positions.items(),
                                key=lambda x: -x[1].weight):
            lines.append(f"  {code} {pos.name:<8s} 权重={pos.weight:.2%}")
        return "\n".join(lines)
