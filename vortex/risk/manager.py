"""
manager.py
L8 RiskManager — 风控管理

职责:
  - 盘前: 检查持仓集中度、行业暴露
  - 盘中: 模拟监控回撤和单日亏损
  - 盘后: 归因、风险报告
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class RiskCheckResult:
    """风控检查结果"""
    passed: bool
    level: str = "INFO"     # INFO / WARNING / CRITICAL
    rule: str = ""
    detail: str = ""


@dataclass
class RiskReport:
    """风控报告"""
    date: str
    checks: List[RiskCheckResult] = field(default_factory=list)
    metrics: Dict = field(default_factory=dict)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def summary(self) -> str:
        lines = [
            f"风控报告 | {self.date}",
            f"整体: {'✓ 通过' if self.passed else '✗ 触发风控'}",
        ]
        for c in self.checks:
            status = "✓" if c.passed else "✗"
            lines.append(f"  {status} [{c.level}] {c.rule}: {c.detail}")
        if self.metrics:
            lines.append("指标:")
            for k, v in self.metrics.items():
                lines.append(f"  {k}: {v}")
        return "\n".join(lines)


class RiskManager:
    """
    风控管理器

    Parameters
    ----------
    max_daily_loss : float
        单日最大亏损比例 (e.g., 0.02 = 2%)
    max_drawdown : float
        最大回撤限制 (e.g., 0.15 = 15%)
    max_position_pct : float
        单票最大仓位 (e.g., 0.10 = 10%)
    max_industry_pct : float
        行业最大仓位 (e.g., 0.30 = 30%)
    """

    def __init__(
        self,
        max_daily_loss: float = 0.02,
        max_drawdown: float = 0.15,
        max_position_pct: float = 0.10,
        max_industry_pct: float = 0.30,
    ):
        self.max_daily_loss = max_daily_loss
        self.max_drawdown = max_drawdown
        self.max_position_pct = max_position_pct
        self.max_industry_pct = max_industry_pct
        self._nav_history: List[float] = []

    def pre_trade_check(
        self,
        target_weights: pd.Series,
        industry_map: Optional[pd.Series] = None,
    ) -> RiskReport:
        """
        盘前风控检查

        Parameters
        ----------
        target_weights : pd.Series
            目标持仓权重 (index=ts_code)
        industry_map : pd.Series
            ts_code → 行业名称映射
        """
        checks: List[RiskCheckResult] = []

        # 1. 单票集中度
        if not target_weights.empty:
            max_w = target_weights.max()
            checks.append(RiskCheckResult(
                passed=max_w <= self.max_position_pct + 1e-6,
                level="WARNING" if max_w > self.max_position_pct else "INFO",
                rule="单票集中度",
                detail=f"最大权重 {max_w:.2%} (限 {self.max_position_pct:.0%})",
            ))

        # 2. 行业集中度
        if industry_map is not None and not target_weights.empty:
            industries = industry_map.reindex(target_weights.index).fillna("未知")
            ind_weights = target_weights.groupby(industries).sum()
            max_ind_w = ind_weights.max()
            max_ind_name = ind_weights.idxmax()
            checks.append(RiskCheckResult(
                passed=max_ind_w <= self.max_industry_pct + 1e-6,
                level="WARNING" if max_ind_w > self.max_industry_pct else "INFO",
                rule="行业集中度",
                detail=f"{max_ind_name} {max_ind_w:.2%} (限 {self.max_industry_pct:.0%})",
            ))

        # 3. 持仓数量
        n_pos = (target_weights > 1e-6).sum()
        checks.append(RiskCheckResult(
            passed=n_pos >= 5,
            level="WARNING" if n_pos < 5 else "INFO",
            rule="分散度",
            detail=f"持仓 {n_pos} 只 (建议>=5)",
        ))

        return RiskReport(date="pre_trade", checks=checks)

    def monitor(
        self,
        daily_return: float,
        nav: float,
    ) -> RiskReport:
        """
        盘中/盘后监控

        Parameters
        ----------
        daily_return : float
            当日收益率
        nav : float
            当前净值

        Returns
        -------
        RiskReport
        """
        self._nav_history.append(nav)
        checks: List[RiskCheckResult] = []

        # 1. 单日亏损
        checks.append(RiskCheckResult(
            passed=daily_return >= -self.max_daily_loss,
            level="CRITICAL" if daily_return < -self.max_daily_loss else "INFO",
            rule="单日亏损",
            detail=f"日收益 {daily_return:.2%} (限 -{self.max_daily_loss:.0%})",
        ))

        # 2. 最大回撤
        if len(self._nav_history) > 1:
            peak = max(self._nav_history)
            drawdown = (peak - nav) / peak
            checks.append(RiskCheckResult(
                passed=drawdown <= self.max_drawdown,
                level="CRITICAL" if drawdown > self.max_drawdown else "INFO",
                rule="最大回撤",
                detail=f"回撤 {drawdown:.2%} (限 {self.max_drawdown:.0%})",
            ))

        metrics = {
            "daily_return": f"{daily_return:.4%}",
            "nav": f"{nav:.4f}",
            "peak": f"{max(self._nav_history):.4f}" if self._nav_history else "N/A",
        }

        return RiskReport(date="monitor", checks=checks, metrics=metrics)

    def post_trade_attribution(
        self,
        weights: pd.Series,
        returns: pd.Series,
    ) -> Dict:
        """
        盘后归因分析

        Parameters
        ----------
        weights : pd.Series
            持仓权重
        returns : pd.Series
            个股日收益率

        Returns
        -------
        Dict
            归因结果 {portfolio_return, top_contributors, bottom_contributors}
        """
        aligned = pd.DataFrame({
            "weight": weights,
            "return": returns,
        }).dropna()

        if aligned.empty:
            return {"portfolio_return": 0.0, "contributors": []}

        aligned["contribution"] = aligned["weight"] * aligned["return"]
        port_return = aligned["contribution"].sum()

        sorted_contrib = aligned["contribution"].sort_values()
        top = sorted_contrib.tail(3).to_dict()
        bottom = sorted_contrib.head(3).to_dict()

        return {
            "portfolio_return": port_return,
            "top_contributors": top,
            "bottom_contributors": bottom,
        }
