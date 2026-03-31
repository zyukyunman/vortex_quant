"""
models.py
领域模型 — Signal / FactorValue / Order 等核心数据对象

所有策略的输出统一为 Signal，下游只消费 Signal。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Signal:
    """策略产出的交易信号（统一输出协议）"""

    date: str               # 信号日期 "20260328"
    strategy: str           # 策略名 "dividend_quality_fcf"
    ts_code: str            # 股票代码 "000651.SZ"
    name: str               # 股票简称 "格力电器"
    action: str             # buy / sell / hold
    weight: float           # 目标仓位权重 0.033
    score: float            # 综合评分 0~1
    reason: str             # 可读理由
    confidence: float = 1.0 # 置信度 0~1
    metadata: Dict = field(default_factory=dict)  # 附加信息


@dataclass
class FactorExposure:
    """单只股票在某日的因子暴露"""

    ts_code: str
    date: str
    factor_name: str
    raw_value: float        # 原始值
    z_score: float = 0.0    # 标准化后的值


@dataclass
class SelectionResult:
    """选股结果（策略一次运行的输出）"""

    date: str
    strategy: str
    signals: List[Signal]
    universe_size: int       # 初始样本空间大小
    after_filter_size: int   # 通过硬筛选的数量
    top_n: int               # 最终选出的数量
    metadata: Dict = field(default_factory=dict)

    def summary(self) -> str:
        """生成可读摘要"""
        lines = [
            f"{'='*60}",
            f"  选股结果 | {self.strategy} | {self.date}",
            f"{'='*60}",
            f"  样本空间: {self.universe_size} → 通过筛选: {self.after_filter_size}"
            f" → 最终入选: {self.top_n}",
            f"{'─'*60}",
        ]
        for i, sig in enumerate(self.signals, 1):
            lines.append(
                f"  {i:>2}. {sig.ts_code} {sig.name:<8s} "
                f"权重={sig.weight:.1%}  得分={sig.score:.4f}  "
                f"| {sig.reason}"
            )
        lines.append(f"{'='*60}")
        return "\n".join(lines)
