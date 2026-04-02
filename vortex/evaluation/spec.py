"""
spec.py
因子评测数据类 — FactorRole, EvalSpec, EvalResult

所有评测器的输入 (EvalSpec) 和输出 (EvalResult) 在此统一定义。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

import pandas as pd


class FactorRole(str, Enum):
    """因子在策略中的角色"""

    SCORING = "scoring"   # 参与综合打分排序
    FILTER = "filter"     # 硬门槛过滤
    TIMING = "timing"     # 择时 / 情绪 / 动量 (预留)
    RISK = "risk"         # 风险约束


@dataclass
class EvalSpec:
    """
    单个因子的评测规格

    由策略 eval_specs() 方法生成，描述"用什么方式评测这个因子"。

    Parameters
    ----------
    factor_name : str
        因子名 (对应 BaseFactor.name)
    role : FactorRole
        因子角色
    horizons : tuple[int, ...]
        IC 计算周期列表 (交易日)
    ls_horizon : int
        多空组合前瞻收益天数
    ls_groups : int
        多空分组数
    threshold : float | None
        FILTER 角色的门槛值
    threshold_op : str
        门槛比较运算符 (">=", ">", "<=", "<", "==")
    factor_family : str
        因子类别标签，用于自动推荐评测 horizon，例如 value / quality / growth / momentum
    data_source : str
        数据来源描述，方便审计
    description : str
        因子含义
    """

    factor_name: str
    role: FactorRole
    horizons: Tuple[int, ...] = (1, 5, 20)
    ls_horizon: int = 5
    ls_groups: int = 5
    threshold: Optional[float] = None
    threshold_op: str = ">="
    factor_family: str = ""
    data_source: str = ""
    description: str = ""


@dataclass
class EvalResult:
    """
    单因子评测结果

    所有评测器返回统一格式。

    Parameters
    ----------
    factor_name : str
        因子名
    role : FactorRole
        因子角色
    passed : bool
        是否通过准入
    metrics : dict
        指标集合，key 含义由 role 决定
    detail : pd.DataFrame | None
        明细数据 (IC 时序、分组收益等)
    reason : str
        人可读结论
    """

    factor_name: str
    role: FactorRole
    passed: bool
    metrics: Dict[str, float] = field(default_factory=dict)
    detail: Optional[pd.DataFrame] = None
    reason: str = ""
