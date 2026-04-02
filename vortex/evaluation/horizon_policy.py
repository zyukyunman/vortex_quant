"""
horizon_policy.py
根据策略调仓频率和因子类别推荐评测 horizon。

目标:
  1. 不再把 IC 周期硬编码成单一默认值
  2. 允许显式命令行参数覆盖自动推荐
  3. 让报告里能追溯 horizon 是如何得到的
"""
from __future__ import annotations

from dataclasses import replace
from typing import Iterable, Sequence

from vortex.evaluation.spec import EvalSpec, FactorRole

VALID_FREQ = {"M", "Q", "SA"}

DEFAULT_WEIGHT_HORIZON = {
    "M": 20,
    "Q": 60,
    "SA": 120,
}

FACTOR_FAMILY_OVERRIDES = {
    "dividend_yield": "dividend",
    "dividend_yield_3y": "dividend",
    "consecutive_div_years": "dividend",
    "payout_ratio_3y": "dividend",
    "fcf_yield": "quality",
    "fcf_ttm": "cashflow",
    "ocf_3y_positive": "cashflow",
    "ocf_to_op": "cashflow",
    "opcfd": "quality",
    "roe_ttm": "quality",
    "roe_stability": "quality",
    "delta_roe": "quality",
    "netprofit_yoy": "growth",
    "ep": "value",
    "roe_over_pb": "value",
    "debt_to_assets": "risk",
}

FACTOR_FAMILY_KEYWORDS = {
    "dividend": ("dividend", "payout"),
    "cashflow": ("fcf", "cashflow", "ocf"),
    "quality": ("roe", "roa", "roic", "quality", "stability", "opcfd"),
    "growth": ("yoy", "qoq", "delta_", "growth", "revision", "accel"),
    "momentum": ("momentum", "reversal", "ridge", "peak", "valley", "eruption", "flow"),
    "risk": ("volatility", "beta", "drawdown", "debt", "leverage", "risk"),
    "value": ("ep", "bp", "pe", "pb", "ps", "ev", "yield"),
}

FAMILY_BUCKET = {
    "dividend": "long",
    "cashflow": "long",
    "quality": "long",
    "value": "long",
    "growth": "medium",
    "risk": "medium",
    "momentum": "short",
}

IC_HORIZONS_BY_BUCKET = {
    "short": {
        "M": (1, 5, 20),
        "Q": (5, 20, 60),
        "SA": (5, 20, 60),
    },
    "medium": {
        "M": (5, 20, 60),
        "Q": (20, 60, 120),
        "SA": (20, 60, 120),
    },
    "long": {
        "M": (20, 60, 120),
        "Q": (20, 60, 120),
        "SA": (20, 60, 120, 250),
    },
}

LS_HORIZON_BY_BUCKET = {
    "short": {"M": 5, "Q": 5, "SA": 5},
    "medium": {"M": 5, "Q": 20, "SA": 20},
    "long": {"M": 20, "Q": 20, "SA": 20},
}


def _validate_freq(freq: str) -> str:
    freq_key = freq.upper()
    if freq_key not in VALID_FREQ:
        raise ValueError(f"unsupported freq: {freq}")
    return freq_key


def _normalize_horizons(horizons: Iterable[int]) -> tuple[int, ...]:
    values = sorted({int(day) for day in horizons if int(day) > 0})
    if not values:
        raise ValueError("horizons 不能为空")
    return tuple(values)


def infer_factor_family(factor_name: str) -> str:
    """根据因子名猜测类别，供未显式标注 factor_family 时回退使用。"""
    if factor_name in FACTOR_FAMILY_OVERRIDES:
        return FACTOR_FAMILY_OVERRIDES[factor_name]

    lower_name = factor_name.lower()
    for family, keywords in FACTOR_FAMILY_KEYWORDS.items():
        if any(keyword in lower_name for keyword in keywords):
            return family
    return "value"


def resolve_factor_family(spec: EvalSpec) -> str:
    """优先使用策略显式声明的 factor_family，否则按因子名推断。"""
    if spec.factor_family:
        return spec.factor_family
    return infer_factor_family(spec.factor_name)


def resolve_factor_bucket(spec: EvalSpec) -> str:
    """把具体因子类别映射成 short / medium / long 期限属性。"""
    family = resolve_factor_family(spec)
    return FAMILY_BUCKET.get(family, "medium")


def recommend_ic_horizons(spec: EvalSpec, freq: str) -> tuple[int, ...]:
    """根据调仓频率和因子类别，给出更合适的 IC horizon 组合。"""
    freq_key = _validate_freq(freq)
    bucket = resolve_factor_bucket(spec)
    return IC_HORIZONS_BY_BUCKET[bucket][freq_key]


def recommend_ls_horizon(spec: EvalSpec, freq: str) -> int:
    """根据调仓频率和因子类别，给出多空检验的默认 horizon。"""
    freq_key = _validate_freq(freq)
    bucket = resolve_factor_bucket(spec)
    return LS_HORIZON_BY_BUCKET[bucket][freq_key]


def recommend_weight_horizon(freq: str) -> int:
    """权重优化优先跟随策略调仓节奏，而不是单因子最短衰减期。"""
    freq_key = _validate_freq(freq)
    return DEFAULT_WEIGHT_HORIZON[freq_key]


def apply_scoring_horizon_policy(
    specs: Sequence[EvalSpec],
    freq: str,
    forward_days_list: Sequence[int] | None = None,
    ls_horizon: int | None = None,
) -> list[EvalSpec]:
    """
    仅对 SCORING 因子应用自动/手动 horizon 规则。

    Parameters
    ----------
    specs : Sequence[EvalSpec]
        策略给出的原始评测规格
    freq : str
        调仓频率，M / Q / SA
    forward_days_list : Sequence[int] | None
        手工覆盖的 IC horizons；为空时自动推荐
    ls_horizon : int | None
        手工覆盖的多空 horizon；为空时自动推荐
    """
    freq_key = _validate_freq(freq)
    manual_horizons = _normalize_horizons(forward_days_list) if forward_days_list else None
    manual_ls_horizon = int(ls_horizon) if ls_horizon and ls_horizon > 0 else None

    resolved_specs: list[EvalSpec] = []
    for spec in specs:
        if spec.role != FactorRole.SCORING:
            resolved_specs.append(spec)
            continue

        resolved_specs.append(replace(
            spec,
            horizons=manual_horizons or recommend_ic_horizons(spec, freq_key),
            ls_horizon=manual_ls_horizon or recommend_ls_horizon(spec, freq_key),
        ))

    return resolved_specs


def collect_scoring_horizons(specs: Sequence[EvalSpec]) -> list[int]:
    """汇总所有打分因子的 IC horizons，便于统一展示和权重比较。"""
    horizons = {
        horizon
        for spec in specs
        if spec.role == FactorRole.SCORING
        for horizon in spec.horizons
    }
    return sorted(horizons)


def collect_scoring_ls_horizons(specs: Sequence[EvalSpec]) -> list[int]:
    """汇总所有打分因子的多空 horizon。"""
    horizons = {
        spec.ls_horizon
        for spec in specs
        if spec.role == FactorRole.SCORING and spec.ls_horizon > 0
    }
    return sorted(horizons)
