"""每种 Profile 类型的默认值模板。

为什么单独放一个 defaults 文件，而不是只依赖 dataclass 里的默认参数？

1. dataclass 默认值更偏“对象构造时的兜底”
2. 这里的默认值更偏“配置系统层面的模板”
3. 把模板集中放在一起，便于统一查看、统一解释来源

resolver 会先取这里的默认值字典，再与父配置/用户配置/override 合并。
"""
from __future__ import annotations

from vortex.data.provider.tushare_registry import get_default_tushare_datasets

def _build_data_defaults() -> dict:
    """按当前账号可访问的数据集动态构建 Data 默认模板。"""
    return {
        "provider": "tushare",
        "datasets": get_default_tushare_datasets(),
        "exclude_datasets": [],
        "priority_datasets": [],
        "history_start": "20170101",
        "schedule": None,
        "quality_pack": "default",
        "pit_pack": "default",
        "publish_pack": "default",
        "storage_pack": "default",
        "notification": {
            "enabled": True,
            "level": "warning",
            "channel": "feishu",
        },
    }


# 兼容旧调用方：模块导入时保留一个静态快照；真正执行时优先用 get_defaults("data")。
DATA_DEFAULTS: dict = _build_data_defaults()

# Research 域默认配置：面向因子评测 / 研究任务。
RESEARCH_DEFAULTS: dict = {
    "snapshot": None,
    "market": "cn_stock",
    "label_periods": [1, 5, 20],
    "n_groups": 5,
    "max_concurrent": 3,
}

# Strategy 域默认配置：面向回测 / 策略流水线。
STRATEGY_DEFAULTS: dict = {
    "snapshot": None,
    "signal_ids": [],
    "pipeline": {},
    "backtest": {},
    "benchmark": {},
}

# Trade 域默认配置：面向交易执行 / 风控 / 对账。
TRADE_DEFAULTS: dict = {
    "gateway": "paper",
    "order_policy": {},
    "risk_pack": "default",
    "retry_policy": {},
    "reconcile_policy": {},
}

# 统一的“类型 -> 默认模板”映射，方便上层按字符串类型分发。
_DEFAULTS_MAP: dict[str, dict] = {
    "research": RESEARCH_DEFAULTS,
    "strategy": STRATEGY_DEFAULTS,
    "trade": TRADE_DEFAULTS,
}


def get_defaults(profile_type: str) -> dict:
    """获取指定类型的默认值字典。

    返回的是深拷贝，而不是原始模板本身。
    这样调用方在后续 merge 时修改字典，不会污染全局默认值模板。
    """
    if profile_type not in _DEFAULTS_MAP:
        if profile_type != "data":
            raise ValueError(f"未知 profile 类型: {profile_type}")
    # 返回深拷贝，避免外部修改影响默认值模板。
    import copy

    if profile_type == "data":
        return copy.deepcopy(_build_data_defaults())
    return copy.deepcopy(_DEFAULTS_MAP[profile_type])
