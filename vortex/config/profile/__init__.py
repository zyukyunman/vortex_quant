"""Profile 子系统的包入口。

可以把 `vortex.config.profile` 想成一个小型配置流水线：

1. `store.py` 读取磁盘上的 YAML
2. `defaults.py` 提供该类型的默认值模板
3. `merger.py` 合并 defaults / parent / user / override
4. `loader.py` 把 dict 变成 dataclass 对象
5. `validator.py` 校验字段和语义
6. `resolver.py` 串起整条链路，并记录字段来源
7. `service.py` 提供更适合业务侧调用的统一入口

这个 `__init__.py` 本身不处理配置逻辑，它主要承担“重新导出公开 API”的角色。
"""
from __future__ import annotations

# 默认值模板与取值函数：告诉外部“每种 profile 类型的兜底配置长什么样”。
from vortex.config.profile.defaults import (
    DATA_DEFAULTS,
    RESEARCH_DEFAULTS,
    STRATEGY_DEFAULTS,
    TRADE_DEFAULTS,
    get_defaults,
)
from vortex.config.profile.loader import ProfileLoader
from vortex.config.profile.merger import ProfileMerger
from vortex.config.profile.models import (
    BaseProfile,
    DataProfile,
    ResearchProfile,
    StrategyProfile,
    TradeProfile,
)
from vortex.config.profile.resolver import ProfileResolver, ResolvedField
from vortex.config.profile.service import (
    ProfileDumper,
    ProfileService,
    build_profile_service,
)
from vortex.config.profile.store import ProfileStore
from vortex.config.profile.validator import ProfileValidator, ValidationError

# 这里列出的名字，构成 profile 子包对外暴露的稳定入口。
__all__ = [
    # models
    "BaseProfile",
    "DataProfile",
    "ResearchProfile",
    "StrategyProfile",
    "TradeProfile",
    # defaults
    "DATA_DEFAULTS",
    "RESEARCH_DEFAULTS",
    "STRATEGY_DEFAULTS",
    "TRADE_DEFAULTS",
    "get_defaults",
    # store / loader / merger / validator / resolver
    "ProfileStore",
    "ProfileLoader",
    "ProfileMerger",
    "ProfileValidator",
    "ValidationError",
    "ProfileResolver",
    "ResolvedField",
    # service (06 §2.5 — 架构文档定义的公开入口)
    "ProfileService",
    "ProfileDumper",
    "build_profile_service",
]
