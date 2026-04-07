"""Vortex 配置体系的对外入口。

这个文件本身几乎不放“业务逻辑”，而是扮演一个包级门面（facade）：
把 ``vortex.config.profile`` 子包里最常用的类型重新导出到更短的导入路径。

这样业务代码可以直接写：

    from vortex.config import ProfileResolver

而不必记住每个类具体落在哪个文件里。对于学习代码的人来说，可以把它理解成
“config 这层公开承诺给外部使用的 API 列表”。
"""
from __future__ import annotations

# 统一从 profile 子系统导出公开对象。
# 这些名字是 config 包希望外部优先使用的稳定入口。
from vortex.config.profile import (
    BaseProfile,
    DataProfile,
    ProfileLoader,
    ProfileMerger,
    ProfileResolver,
    ProfileStore,
    ProfileValidator,
    ResearchProfile,
    ResolvedField,
    StrategyProfile,
    TradeProfile,
    ValidationError,
)

# __all__ 相当于“公开接口清单”：
# 1. 控制 `from vortex.config import *` 时会导出哪些名字；
# 2. 更重要的是告诉阅读者：这些类/函数是这个包愿意对外暴露的正式 API。
__all__ = [
    "BaseProfile",
    "DataProfile",
    "ResearchProfile",
    "StrategyProfile",
    "TradeProfile",
    "ProfileStore",
    "ProfileLoader",
    "ProfileMerger",
    "ProfileValidator",
    "ProfileResolver",
    "ResolvedField",
    "ValidationError",
]
