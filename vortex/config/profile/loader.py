"""把原始 dict 反序列化为 dataclass Profile 对象。

你可以把这个文件理解成“结构化加载器”：

- 输入：一个普通 Python dict（通常来自 YAML）
- 输出：`DataProfile` / `ResearchProfile` / `StrategyProfile` / `TradeProfile`

它的职责很克制：只负责“dict -> 对象”这一步。
默认值怎么来、配置怎么合并、字段值是否合理，都不在这里处理。
"""
from __future__ import annotations

from vortex.config.profile.models import (
    BaseProfile,
    DataProfile,
    ResearchProfile,
    StrategyProfile,
    TradeProfile,
)
from vortex.shared.errors import ConfigError

_TYPE_MAP: dict[str, type[BaseProfile]] = {
    "data": DataProfile,
    "research": ResearchProfile,
    "strategy": StrategyProfile,
    "trade": TradeProfile,
}


class ProfileLoader:
    """解析 raw dict 为 typed Profile dataclass。"""

    def load(self, raw: dict, profile_type: str) -> BaseProfile:
        """根据 profile_type 选择目标 dataclass，并实例化对象。"""
        cls = _TYPE_MAP.get(profile_type)
        if cls is None:
            raise ConfigError(
                code="CONFIG_PROFILE_UNKNOWN_TYPE",
                message=f"未知 profile 类型: {profile_type}",
            )
        # `cls(**raw)` 要求 raw 里的每个 key 都必须是 dataclass 构造函数认识的字段名。
        # 因此这里先取出“这个 dataclass 定义了哪些字段”，再把 raw 过滤一遍。
        #
        # 这段逻辑的核心含义：
        # 1. `dataclasses.fields(cls)` 取出这个 dataclass 的字段定义
        # 2. `{f.name for f in ...}` 把字段对象变成纯字段名集合
        # 3. 只保留 raw 中那些 dataclass 真正接受的 key
        #
        # 这样做可以避免 `cls(**raw)` 时因为多余字段直接抛 TypeError。
        # 当前实现采取“忽略未知字段”的宽松策略；这对兼容性友好，
        # 但也意味着拼错字段名时，不会在 loader 这一层立刻暴露。
        import dataclasses

        valid_fields = {f.name for f in dataclasses.fields(cls)}
        filtered = {k: v for k, v in raw.items() if k in valid_fields}
        try:
            # `**filtered` 表示把字典拆成关键字参数，例如 {"name": "x"}
            # 会变成 `cls(name="x")`。
            return cls(**filtered)
        except TypeError as e:
            raise ConfigError(
                code="CONFIG_PROFILE_LOAD_FAILED",
                message=f"Profile 实例化失败: {e}",
                detail={"raw_keys": list(raw.keys()), "valid_fields": list(valid_fields)},
            ) from e
