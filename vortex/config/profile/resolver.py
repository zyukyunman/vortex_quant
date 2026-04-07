"""Profile 解析总控。

这是 profile 子系统里最像“编排器”的文件：它负责把前面几个零件串起来，
从一个 profile 名称出发，最终产出：

1. 结构化的 Profile 对象
2. 每个字段的来源说明（default / parent / user / override）

从学习角度看，`resolve()` 是最值得精读的函数，因为它把整个配置流水线串成了
一条清晰的执行路径。
"""
from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any

from vortex.config.profile.defaults import get_defaults
from vortex.config.profile.loader import ProfileLoader
from vortex.config.profile.merger import ProfileMerger
from vortex.config.profile.models import BaseProfile
from vortex.config.profile.store import ProfileStore
from vortex.config.profile.validator import ProfileValidator, ValidationError
from vortex.shared.errors import ConfigError

_MAX_INHERITANCE_DEPTH = 32


@dataclass
class ResolvedField:
    """记录单个字段的最终值和来源。

    这类结构很适合做“可解释配置”：
    不仅告诉你最终值是什么，还告诉你它是从哪里来的。
    """

    value: Any
    source: str  # "default" | "parent:{name}" | "user" | "override"


class ProfileResolver:
    """完整解析链：Store → Defaults → Merger → Loader → Validator。"""

    def __init__(self, store: ProfileStore) -> None:
        self._store = store
        self._merger = ProfileMerger()
        self._loader = ProfileLoader()
        self._validator = ProfileValidator()

    def resolve(
        self,
        name: str,
        profile_type: str,
        overrides: dict | None = None,
    ) -> tuple[BaseProfile, dict[str, ResolvedField]]:
        """解析 profile 并返回 (typed profile, 字段来源映射)。

        继承链展开：从最远祖先到直接父级逐层合并。
        """
        # 先拿到该 profile 类型的默认模板，再读取用户 YAML。
        defaults = get_defaults(profile_type)
        user_raw = self._store.load(name)

        # 展开继承链：[grandparent, parent, ...]（从远到近）。
        ancestor_chain = self._resolve_ancestor_chain(user_raw, name)

        # 合并顺序：defaults → 最远祖先 → ... → 直接父级 → user → overrides
        merged = self._merger.merge(defaults, None, {})  # 从 defaults 开始
        for anc_name, anc_raw in ancestor_chain:
            merged = self._merger.merge(merged, None, anc_raw)
        merged = self._merger.merge(merged, None, user_raw, overrides)

        # name 是外部引用主键，始终以调用者请求的名字为准。
        merged["name"] = name

        # dict -> dataclass 对象。
        profile = self._loader.load(merged, profile_type)

        # validator 只负责“收集错误”，resolver 决定是否把 error 级问题升级为异常。
        errors = self._validator.validate(profile)
        real_errors = [e for e in errors if e.level == "error"]
        if real_errors:
            raise ConfigError(
                code="CONFIG_PROFILE_VALIDATION_FAILED",
                message=f"Profile '{name}' 校验失败",
                detail={"errors": [{"field": e.field, "message": e.message} for e in real_errors]},
            )

        # 追踪字段来源，供 explain 命令或调试使用。
        sources = self._compute_sources(
            defaults, ancestor_chain, user_raw, overrides, profile,
        )
        return profile, sources

    def _resolve_ancestor_chain(
        self, user_raw: dict, origin_name: str,
    ) -> list[tuple[str, dict]]:
        """展开继承链并检测循环。

        返回 [(ancestor_name, ancestor_raw), ...] 从最远祖先到直接父级排列。
        """
        chain: list[tuple[str, dict]] = []
        seen: set[str] = {origin_name}
        current_raw = user_raw

        depth = 0
        while True:
            parent_name = current_raw.get("extends")
            if not parent_name:
                break

            depth += 1
            if depth > _MAX_INHERITANCE_DEPTH:
                raise ConfigError(
                    code="CONFIG_PROFILE_INHERITANCE_TOO_DEEP",
                    message=f"Profile 继承深度超过上限 ({_MAX_INHERITANCE_DEPTH})",
                    detail={"chain": [n for n, _ in chain]},
                )

            # seen 用于检测 A -> B -> C -> A 这类循环继承。
            if parent_name in seen:
                raise ConfigError(
                    code="CONFIG_PROFILE_CIRCULAR_INHERITANCE",
                    message=f"检测到循环继承：'{parent_name}' 已在继承链中",
                    detail={"chain": [origin_name] + [n for n, _ in chain] + [parent_name]},
                )

            if not self._store.exists(parent_name):
                raise ConfigError(
                    code="CONFIG_PROFILE_PARENT_NOT_FOUND",
                    message=f"父 profile '{parent_name}' 不存在",
                )

            seen.add(parent_name)
            parent_raw = self._store.load(parent_name)
            chain.append((parent_name, parent_raw))
            current_raw = parent_raw

        # 翻转：从最远祖先到直接父级，便于后续按继承顺序合并。
        chain.reverse()
        return chain

    def _compute_sources(
        self,
        defaults: dict,
        ancestor_chain: list[tuple[str, dict]],
        user_raw: dict,
        overrides: dict | None,
        profile: BaseProfile,
    ) -> dict[str, ResolvedField]:
        """确定每个字段的值来源（最近覆盖者优先标注）。

        来源判断顺序是倒着看的：

        - override 有值 → 来源是 override
        - 否则用户 YAML 显式写了 → 来源是 user
        - 否则祖先链里最近的一层提供了 → 来源是 parent:xxx
        - 否则 → 来源是 default
        """
        result: dict[str, ResolvedField] = {}
        for f in fields(profile):
            # `fields(profile)` 遍历 dataclass 的字段定义；
            # `getattr(profile, f.name)` 则按字段名动态取出对象上的当前值。
            val = getattr(profile, f.name)
            source = "default"

            if overrides and f.name in overrides:
                source = "override"
            elif f.name in user_raw:
                source = "user"
            else:
                # 从直接父级到最远祖先查找最近提供者
                for anc_name, anc_raw in reversed(ancestor_chain):
                    if f.name in anc_raw:
                        source = f"parent:{anc_name}"
                        break

            result[f.name] = ResolvedField(value=val, source=source)
        return result

    def explain(self, name: str, profile_type: str) -> str:
        """返回人类可读的 profile 解释文本。

        它不是给程序消费的，而是给人看：
        让你一眼看出“最终值是什么、它为什么会是这个值”。
        """
        profile, sources = self.resolve(name, profile_type)
        lines = [f"Profile: {name} (type={profile_type})", "=" * 50]
        for field_name, resolved in sources.items():
            lines.append(f"  {field_name}: {resolved.value!r}  <- [{resolved.source}]")
        return "\n".join(lines)
