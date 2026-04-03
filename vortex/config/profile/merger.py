"""Profile 默认值、继承链与 override 合并。"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from .defaults import ProfileDefaultsProvider
from .exceptions import ProfileValidationError
from .loader import ProfileLoader
from .models import BaseProfile, profile_from_dict
from .overrides import RuntimeOverride


def deep_merge(base: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


class ProfileMerger:
    def __init__(self, loader: ProfileLoader, defaults_provider: ProfileDefaultsProvider) -> None:
        self.loader = loader
        self.defaults_provider = defaults_provider

    def expand(
        self,
        profile: BaseProfile,
        override: RuntimeOverride | None = None,
    ) -> BaseProfile:
        merged_payload = self._expand_to_dict(profile=profile, seen=set())
        if override and override.values:
            merged_payload = deep_merge(merged_payload, override.values)
        return profile_from_dict(merged_payload)

    def _expand_to_dict(self, profile: BaseProfile, seen: set[str]) -> dict[str, Any]:
        if profile.name in seen:
            raise ProfileValidationError(f"检测到循环继承: {profile.name}")

        # 这里刻意读取原始 YAML，而不是直接使用 dataclass 实例的 `to_dict()`。
        # 原因是 dataclass 会把未填写字段补成空字符串/空列表，
        # 那样在继承场景下，子 profile 会错误地把父模板的有效值覆盖掉。
        raw_profile_mapping = self.loader.load_mapping(name=profile.name, profile_type=profile.type)
        payload = self.defaults_provider.get_defaults(
            profile_type=profile.type,
            market=raw_profile_mapping.get("market"),
        )

        if profile.extends:
            parent = self.loader.load(name=profile.extends, profile_type=profile.type)
            payload = deep_merge(payload, self._expand_to_dict(parent, seen | {profile.name}))

        return deep_merge(payload, raw_profile_mapping)