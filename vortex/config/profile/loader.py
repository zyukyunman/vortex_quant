"""Profile 读取与反序列化。"""

from __future__ import annotations

from typing import Any

import yaml

from .exceptions import ProfileValidationError
from .models import BaseProfile, profile_from_dict
from .store import ProfileStore


class ProfileLoader:
    def __init__(self, store: ProfileStore) -> None:
        self.store = store

    def load_mapping(self, name: str, profile_type: str | None = None) -> dict[str, Any]:
        raw_text = self.store.load_text(name=name, profile_type=profile_type)
        payload = yaml.safe_load(raw_text) or {}
        if not isinstance(payload, dict):
            raise ProfileValidationError(f"profile 文件必须解析为对象: {name}")
        payload.setdefault("name", name)
        if profile_type:
            payload.setdefault("type", profile_type)
        return payload

    def load(self, name: str, profile_type: str | None = None) -> BaseProfile:
        return profile_from_dict(self.load_mapping(name=name, profile_type=profile_type))