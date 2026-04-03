"""运行时 override 解析。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import yaml

from .exceptions import ProfileValidationError


@dataclass(frozen=True)
class RuntimeOverride:
    values: dict[str, Any] = field(default_factory=dict)


class OverrideParser:
    """把 `--set key=value` 解析成嵌套 dict。"""

    def parse(self, items: list[str] | None) -> RuntimeOverride:
        if not items:
            return RuntimeOverride()

        result: dict[str, Any] = {}
        for item in items:
            if "=" not in item:
                raise ProfileValidationError(f"override 格式错误，必须为 key=value: {item}")
            key, raw_value = item.split("=", 1)
            value = yaml.safe_load(raw_value)
            self._set_nested(result, key.split("."), value)
        return RuntimeOverride(values=result)

    def _set_nested(self, payload: dict[str, Any], path_parts: list[str], value: Any) -> None:
        cursor = payload
        for part in path_parts[:-1]:
            cursor = cursor.setdefault(part, {})
        cursor[path_parts[-1]] = value