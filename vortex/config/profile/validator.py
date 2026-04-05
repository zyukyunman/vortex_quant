"""Profile 校验器。"""

from __future__ import annotations

from .exceptions import ProfileValidationError
from .models import BaseProfile, DataProfile


class ProfileValidator:
    def validate(self, profile: BaseProfile, command_scope: str | None = None) -> None:
        if not profile.name:
            raise ProfileValidationError("profile.name 不能为空")
        if profile.type not in {"data", "research", "strategy", "trade"}:
            raise ProfileValidationError(f"profile.type 非法: {profile.type}")
        if not profile.enabled:
            raise ProfileValidationError(f"profile 已禁用: {profile.name}")

        if command_scope:
            expected_prefix = f"{profile.type}."
            if not command_scope.startswith(expected_prefix):
                raise ProfileValidationError(
                    f"命令作用域 {command_scope} 与 profile 类型 {profile.type} 不匹配"
                )

        if isinstance(profile, DataProfile):
            self._validate_data_profile(profile)

    def _validate_data_profile(self, profile: DataProfile) -> None:
        if not profile.market:
            raise ProfileValidationError("DataProfile.market 不能为空")
        if not profile.provider:
            raise ProfileValidationError("DataProfile.provider 不能为空")
        if not profile.datasets:
            raise ProfileValidationError("DataProfile.datasets 不能为空")