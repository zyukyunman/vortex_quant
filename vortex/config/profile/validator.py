"""Profile 校验器。

这个文件负责做“字段值是否合理”的判断，但它本身不直接抛异常终止流程，
而是先把发现的问题收集成 `ValidationError` 列表，交给更上层的 resolver
决定是否阻断。

这种分层的好处是：

- validator 专注于“发现问题”
- resolver 专注于“处理问题”
"""
from __future__ import annotations

from dataclasses import dataclass

from vortex.config.profile.models import (
    BaseProfile,
    DataProfile,
    ResearchProfile,
    StrategyProfile,
    TradeProfile,
)


@dataclass
class ValidationError:
    """一条校验结果。

    这里故意不用 Python 的 Exception，而是用普通 dataclass，
    因为当前设计更偏“收集错误列表”而不是“遇错即抛”。
    """

    field: str
    message: str
    level: str  # "error" | "warning"


class ProfileValidator:
    """校验 profile 字段合法性。"""

    def validate(self, profile: BaseProfile) -> list[ValidationError]:
        """返回这个 profile 的所有校验结果。"""
        errors: list[ValidationError] = []

        # 通用校验：所有 profile 都必须有非空 name。
        if not profile.name or not profile.name.strip():
            errors.append(
                ValidationError(
                    field="name", message="profile name 不能为空", level="error"
                )
            )

        # 再根据具体类型分发到各自的专属校验函数。
        if isinstance(profile, DataProfile):
            errors.extend(self._validate_data(profile))
        elif isinstance(profile, ResearchProfile):
            errors.extend(self._validate_research(profile))
        elif isinstance(profile, StrategyProfile):
            errors.extend(self._validate_strategy(profile))
        elif isinstance(profile, TradeProfile):
            errors.extend(self._validate_trade(profile))

        return errors

    def _validate_data(self, p: DataProfile) -> list[ValidationError]:
        """数据域字段校验。"""
        errors: list[ValidationError] = []
        if not p.datasets:
            errors.append(
                ValidationError(
                    field="datasets",
                    message="datasets 列表不能为空",
                    level="error",
                )
            )
        if len(p.history_start) != 8 or not p.history_start.isdigit():
            errors.append(
                ValidationError(
                    field="history_start",
                    message="history_start 格式应为 YYYYMMDD",
                    level="error",
                )
            )
        return errors

    def _validate_research(self, p: ResearchProfile) -> list[ValidationError]:
        """研究域字段校验。"""
        errors: list[ValidationError] = []
        if p.n_groups < 2:
            errors.append(
                ValidationError(
                    field="n_groups",
                    message="n_groups 至少为 2",
                    level="error",
                )
            )
        if p.max_concurrent < 1:
            errors.append(
                ValidationError(
                    field="max_concurrent",
                    message="max_concurrent 至少为 1",
                    level="error",
                )
            )
        return errors

    def _validate_strategy(self, p: StrategyProfile) -> list[ValidationError]:
        """策略域字段校验。

        当前这里还是轻量实现，很多策略级语义约束还没有下沉到这一层。
        """
        errors: list[ValidationError] = []
        # strategy 的 signal_ids 可以在解析阶段为空（延迟绑定）
        return errors

    def _validate_trade(self, p: TradeProfile) -> list[ValidationError]:
        """交易域字段校验。"""
        errors: list[ValidationError] = []
        valid_gateways = {"paper", "live"}
        if p.gateway not in valid_gateways:
            errors.append(
                ValidationError(
                    field="gateway",
                    message=f"gateway 必须为 {valid_gateways} 之一",
                    level="error",
                )
            )
        return errors
