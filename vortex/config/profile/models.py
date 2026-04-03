"""Profile 数据模型。"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .exceptions import ProfileValidationError


@dataclass
class BaseProfile:
    """所有 profile 共享的基础字段。"""

    name: str
    type: str
    extends: str | None = None
    description: str = ""
    owner: str = ""
    tags: list[str] = field(default_factory=list)
    enabled: bool = True

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class DataProfile(BaseProfile):
    """数据下载与快照生产线配置。"""

    market: str = ""
    universe: str = "all_a"
    freqs: list[str] = field(default_factory=list)
    provider: str = ""
    datasets: list[str] = field(default_factory=list)
    timezone: str = ""
    calendar: str = ""
    quality_policy: dict[str, Any] = field(default_factory=dict)
    pit_policy: dict[str, Any] = field(default_factory=dict)
    snapshot_policy: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResearchProfile(BaseProfile):
    data_profile: str = ""
    market: str = ""
    universe: str = ""
    freq: str = ""
    feature_set: str = ""
    label_spec: dict[str, Any] = field(default_factory=dict)
    qlib_workflow: str = ""
    experiment_namespace: str = ""
    signal_output: dict[str, Any] = field(default_factory=dict)


@dataclass
class StrategyProfile(BaseProfile):
    research_profile: str = ""
    benchmark: str = ""
    rebalance_rule: dict[str, Any] = field(default_factory=dict)
    portfolio_constraints: dict[str, Any] = field(default_factory=dict)
    cost_model: dict[str, Any] = field(default_factory=dict)
    slippage_model: dict[str, Any] = field(default_factory=dict)
    risk_pack: str = ""
    backtest_engine: str = ""
    report_template: str = ""


@dataclass
class TradeProfile(BaseProfile):
    strategy_profile: str = ""
    mode: str = ""
    gateway: str = ""
    account: str = ""
    trading_window: dict[str, Any] = field(default_factory=dict)
    order_policy: dict[str, Any] = field(default_factory=dict)
    retry_policy: dict[str, Any] = field(default_factory=dict)
    risk_pack: str = ""
    reconcile_policy: dict[str, Any] = field(default_factory=dict)


@dataclass
class ResolvedProfile:
    """运行时 profile。

    这里会放入运行时派生字段，例如存储目录、DuckDB catalog 路径和密钥引用结果。
    `secrets` 不应被直接打印，因此在公开输出中会被过滤。
    """

    profile: BaseProfile
    storage_backend: str
    storage_root: Path
    manifest_root: Path
    catalog_db_path: Path
    requested_snapshot: str | None = None
    resolved_snapshot: str | None = None
    defaults_applied: dict[str, Any] = field(default_factory=dict)
    overrides_applied: dict[str, Any] = field(default_factory=dict)
    secrets: dict[str, str] = field(default_factory=dict, repr=False)

    def to_public_dict(self) -> dict[str, Any]:
        return {
            "name": self.profile.name,
            "type": self.profile.type,
            "profile": self.profile.to_dict(),
            "storage_backend": self.storage_backend,
            "storage_root": str(self.storage_root),
            "manifest_root": str(self.manifest_root),
            "catalog_db_path": str(self.catalog_db_path),
            "requested_snapshot": self.requested_snapshot,
            "resolved_snapshot": self.resolved_snapshot,
            "defaults_applied": self.defaults_applied,
            "overrides_applied": self.overrides_applied,
        }


PROFILE_CLASS_MAP = {
    "data": DataProfile,
    "research": ResearchProfile,
    "strategy": StrategyProfile,
    "trade": TradeProfile,
}


def profile_from_dict(data: dict[str, Any]) -> BaseProfile:
    profile_type = data.get("type")
    if profile_type not in PROFILE_CLASS_MAP:
        raise ProfileValidationError(f"不支持的 profile 类型: {profile_type!r}")

    profile_cls = PROFILE_CLASS_MAP[profile_type]
    valid_fields = {field_def.name for field_def in profile_cls.__dataclass_fields__.values()}
    init_values = {key: value for key, value in data.items() if key in valid_fields}
    return profile_cls(**init_values)