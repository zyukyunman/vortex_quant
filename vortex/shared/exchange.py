"""域间交换对象（Exchange Objects）。

所有交换对象均为 frozen dataclass，不可变以保证跨域传递安全。
提供 to_dict / from_dict / to_json / from_json 进行显式序列化控制。
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field


def _to_json(obj: object) -> str:
    """序列化为紧凑 JSON 字符串。"""
    return json.dumps(asdict(obj), ensure_ascii=False, separators=(",", ":"))  # type: ignore[arg-type]


def _from_json(cls: type, raw: str) -> object:
    """从 JSON 字符串反序列化。"""
    return cls(**json.loads(raw))


# ------------------------------------------------------------------
# Data → Research 交换对象
# ------------------------------------------------------------------


@dataclass(frozen=True)
class SnapshotDescriptor:
    """数据快照描述符，由 Data Publish 产出，传递给 Research。"""

    snapshot_id: str
    profile: str
    as_of: str  # YYYYMMDD
    revision: int
    datasets: list[str]
    row_counts: dict[str, int]
    quality_passed: bool
    created_at: str  # ISO datetime
    vortex_version: str
    lineage: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> SnapshotDescriptor:
        return cls(**d)

    def to_json(self) -> str:
        return _to_json(self)

    @classmethod
    def from_json(cls, raw: str) -> SnapshotDescriptor:
        return _from_json(cls, raw)  # type: ignore[return-value]


# ------------------------------------------------------------------
# Research → Strategy 交换对象
# ------------------------------------------------------------------


@dataclass(frozen=True)
class SignalSnapshotDescriptor:
    """因子信号快照描述符，由 Research 产出，传递给 Strategy。"""

    signal_id: str
    factor_name: str
    profile: str
    snapshot_id: str  # 关联的 Data snapshot
    evaluated_type: str
    tag: str
    published_at: str
    lineage: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> SignalSnapshotDescriptor:
        return cls(**d)

    def to_json(self) -> str:
        return _to_json(self)

    @classmethod
    def from_json(cls, raw: str) -> SignalSnapshotDescriptor:
        return _from_json(cls, raw)  # type: ignore[return-value]


# ------------------------------------------------------------------
# Strategy → Trade 交换对象
# ------------------------------------------------------------------


@dataclass(frozen=True)
class TargetPortfolio:
    """目标组合，由 Strategy 产出，传递给 Trade。

    holdings 列表元素格式: {"symbol": str, "weight": float, "shares": int}
    status 取值: draft / frozen / superseded
    """

    portfolio_id: str
    profile: str
    signal_ids: list[str]
    snapshot_id: str
    holdings: list[dict]
    status: str
    created_at: str
    lineage: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> TargetPortfolio:
        return cls(**d)

    def to_json(self) -> str:
        return _to_json(self)

    @classmethod
    def from_json(cls, raw: str) -> TargetPortfolio:
        return _from_json(cls, raw)  # type: ignore[return-value]


# ------------------------------------------------------------------
# Trade 执行报告
# ------------------------------------------------------------------


@dataclass(frozen=True)
class ExecutionReport:
    """执行报告，由 Trade 产出。

    mode: paper / live
    status: pending / partial / completed / failed
    """

    exec_id: str
    profile: str
    portfolio_id: str
    mode: str
    status: str
    order_count: int
    filled_count: int
    rejected_count: int
    created_at: str
    lineage: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> ExecutionReport:
        return cls(**d)

    def to_json(self) -> str:
        return _to_json(self)

    @classmethod
    def from_json(cls, raw: str) -> ExecutionReport:
        return _from_json(cls, raw)  # type: ignore[return-value]
