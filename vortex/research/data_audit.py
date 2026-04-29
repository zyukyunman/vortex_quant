"""量化研究数据需求审计。

策略研究进入下一轮前，应先检查关键数据是否已落盘。
该模块只审计本地工作区，不触发真实抓取；缺口会转成下一步动作。
"""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class ResearchDatasetRequirement:
    """研究数据需求定义。"""

    dataset: str
    purpose: str
    required_for: str
    permission_key: str | None = None
    default_enabled: bool = True


@dataclass(frozen=True)
class DatasetAuditItem:
    """单个数据集审计结果。"""

    dataset: str
    available: bool
    parquet_files: int
    purpose: str
    required_for: str
    permission_key: str | None
    permission_granted: bool | None
    next_action: str

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset": self.dataset,
            "available": self.available,
            "parquet_files": self.parquet_files,
            "purpose": self.purpose,
            "required_for": self.required_for,
            "permission_key": self.permission_key,
            "permission_granted": self.permission_granted,
            "next_action": self.next_action,
        }


DEFAULT_RESEARCH_REQUIREMENTS: tuple[ResearchDatasetRequirement, ...] = (
    ResearchDatasetRequirement(
        dataset="adj_factor",
        purpose="复权价格；长期动量、支撑压力和红利策略必须使用",
        required_for="所有日频价格研究",
    ),
    ResearchDatasetRequirement(
        dataset="index_daily",
        purpose="指数动量、指数支撑压力、市场状态和基准收益",
        required_for="市场状态过滤和回撤控制",
    ),
    ResearchDatasetRequirement(
        dataset="stk_mins",
        purpose="分钟量价；构建量峰、量岭、量谷等微观结构因子",
        required_for="峰岭谷因子和周频短线 alpha",
        permission_key="stock_minutes",
        default_enabled=False,
    ),
    ResearchDatasetRequirement(
        dataset="index_member_all",
        purpose="行业成分和行业轮动研究",
        required_for="行业领先滞后和行业约束",
    ),
    ResearchDatasetRequirement(
        dataset="moneyflow",
        purpose="日频资金流；作为分钟资金结构缺失时的弱替代",
        required_for="资金流候选因子",
    ),
)


def audit_research_datasets(
    data_root: str | Path,
    requirements: tuple[ResearchDatasetRequirement, ...] = DEFAULT_RESEARCH_REQUIREMENTS,
    *,
    granted_permissions: set[str] | None = None,
) -> list[DatasetAuditItem]:
    """审计研究所需数据是否已落盘。"""

    root = Path(data_root).expanduser()
    granted_permissions = _resolve_permissions(granted_permissions)
    items: list[DatasetAuditItem] = []
    for requirement in requirements:
        dataset_path = root / requirement.dataset
        parquet_files = _count_parquet_files(dataset_path)
        available = parquet_files > 0
        permission_granted = (
            requirement.permission_key in granted_permissions
            if requirement.permission_key
            else None
        )
        items.append(
            DatasetAuditItem(
                dataset=requirement.dataset,
                available=available,
                parquet_files=parquet_files,
                purpose=requirement.purpose,
                required_for=requirement.required_for,
                permission_key=requirement.permission_key,
                permission_granted=permission_granted,
                next_action=_next_action(requirement, available, permission_granted),
            )
        )
    return items


def missing_research_datasets(items: list[DatasetAuditItem]) -> list[DatasetAuditItem]:
    """返回缺失的数据集。"""

    return [item for item in items if not item.available]


def _count_parquet_files(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file() and path.suffix == ".parquet":
        return 1
    if not path.is_dir():
        return 0
    return sum(1 for _ in path.rglob("*.parquet"))


def _next_action(
    requirement: ResearchDatasetRequirement,
    available: bool,
    permission_granted: bool | None,
) -> str:
    if available:
        return "可用于研究；进入字段口径和覆盖率检查。"
    if requirement.permission_key:
        if permission_granted is False:
            return f"缺少独立权限 {requirement.permission_key}；不要盲目试抓，先配置权限或改用日频替代因子。"
        return f"先确认 TUSHARE_EXTRA_PERMISSIONS 是否包含 {requirement.permission_key}，再做小样本试抓和容量评估。"
    if requirement.default_enabled:
        return "检查 data profile/bootstrap 是否启用该 dataset；必要时单独补抓。"
    return "按需启用 dataset，并先做小样本试抓。"


def _resolve_permissions(granted_permissions: set[str] | None) -> set[str]:
    if granted_permissions is not None:
        return {item.strip() for item in granted_permissions if item.strip()}
    raw = os.environ.get("TUSHARE_EXTRA_PERMISSIONS", "")
    return {item.strip() for item in raw.split(",") if item.strip()}
