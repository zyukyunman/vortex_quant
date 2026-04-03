"""Profile 运行时解析器。"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .exceptions import ProfileResolutionError
from .models import BaseProfile, ResolvedProfile
from .store import workspace_root


def _read_env_file(env_path: Path) -> dict[str, str]:
    if not env_path.exists():
        return {}

    result: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = value.strip().strip('"').strip("'")
    return result


class SnapshotSelector:
    """解析 snapshot 别名。

    第一版只实现 `latest*` 的目录扫描逻辑，够支撑后续 research/profile 调试。
    当前数据下载命令本身不会依赖 snapshot，但把能力留在这里可以保持入口一致。
    """

    def resolve(self, snapshot_ref: str | None, profile: BaseProfile) -> str | None:
        if snapshot_ref is None:
            return None
        if not snapshot_ref.startswith("latest"):
            return snapshot_ref

        manifest_root = workspace_root() / "data" / "manifests" / profile.name
        if not manifest_root.exists():
            raise ProfileResolutionError(f"无法解析 snapshot 别名，尚不存在 manifest: {snapshot_ref}")

        candidates = sorted(path.name.removeprefix("as_of=") for path in manifest_root.glob("as_of=*") if path.is_dir())
        if not candidates:
            raise ProfileResolutionError(f"无法解析 snapshot 别名，尚不存在历史 snapshot: {snapshot_ref}")
        return candidates[-1]


class ProfileResolver:
    def __init__(self, snapshot_selector: SnapshotSelector | None = None) -> None:
        self.snapshot_selector = snapshot_selector or SnapshotSelector()

    def resolve(
        self,
        profile: BaseProfile,
        snapshot_ref: str | None = None,
    ) -> ResolvedProfile:
        project_root = workspace_root()
        env_map = _read_env_file(project_root / ".env")

        storage_root = project_root / "data" / "authoritative" / profile.name
        manifest_root = project_root / "data" / "manifests" / profile.name
        catalog_db_path = manifest_root / "catalog.duckdb"

        secrets: dict[str, str] = {}
        provider_name = getattr(profile, "provider", "")
        if profile.type == "data" and provider_name == "tushare":
            token = os.getenv("TUSHARE_TOKEN") or env_map.get("TUSHARE_TOKEN")
            if not token:
                raise ProfileResolutionError(
                    "当前 DataProfile 使用 tushare，但未在环境变量或 .env 中找到 TUSHARE_TOKEN"
                )
            secrets["tushare_token"] = token

        requested_snapshot = snapshot_ref
        resolved_snapshot = self.snapshot_selector.resolve(snapshot_ref=snapshot_ref, profile=profile)
        return ResolvedProfile(
            profile=profile,
            storage_backend="parquet_duckdb",
            storage_root=storage_root,
            manifest_root=manifest_root,
            catalog_db_path=catalog_db_path,
            requested_snapshot=requested_snapshot,
            resolved_snapshot=resolved_snapshot,
            secrets=secrets,
        )