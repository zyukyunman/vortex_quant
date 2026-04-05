"""Profile 文件存储层。"""

from __future__ import annotations

from pathlib import Path

from .exceptions import ProfileNotFoundError


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[3]


class ProfileStore:
    """屏蔽 profile 的目录组织细节。"""

    def __init__(self, root_dir: Path | None = None) -> None:
        self.root_dir = root_dir or workspace_root() / "profiles"

    def list_profiles(self, profile_type: str | None = None) -> list[str]:
        if profile_type:
            target_dir = self.root_dir / profile_type
            if not target_dir.exists():
                return []
            return sorted(path.stem for path in target_dir.glob("*.yaml"))

        names: list[str] = []
        for directory in sorted(path for path in self.root_dir.iterdir() if path.is_dir()):
            names.extend(path.stem for path in directory.glob("*.yaml"))
        return sorted(names)

    def exists(self, name: str, profile_type: str | None = None) -> bool:
        try:
            self.resolve_path(name=name, profile_type=profile_type)
            return True
        except ProfileNotFoundError:
            return False

    def resolve_path(self, name: str, profile_type: str | None = None) -> Path:
        if profile_type:
            candidate = self.root_dir / profile_type / f"{name}.yaml"
            if candidate.exists():
                return candidate
            raise ProfileNotFoundError(f"找不到 {profile_type} profile: {name}")

        candidates = list(self.root_dir.glob(f"*/{name}.yaml"))
        if not candidates:
            raise ProfileNotFoundError(f"找不到 profile: {name}")
        if len(candidates) > 1:
            raise ProfileNotFoundError(f"profile 名称不唯一，请显式指定类型: {name}")
        return candidates[0]

    def load_text(self, name: str, profile_type: str | None = None) -> str:
        return self.resolve_path(name=name, profile_type=profile_type).read_text(encoding="utf-8")