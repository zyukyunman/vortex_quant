"""Profile 存储层：只负责“从哪里读取配置”。

这个文件刻意保持职责单一：

- 负责根据 profile 名推导 YAML 路径
- 负责读取 YAML 文本并转成 Python dict
- 不负责默认值、继承、字段解释或业务校验

这样做的好处是，读取失败和业务语义失败可以清晰分层。
"""
from __future__ import annotations

from pathlib import Path

import yaml

from vortex.shared.errors import ConfigError


class ProfileStore:
    """从 YAML 文件加载 profile。

    Profile 文件路径规则: {profiles_dir}/{name}.yaml
    """

    def __init__(self, profiles_dir: Path) -> None:
        self._dir = profiles_dir

    def _path(self, name: str) -> Path:
        """把逻辑名称转换成磁盘路径。"""
        return self._dir / f"{name}.yaml"

    def load(self, name: str) -> dict:
        """按 profile 名称读取原始 YAML，并返回 dict。

        这里返回的是“还没做默认值填充、也没做类型转换”的原始映射。
        后续是否能变成合法的 Profile，要交给 loader / validator / resolver。
        """
        path = self._path(name)
        if not path.exists():
            raise ConfigError(
                code="CONFIG_PROFILE_NOT_FOUND",
                message=f"Profile 文件不存在: {path}",
            )
        # safe_load 会把 YAML 解析成 Python 基本对象（dict/list/str/int/...）。
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        # 我们要求 profile 文件顶层必须是一个对象（dict），
        # 因为后面要按字段名去解析，而不是解析成一个纯列表或单个值。
        if not isinstance(data, dict):
            raise ConfigError(
                code="CONFIG_PROFILE_INVALID_FORMAT",
                message=f"Profile 文件格式错误（应为 YAML dict）: {path}",
            )
        return data

    def list_profiles(self) -> list[str]:
        """列出目录下所有可用的 profile 名称。"""
        if not self._dir.exists():
            return []
        return sorted(p.stem for p in self._dir.glob("*.yaml"))

    def exists(self, name: str) -> bool:
        """快速判断某个 profile 文件是否存在。"""
        return self._path(name).exists()
