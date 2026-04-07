"""ProfileService — Profile 解析入口（06 §2.5）。

架构文档定义的 Profile 解析链入口。
封装 Store → Loader → Merger → Validator → Resolver → Dumper 全流程。

用法：
    service = build_profile_service(profiles_dir, resolved_dir)
    resolved = service.prepare("my_profile", "data")
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from vortex.config.profile.models import BaseProfile
from vortex.config.profile.resolver import ProfileResolver, ResolvedField
from vortex.config.profile.store import ProfileStore
from vortex.shared.logging import get_logger

logger = get_logger(__name__)


class ProfileDumper:
    """将 ResolvedProfile 输出为 .resolved.yaml（06 §2.5）。

    输出文件包含字段来源注释，便于审计和 debug。
    """

    def __init__(self, output_dir: Path) -> None:
        self._output_dir = output_dir

    def dump(
        self,
        profile: BaseProfile,
        sources: dict[str, ResolvedField],
        output_path: Path | None = None,
    ) -> Path:
        """写入 `.resolved.yaml` 并返回路径。

        这个步骤的意义不是“运行必需”，而是为了把解析后的最终配置固化下来，
        方便后续审计、排障和学习：

        - 最终 profile 长什么样
        - 每个字段从哪里继承/覆盖而来
        """
        from dataclasses import asdict

        if output_path is None:
            self._output_dir.mkdir(parents=True, exist_ok=True)
            output_path = self._output_dir / f"{profile.name}.resolved.yaml"

        # `asdict(profile)` 会把 dataclass 递归转成普通 dict，便于写 YAML。
        # 这里额外补一个 `_meta.sources`，专门记录字段来源。
        data = asdict(profile)
        source_annotations = {
            k: rf.source for k, rf in sources.items()
        }
        output = {
            "_meta": {
                "profile_name": profile.name,
                "sources": source_annotations,
            },
            **data,
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            yaml.dump(output, f, allow_unicode=True, default_flow_style=False)

        logger.info("Resolved profile 已写入: %s", output_path)
        return output_path


class ProfileService:
    """Profile 解析入口（06 §2.5）。

    加载 → 合并 → 校验 → 解析 → dump → 返回冻结的 ResolvedProfile。
    """

    def __init__(
        self,
        resolver: ProfileResolver,
        dumper: ProfileDumper | None = None,
    ) -> None:
        self._resolver = resolver
        self._dumper = dumper

    def prepare(
        self,
        profile_name: str,
        profile_type: str,
        snapshot_ref: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> BaseProfile:
        """加载 → 合并 → 校验 → 解析 → dump → 返回 ResolvedProfile。

        Args:
            profile_name: profile 名称（对应 YAML 文件名）。
            profile_type: profile 类型（data / research / strategy / trade）。
            snapshot_ref: 快照引用（预留，当前不使用）。
            overrides: CLI 级别 override 字典。

        Returns:
            解析完成的 BaseProfile 实例。
        """
        # 真正的解析工作还是由 resolver 完成，service 只是额外包了一层
        # 更适合业务代码调用的门面。
        profile, sources = self._resolver.resolve(
            profile_name, profile_type, overrides=overrides,
        )

        # 如果配置了 dumper，就把最终结果落盘成 `.resolved.yaml`，
        # 方便把“运行时看到的真实配置”保存下来。
        if self._dumper:
            self._dumper.dump(profile, sources)

        return profile

    def explain(self, profile_name: str, profile_type: str) -> str:
        """返回人类可读的 profile 解释。"""
        return self._resolver.explain(profile_name, profile_type)


def build_profile_service(
    profiles_dir: Path,
    resolved_dir: Path | None = None,
) -> ProfileService:
    """工厂函数：构造完整的 ProfileService。

    Args:
        profiles_dir: YAML profile 文件目录。
        resolved_dir: .resolved.yaml 输出目录（可选）。
    """
    # 工厂函数的意义是：把对象之间的装配细节收口到一处，
    # 调用方不需要自己手动 new Store / Resolver / Dumper。
    store = ProfileStore(profiles_dir)
    resolver = ProfileResolver(store)
    dumper = ProfileDumper(resolved_dir) if resolved_dir else None
    return ProfileService(resolver=resolver, dumper=dumper)
