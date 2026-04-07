"""ProviderRegistry — 按名称查找和注册 DataProvider（06 §3.1）。"""
from __future__ import annotations

from vortex.data.provider.base import DataProvider
from vortex.shared.errors import DataError
from vortex.shared.logging import get_logger

logger = get_logger(__name__)


class ProviderRegistry:
    """按名称查找和注册 DataProvider。"""

    def __init__(self) -> None:
        self._providers: dict[str, DataProvider] = {}

    def register(self, provider: DataProvider) -> None:
        """注册一个 provider 实例。重复注册同名 provider 会覆盖。"""
        name = provider.name
        self._providers[name] = provider
        logger.info("注册 DataProvider: %s", name)

    def get(self, name: str) -> DataProvider:
        """按名称获取 provider。找不到时抛出 DataError。"""
        if name not in self._providers:
            raise DataError(
                code="DATA_PROVIDER_NOT_FOUND",
                message=f"未注册的 DataProvider: {name}",
                detail={"name": name, "available": list(self._providers.keys())},
            )
        return self._providers[name]

    def list_providers(self) -> list[str]:
        """返回已注册的 provider 名称列表（排序）。"""
        return sorted(self._providers.keys())
