"""数据源适配层。"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from vortex.data.provider.base import DataProvider
    from vortex.data.provider.registry import ProviderRegistry

__all__ = ["DataProvider", "ProviderRegistry"]


def __getattr__(name: str):
    if name == "DataProvider":
        from vortex.data.provider.base import DataProvider

        return DataProvider
    if name == "ProviderRegistry":
        from vortex.data.provider.registry import ProviderRegistry

        return ProviderRegistry
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
