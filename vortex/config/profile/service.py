"""Profile 门面服务。"""

from __future__ import annotations

from .defaults import ProfileDefaultsProvider
from .loader import ProfileLoader
from .merger import ProfileMerger
from .models import ResolvedProfile
from .overrides import RuntimeOverride
from .resolver import ProfileResolver, SnapshotSelector
from .store import ProfileStore
from .validator import ProfileValidator


class ProfileService:
    """统一串联 profile 的读取、展开、校验与运行时解析。"""

    def __init__(
        self,
        store: ProfileStore,
        loader: ProfileLoader,
        merger: ProfileMerger,
        validator: ProfileValidator,
        resolver: ProfileResolver,
    ) -> None:
        self.store = store
        self.loader = loader
        self.merger = merger
        self.validator = validator
        self.resolver = resolver

    def prepare(
        self,
        name: str,
        profile_type: str,
        command_scope: str,
        snapshot_ref: str | None = None,
        override: RuntimeOverride | None = None,
    ) -> ResolvedProfile:
        raw_profile = self.loader.load(name=name, profile_type=profile_type)
        expanded_profile = self.merger.expand(profile=raw_profile, override=override)
        self.validator.validate(profile=expanded_profile, command_scope=command_scope)
        resolved = self.resolver.resolve(profile=expanded_profile, snapshot_ref=snapshot_ref)
        if override:
            resolved.overrides_applied = override.values
        return resolved


def build_profile_service() -> ProfileService:
    store = ProfileStore()
    loader = ProfileLoader(store=store)
    merger = ProfileMerger(loader=loader, defaults_provider=ProfileDefaultsProvider())
    validator = ProfileValidator()
    resolver = ProfileResolver(snapshot_selector=SnapshotSelector())
    return ProfileService(
        store=store,
        loader=loader,
        merger=merger,
        validator=validator,
        resolver=resolver,
    )