"""Phase 0 — config/profile 模块测试。"""
from __future__ import annotations

import pytest
import yaml
from pathlib import Path

from vortex.config.profile.models import DataProfile, ResearchProfile, BaseProfile
from vortex.config.profile.defaults import get_defaults
from vortex.data.provider.tushare_registry import get_default_tushare_datasets
from vortex.config.profile.loader import ProfileLoader
from vortex.config.profile.merger import ProfileMerger
from vortex.config.profile.validator import ProfileValidator
from vortex.config.profile.resolver import ProfileResolver
from vortex.config.profile.store import ProfileStore
from vortex.config.profile.service import ProfileService, ProfileDumper, build_profile_service
from vortex.shared.errors import ConfigError


class TestProfileModels:
    def test_data_profile_has_required_fields(self):
        dp = DataProfile(name="test")
        assert dp.name == "test"
        assert dp.provider == "tushare"
        assert isinstance(dp.datasets, list)

    def test_data_profile_default_datasets_include_all_1a(self):
        """默认 datasets 应与 provider 默认全量列表一致。"""
        dp = DataProfile(name="test")
        assert dp.datasets == get_default_tushare_datasets()

    def test_data_profile_exclude_datasets(self):
        """exclude_datasets 过滤生效。"""
        dp = DataProfile(name="test", exclude_datasets=["events"])
        assert "events" not in dp.effective_datasets
        assert "bars" in dp.effective_datasets

    def test_data_profile_effective_datasets(self):
        """effective_datasets 返回 datasets - exclude_datasets。"""
        dp = DataProfile(
            name="test",
            datasets=["a", "b", "c"],
            exclude_datasets=["b"],
        )
        assert dp.effective_datasets == ["a", "c"]

    def test_data_profile_priority_datasets(self):
        dp = DataProfile(name="test", priority_datasets=["instruments", "calendar"])
        assert dp.priority_datasets == ["instruments", "calendar"]

    def test_research_profile_has_required_fields(self):
        rp = ResearchProfile(name="test")
        assert rp.name == "test"


class TestProfileDefaults:
    def test_data_defaults_exist(self):
        defaults = get_defaults("data")
        assert isinstance(defaults, dict)
        assert len(defaults) > 0

    def test_data_defaults_include_all_1a_datasets(self):
        """默认配置模板应与 provider 默认全量列表一致。"""
        defaults = get_defaults("data")
        assert defaults["datasets"] == get_default_tushare_datasets()

    def test_data_defaults_have_exclude_datasets(self):
        defaults = get_defaults("data")
        assert "exclude_datasets" in defaults
        assert defaults["exclude_datasets"] == []

    def test_unknown_domain_returns_empty(self):
        with pytest.raises(ValueError):
            get_defaults("nonexistent")


class TestProfileLoader:
    def test_load_from_dict(self):
        raw = {"name": "test_profile", "provider": "tushare"}
        loader = ProfileLoader()
        profile = loader.load(raw, "data")
        assert profile.name == "test_profile"


class TestProfileStore:
    def test_save_and_load(self, tmp_path):
        store = ProfileStore(tmp_path)
        profile_data = {
            "name": "cn_daily",
            "provider": "tushare",
            "history_start": "20170101",
        }
        profile_path = tmp_path / "cn_daily.yaml"
        with open(profile_path, "w") as f:
            yaml.dump(profile_data, f)

        loaded = store.load("cn_daily")
        assert loaded["name"] == "cn_daily"

    def test_list_profiles(self, tmp_path):
        for name in ["p1", "p2", "p3"]:
            with open(tmp_path / f"{name}.yaml", "w") as f:
                yaml.dump({"name": name}, f)
        store = ProfileStore(tmp_path)
        profiles = store.list_profiles()
        assert len(profiles) >= 3


class TestProfileResolver:
    def test_resolve_applies_defaults(self, tmp_path):
        profile_data = {"name": "test"}
        with open(tmp_path / "test.yaml", "w") as f:
            yaml.dump(profile_data, f)

        store = ProfileStore(tmp_path)
        resolver = ProfileResolver(store)
        profile, sources = resolver.resolve("test", "data")
        assert profile.name == "test"
        assert profile.provider == "tushare"  # 默认值

    def test_single_inheritance(self, tmp_path):
        """单层继承：child extends parent。"""
        with open(tmp_path / "base.yaml", "w") as f:
            yaml.dump({"name": "base", "provider": "custom_provider", "history_start": "20100101"}, f)
        with open(tmp_path / "child.yaml", "w") as f:
            yaml.dump({"name": "child", "extends": "base", "history_start": "20200101"}, f)

        store = ProfileStore(tmp_path)
        resolver = ProfileResolver(store)
        profile, sources = resolver.resolve("child", "data")

        assert profile.provider == "custom_provider"  # 继承自 base
        assert profile.history_start == "20200101"  # child 自己覆盖
        assert sources["provider"].source == "parent:base"
        assert sources["history_start"].source == "user"

    def test_multi_level_inheritance(self, tmp_path):
        """三层继承：grandchild → parent → grandparent，字段逐层覆盖。"""
        with open(tmp_path / "grandparent.yaml", "w") as f:
            yaml.dump({"name": "grandparent", "provider": "gp_provider", "history_start": "20050101"}, f)
        with open(tmp_path / "parent.yaml", "w") as f:
            yaml.dump({"name": "parent", "extends": "grandparent", "history_start": "20100101"}, f)
        with open(tmp_path / "child.yaml", "w") as f:
            yaml.dump({"name": "child", "extends": "parent"}, f)

        store = ProfileStore(tmp_path)
        resolver = ProfileResolver(store)
        profile, sources = resolver.resolve("child", "data")

        # provider 来自 grandparent（parent 和 child 都没覆盖）
        assert profile.provider == "gp_provider"
        assert sources["provider"].source == "parent:grandparent"
        # history_start 来自 parent（离 child 最近的覆盖者）
        assert profile.history_start == "20100101"
        assert sources["history_start"].source == "parent:parent"

    def test_circular_inheritance_detected(self, tmp_path):
        """循环继承应抛出 ConfigError。"""
        with open(tmp_path / "a.yaml", "w") as f:
            yaml.dump({"name": "a", "extends": "b"}, f)
        with open(tmp_path / "b.yaml", "w") as f:
            yaml.dump({"name": "b", "extends": "a"}, f)

        store = ProfileStore(tmp_path)
        resolver = ProfileResolver(store)
        with pytest.raises(ConfigError, match="循环继承"):
            resolver.resolve("a", "data")

    def test_missing_parent_raises(self, tmp_path):
        """引用不存在的 parent 应抛出 ConfigError。"""
        with open(tmp_path / "orphan.yaml", "w") as f:
            yaml.dump({"name": "orphan", "extends": "nonexistent"}, f)

        store = ProfileStore(tmp_path)
        resolver = ProfileResolver(store)
        with pytest.raises(ConfigError, match="不存在"):
            resolver.resolve("orphan", "data")

    def test_override_takes_priority(self, tmp_path):
        """CLI override 优先级最高。"""
        with open(tmp_path / "base.yaml", "w") as f:
            yaml.dump({"name": "base", "provider": "tushare"}, f)

        store = ProfileStore(tmp_path)
        resolver = ProfileResolver(store)
        profile, sources = resolver.resolve(
            "base", "data", overrides={"provider": "override_provider"},
        )
        assert profile.provider == "override_provider"
        assert sources["provider"].source == "override"


# ── ProfileService (06 §2.5) ─────────────────────────────────────


class TestProfileService:
    def test_prepare_returns_profile(self, tmp_path):
        """ProfileService.prepare() 返回解析后的 profile。"""
        with open(tmp_path / "test.yaml", "w") as f:
            yaml.dump({"name": "test"}, f)

        service = build_profile_service(tmp_path)
        profile = service.prepare("test", "data")
        assert profile.name == "test"
        assert profile.provider == "tushare"

    def test_prepare_dumps_resolved_yaml(self, tmp_path):
        """配置了 resolved_dir 时，prepare 应写出 .resolved.yaml。"""
        profiles_dir = tmp_path / "profiles"
        resolved_dir = tmp_path / "resolved"
        profiles_dir.mkdir()
        with open(profiles_dir / "test.yaml", "w") as f:
            yaml.dump({"name": "test"}, f)

        service = build_profile_service(profiles_dir, resolved_dir)
        service.prepare("test", "data")

        resolved_file = resolved_dir / "test.resolved.yaml"
        assert resolved_file.exists()

        with open(resolved_file) as f:
            data = yaml.safe_load(f)
        assert data["name"] == "test"
        assert "_meta" in data
        assert "sources" in data["_meta"]

    def test_prepare_with_overrides(self, tmp_path):
        """prepare 接受 overrides 参数。"""
        with open(tmp_path / "test.yaml", "w") as f:
            yaml.dump({"name": "test"}, f)

        service = build_profile_service(tmp_path)
        profile = service.prepare(
            "test", "data", overrides={"provider": "custom"},
        )
        assert profile.provider == "custom"
