"""DataPipeline 排序逻辑测试。"""
from __future__ import annotations

from vortex.config.profile.models import DataProfile
from vortex.data.pipeline import _ordered_datasets


class TestOrderedDatasets:
    def test_priority_datasets_are_applied_first(self):
        profile = DataProfile(
            name="default",
            datasets=["instruments", "calendar", "bars", "fundamental", "events"],
            priority_datasets=["bars", "calendar"],
        )

        assert _ordered_datasets(profile) == [
            "bars",
            "calendar",
            "instruments",
            "fundamental",
            "events",
        ]

    def test_excluded_datasets_are_removed_before_ordering(self):
        profile = DataProfile(
            name="default",
            datasets=["instruments", "calendar", "bars", "fundamental", "events"],
            exclude_datasets=["calendar", "events"],
            priority_datasets=["events", "bars", "calendar"],
        )

        assert _ordered_datasets(profile) == [
            "bars",
            "instruments",
            "fundamental",
        ]

    def test_unknown_priority_dataset_is_ignored(self):
        profile = DataProfile(
            name="default",
            datasets=["instruments", "calendar", "bars"],
            priority_datasets=["valuation", "bars"],
        )

        assert _ordered_datasets(profile) == [
            "bars",
            "instruments",
            "calendar",
        ]

    def test_remaining_datasets_are_grouped_by_update_frequency(self):
        profile = DataProfile(
            name="default",
            datasets=["fundamental", "bars", "monthly", "index_weight", "events"],
            priority_datasets=["events"],
        )
        frequency_map = {
            "fundamental": "quarterly",
            "bars": "daily",
            "monthly": "monthly",
            "index_weight": "weekly",
            "events": "other",
        }

        assert _ordered_datasets(
            profile,
            frequency_resolver=lambda dataset: frequency_map[dataset],
            frequency_priority=("daily", "weekly", "monthly", "quarterly", "other"),
        ) == [
            "events",
            "bars",
            "index_weight",
            "monthly",
            "fundamental",
        ]
