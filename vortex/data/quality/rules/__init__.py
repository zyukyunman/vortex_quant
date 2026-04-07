"""内置质量规则集。"""
from __future__ import annotations

from vortex.data.quality.rules.date_continuity import DateContinuityRule
from vortex.data.quality.rules.missing_ratio import MissingRatioRule
from vortex.data.quality.rules.no_all_nan import NoAllNanRule
from vortex.data.quality.rules.not_empty import NotEmptyRule
from vortex.data.quality.rules.ohlcv_range import OhlcvRangeRule
from vortex.data.quality.rules.volume_zero_ratio import VolumeZeroRatioRule

ALL_RULES = [
    NotEmptyRule(),
    NoAllNanRule(),
    DateContinuityRule(),
    OhlcvRangeRule(),
    MissingRatioRule(),
    VolumeZeroRatioRule(),
]

__all__ = [
    "ALL_RULES",
    "DateContinuityRule",
    "MissingRatioRule",
    "NoAllNanRule",
    "NotEmptyRule",
    "OhlcvRangeRule",
    "VolumeZeroRatioRule",
]
