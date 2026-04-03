"""Profile 默认值提供器。"""

from __future__ import annotations

from copy import deepcopy
from typing import Any


class ProfileDefaultsProvider:
    """集中维护默认值，避免每个 profile 文件重复填写。"""

    _DEFAULTS: dict[str, dict[str, Any]] = {
        "data": {
            "enabled": True,
            "tags": [],
            "universe": "all_a",
            "freqs": ["1d"],
            "provider": "tushare",
            "datasets": [
                "instruments",
                "trading_calendar",
                "bar_1d",
                "adj_factor",
                "daily_basic",
            ],
            "timezone": "Asia/Shanghai",
            "calendar": "xshg",
            "quality_policy": {
                "mode": "fail_closed",
                "required_rules": ["required_columns", "non_empty"],
            },
            "pit_policy": {"mode": "strict"},
            "snapshot_policy": {
                "publish_time": "22:00",
                "selector": "latest_success",
                "retain_days": 365,
            },
        },
        "research": {
            "enabled": True,
            "tags": [],
            "signal_output": {"format": "signal_snapshot", "topk": 100},
        },
        "strategy": {"enabled": True, "tags": [], "backtest_engine": "backtrader"},
        "trade": {"enabled": True, "tags": [], "mode": "paper", "gateway": "miniqmt"},
    }

    def get_defaults(self, profile_type: str, market: str | None = None) -> dict[str, Any]:
        defaults = deepcopy(self._DEFAULTS.get(profile_type, {}))
        if profile_type == "data" and market in {None, "cn_stock"}:
            defaults.setdefault("market", "cn_stock")
        return defaults