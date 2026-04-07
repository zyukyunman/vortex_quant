"""Phase 0 — shared 模块测试。"""
from __future__ import annotations

from datetime import date, timedelta
from dataclasses import FrozenInstanceError

import pandas as pd
import pytest

from vortex.shared.calendar import TradingCalendar
from vortex.shared.errors import DataError, VortexError
from vortex.shared.events import Event, EventBus, get_event_bus
from vortex.shared.exchange import (
    ExecutionReport,
    SignalSnapshotDescriptor,
    SnapshotDescriptor,
    TargetPortfolio,
)
from vortex.shared.ids import generate_run_id, generate_short_hash
from vortex.shared.rounding import round_ic, round_price, round_sharpe, round_weight
from vortex.shared.timezone import MARKET_TZ, as_of_date, now_market
from vortex.shared.types import Domain, EventLevel

# Domain 是 Literal 类型而非 Enum
# EventLevel 是字符串常量
# Event 字段: event_type, domain, level, payload, timestamp, run_id


# ── errors ─────────────────────────────────────────────────────────

class TestErrors:
    def test_vortex_error_has_code(self):
        e = VortexError(code="TEST_001", message="test")
        assert e.code == "TEST_001"
        assert "test" in str(e)

    def test_data_error_is_vortex_error(self):
        e = DataError(code="DATA_PROVIDER_FETCH_FAILED", message="timeout")
        assert isinstance(e, VortexError)


# ── ids ────────────────────────────────────────────────────────────

class TestIds:
    def test_run_id_format(self):
        rid = generate_run_id("data")
        assert rid.startswith("data_")
        parts = rid.split("_")
        assert len(parts) >= 3  # domain_YYYYMMDD_HHMMSS_hash

    def test_run_id_unique(self):
        ids = {generate_run_id("test") for _ in range(50)}
        assert len(ids) == 50

    def test_short_hash_length(self):
        h = generate_short_hash("test_content")
        assert len(h) >= 4
        assert h.isalnum()


# ── rounding ───────────────────────────────────────────────────────

class TestRounding:
    def test_price_2_decimals(self):
        assert round_price(1800.12567) == 1800.13

    def test_weight_4_decimals(self):
        assert round_weight(0.123456789) == 0.1235

    def test_ic_6_decimals(self):
        assert round_ic(0.0567891234) == 0.056789

    def test_sharpe_4_decimals(self):
        assert round_sharpe(1.23456789) == 1.2346


# ── timezone ───────────────────────────────────────────────────────

class TestTimezone:
    def test_market_tz_is_shanghai(self):
        assert str(MARKET_TZ) in ("Asia/Shanghai", "PRC")

    def test_now_market_has_tz(self):
        dt = now_market()
        assert dt.tzinfo is not None

    def test_as_of_date_returns_date(self):
        from datetime import datetime
        d = as_of_date(now_market())
        assert isinstance(d, date)


# ── calendar ───────────────────────────────────────────────────────

class TestTradingCalendar:
    def test_load_and_query(self, trading_calendar):
        # 2026-04-01 是周三，应该是交易日
        assert trading_calendar.is_trading_day(date(2026, 4, 1))

    def test_weekend_not_trading(self, trading_calendar):
        # 2026-04-04 是周六
        assert not trading_calendar.is_trading_day(date(2026, 4, 4))

    def test_next_trading_day(self, trading_calendar):
        # 周五 → 下周一
        friday = date(2026, 4, 3)
        nxt = trading_calendar.next_trading_day(friday)
        assert nxt == date(2026, 4, 6)

    def test_prev_trading_day(self, trading_calendar):
        monday = date(2026, 4, 6)
        prev = trading_calendar.prev_trading_day(monday)
        assert prev == date(2026, 4, 3)

    def test_trading_days_between(self, trading_calendar):
        days = trading_calendar.trading_days_between(date(2026, 4, 1), date(2026, 4, 7))
        assert date(2026, 4, 4) not in days  # 周六
        assert date(2026, 4, 5) not in days  # 周日
        assert date(2026, 4, 1) in days
        assert date(2026, 4, 7) in days

    def test_empty_calendar_raises(self):
        cal = TradingCalendar()
        with pytest.raises(ValueError):
            cal.next_trading_day(date(2026, 4, 1))


# ── events ─────────────────────────────────────────────────────────

class TestEvents:
    def test_event_creation(self):
        evt = Event(
            event_type="data.sync.completed", domain="data",
            level="info", payload={"rows": 100},
            timestamp="2026-04-07T00:00:00",
        )
        assert evt.event_type == "data.sync.completed"

    def test_bus_pub_sub(self):
        bus = EventBus()
        received = []
        bus.subscribe("test.hello", lambda e: received.append(e))
        bus.publish(Event(
            event_type="test.hello", domain="data",
            level="info", payload={},
            timestamp="2026-04-07T00:00:00",
        ))
        assert len(received) == 1
        assert received[0].event_type == "test.hello"

    def test_bus_singleton(self):
        b1 = get_event_bus()
        b2 = get_event_bus()
        assert b1 is b2


# ── exchange objects ───────────────────────────────────────────────

class TestExchangeObjects:
    def test_snapshot_descriptor_roundtrip(self):
        sd = SnapshotDescriptor(
            snapshot_id="snap_001",
            profile="cn_stock_daily",
            as_of="20260401",
            revision=1,
            datasets=["bars", "fundamental"],
            row_counts={"bars": 1000},
            quality_passed=True,
            created_at="2026-04-01T00:00:00",
            vortex_version="0.1.0",
            lineage={},
        )
        d = sd.to_dict()
        sd2 = SnapshotDescriptor.from_dict(d)
        assert sd2.snapshot_id == sd.snapshot_id
        assert sd2.datasets == sd.datasets

    def test_snapshot_descriptor_immutable(self):
        sd = SnapshotDescriptor(
            snapshot_id="snap_001", profile="test",
            as_of="20260401", revision=1, datasets=[],
            row_counts={}, quality_passed=True,
            created_at="2026-04-01T00:00:00",
            vortex_version="0.1.0", lineage={},
        )
        with pytest.raises((AttributeError, TypeError, FrozenInstanceError)):
            sd.snapshot_id = "changed"  # type: ignore


# ── types ──────────────────────────────────────────────────────────

class TestTypes:
    def test_domain_values(self):
        # Domain 是 Literal 类型：'data' | 'research' | 'strategy' | 'trade' | 'notification'
        from typing import get_args
        args = get_args(Domain)
        assert "data" in args
        assert "research" in args
        assert "strategy" in args
