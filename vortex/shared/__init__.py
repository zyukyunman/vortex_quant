"""Vortex 共享工具与类型。"""
from __future__ import annotations

from vortex.shared.calendar import TradingCalendar
from vortex.shared.errors import (
    ConfigError,
    DataError,
    NotificationError,
    ResearchError,
    RuntimeError_,
    StrategyError,
    TradeError,
    VortexError,
)
from vortex.shared.events import Event, EventBus, get_event_bus
from vortex.shared.exchange import (
    ExecutionReport,
    SignalSnapshotDescriptor,
    SnapshotDescriptor,
    TargetPortfolio,
)
from vortex.shared.ids import generate_run_id, generate_short_hash
from vortex.shared.logging import get_logger, setup_logging
from vortex.shared.rounding import (
    ceil_commission,
    floor_shares,
    round_ic,
    round_price,
    round_sharpe,
    round_weight,
)
from vortex.shared.timezone import MARKET_TZ, as_of_date, now_market, pit_effective_date
from vortex.shared.types import Domain, EventLevel, RunId

__all__ = [
    # errors
    "VortexError",
    "DataError",
    "ResearchError",
    "StrategyError",
    "TradeError",
    "NotificationError",
    "ConfigError",
    "RuntimeError_",
    # logging
    "setup_logging",
    "get_logger",
    # types
    "RunId",
    "Domain",
    "EventLevel",
    # ids
    "generate_run_id",
    "generate_short_hash",
    # calendar
    "TradingCalendar",
    # rounding
    "round_price",
    "round_weight",
    "round_ic",
    "round_sharpe",
    "floor_shares",
    "ceil_commission",
    # timezone
    "MARKET_TZ",
    "now_market",
    "as_of_date",
    "pit_effective_date",
    # exchange objects
    "SnapshotDescriptor",
    "SignalSnapshotDescriptor",
    "TargetPortfolio",
    "ExecutionReport",
    # events
    "Event",
    "EventBus",
    "get_event_bus",
]
