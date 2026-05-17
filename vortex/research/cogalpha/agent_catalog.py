"""Static CogAlpha agent catalog for Vortex research."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AgentSpec:
    """Metadata for one CogAlpha research agent."""

    name: str
    layer: str
    focus: str
    default_fields: tuple[str, ...]
    default_horizons: tuple[int, ...]
    risk_notes: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "layer": self.layer,
            "focus": self.focus,
            "default_fields": list(self.default_fields),
            "default_horizons": list(self.default_horizons),
            "risk_notes": list(self.risk_notes),
        }


_AGENTS: tuple[AgentSpec, ...] = (
    AgentSpec("AgentMarketCycle", "Market Structure & Cycle", "trend phase and market cycle shifts", ("close",), (20, 60, 120), ("regime must be known at decision time",)),
    AgentSpec("AgentVolatilityRegime", "Market Structure & Cycle", "calm-to-stress volatility transitions", ("close", "high", "low"), (10, 20, 60), ("future realized volatility cannot define current state",)),
    AgentSpec("AgentTailRisk", "Extreme Risk & Fragility", "downside sensitivity and left-tail exposure", ("close",), (20, 60, 120), ("future drawdown labels are forbidden",)),
    AgentSpec("AgentCrashPredictor", "Extreme Risk & Fragility", "fragility before crash-like moves", ("close", "high", "low", "amount"), (5, 10, 20), ("avoid selecting only ex-post crash windows",)),
    AgentSpec("AgentLiquidity", "Price-Volume Dynamics", "price impact and trading friction", ("high", "low", "close", "amount", "volume"), (1, 5, 20), ("capacity must be checked for low-liquidity signals",)),
    AgentSpec("AgentOrderImbalance", "Price-Volume Dynamics", "directional pressure inferred from OHLCV", ("open", "high", "low", "close", "amount"), (1, 5, 10), ("do not assume unavailable order-book fields",)),
    AgentSpec("AgentPriceVolumeCoherence", "Price-Volume Dynamics", "price-volume synchronization and divergence", ("close", "volume", "amount"), (5, 20, 60), ("direction may flip across markets",)),
    AgentSpec("AgentVolumeStructure", "Price-Volume Dynamics", "shape, concentration, and rhythm of trading activity", ("volume", "amount"), (1, 5, 20), ("minute data coverage and permissions must be audited",)),
    AgentSpec("AgentDailyTrend", "Price-Volatility Behavior", "multi-day directional continuation", ("close",), (20, 60, 120), ("separate recent reversal from medium momentum",)),
    AgentSpec("AgentReversal", "Price-Volatility Behavior", "mean reversion after overreaction", ("close", "amount"), (1, 5, 20), ("turnover and limit-up/down tradability matter",)),
    AgentSpec("AgentRangeVol", "Price-Volatility Behavior", "range compression and expansion", ("high", "low", "close"), (5, 20, 60), ("adjusted OHLC and limit prices must not be mixed",)),
    AgentSpec("AgentLagResponse", "Price-Volatility Behavior", "lagged feedback among returns, volume, and volatility", ("close", "volume", "amount"), (5, 20), ("lag direction mistakes can create leakage",)),
    AgentSpec("AgentVolAsymmetry", "Price-Volatility Behavior", "asymmetric up/down volatility", ("close", "high", "low"), (10, 20, 60), ("bear-market-only signals need regime checks",)),
    AgentSpec("AgentDrawdown", "Multi-Scale Complexity", "drawdown depth, duration, and recovery geometry", ("close",), (20, 60, 120), ("future recovery speed is a label, not a feature",)),
    AgentSpec("AgentFractal", "Multi-Scale Complexity", "multi-scale roughness and long-memory proxies", ("close",), (20, 60, 120), ("complex transforms need economic explanation",)),
    AgentSpec("AgentRegimeGating", "Stability & Regime-Gating", "when a signal should be active", ("close", "amount"), (20, 60), ("gate state must be PIT-safe",)),
    AgentSpec("AgentStability", "Stability & Regime-Gating", "time consistency and smoothness of signals", ("close",), (20, 60, 120), ("in-sample stability is not out-of-sample proof",)),
    AgentSpec("AgentComposite", "Geometric & Fusion", "synergy and orthogonality among factors", ("close", "amount"), (20, 60), ("avoid full-sample optimized weights",)),
    AgentSpec("AgentCreative", "Geometric & Fusion", "nonlinear transforms and soft gates", ("close", "amount"), (5, 20, 60), ("math-only transformations must be justified",)),
    AgentSpec("AgentBarShape", "Geometric & Fusion", "candle body, shadow, and close-location geometry", ("open", "high", "low", "close"), (1, 5, 10), ("short-horizon costs can dominate",)),
    AgentSpec("AgentHerding", "Geometric & Fusion", "crowding and directional herding", ("close", "amount", "volume"), (1, 5, 20), ("sentiment and limit data need visible-time checks",)),
)

_AGENTS_BY_NAME = {agent.name: agent for agent in _AGENTS}


def registered_agents() -> tuple[AgentSpec, ...]:
    """Return the registered CogAlpha research agents."""

    return _AGENTS


def is_registered_agent(name: str) -> bool:
    """Return whether `name` is a known CogAlpha agent."""

    return name in _AGENTS_BY_NAME


def agent_by_name(name: str) -> AgentSpec:
    """Return one CogAlpha agent or raise a clear error."""

    try:
        return _AGENTS_BY_NAME[name]
    except KeyError as exc:
        raise KeyError(f"unknown CogAlpha agent: {name}") from exc
