"""Executable baseline-proxy recipes for CogAlpha agent ideas."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

from vortex.research.alpha101_registry import DailyFactorInputs, FormulaBuilder, FormulaSpec
from vortex.research.cogalpha.agent_catalog import agent_by_name
from vortex.research.cogalpha.schema import AlphaCandidate, Direction, LineageRecord
from vortex.research.factor_ops import correlation, cs_rank, cs_zscore, signed_power, ts_mean, ts_std


RecipeStatus = Literal["executable", "planned"]
SemanticStatus = Literal["proxy", "faithful_proxy", "mutation_proxy"]


@dataclass(frozen=True)
class CogAlphaAgentRecipe:
    """A runnable factor-research unit for one CogAlpha agent idea."""

    agent: str
    template_id: str
    name: str
    hypothesis: str
    expression: str
    required_fields: tuple[str, ...]
    default_horizons: tuple[int, ...]
    direction: Direction
    builder: FormulaBuilder | None
    status: RecipeStatus = "executable"
    semantic_status: SemanticStatus = "proxy"
    semantic_notes: str = ""
    parent_templates: tuple[str, ...] = ()
    lookback_windows: tuple[int, ...] = ()
    risk_notes: tuple[str, ...] = ()
    description: str = ""

    def build_candidate(self, *, alpha_id: str | None = None) -> AlphaCandidate:
        """Create AlphaCandidate metadata for this recipe."""

        if self.status != "executable":
            raise ValueError(f"recipe is not executable: {self.template_id}")
        resolved_alpha_id = alpha_id or f"vtx_cogalpha_{self.template_id}"
        return AlphaCandidate(
            alpha_id=resolved_alpha_id,
            name=self.name,
            agent=self.agent,
            hypothesis=self.hypothesis,
            expression=self.expression,
            required_fields=self.required_fields,
            lookback_windows=self.lookback_windows,
            horizons=self.default_horizons,
            direction=self.direction,
            lineage=LineageRecord(generation=0, guidance_type="recipe"),
            metadata={
                "template_id": self.template_id,
                "recipe_status": self.status,
                "semantic_status": self.semantic_status,
                "semantic_notes": self.semantic_notes,
                "parent_templates": list(self.parent_templates),
                "risk_notes": list(self.risk_notes),
            },
        )

    def build_formula_spec(self, *, formula_id: str | None = None) -> FormulaSpec:
        """Create a FormulaSpec backed by a safe local builder."""

        if self.status != "executable" or self.builder is None:
            raise ValueError(f"recipe is not executable: {self.template_id}")
        return FormulaSpec(
            formula_id=formula_id or f"cogalpha_{self.template_id}",
            name=self.name,
            family=f"cogalpha:{self.agent}",
            role_hint="cogalpha_agent_recipe",
            required_fields=self.required_fields,
            default_horizons=self.default_horizons,
            builder=self.builder,
            description=self.description or self.hypothesis,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "agent": self.agent,
            "template_id": self.template_id,
            "name": self.name,
            "hypothesis": self.hypothesis,
            "expression": self.expression,
            "required_fields": list(self.required_fields),
            "default_horizons": list(self.default_horizons),
            "direction": self.direction,
            "status": self.status,
            "semantic_status": self.semantic_status,
            "semantic_notes": self.semantic_notes,
            "parent_templates": list(self.parent_templates),
            "lookback_windows": list(self.lookback_windows),
            "risk_notes": list(self.risk_notes),
            "description": self.description,
        }


def executable_recipes() -> tuple[CogAlphaAgentRecipe, ...]:
    """Return executable baseline-proxy recipes for CogAlpha catalog agents."""

    return _EXECUTABLE_RECIPES


def planned_recipes() -> tuple[CogAlphaAgentRecipe, ...]:
    """Return planned recipes.

    Phase 2.6 makes all catalog agents executable as baseline proxies, so this
    remains only as a backward-compatible empty hook.
    """

    return ()


def all_agent_recipes() -> tuple[CogAlphaAgentRecipe, ...]:
    """Return executable recipes for every catalog agent."""

    return executable_recipes()


def executable_recipe_by_template(template_id: str) -> CogAlphaAgentRecipe:
    """Return one executable recipe by template id."""

    recipes = {recipe.template_id: recipe for recipe in executable_recipes()}
    try:
        return recipes[template_id]
    except KeyError as exc:
        raise KeyError(f"unknown executable CogAlpha recipe: {template_id}") from exc


def _safe_div(numerator: pd.DataFrame, denominator: pd.DataFrame) -> pd.DataFrame:
    return numerator.div(denominator.replace(0.0, np.nan))


def _clean(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.replace([np.inf, -np.inf], np.nan)


def _pct_change(frame: pd.DataFrame) -> pd.DataFrame:
    return _clean(frame / frame.shift(1) - 1.0)


def _ret(frame: pd.DataFrame, window: int) -> pd.DataFrame:
    return _clean(frame / frame.shift(window) - 1.0)


def _range_ratio(inputs: DailyFactorInputs) -> pd.DataFrame:
    return _clean(_safe_div(inputs.high - inputs.low, inputs.close))


def _zero_fill(frame: pd.DataFrame) -> pd.DataFrame:
    return _clean(frame).fillna(0.0)


def _broadcast_series(series: pd.Series, columns: pd.Index) -> pd.DataFrame:
    return pd.DataFrame(
        np.tile(series.to_numpy()[:, None], (1, len(columns))),
        index=series.index,
        columns=columns,
    )


def _market_cycle_relative_trend_60d(inputs: DailyFactorInputs) -> pd.DataFrame:
    daily_ret = _pct_change(inputs.close)
    stock_trend = _ret(inputs.close, 60)
    market_trend = stock_trend.mean(axis=1)
    breadth = (daily_ret > 0).rolling(20, min_periods=10).mean().mean(axis=1)
    market_vol = daily_ret.mean(axis=1).rolling(20, min_periods=10).std()
    regime = (market_trend.fillna(0.0) + (breadth.fillna(0.5) - 0.5) - market_vol.fillna(0.0) * 8.0)
    regime_gate = _broadcast_series(regime.clip(lower=-1.0, upper=1.0), inputs.close.columns)
    low_vol_defense = cs_rank(-ts_std(daily_ret, 20, min_periods=10))
    trend_signal = cs_rank(stock_trend)
    return cs_rank(trend_signal * regime_gate + low_vol_defense * (1.0 - regime_gate.abs()))


def _volatility_regime_compression_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    ret = _pct_change(inputs.close)
    short_vol = ts_std(ret, 20, min_periods=10)
    long_vol = ts_std(ret, 60, min_periods=30)
    return cs_rank(-_safe_div(short_vol, long_vol))


def _tail_risk_downside_vol_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    ret = _pct_change(inputs.close)
    downside = ret.where(ret < 0.0, 0.0)
    return cs_rank(-ts_std(downside, 20, min_periods=10))


def _crash_fragility_high_range_low_liquidity_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    daily_ret = _pct_change(inputs.close)
    range_pressure = ts_mean(_range_ratio(inputs), 20, min_periods=10)
    short_vol = ts_std(daily_ret, 10, min_periods=5)
    long_vol = ts_std(daily_ret, 60, min_periods=30)
    vol_state = _safe_div(short_vol, long_vol)
    vol_expansion = _safe_div(short_vol, short_vol.shift(10))
    liquidity_ratio = _safe_div(inputs.amount, ts_mean(inputs.amount, 20, min_periods=10))
    downside_range = ts_mean(_range_ratio(inputs).where(daily_ret < 0.0, 0.0), 20, min_periods=10)
    market_ret = _broadcast_series(daily_ret.mean(axis=1), inputs.close.columns)
    market_sync = correlation(daily_ret, market_ret, 20, min_periods=10)
    fragility = (
        _zero_fill(cs_zscore(range_pressure))
        + _zero_fill(cs_zscore(vol_state))
        + _zero_fill(cs_zscore(vol_expansion))
        + _zero_fill(cs_zscore(downside_range))
        + _zero_fill(cs_zscore(market_sync))
        - _zero_fill(cs_zscore(liquidity_ratio))
    )
    return cs_rank(-fragility)


def _liquidity_range_impact(inputs: DailyFactorInputs) -> pd.DataFrame:
    impact = _safe_div(inputs.high - inputs.low, inputs.amount)
    return cs_rank(-impact)


def _order_imbalance_close_strength_5d(inputs: DailyFactorInputs) -> pd.DataFrame:
    intraday_range = inputs.high - inputs.low
    close_location = _safe_div(inputs.close - inputs.low, intraday_range) - 0.5
    body_strength = _safe_div(inputs.close - inputs.open, intraday_range)
    amount_ratio = _safe_div(inputs.amount, ts_mean(inputs.amount, 20, min_periods=10))
    return cs_rank(ts_mean((close_location + body_strength) * amount_ratio, 5, min_periods=3))


def _short_reversal_5d(inputs: DailyFactorInputs) -> pd.DataFrame:
    ret_5d = _ret(inputs.close, 5)
    return cs_rank(-ret_5d)


def _volume_structure_surge_decay_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    volume_ratio = _safe_div(inputs.volume, ts_mean(inputs.volume, 20, min_periods=10))
    return cs_rank(-np.abs(volume_ratio - 1.0))


def _daily_trend_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    trend = _ret(inputs.close, 20)
    return cs_rank(trend)


def _range_vol_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    range_ratio = _safe_div(inputs.high - inputs.low, inputs.close)
    return cs_rank(-ts_std(range_ratio, 20))


def _price_volume_coherence_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    price_ret = _pct_change(inputs.close)
    amount_chg = _pct_change(inputs.amount)
    return cs_zscore(_clean(correlation(price_ret, amount_chg, 20)))


def _lag_response_volume_leads_price_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    amount_lead = _ret(inputs.amount, 5).shift(5)
    price_response = _ret(inputs.close, 5)
    return cs_zscore(ts_mean(amount_lead - price_response, 20, min_periods=10))


def _vol_asymmetry_downside_upside_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    ret = _pct_change(inputs.close)
    downside = ret.where(ret < 0.0, 0.0)
    upside = ret.where(ret > 0.0, 0.0)
    asymmetry = ts_std(downside, 20, min_periods=10) - ts_std(upside, 20, min_periods=10)
    return cs_rank(-asymmetry)


def _drawdown_recovery_position_60d(inputs: DailyFactorInputs) -> pd.DataFrame:
    rolling_high = inputs.close.rolling(60, min_periods=30).max()
    drawdown = _safe_div(inputs.close, rolling_high) - 1.0
    return cs_rank(drawdown)


def _fractal_multiscale_consistency_20_60d(inputs: DailyFactorInputs) -> pd.DataFrame:
    path_length_60d = inputs.close.diff().abs().rolling(60, min_periods=30).sum()
    displacement_60d = (inputs.close - inputs.close.shift(60)).abs()
    efficiency_ratio = _safe_div(displacement_60d, path_length_60d)
    ret = _pct_change(inputs.close)
    short_var = ts_std(ret, 20, min_periods=10) ** 2
    long_var = ts_std(ret, 60, min_periods=30) ** 2
    variance_ratio_proxy = _safe_div(long_var, short_var * 3.0)
    multiscale_gap = np.abs(cs_zscore(_ret(inputs.close, 20)) - cs_zscore(_ret(inputs.close, 60)))
    roughness = (1.0 - efficiency_ratio) + np.abs(variance_ratio_proxy - 1.0) + multiscale_gap
    return cs_rank(-roughness)


def _regime_gated_trend_lowvol_60d(inputs: DailyFactorInputs) -> pd.DataFrame:
    trend = cs_rank(_ret(inputs.close, 60))
    low_vol = cs_rank(-ts_std(_pct_change(inputs.close), 20, min_periods=10))
    return cs_rank(trend * low_vol)


def _stability_signal_smoothness_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    ret = _pct_change(inputs.close)
    smoothness = -ts_std(ret, 20, min_periods=10)
    return cs_rank(smoothness)


def _composite_trend_reversal_liquidity(inputs: DailyFactorInputs) -> pd.DataFrame:
    trend = cs_zscore(_ret(inputs.close, 60))
    reversal = cs_zscore(-_ret(inputs.close, 5))
    liquidity = cs_zscore(-_safe_div(inputs.high - inputs.low, inputs.amount))
    return cs_rank(trend + reversal + liquidity)


def _creative_soft_rank_range_liquidity(inputs: DailyFactorInputs) -> pd.DataFrame:
    trend_parent = cs_rank(_ret(inputs.close, 60)) - 0.5
    reversal_parent = cs_rank(-_ret(inputs.close, 5)) - 0.5
    liquidity_parent = cs_rank(-_safe_div(inputs.high - inputs.low, inputs.amount)) - 0.5
    volatility_gate = cs_rank(-ts_std(_pct_change(inputs.close), 20, min_periods=10))
    blended_parent = trend_parent + reversal_parent + liquidity_parent
    return signed_power(blended_parent, 3.0) * volatility_gate


def _bar_shape_close_location(inputs: DailyFactorInputs) -> pd.DataFrame:
    intraday_range = inputs.high - inputs.low
    close_location = _safe_div(inputs.close - inputs.low, intraday_range) - 0.5
    body_strength = _safe_div(inputs.close - inputs.open, intraday_range)
    return cs_rank(ts_mean(close_location + body_strength, 5))


def _herding_amount_crowding_reversal_20d(inputs: DailyFactorInputs) -> pd.DataFrame:
    ret = _pct_change(inputs.close)
    stock_direction = np.sign(ret)
    consensus = stock_direction.mean(axis=1).rolling(10, min_periods=5).mean()
    consensus_frame = _broadcast_series(consensus, inputs.close.columns)
    alignment = stock_direction * consensus_frame
    crowding = _safe_div(inputs.amount, ts_mean(inputs.amount, 20, min_periods=10))
    runup = _ret(inputs.close, 20)
    herding_pressure = ts_mean(alignment * crowding, 10, min_periods=5) * runup
    return cs_rank(-herding_pressure)


_EXECUTABLE_RECIPES: tuple[CogAlphaAgentRecipe, ...] = (
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentMarketCycle").name,
        template_id="market_cycle_relative_trend_60d",
        name="cogalpha_market_cycle_relative_trend_60d",
        hypothesis="Stock trend should be interpreted through a PIT-safe market-cycle gate built from breadth, market trend, and market volatility.",
        expression="cs_rank(cs_rank(close / delay(close, 60) - 1) * ts_mean(safe_div(delta(close, 1), delay(close, 1)), 20) + cs_rank(-ts_std(safe_div(delta(close, 1), delay(close, 1)), 20)))",
        required_fields=("close",),
        default_horizons=(20, 60, 120),
        direction="positive",
        builder=_market_cycle_relative_trend_60d,
        semantic_status="faithful_proxy",
        semantic_notes="OHLCV 内语义强化：用等权市场趋势、上涨宽度和市场波动构造 regime gate，再作用到个股趋势/低波防守；仍不是完整宏观周期识别 agent。",
        lookback_windows=(60,),
        risk_notes=("market regime proxy still needs index-level confirmation before strategy use",),
        description="Regime-gated stock trend using market breadth, equal-weight market trend, and volatility state.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentVolatilityRegime").name,
        template_id="volatility_regime_compression_20d",
        name="cogalpha_volatility_regime_compression_20d",
        hypothesis="Short volatility compressed versus long volatility can precede more stable forward returns.",
        expression="cs_rank(-safe_div(ts_std(safe_div(delta(close, 1), delay(close, 1)), 20), ts_std(safe_div(delta(close, 1), delay(close, 1)), 60)))",
        required_fields=("close",),
        default_horizons=(10, 20, 60),
        direction="positive",
        builder=_volatility_regime_compression_20d,
        lookback_windows=(20, 60),
        risk_notes=("volatility compression can also precede breakouts in either direction",),
        description="Short realized volatility divided by long realized volatility, ranked low to high.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentTailRisk").name,
        template_id="tail_risk_downside_vol_20d",
        name="cogalpha_tail_risk_downside_vol_20d",
        hypothesis="Lower recent downside volatility can reduce left-tail exposure in candidate pools.",
        expression="cs_rank(-ts_std(where(safe_div(delta(close, 1), delay(close, 1)) < 0, safe_div(delta(close, 1), delay(close, 1)), 0), 20))",
        required_fields=("close",),
        default_horizons=(20, 60, 120),
        direction="positive",
        builder=_tail_risk_downside_vol_20d,
        lookback_windows=(20,),
        risk_notes=("tail-risk filters should be combined with liquidity and suspension checks",),
        description="Negative 20-day downside-return volatility.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentCrashPredictor").name,
        template_id="crash_fragility_high_range_low_liquidity_20d",
        name="cogalpha_crash_fragility_high_range_low_liquidity_20d",
        hypothesis="Crash fragility accumulates when range pressure, volatility expansion, downside range, market synchronization, and liquidity dry-up appear together.",
        expression="cs_rank(-(cs_zscore(ts_mean(safe_div(high - low, close), 20)) + cs_zscore(safe_div(ts_std(safe_div(delta(close, 1), delay(close, 1)), 10), ts_std(safe_div(delta(close, 1), delay(close, 1)), 60))) - cs_zscore(safe_div(amount, ts_mean(amount, 20)))))",
        required_fields=("high", "low", "close", "amount"),
        default_horizons=(5, 10, 20),
        direction="positive",
        builder=_crash_fragility_high_range_low_liquidity_20d,
        semantic_status="faithful_proxy",
        semantic_notes="OHLCV 内风险过滤代理：加入波动短长比、波动扩张、下行 range、市场同步和成交额枯竭；输出低脆弱性偏好，不是崩盘标签预测。",
        lookback_windows=(20,),
        risk_notes=("fragility is a risk filter, not a standalone long thesis",),
        description="Negative crash-fragility proxy combining range pressure, volatility expansion, liquidity dry-up, downside range, and market synchronization.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentLiquidity").name,
        template_id="liquidity_range_impact",
        name="liquidity_range_impact",
        hypothesis="Lower range-per-amount impact can proxy better tradability and lower liquidity stress.",
        expression="cs_rank(-safe_div(high - low, amount))",
        required_fields=("high", "low", "amount"),
        default_horizons=(1, 5, 20),
        direction="positive",
        builder=_liquidity_range_impact,
        lookback_windows=(1,),
        risk_notes=("capacity still requires amount and turnover review",),
        description="Cross-sectional rank of negative daily range divided by amount.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentOrderImbalance").name,
        template_id="order_imbalance_close_strength_5d",
        name="cogalpha_order_imbalance_close_strength_5d",
        hypothesis="Strong close location and positive body with participation can proxy buy-side pressure.",
        expression="cs_rank(ts_mean((safe_div(close - low, high - low) - 0.5 + safe_div(close - open, high - low)) * safe_div(amount, ts_mean(amount, 20)), 5))",
        required_fields=("open", "high", "low", "close", "amount"),
        default_horizons=(1, 5, 10),
        direction="positive",
        builder=_order_imbalance_close_strength_5d,
        lookback_windows=(5, 20),
        risk_notes=("daily OHLCV imbalance is only a proxy, not true order-book imbalance",),
        description="5-day close-location and body-strength signal scaled by amount participation.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentPriceVolumeCoherence").name,
        template_id="price_volume_coherence_20d",
        name="cogalpha_price_volume_coherence_20d",
        hypothesis="Price and amount coherence can reveal sustained participation behind moves.",
        expression="cs_zscore(correlation(safe_div(delta(close, 1), delay(close, 1)), safe_div(delta(amount, 1), delay(amount, 1)), 20))",
        required_fields=("close", "amount"),
        default_horizons=(5, 20, 60),
        direction="unknown",
        builder=_price_volume_coherence_20d,
        lookback_windows=(20,),
        risk_notes=("direction may flip between momentum and exhaustion regimes",),
        description="20-day rolling correlation between close returns and amount changes.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentVolumeStructure").name,
        template_id="volume_structure_surge_decay_20d",
        name="cogalpha_volume_structure_surge_decay_20d",
        hypothesis="Stable volume near its own baseline can avoid one-off crowding and stale inactivity.",
        expression="cs_rank(-abs(safe_div(volume, ts_mean(volume, 20)) - 1))",
        required_fields=("volume",),
        default_horizons=(1, 5, 20),
        direction="positive",
        builder=_volume_structure_surge_decay_20d,
        lookback_windows=(20,),
        risk_notes=("stable volume is not sufficient capacity evidence without amount checks",),
        description="Negative absolute deviation of volume from its 20-day mean.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentReversal").name,
        template_id="short_reversal_5d",
        name="cogalpha_short_reversal_5d",
        hypothesis="Short-term overreaction can mean-revert after recent weak performance.",
        expression="cs_rank(-(close / delay(close, 5) - 1))",
        required_fields=("close",),
        default_horizons=(1, 5, 10),
        direction="positive",
        builder=_short_reversal_5d,
        lookback_windows=(5,),
        risk_notes=("short-horizon cost and limit-up/down tradability can dominate",),
        description="Negative 5-day return as a short-horizon reversal recipe.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentDailyTrend").name,
        template_id="daily_trend_20d",
        name="cogalpha_daily_trend_20d",
        hypothesis="Recent 20-day relative strength can rank continuation candidates.",
        expression="cs_rank(close / delay(close, 20) - 1)",
        required_fields=("close",),
        default_horizons=(5, 20, 60),
        direction="positive",
        builder=_daily_trend_20d,
        lookback_windows=(20,),
        risk_notes=("trend and reversal regimes must be separated before strategy promotion",),
        description="20-day cross-sectional momentum recipe.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentRangeVol").name,
        template_id="range_vol_20d",
        name="cogalpha_range_vol_20d",
        hypothesis="Lower recent intraday range volatility can proxy defensive stability.",
        expression="cs_rank(-ts_std(safe_div(high - low, close), 20))",
        required_fields=("high", "low", "close"),
        default_horizons=(10, 20, 60),
        direction="positive",
        builder=_range_vol_20d,
        lookback_windows=(20,),
        risk_notes=("low range can also mean stale trading or suspension risk",),
        description="Negative 20-day volatility of daily high-low range ratio.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentLagResponse").name,
        template_id="lag_response_volume_leads_price_20d",
        name="cogalpha_lag_response_volume_leads_price_20d",
        hypothesis="Past amount acceleration not yet matched by price can proxy delayed response.",
        expression="cs_zscore(ts_mean(delay(amount / delay(amount, 5) - 1, 5) - (close / delay(close, 5) - 1), 20))",
        required_fields=("close", "amount"),
        default_horizons=(5, 20),
        direction="unknown",
        builder=_lag_response_volume_leads_price_20d,
        lookback_windows=(5, 20),
        risk_notes=("lagged relationship direction must be reviewed by fitness, not assumed",),
        description="Lagged amount momentum minus current price response over a 20-day window.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentVolAsymmetry").name,
        template_id="vol_asymmetry_downside_upside_20d",
        name="cogalpha_vol_asymmetry_downside_upside_20d",
        hypothesis="Lower downside volatility relative to upside volatility can identify better risk asymmetry.",
        expression="cs_rank(-(ts_std(where(safe_div(delta(close, 1), delay(close, 1)) < 0, safe_div(delta(close, 1), delay(close, 1)), 0), 20) - ts_std(where(safe_div(delta(close, 1), delay(close, 1)) > 0, safe_div(delta(close, 1), delay(close, 1)), 0), 20)))",
        required_fields=("close",),
        default_horizons=(10, 20, 60),
        direction="positive",
        builder=_vol_asymmetry_downside_upside_20d,
        lookback_windows=(20,),
        risk_notes=("downside/upside asymmetry can be regime-dependent",),
        description="Negative downside-volatility minus upside-volatility asymmetry.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentDrawdown").name,
        template_id="drawdown_recovery_position_60d",
        name="cogalpha_drawdown_recovery_position_60d",
        hypothesis="Stocks closer to their trailing high may have stronger recovery position and lower drawdown drag.",
        expression="cs_rank(safe_div(close, ts_max(close, 60)) - 1)",
        required_fields=("close",),
        default_horizons=(20, 60, 120),
        direction="positive",
        builder=_drawdown_recovery_position_60d,
        lookback_windows=(60,),
        risk_notes=("drawdown position should not use future recovery speed",),
        description="Current close relative to trailing 60-day high.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentFractal").name,
        template_id="fractal_multiscale_consistency_20_60d",
        name="cogalpha_fractal_multiscale_consistency_20_60d",
        hypothesis="Lower path roughness and more coherent variance scaling can proxy better multi-scale structure and long-memory behavior.",
        expression="cs_rank(-(1 - safe_div(abs(delta(close, 60)), ts_sum(abs(delta(close, 1)), 60)) + abs(safe_div(signed_power(ts_std(safe_div(delta(close, 1), delay(close, 1)), 60), 2), signed_power(ts_std(safe_div(delta(close, 1), delay(close, 1)), 20), 2) * 3) - 1)))",
        required_fields=("close",),
        default_horizons=(20, 60, 120),
        direction="positive",
        builder=_fractal_multiscale_consistency_20_60d,
        semantic_status="faithful_proxy",
        semantic_notes="OHLCV 内语义强化：用 efficiency ratio、20/60 日方差比例和多 horizon gap 代理 path roughness / long-memory；不是严格分形维度估计。",
        lookback_windows=(20, 60),
        risk_notes=("multi-scale consistency is a proxy, not a true fractal estimator",),
        description="Negative multi-scale roughness proxy using path efficiency, variance-ratio deviation, and 20/60-day return gap.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentRegimeGating").name,
        template_id="regime_gated_trend_lowvol_60d",
        name="cogalpha_regime_gated_trend_lowvol_60d",
        hypothesis="Trend works better when gated by low realized volatility.",
        expression="cs_rank(cs_rank(close / delay(close, 60) - 1) * cs_rank(-ts_std(safe_div(delta(close, 1), delay(close, 1)), 20)))",
        required_fields=("close",),
        default_horizons=(20, 60),
        direction="positive",
        builder=_regime_gated_trend_lowvol_60d,
        lookback_windows=(20, 60),
        risk_notes=("gate uses only realized past volatility, not future regime labels",),
        description="60-day trend multiplied by low-volatility gate.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentStability").name,
        template_id="stability_signal_smoothness_20d",
        name="cogalpha_stability_signal_smoothness_20d",
        hypothesis="Smoother recent return path can reduce turnover and fragile signal jumps.",
        expression="cs_rank(-ts_std(safe_div(delta(close, 1), delay(close, 1)), 20))",
        required_fields=("close",),
        default_horizons=(20, 60, 120),
        direction="positive",
        builder=_stability_signal_smoothness_20d,
        lookback_windows=(20,),
        risk_notes=("smoothness is not out-of-sample stability proof",),
        description="Negative 20-day return volatility as a signal smoothness proxy.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentComposite").name,
        template_id="composite_trend_reversal_liquidity",
        name="cogalpha_composite_trend_reversal_liquidity",
        hypothesis="Trend, short reversal, and liquidity can be complementary weak signals.",
        expression="cs_rank(cs_zscore(close / delay(close, 60) - 1) + cs_zscore(-(close / delay(close, 5) - 1)) + cs_zscore(-safe_div(high - low, amount)))",
        required_fields=("high", "low", "close", "amount"),
        default_horizons=(20, 60),
        direction="positive",
        builder=_composite_trend_reversal_liquidity,
        lookback_windows=(5, 60),
        risk_notes=("composite weights are fixed, not optimized on full sample",),
        description="Equal-weight z-score blend of trend, reversal, and liquidity impact.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentCreative").name,
        template_id="creative_soft_rank_range_liquidity",
        name="cogalpha_creative_soft_rank_range_liquidity",
        hypothesis="A deterministic mutation can nonlinearly blend parent trend, reversal, and liquidity recipes under a low-volatility soft gate.",
        expression="signed_power(cs_rank(close / delay(close, 60) - 1) - 0.5 + cs_rank(-(close / delay(close, 5) - 1)) - 0.5 + cs_rank(-safe_div(high - low, amount)) - 0.5, 3) * cs_rank(-ts_std(safe_div(delta(close, 1), delay(close, 1)), 20))",
        required_fields=("high", "low", "close", "amount"),
        default_horizons=(5, 20, 60),
        direction="positive",
        builder=_creative_soft_rank_range_liquidity,
        semantic_status="mutation_proxy",
        semantic_notes="确定性 mutation proxy：以 trend/reversal/liquidity 三个父模板为输入，做 nonlinear rank blend 和 low-vol soft gate；不是独立经济因子。",
        parent_templates=("daily_trend_20d", "short_reversal_5d", "liquidity_range_impact"),
        lookback_windows=(1,),
        risk_notes=("nonlinear transform must be checked for duplicate exposure",),
        description="Deterministic nonlinear mutation of trend, reversal, and liquidity parent templates under a low-volatility gate.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentBarShape").name,
        template_id="bar_shape_close_location_5d",
        name="cogalpha_bar_shape_close_location_5d",
        hypothesis="Persistent strong close location within the daily bar can indicate short-term demand.",
        expression="cs_rank(ts_mean(safe_div(close - low, high - low) - 0.5 + safe_div(close - open, high - low), 5))",
        required_fields=("open", "high", "low", "close"),
        default_horizons=(1, 5, 10),
        direction="positive",
        builder=_bar_shape_close_location,
        lookback_windows=(5,),
        risk_notes=("short-horizon execution costs must be checked",),
        description="5-day average of close-location and body-strength bar geometry.",
    ),
    CogAlphaAgentRecipe(
        agent=agent_by_name("AgentHerding").name,
        template_id="herding_amount_crowding_reversal_20d",
        name="cogalpha_herding_amount_crowding_reversal_20d",
        hypothesis="Crowding risk rises when a stock aligns with cross-sectional directional consensus while amount participation is elevated.",
        expression="cs_rank(-(safe_div(amount, ts_mean(amount, 20)) * (close / delay(close, 20) - 1) * ts_mean(safe_div(delta(close, 1), delay(close, 1)), 10)))",
        required_fields=("close", "amount"),
        default_horizons=(1, 5, 20),
        direction="positive",
        builder=_herding_amount_crowding_reversal_20d,
        semantic_status="faithful_proxy",
        semantic_notes="OHLCV 内羊群代理：用截面方向共识、个股与群体方向一致性和成交额拥挤衡量 herding pressure；仍缺真实盘口/资金流/情绪数据。",
        lookback_windows=(20,),
        risk_notes=("herding proxies require turnover and limit-status checks before strategy use",),
        description="Negative herding-pressure proxy from directional consensus alignment, amount crowding, and 20-day run-up.",
    ),
)
