"""TushareProvider — Tushare 数据源适配。

当前实现目标：

1. 保留已有核心专用 fetch（instruments / calendar / bars / fundamental / events）
2. 对其余 dataset 统一走 registry 驱动的通用抓取入口 `fetch_dataset`
3. 默认 datasets 覆盖全部可落盘、可批处理的 Tushare dataset
4. 对少数 `symbol_range + date` 数据集，在 provider 内部自动比较“日期整批抓”和“股票历史抓”

说明：
- `bars` / `fundamental` / `events` / `valuation` 是 Vortex 内部稳定 dataset 名
  - `bars`       -> Tushare `daily`
  - `fundamental`-> Tushare `income`
  - `events`     -> Tushare `dividend`
  - `valuation`  -> Tushare `daily_basic`
- 其余 dataset 大多直接沿用 Tushare API 名称
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any, Callable

import pandas as pd

from vortex.data.provider.tushare_registry import (
    DEFAULT_TUSHARE_PRIORITY_DATASETS,
    DEFAULT_TUSHARE_POINTS,
    DEFAULT_TUSHARE_INDEX_DAILY_CODES,
    TUSHARE_DATASET_REGISTRY,
    TUSHARE_FUND_MARKETS,
    TUSHARE_INDEX_DAILY_MARKETS,
    TUSHARE_INDEX_MARKETS,
    TUSHARE_STOCK_EXCHANGES,
    get_default_tushare_datasets,
    get_tushare_api_access_rule,
    get_tushare_dataset_spec,
    parse_tushare_permission_keys,
    parse_tushare_points,
    resolve_tushare_dataset_name,
    resolve_tushare_points_rpm,
)
from vortex.shared.errors import DataError
from vortex.shared.logging import get_logger

logger = get_logger(__name__)

_RATE_LIMIT_SECONDS = 0.3
_MAX_RETRIES = 3
_BACKOFF_BASE = 1.0
_SAFE_RPM_FACTOR = 0.8
_QUARTER_VIP_APIS = {
    "income",
    "balancesheet",
    "cashflow",
    "fina_indicator",
    "forecast",
    "express",
}


def _try_import_tushare() -> Any:
    """尝试导入 tushare，未安装时给出清晰提示。"""
    try:
        import tushare  # type: ignore[import-untyped]

        return tushare
    except ImportError as exc:
        raise DataError(
            code="DATA_PROVIDER_IMPORT_FAILED",
            message=(
                "tushare 未安装到当前 vortex 运行环境。"
                f"当前解释器: {sys.executable}。"
                f"请运行: {sys.executable} -m pip install tushare。"
                "注意：给其他 Python 执行 pip install 不会影响当前 vortex。"
            ),
            detail={"python": sys.executable},
        ) from exc


def _to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _parse_yyyymmdd(value: str) -> date:
    normalized = _normalize_date_key(value)
    if normalized is None or len(normalized) != 8:
        raise ValueError(f"invalid yyyymmdd value: {value}")
    return datetime.strptime(normalized, "%Y%m%d").date()


def _normalize_date_key(value: object) -> str | None:
    """把各种日期/月份表达尽量归一到可比较的 YYYYMMDD。"""
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    if len(digits) == 6:
        return digits + "01"
    if len(digits) == 4:
        return digits + "0101"
    return None


def _concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


class TushareProvider:
    """Tushare 数据源。实现 DataProvider Protocol。"""

    def __init__(self, token: str | None = None) -> None:
        self._token = token or os.environ.get("TUSHARE_TOKEN", "")
        if not self._token:
            raise DataError(
                code="DATA_PROVIDER_CONFIG_ERROR",
                message="Tushare token 未提供。请设置 TUSHARE_TOKEN 环境变量或传入 token 参数。",
            )

        self._ts = _try_import_tushare()
        self._ts.set_token(self._token)
        self._api = self._ts.pro_api()
        self._account_points = parse_tushare_points(
            os.environ.get("TUSHARE_POINTS"),
            default=DEFAULT_TUSHARE_POINTS,
        )
        self._extra_permissions = parse_tushare_permission_keys(
            os.environ.get("TUSHARE_EXTRA_PERMISSIONS")
        )
        self._account_rpm = resolve_tushare_points_rpm(self._account_points)
        self._global_effective_rpm = (
            max(1, int(self._account_rpm * _SAFE_RPM_FACTOR))
            if self._account_rpm
            else 0
        )
        self._last_call_time = 0.0
        self._frame_cache: dict[str, pd.DataFrame] = {}
        logger.info(
            "Tushare 权限档位: points=%s, regular_rpm=%s, effective_global_rpm=%s, extra_permissions=%s",
            self._account_points,
            self._account_rpm,
            self._global_effective_rpm,
            sorted(self._extra_permissions),
        )

    # ------------------------------------------------------------------
    # Provider 元信息
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return "tushare"

    @property
    def supported_markets(self) -> list[str]:
        return ["cn_stock", "cn_index", "cn_fund", "hk", "us", "macro", "global"]

    @property
    def dataset_registry(self) -> dict[str, dict[str, object]]:
        return TUSHARE_DATASET_REGISTRY

    def resolve_dataset(self, dataset: str) -> str:
        return resolve_tushare_dataset_name(dataset)

    def get_active_datasets(self, phase: str = "5") -> list[str]:
        """返回指定 phase 及之前已启用的数据集。"""
        phase_order = ["1A", "1B", "2", "3A", "3B", "4", "5"]
        max_idx = phase_order.index(phase) if phase in phase_order else len(phase_order) - 1
        active_phases = set(phase_order[: max_idx + 1])
        return [
            name
            for name, meta in TUSHARE_DATASET_REGISTRY.items()
            if meta.get("phase", "") in active_phases
        ]

    def smoke_test(self) -> bool:
        """冒烟测试：尝试拉取最近交易日历。"""
        try:
            today = date.today()
            df = self._call_dataset_api(
                "trade_cal",
                exchange="SSE",
                start_date=_to_yyyymmdd(today - timedelta(days=10)),
                end_date=_to_yyyymmdd(today),
                is_open="1",
            )
            return df is not None and not df.empty
        except Exception:
            return False

    def describe_dataset_access(self, dataset: str) -> dict[str, object]:
        """描述当前账号对某个 dataset 的访问能力。"""
        canonical = self.resolve_dataset(dataset)
        spec = get_tushare_dataset_spec(canonical)
        api_name = str(spec.get("api") or canonical)
        access = self._describe_api_access(api_name)
        return {
            "dataset": canonical,
            "api": api_name,
            **access,
        }

    def _describe_api_access(self, api_name: str) -> dict[str, object]:
        rule = get_tushare_api_access_rule(api_name)
        access_kind = str(rule.get("access", "points"))
        if access_kind == "permission":
            permission_key = str(rule.get("permission_key", ""))
            allowed = permission_key in self._extra_permissions
            max_rpm = int(rule.get("rpm", 0)) if allowed else 0
            reason = None if allowed else f"缺少独立权限: {permission_key}"
            effective_rpm = (
                min(self._global_effective_rpm, max(1, int(max_rpm * _SAFE_RPM_FACTOR)))
                if max_rpm and self._global_effective_rpm
                else 0
            )
            return {
                "allowed": allowed,
                "access": access_kind,
                "permission_key": permission_key,
                "max_rpm": max_rpm,
                "effective_rpm": effective_rpm,
                "reason": reason,
            }

        min_points = int(rule.get("min_points", 2000))
        allowed = self._account_points >= min_points
        max_rpm = min(
            self._account_rpm,
            int(rule.get("rpm", self._account_rpm or 0)),
        ) if allowed else 0
        reason = None if allowed else f"当前积分 {self._account_points} 低于接口要求 {min_points}"
        effective_rpm = (
            min(self._global_effective_rpm, max(1, int(max_rpm * _SAFE_RPM_FACTOR)))
            if max_rpm and self._global_effective_rpm
            else 0
        )
        return {
            "allowed": allowed,
            "access": access_kind,
            "min_points": min_points,
            "account_points": self._account_points,
            "max_rpm": max_rpm,
            "effective_rpm": effective_rpm,
            "reason": reason,
        }

    def _assert_api_access(self, api_name: str) -> dict[str, object]:
        access = self._describe_api_access(api_name)
        if access.get("allowed"):
            return access
        code = "DATA_PROVIDER_PERMISSION_REQUIRED"
        if access.get("access") == "points":
            code = "DATA_PROVIDER_PERMISSION_DENIED"
        raise DataError(
            code=code,
            message=f"Tushare 接口不可访问: {api_name}",
            detail=access,
        )

    # ------------------------------------------------------------------
    # 兼容的专用 fetch
    # ------------------------------------------------------------------

    def fetch_instruments(self, market: str) -> pd.DataFrame:
        """获取 A 股标的列表。"""
        self._check_market(market)
        df = self._call_dataset_api(
            "stock_basic",
            exchange="",
            list_status="L",
            fields="ts_code,name,list_date,delist_date,industry,market",
        )
        df = self._normalize_dataset_frame("instruments", df)
        if "market_cap" not in df.columns:
            df["market_cap"] = None
        keep_cols = ["symbol", "name", "list_date", "delist_date", "industry", "market_cap"]
        for col in keep_cols:
            if col not in df.columns:
                df[col] = None
        return df[keep_cols].sort_values("symbol").reset_index(drop=True)

    def fetch_calendar(self, market: str, start: date, end: date) -> list[date]:
        """获取交易日历。"""
        self._check_market(market)
        df = self._call_dataset_api(
            "trade_cal",
            exchange="SSE",
            start_date=_to_yyyymmdd(start),
            end_date=_to_yyyymmdd(end),
            is_open="1",
        )
        if df is None or df.empty:
            return []
        days: list[date] = []
        for val in df["cal_date"]:
            key = _normalize_date_key(val)
            if key is not None:
                days.append(date(int(key[:4]), int(key[4:6]), int(key[6:8])))
        return sorted(d for d in days if start <= d <= end)

    def fetch_bars(
        self,
        market: str,
        symbols: list[str],
        freq: str,
        start: date,
        end: date,
        *,
        trading_days: list[date] | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
    ) -> pd.DataFrame:
        """获取日线行情。"""
        self._check_market(market)
        trading_days = (
            list(trading_days)
            if trading_days is not None
            else self.fetch_calendar(market, start, end)
        )
        df = self._fetch_trade_day_all(
            "daily",
            trading_days,
            progress_callback=progress_callback,
            cancel_check=cancel_check,
            progress_label="bars",
        )
        df = self._normalize_dataset_frame("bars", df, start=start, end=end)
        if symbols and "symbol" in df.columns:
            df = df[df["symbol"].isin(symbols)]
        keep_cols = ["symbol", "date", "open", "high", "low", "close", "volume", "amount"]
        for col in keep_cols:
            if col not in df.columns:
                df[col] = None
        return df[keep_cols].sort_values(["date", "symbol"]).reset_index(drop=True)

    def fetch_fundamental(
        self,
        market: str,
        symbols: list[str],
        fields: list[str],
        start: date,
        end: date,
        *,
        partition_values: list[str] | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
    ) -> pd.DataFrame:
        """获取利润表数据。"""
        self._check_market(market)
        api_fields = "ts_code,ann_date,end_date," + ",".join(fields)
        raw = self._fetch_quarter_statement_range(
            "income",
            symbols,
            start,
            end,
            fields=api_fields,
            partition_values=partition_values,
            progress_callback=progress_callback,
            cancel_check=cancel_check,
            progress_label="fundamental",
        )
        result = self._normalize_dataset_frame(
            "fundamental",
            raw,
            start=start,
            end=end,
        )
        if symbols and "symbol" in result.columns:
            result = result[result["symbol"].isin(symbols)]
        if result.empty:
            return pd.DataFrame(columns=["symbol", "ann_date", "report_date"] + fields)
        required = ["symbol", "ann_date", "report_date"] + fields
        for col in required:
            if col not in result.columns:
                result[col] = None
        return result[required].sort_values(["ann_date", "symbol"]).reset_index(drop=True)

    def fetch_events(
        self,
        market: str,
        symbols: list[str],
        start: date,
        end: date,
        *,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
    ) -> pd.DataFrame:
        """获取分红事件。"""
        self._check_market(market)
        frames: list[pd.DataFrame] = []
        total_steps = len(symbols)
        for index, symbol in enumerate(symbols, start=1):
            self._check_cancel_requested(cancel_check)
            df = self._call_dataset_api("dividend", ts_code=symbol)
            if df is not None and not df.empty:
                frames.append(df)
            self._emit_loop_progress(
                progress_callback,
                index,
                total_steps,
                f"events {symbol}",
            )
        result = self._normalize_dataset_frame("events", _concat_frames(frames), start=start, end=end)
        if result.empty:
            return pd.DataFrame(columns=["symbol", "date"])
        if "date" in result.columns:
            return result.sort_values(["date", "symbol"]).reset_index(drop=True)
        return result

    # ------------------------------------------------------------------
    # 通用 dataset 入口
    # ------------------------------------------------------------------

    def fetch_dataset(
        self,
        dataset: str,
        market: str,
        start: date,
        end: date,
        *,
        symbols: list[str] | None = None,
        trading_days: list[date] | None = None,
        partition_values: list[str] | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
    ) -> pd.DataFrame:
        """按 dataset 名抓取数据。"""
        self._check_market(market)
        canonical = self.resolve_dataset(dataset)
        symbols = symbols or []
        trading_days = trading_days or []

        if canonical == "instruments":
            self._check_cancel_requested(cancel_check)
            return self.fetch_instruments(market)
        if canonical == "calendar":
            self._check_cancel_requested(cancel_check)
            days = self.fetch_calendar(market, start, end)
            self._emit_loop_progress(progress_callback, 1, 1, "calendar")
            return pd.DataFrame({"cal_date": [d.strftime("%Y%m%d") for d in days]})
        if canonical == "bars":
            return self.fetch_bars(
                market,
                symbols,
                "1d",
                start,
                end,
                trading_days=trading_days,
                progress_callback=progress_callback,
                cancel_check=cancel_check,
            )
        if canonical == "fundamental":
            return self.fetch_fundamental(
                market,
                symbols,
                ["revenue", "net_profit", "total_assets"],
                start,
                end,
                partition_values=partition_values,
                progress_callback=progress_callback,
                cancel_check=cancel_check,
            )
        if canonical == "events":
            return self.fetch_events(
                market,
                symbols,
                start,
                end,
                progress_callback=progress_callback,
                cancel_check=cancel_check,
            )

        spec = get_tushare_dataset_spec(canonical)
        mode = str(spec.get("fetch_mode", "reference_once"))

        if mode == "reference_once":
            raw = self._fetch_reference_once(
                str(spec["api"]),
                progress_callback=progress_callback,
                cancel_check=cancel_check,
                progress_label=canonical,
            )
        elif mode == "fund_reference":
            raw = self._fetch_fund_reference(
                str(spec["api"]),
                progress_callback=progress_callback,
                cancel_check=cancel_check,
                progress_label=canonical,
            )
        elif mode == "index_reference":
            raw = self._fetch_index_reference(
                str(spec["api"]),
                progress_callback=progress_callback,
                cancel_check=cancel_check,
                progress_label=canonical,
            )
        elif mode == "exchange_reference":
            raw = self._fetch_exchange_reference(
                str(spec["api"]),
                str(spec.get("param_name", "exchange")),
                [
                    str(value)
                    for value in (
                        spec.get("loop_values")
                        or TUSHARE_STOCK_EXCHANGES
                    )
                ],
                progress_callback=progress_callback,
                cancel_check=cancel_check,
                progress_label=canonical,
            )
        elif mode == "trade_day_all":
            raw = self._fetch_trade_day_all(
                str(spec["api"]),
                trading_days or self.fetch_calendar(market, start, end),
                api_params=dict(spec.get("date_batch_params") or {}),
                progress_callback=progress_callback,
                cancel_check=cancel_check,
                progress_label=canonical,
            )
        elif mode == "symbol_once":
            raw = self._fetch_symbol_once(
                str(spec["api"]),
                symbols,
                progress_callback=progress_callback,
                cancel_check=cancel_check,
                progress_label=canonical,
            )
        elif mode == "symbol_range":
            selected_mode, selected_api, selected_api_params = self._select_symbol_range_route(
                canonical,
                spec,
                symbols,
                start,
                end,
                trading_days=trading_days,
                partition_values=partition_values,
            )
            if selected_mode == "trade_day_all":
                raw = self._fetch_trade_day_all(
                    selected_api,
                    trading_days,
                    api_params=selected_api_params,
                    progress_callback=progress_callback,
                    cancel_check=cancel_check,
                    progress_label=canonical,
                )
            else:
                raw = self._fetch_symbol_range(
                    str(spec["api"]),
                    symbols,
                    start,
                    end,
                    partition_values=partition_values,
                    progress_callback=progress_callback,
                    cancel_check=cancel_check,
                    progress_label=canonical,
                )
        elif mode == "symbol_quarter_range":
            raw = self._fetch_symbol_quarter_range(
                str(spec["api"]),
                symbols,
                start,
                end,
                partition_values=partition_values,
                progress_callback=progress_callback,
                cancel_check=cancel_check,
                progress_label=canonical,
            )
        elif mode == "index_loop_range":
            raw = self._fetch_index_loop_range(
                str(spec["api"]),
                start,
                end,
                str(spec.get("param_name", "index_code")),
                partition_values=partition_values,
                progress_callback=progress_callback,
                cancel_check=cancel_check,
                progress_label=canonical,
            )
        elif mode == "member_loop":
            raw = self._fetch_member_loop(
                str(spec["api"]),
                str(spec.get("loop_source", "")),
                str(spec.get("param_name", "ts_code")),
                progress_callback=progress_callback,
                cancel_check=cancel_check,
                progress_label=canonical,
            )
        elif mode == "date_range":
            raw = self._fetch_date_range(
                str(spec["api"]),
                start,
                end,
                progress_callback=progress_callback,
                cancel_check=cancel_check,
                progress_label=canonical,
            )
        elif mode == "pro_bar":
            raw = self._fetch_pro_bar(
                symbols,
                start,
                end,
                progress_callback=progress_callback,
                cancel_check=cancel_check,
                progress_label=canonical,
            )
        elif mode == "minute_range":
            raw = self._fetch_minute_range(
                symbols,
                start,
                end,
                progress_callback=progress_callback,
                cancel_check=cancel_check,
                progress_label=canonical,
            )
        elif mode == "realtime_snapshot":
            raw = self._fetch_realtime_snapshot(
                str(spec["api"]),
                progress_callback=progress_callback,
                cancel_check=cancel_check,
                progress_label=canonical,
            )
        else:
            raise DataError(
                code="DATA_PROVIDER_UNSUPPORTED_FETCH_MODE",
                message=f"未知 fetch_mode: {mode}",
                detail={"dataset": canonical, "mode": mode},
            )

        result = self._normalize_dataset_frame(canonical, raw, start=start, end=end)
        if symbols and "symbol" in result.columns and mode in {
            "trade_day_all",
            "symbol_once",
            "symbol_range",
            "symbol_quarter_range",
            "pro_bar",
            "minute_range",
        }:
            result = result[result["symbol"].isin(symbols)]
        return result

    # ------------------------------------------------------------------
    # 通用抓取模式
    # ------------------------------------------------------------------

    def _fetch_reference_once(
        self,
        api_name: str,
        *,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        self._check_cancel_requested(cancel_check)
        if api_name == "st":
            df = self._call_dataset_api(api_name)
            self._emit_loop_progress(progress_callback, 1, 1, progress_label or api_name)
            return df
        if api_name in {"ths_index", "dc_index", "index_classify", "index_member_all"}:
            df = self._call_dataset_api(api_name)
            self._emit_loop_progress(progress_callback, 1, 1, progress_label or api_name)
            return df
        raise DataError(
            code="DATA_PROVIDER_UNSUPPORTED_REFERENCE",
            message=f"不支持的 reference_once dataset: {api_name}",
        )

    def _fetch_fund_reference(
        self,
        api_name: str,
        *,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        frames = []
        total_steps = len(TUSHARE_FUND_MARKETS)
        for index, market in enumerate(TUSHARE_FUND_MARKETS, start=1):
            self._check_cancel_requested(cancel_check)
            df = self._call_dataset_api(api_name, market=market)
            if df is not None and not df.empty:
                frames.append(df)
            self._emit_loop_progress(
                progress_callback,
                index,
                total_steps,
                f"{progress_label or api_name} market={market}",
            )
        return _concat_frames(frames)

    def _fetch_index_reference(
        self,
        api_name: str,
        *,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        frames = []
        total_steps = len(TUSHARE_INDEX_MARKETS)
        for index, market in enumerate(TUSHARE_INDEX_MARKETS, start=1):
            self._check_cancel_requested(cancel_check)
            df = self._call_dataset_api(api_name, market=market)
            if df is not None and not df.empty:
                frames.append(df)
            self._emit_loop_progress(
                progress_callback,
                index,
                total_steps,
                f"{progress_label or api_name} market={market}",
        )
        return _concat_frames(frames)

    def _fetch_exchange_reference(
        self,
        api_name: str,
        param_name: str,
        loop_values: list[str],
        *,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        frames = []
        total_steps = len(loop_values)
        for index, value in enumerate(loop_values, start=1):
            self._check_cancel_requested(cancel_check)
            df = self._call_dataset_api(api_name, **{param_name: value})
            if df is not None and not df.empty:
                frames.append(df)
            self._emit_loop_progress(
                progress_callback,
                index,
                total_steps,
                f"{progress_label or api_name} {param_name}={value}",
            )
        return _concat_frames(frames)

    def _fetch_trade_day_all(
        self,
        api_name: str,
        trading_days: list[date],
        *,
        api_params: dict[str, Any] | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        frames = []
        total_steps = len(trading_days)
        extra_params = dict(api_params or {})
        for index, day in enumerate(trading_days, start=1):
            self._check_cancel_requested(cancel_check)
            df = self._call_dataset_api(
                api_name,
                trade_date=_to_yyyymmdd(day),
                **extra_params,
            )
            if df is not None and not df.empty:
                frames.append(df)
            self._emit_loop_progress(
                progress_callback,
                index,
                total_steps,
                f"{progress_label or api_name} trade_date={_to_yyyymmdd(day)}",
            )
        return _concat_frames(frames)

    def _select_symbol_range_route(
        self,
        dataset: str,
        spec: dict[str, Any],
        symbols: list[str],
        start: date,
        end: date,
        *,
        trading_days: list[date],
        partition_values: list[str] | None = None,
    ) -> tuple[str, str, dict[str, Any]]:
        """为 symbol_range + date 数据集选择最省请求数的安全抓法。"""
        api_name = str(spec["api"])
        date_batch_api = str(spec.get("date_batch_api") or api_name)
        date_batch_params = dict(spec.get("date_batch_params") or {})

        if (
            str(spec.get("partition_by") or "").strip() != "date"
            or not spec.get("date_batch_supported")
            or not symbols
            or not trading_days
        ):
            return "symbol_range", date_batch_api, date_batch_params

        row_limit = spec.get("date_batch_row_limit")
        if isinstance(row_limit, int) and row_limit > 0 and len(symbols) > row_limit:
            logger.info(
                "dataset=%s 路径选择: 日期整批抓不可用，single_request_limit=%d < symbol_count=%d，回退股票历史抓取",
                dataset,
                row_limit,
                len(symbols),
            )
            return "symbol_range", date_batch_api, date_batch_params

        date_batch_requests = len(trading_days)
        symbol_windows = self._resolve_symbol_range_windows(
            start,
            end,
            partition_values=partition_values,
        )
        symbol_range_requests = len(symbols) * len(symbol_windows)
        selected_mode = (
            "trade_day_all"
            if date_batch_requests <= symbol_range_requests
            else "symbol_range"
        )
        logger.info(
            "dataset=%s 路径选择: date_batch_requests=%d, symbol_range_requests=%d, selected=%s",
            dataset,
            date_batch_requests,
            symbol_range_requests,
            selected_mode,
        )
        return selected_mode, date_batch_api, date_batch_params

    def _fetch_symbol_once(
        self,
        api_name: str,
        symbols: list[str],
        *,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        if not symbols:
            return pd.DataFrame()
        frames = []
        total_steps = len(symbols)
        for index, symbol in enumerate(symbols, start=1):
            self._check_cancel_requested(cancel_check)
            df = self._call_dataset_api(api_name, ts_code=symbol)
            if df is not None and not df.empty:
                frames.append(df)
            self._emit_loop_progress(
                progress_callback,
                index,
                total_steps,
                f"{progress_label or api_name} {symbol}",
            )
        return _concat_frames(frames)

    def _fetch_symbol_range(
        self,
        api_name: str,
        symbols: list[str],
        start: date,
        end: date,
        *,
        partition_values: list[str] | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        if not symbols:
            return pd.DataFrame()
        frames = []
        year_ranges = self._resolve_symbol_range_windows(
            start,
            end,
            partition_values=partition_values,
        )
        total_steps = len(year_ranges) * len(symbols)
        current_step = 0
        for year_start, year_end in year_ranges:
            for symbol in symbols:
                self._check_cancel_requested(cancel_check)
                df = self._call_dataset_api(
                    api_name,
                    ts_code=symbol,
                    start_date=_to_yyyymmdd(year_start),
                    end_date=_to_yyyymmdd(year_end),
                )
                if df is not None and not df.empty:
                    frames.append(df)
                current_step += 1
                self._emit_loop_progress(
                    progress_callback,
                    current_step,
                    total_steps,
                    f"{progress_label or api_name} {symbol} {year_start:%Y%m%d}-{year_end:%Y%m%d}",
                )
        return _concat_frames(frames)

    def _fetch_symbol_quarter_range(
        self,
        api_name: str,
        symbols: list[str],
        start: date,
        end: date,
        *,
        partition_values: list[str] | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        return self._fetch_quarter_statement_range(
            api_name,
            symbols,
            start,
            end,
            partition_values=partition_values,
            progress_callback=progress_callback,
            cancel_check=cancel_check,
            progress_label=progress_label,
        )

    def _fetch_quarter_statement_range(
        self,
        api_name: str,
        symbols: list[str],
        start: date,
        end: date,
        *,
        fields: str | None = None,
        partition_values: list[str] | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        if not symbols:
            return pd.DataFrame()
        frames = []
        quarter_ranges = self._resolve_quarter_ranges(
            start,
            end,
            partition_values=partition_values,
        )
        fetch_api_name = self._resolve_quarter_fetch_api(api_name)
        if fetch_api_name != api_name:
            logger.info(
                "%s 使用季度批量接口: api=%s, quarters=%d",
                progress_label or api_name,
                fetch_api_name,
                len(quarter_ranges),
            )
            total_steps = len(quarter_ranges)
            for index, (_q_start, q_end) in enumerate(quarter_ranges, start=1):
                self._check_cancel_requested(cancel_check)
                params: dict[str, Any] = {"period": _to_yyyymmdd(q_end)}
                if fields:
                    params["fields"] = fields
                df = self._call_dataset_api(fetch_api_name, **params)
                if df is not None and not df.empty:
                    frames.append(df)
                self._emit_loop_progress(
                    progress_callback,
                    index,
                    total_steps,
                    f"{progress_label or api_name} period={q_end:%Y%m%d} via={fetch_api_name}",
                )
            return _concat_frames(frames)

        total_steps = len(quarter_ranges) * len(symbols)
        current_step = 0
        for q_start, q_end in quarter_ranges:
            for symbol in symbols:
                self._check_cancel_requested(cancel_check)
                params: dict[str, Any] = {
                    "ts_code": symbol,
                    "start_date": _to_yyyymmdd(q_start),
                    "end_date": _to_yyyymmdd(q_end),
                }
                if fields:
                    params["fields"] = fields
                df = self._call_dataset_api(api_name, **params)
                if df is not None and not df.empty:
                    frames.append(df)
                current_step += 1
                self._emit_loop_progress(
                    progress_callback,
                    current_step,
                    total_steps,
                    f"{progress_label or api_name} {symbol} {q_start:%Y%m%d}-{q_end:%Y%m%d}",
                )
        return _concat_frames(frames)

    def _resolve_quarter_ranges(
        self,
        start: date,
        end: date,
        *,
        partition_values: list[str] | None = None,
    ) -> list[tuple[date, date]]:
        if not partition_values:
            return self._split_by_quarter(start, end)
        quarter_ranges: list[tuple[date, date]] = []
        seen: set[str] = set()
        for partition_value in partition_values:
            if partition_value in seen:
                continue
            seen.add(partition_value)
            quarter_end = _parse_yyyymmdd(partition_value)
            quarter_start_month = quarter_end.month - 2
            quarter_ranges.append(
                (date(quarter_end.year, quarter_start_month, 1), quarter_end)
            )
        return quarter_ranges

    def _fetch_index_loop_range(
        self,
        api_name: str,
        start: date,
        end: date,
        param_name: str,
        *,
        partition_values: list[str] | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        codes = self._load_index_codes()
        frames = []
        year_ranges = self._resolve_symbol_range_windows(
            start,
            end,
            partition_values=partition_values,
        )
        total_steps = len(year_ranges) * len(codes)
        current_step = 0
        for year_start, year_end in year_ranges:
            for code in codes:
                self._check_cancel_requested(cancel_check)
                df = self._call_dataset_api(
                    api_name,
                    **{
                        param_name: code,
                        "start_date": _to_yyyymmdd(year_start),
                        "end_date": _to_yyyymmdd(year_end),
                    },
                )
                if df is not None and not df.empty:
                    frames.append(df)
                current_step += 1
                self._emit_loop_progress(
                    progress_callback,
                    current_step,
                    total_steps,
                    f"{progress_label or api_name} {code} {year_start:%Y%m%d}-{year_end:%Y%m%d}",
                )
        return _concat_frames(frames)

    def _fetch_member_loop(
        self,
        api_name: str,
        loop_source: str,
        param_name: str,
        *,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        parent_codes = self._load_member_parent_codes(loop_source)
        frames = []
        total_steps = len(parent_codes)
        for index, code in enumerate(parent_codes, start=1):
            self._check_cancel_requested(cancel_check)
            df = self._call_dataset_api(api_name, **{param_name: code})
            if df is not None and not df.empty:
                frames.append(df)
            self._emit_loop_progress(
                progress_callback,
                index,
                total_steps,
                f"{progress_label or api_name} {code}",
            )
        return _concat_frames(frames)

    def _fetch_date_range(
        self,
        api_name: str,
        start: date,
        end: date,
        *,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        frames = []
        year_ranges = self._split_by_year(start, end)
        total_steps = len(year_ranges)
        for index, (year_start, year_end) in enumerate(year_ranges, start=1):
            self._check_cancel_requested(cancel_check)
            df = self._call_dataset_api(
                api_name,
                start_date=_to_yyyymmdd(year_start),
                end_date=_to_yyyymmdd(year_end),
            )
            if df is not None and not df.empty:
                frames.append(df)
            self._emit_loop_progress(
                progress_callback,
                index,
                total_steps,
                f"{progress_label or api_name} {year_start:%Y%m%d}-{year_end:%Y%m%d}",
            )
        return _concat_frames(frames)

    def _fetch_pro_bar(
        self,
        symbols: list[str],
        start: date,
        end: date,
        *,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        if not symbols:
            return pd.DataFrame()
        frames = []
        year_ranges = self._split_by_year(start, end)
        total_steps = len(year_ranges) * len(symbols)
        current_step = 0
        for year_start, year_end in year_ranges:
            for symbol in symbols:
                self._check_cancel_requested(cancel_check)
                df = self._call_dataset_api(
                    "pro_bar",
                    ts_code=symbol,
                    start_date=_to_yyyymmdd(year_start),
                    end_date=_to_yyyymmdd(year_end),
                    asset="E",
                    freq="D",
                )
                if df is not None and not df.empty:
                    frames.append(df)
                current_step += 1
                self._emit_loop_progress(
                    progress_callback,
                    current_step,
                    total_steps,
                    f"{progress_label or 'pro_bar'} {symbol} {year_start:%Y%m%d}-{year_end:%Y%m%d}",
                )
        return _concat_frames(frames)

    def _fetch_minute_range(
        self,
        symbols: list[str],
        start: date,
        end: date,
        *,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        if not symbols:
            return pd.DataFrame()
        frames = []
        start_text = f"{_to_yyyymmdd(start)} 09:30:00"
        end_text = f"{_to_yyyymmdd(end)} 15:00:00"
        total_steps = len(symbols)
        for index, symbol in enumerate(symbols, start=1):
            self._check_cancel_requested(cancel_check)
            df = self._call_dataset_api(
                "stk_mins",
                ts_code=symbol,
                start_date=start_text,
                end_date=end_text,
            )
            if df is not None and not df.empty:
                frames.append(df)
            self._emit_loop_progress(
                progress_callback,
                index,
                total_steps,
                f"{progress_label or 'minute'} {symbol}",
            )
        return _concat_frames(frames)

    def _fetch_realtime_snapshot(
        self,
        api_name: str,
        *,
        progress_callback: Callable[[int, int, str], None] | None = None,
        cancel_check: Callable[[], bool] | None = None,
        progress_label: str | None = None,
    ) -> pd.DataFrame:
        self._check_cancel_requested(cancel_check)
        df = self._call_dataset_api(api_name)
        if df is None or df.empty:
            self._emit_loop_progress(progress_callback, 1, 1, progress_label or api_name)
            return pd.DataFrame()
        if "date" not in df.columns:
            df = df.copy()
            df["date"] = date.today().strftime("%Y%m%d")
        self._emit_loop_progress(progress_callback, 1, 1, progress_label or api_name)
        return df

    # ------------------------------------------------------------------
    # 内部缓存
    # ------------------------------------------------------------------

    def _load_index_codes(self) -> list[str]:
        if not (
            len(DEFAULT_TUSHARE_INDEX_DAILY_CODES) == 1
            and DEFAULT_TUSHARE_INDEX_DAILY_CODES[0].lower() == "all"
        ):
            return list(DEFAULT_TUSHARE_INDEX_DAILY_CODES)

        if "index_basic" not in self._frame_cache:
            self._frame_cache["index_basic"] = self._fetch_index_reference("index_basic")
        df = self._frame_cache["index_basic"]
        if df.empty or "ts_code" not in df.columns:
            return []
        if "market" in df.columns:
            df = df.loc[df["market"].astype(str).isin(TUSHARE_INDEX_DAILY_MARKETS)]
        codes = sorted(df["ts_code"].dropna().astype(str).unique().tolist())
        return codes

    def _load_member_parent_codes(self, loop_source: str) -> list[str]:
        if loop_source not in self._frame_cache:
            if loop_source == "ths_index":
                self._frame_cache[loop_source] = self._call_dataset_api("ths_index")
            elif loop_source == "dc_index":
                self._frame_cache[loop_source] = self._call_dataset_api("dc_index")
            else:
                raise DataError(
                    code="DATA_PROVIDER_UNKNOWN_LOOP_SOURCE",
                    message=f"未知 loop_source: {loop_source}",
                )
        df = self._frame_cache[loop_source]
        if df.empty:
            return []

        for col in ("ts_code", "code"):
            if col in df.columns:
                return sorted(df[col].dropna().astype(str).unique().tolist())
        return []

    # ------------------------------------------------------------------
    # 标准化 / 排序 / 过滤
    # ------------------------------------------------------------------

    def _normalize_dataset_frame(
        self,
        dataset: str,
        df: pd.DataFrame,
        *,
        start: date | None = None,
        end: date | None = None,
    ) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()

        canonical = self.resolve_dataset(dataset)
        spec = get_tushare_dataset_spec(canonical)
        result = df.copy()
        date_candidates = tuple(
            spec.get("date_field_priority")
            or ("trade_date", "cal_date", "ex_date", "pub_date", "pub_time", "ann_date", "start_date", "end_date", "month")
        )
        report_date_candidates = tuple(
            spec.get("report_date_field_priority")
            or ("end_date", "report_date", "trade_date", "date")
        )
        symbol_candidates = tuple(
            spec.get("symbol_field_priority")
            or ("ts_code", "con_code", "code")
        )

        rename_map = {
            "vol": "volume",
        }
        if "symbol" not in result.columns:
            for source in symbol_candidates:
                if source in result.columns:
                    rename_map[source] = "symbol"
                    break
        if spec.get("partition_by") == "date":
            for source in date_candidates:
                if source in result.columns and "date" not in result.columns:
                    rename_map[source] = "date"
                    break
        if spec.get("partition_by") == "report_date":
            for source in report_date_candidates:
                if source in result.columns and "report_date" not in result.columns:
                    rename_map[source] = "report_date"
                    break

        result = result.rename(columns=rename_map)
        if spec.get("partition_by") == "date":
            result = self._coalesce_date_column(
                result,
                target_column="date",
                candidate_columns=date_candidates,
            )
        elif spec.get("partition_by") == "report_date":
            result = self._coalesce_date_column(
                result,
                target_column="report_date",
                candidate_columns=report_date_candidates,
            )

        if start is not None and end is not None:
            if "date" in result.columns:
                result = self._filter_by_date_range(result, "date", start, end)
            elif "report_date" in result.columns:
                result = self._filter_by_date_range(result, "report_date", start, end)

        sort_cols = []
        if "date" in result.columns:
            sort_cols.append("date")
        elif "report_date" in result.columns:
            sort_cols.append("report_date")
        elif "ann_date" in result.columns:
            sort_cols.append("ann_date")

        if "symbol" in result.columns:
            sort_cols.append("symbol")

        if sort_cols:
            result = result.sort_values(sort_cols).reset_index(drop=True)
        return result

    @staticmethod
    def _filter_by_date_range(
        df: pd.DataFrame,
        column: str,
        start: date,
        end: date,
    ) -> pd.DataFrame:
        start_key = _to_yyyymmdd(start)
        end_key = _to_yyyymmdd(end)
        mask = [
            isinstance(key, str) and start_key <= key <= end_key
            for key in (_normalize_date_key(value) for value in df[column].tolist())
        ]
        return df.loc[mask].reset_index(drop=True)

    @staticmethod
    def _coalesce_date_column(
        df: pd.DataFrame,
        *,
        target_column: str,
        candidate_columns: tuple[str, ...],
    ) -> pd.DataFrame:
        source_columns: list[str] = []
        for column in (target_column, *candidate_columns):
            if column in df.columns and column not in source_columns:
                source_columns.append(column)
        if not source_columns:
            return df

        values: list[str | None] = []
        for row in df[source_columns].itertuples(index=False, name=None):
            normalized = None
            for value in row:
                key = _normalize_date_key(value)
                if key is not None:
                    normalized = key
                    break
            values.append(normalized)

        result = df.copy()
        result[target_column] = values
        return result

    # ------------------------------------------------------------------
    # API 调用与工具
    # ------------------------------------------------------------------

    def _check_market(self, market: str) -> None:
        if market not in self.supported_markets:
            raise DataError(
                code="DATA_PROVIDER_UNSUPPORTED_MARKET",
                message=f"不支持的市场: {market}",
                detail={"market": market, "supported": self.supported_markets},
            )

    @staticmethod
    def _check_cancel_requested(
        cancel_check: Callable[[], bool] | None,
    ) -> None:
        if cancel_check and cancel_check():
            raise DataError(
                code="DATA_TASK_CANCELLED",
                message="数据任务已取消",
            )

    @staticmethod
    def _should_log_progress(current: int, total: int) -> bool:
        if total <= 0:
            return True
        if total <= 10:
            return True
        step = max(1, total // 10)
        return current == 1 or current == total or current % step == 0

    def _emit_loop_progress(
        self,
        progress_callback: Callable[[int, int, str], None] | None,
        current: int,
        total: int,
        label: str,
    ) -> None:
        safe_total = max(total, 1)
        if progress_callback is not None:
            progress_callback(current, safe_total, label)
        if self._should_log_progress(current, safe_total):
            logger.info("%s: %d/%d", label, current, safe_total)

    def _resolve_quarter_fetch_api(self, api_name: str) -> str:
        if api_name not in _QUARTER_VIP_APIS:
            return api_name
        vip_api_name = f"{api_name}_vip"
        vip_access = self._describe_api_access(vip_api_name)
        if vip_access.get("allowed"):
            return vip_api_name
        return api_name

    def _rate_limit(self, api_name: str) -> None:
        access = self._assert_api_access(api_name)
        effective_rpm = int(access.get("effective_rpm", 0) or 0)
        interval = _RATE_LIMIT_SECONDS
        if effective_rpm > 0:
            interval = 60.0 / float(effective_rpm)
        elapsed = time.monotonic() - self._last_call_time
        if elapsed < interval:
            time.sleep(interval - elapsed)
        self._last_call_time = time.monotonic()

    def _call_with_retry(self, api_name: str, func: Any, **kwargs: Any) -> pd.DataFrame:
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            self._rate_limit(api_name)
            try:
                result = func(**kwargs)
                if result is None:
                    return pd.DataFrame()
                return result
            except Exception as exc:
                last_exc = exc
                wait = _BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    "Tushare API 调用失败 (attempt %d/%d): %s, 等待 %.1fs",
                    attempt + 1, _MAX_RETRIES, exc, wait,
                )
                time.sleep(wait)

        raise DataError(
            code="DATA_PROVIDER_FETCH_FAILED",
            message=f"Tushare API 调用失败，已重试 {_MAX_RETRIES} 次",
            detail={"last_error": str(last_exc)},
        )

    def _call_dataset_api(self, api_name: str, **kwargs: Any) -> pd.DataFrame:
        """统一调度到 pro_api 方法 / 顶层 tushare helper。"""
        self._assert_api_access(api_name)
        if api_name == "pro_bar":
            if not hasattr(self._ts, "pro_bar"):
                raise DataError(
                    code="DATA_PROVIDER_API_NOT_FOUND",
                    message="当前 tushare 版本不支持 pro_bar",
                )
            return self._call_with_retry(api_name, self._ts.pro_bar, **kwargs)

        func = getattr(self._api, api_name, None)
        if callable(func):
            return self._call_with_retry(api_name, func, **kwargs)

        top_level = getattr(self._ts, api_name, None)
        if callable(top_level):
            return self._call_with_retry(api_name, top_level, **kwargs)

        query = getattr(self._api, "query", None)
        if callable(query):
            def _query_call(**params: Any) -> pd.DataFrame:
                return query(api_name, **params)

            return self._call_with_retry(api_name, _query_call, **kwargs)

        raise DataError(
            code="DATA_PROVIDER_API_NOT_FOUND",
            message=f"Tushare API 不存在: {api_name}",
        )

    @staticmethod
    def _split_by_year(start: date, end: date) -> list[tuple[date, date]]:
        ranges: list[tuple[date, date]] = []
        current = start
        while current <= end:
            year_end = date(current.year, 12, 31)
            if year_end > end:
                year_end = end
            ranges.append((current, year_end))
            current = date(current.year + 1, 1, 1)
        return ranges

    @staticmethod
    def _resolve_symbol_range_windows(
        start: date,
        end: date,
        *,
        partition_values: list[str] | None = None,
    ) -> list[tuple[date, date]]:
        if not partition_values:
            return TushareProvider._split_by_year(start, end)

        buckets: dict[int, list[date]] = {}
        for value in partition_values:
            parsed = _parse_yyyymmdd(value)
            buckets.setdefault(parsed.year, []).append(parsed)

        ranges: list[tuple[date, date]] = []
        for year in sorted(buckets):
            dates = sorted(buckets[year])
            ranges.append((dates[0], dates[-1]))
        return ranges

    @staticmethod
    def _split_by_quarter(start: date, end: date) -> list[tuple[date, date]]:
        ranges: list[tuple[date, date]] = []
        current = start
        while current <= end:
            q_month = ((current.month - 1) // 3 + 1) * 3
            if q_month == 12:
                q_end = date(current.year, 12, 31)
            else:
                q_end = date(current.year, q_month + 1, 1) - timedelta(days=1)
            if q_end > end:
                q_end = end
            ranges.append((current, q_end))
            current = q_end + timedelta(days=1)
        return ranges


__all__ = [
    "TushareProvider",
    "TUSHARE_DATASET_REGISTRY",
    "DEFAULT_TUSHARE_PRIORITY_DATASETS",
    "get_default_tushare_datasets",
]
