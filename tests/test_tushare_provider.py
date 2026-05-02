"""Tushare Provider 运行时行为测试。"""
from __future__ import annotations

import builtins
import sys
from datetime import date

import pandas as pd
import pytest

import vortex.data.provider.tushare as tushare_provider
from vortex.data.provider.tushare_registry import (
    filter_tushare_datasets_by_update_frequency,
    get_default_tushare_datasets,
    get_tushare_dataset_access_rule,
    get_tushare_dataset_api_doc_url,
    get_tushare_dataset_spec,
    get_tushare_dataset_update_frequency,
    normalize_tushare_update_frequencies,
)
from vortex.shared.errors import DataError


class TestTushareImportError:
    def test_try_import_tushare_reports_current_interpreter(self, monkeypatch):
        real_import = builtins.__import__

        def _fake_import(name, *args, **kwargs):
            if name == "tushare":
                raise ImportError("No module named 'tushare'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", _fake_import)

        with pytest.raises(DataError) as exc_info:
            tushare_provider._try_import_tushare()

        err = exc_info.value
        assert err.code == "DATA_PROVIDER_IMPORT_FAILED"
        assert sys.executable in err.message
        assert "-m pip install tushare" in err.message
        assert err.detail["python"] == sys.executable


class TestTushareRateLimit:
    def test_rate_limit_is_global_across_apis(self, monkeypatch):
        provider = object.__new__(tushare_provider.TushareProvider)
        provider._last_call_time = 0.0

        current = {"value": 0.0}
        sleep_calls: list[float] = []

        def _fake_monotonic():
            return current["value"]

        def _fake_sleep(seconds: float):
            sleep_calls.append(seconds)
            current["value"] += seconds

        monkeypatch.setattr(
            provider,
            "_assert_api_access",
            lambda _api_name: {"effective_rpm": 400},
        )
        monkeypatch.setattr(tushare_provider.time, "monotonic", _fake_monotonic)
        monkeypatch.setattr(tushare_provider.time, "sleep", _fake_sleep)

        provider._rate_limit("daily")
        current["value"] = 0.05
        provider._rate_limit("income")

        assert sleep_calls
        assert sleep_calls[-1] > 0


class TestTushareTradeDayResume:
    def test_fetch_dataset_passes_missing_trading_days_to_bars(self, monkeypatch):
        provider = object.__new__(tushare_provider.TushareProvider)
        requested_days: list[str] = []

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "fetch_calendar",
            lambda *_args, **_kwargs: pytest.fail("bars 缺口续跑不应重新拉完整交易日历"),
        )
        monkeypatch.setattr(
            provider,
            "_fetch_trade_day_all",
            lambda _api_name, trading_days, **_kwargs: (
                requested_days.extend(day.strftime("%Y%m%d") for day in trading_days)
                or pd.DataFrame(
                    {
                        "symbol": ["000001.SZ"] * len(trading_days),
                        "date": [day.strftime("%Y%m%d") for day in trading_days],
                        "open": [10.0] * len(trading_days),
                        "high": [10.5] * len(trading_days),
                        "low": [9.5] * len(trading_days),
                        "close": [10.2] * len(trading_days),
                        "volume": [1000] * len(trading_days),
                        "amount": [10000.0] * len(trading_days),
                    }
                )
            ),
        )
        monkeypatch.setattr(
            provider,
            "_normalize_dataset_frame",
            lambda _dataset, df, **_kwargs: df,
        )

        result = provider.fetch_dataset(
            "bars",
            "cn_stock",
            date(2026, 4, 1),
            date(2026, 4, 8),
            symbols=["000001.SZ"],
            trading_days=[date(2026, 4, 2), date(2026, 4, 8)],
        )

        assert requested_days == ["20260402", "20260408"]
        assert result["date"].tolist() == ["20260402", "20260408"]

    def test_normalize_update_frequencies_accepts_intraday_alias(self):
        assert normalize_tushare_update_frequencies(["hourly"]) == ["intraday"]

    def test_get_dataset_update_frequency_returns_registry_value(self):
        assert get_tushare_dataset_update_frequency("index_weight") == "weekly"
        assert get_tushare_dataset_update_frequency("bars") == "daily"

    def test_filter_default_datasets_by_update_frequency_preserves_order(self):
        daily_datasets = get_default_tushare_datasets(
            points=5000,
            permission_keys=set(),
            update_frequencies=["daily"],
        )

        assert "bars" in daily_datasets
        assert "weekly" not in daily_datasets
        assert "fundamental" not in daily_datasets
        assert daily_datasets == filter_tushare_datasets_by_update_frequency(
            get_default_tushare_datasets(points=5000, permission_keys=set()),
            ["daily"],
        )


class TestTushareMinuteFetch:
    def test_fetch_minute_range_splits_by_freq_safe_windows(self, monkeypatch):
        provider = object.__new__(tushare_provider.TushareProvider)
        calls: list[dict[str, object]] = []

        monkeypatch.setattr(provider, "_check_cancel_requested", lambda _cancel_check: None)
        monkeypatch.setattr(
            provider,
            "_emit_loop_progress",
            lambda *_args, **_kwargs: None,
        )

        def _fake_call(api_name: str, **kwargs):
            calls.append({"api": api_name, **kwargs})
            return pd.DataFrame(
                {
                    "ts_code": [kwargs["ts_code"]],
                    "trade_time": [str(kwargs["start_date"])],
                    "open": [1.0],
                    "close": [1.0],
                    "high": [1.0],
                    "low": [1.0],
                    "vol": [100.0],
                    "amount": [1000.0],
                }
            )

        monkeypatch.setattr(provider, "_call_dataset_api", _fake_call)

        result = provider._fetch_minute_range(
            ["000001.SZ"],
            date(2026, 1, 1),
            date(2026, 2, 5),
            freq="1min",
        )

        assert len(calls) == 2
        assert calls[0]["api"] == "stk_mins"
        assert calls[0]["freq"] == "1min"
        assert calls[0]["start_date"] == "20260101 09:30:00"
        assert calls[0]["end_date"] == "20260130 15:00:00"
        assert calls[1]["start_date"] == "20260131 09:30:00"
        assert calls[1]["end_date"] == "20260205 15:00:00"
        assert result["freq"].tolist() == ["1min", "1min"]

    def test_normalize_stk_mins_derives_date_and_minute(self):
        provider = object.__new__(tushare_provider.TushareProvider)

        result = provider._normalize_dataset_frame(
            "stk_mins",
            pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_time": ["2026-01-05 09:31:00"],
                    "open": [10.0],
                    "close": [10.1],
                    "high": [10.2],
                    "low": [9.9],
                    "vol": [1000.0],
                    "amount": [10000.0],
                    "freq": ["1min"],
                }
            ),
            start=date(2026, 1, 5),
            end=date(2026, 1, 5),
        )

        assert result["symbol"].tolist() == ["000001.SZ"]
        assert result["date"].tolist() == ["20260105"]
        assert result["minute"].tolist() == ["09:31:00"]
        assert result["volume"].tolist() == [1000.0]
        assert result["freq"].tolist() == ["1min"]

    def test_trade_day_all_fallback_uses_current_market(self, monkeypatch):
        provider = object.__new__(tushare_provider.TushareProvider)
        captured: dict[str, object] = {}

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "fetch_calendar",
            lambda market, start, end: (
                captured.update(
                    {
                        "market": market,
                        "start": start,
                        "end": end,
                    }
                )
                or [date(2026, 4, 1)]
            ),
        )
        monkeypatch.setattr(
            provider,
            "_fetch_trade_day_all",
            lambda _api_name, trading_days, **_kwargs: pd.DataFrame(
                {
                    "symbol": ["000001.SZ"] * len(trading_days),
                    "date": [day.strftime("%Y%m%d") for day in trading_days],
                    "pe": [20.0] * len(trading_days),
                    "pb": [8.0] * len(trading_days),
                }
            ),
        )
        monkeypatch.setattr(
            provider,
            "_normalize_dataset_frame",
            lambda _dataset, df, **_kwargs: df,
        )

        result = provider.fetch_dataset(
            "valuation",
            "hk",
            date(2026, 4, 1),
            date(2026, 4, 8),
        )

        assert captured == {
            "market": "hk",
            "start": date(2026, 4, 1),
            "end": date(2026, 4, 8),
        }
        assert result["date"].tolist() == ["20260401"]

    def test_fetch_dataset_passes_partition_values_to_fundamental(self, monkeypatch):
        provider = object.__new__(tushare_provider.TushareProvider)
        captured: dict[str, object] = {}

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "fetch_fundamental",
            lambda market, symbols, fields, start, end, **kwargs: (
                captured.update(
                    {
                        "market": market,
                        "symbols": list(symbols),
                        "fields": list(fields),
                        "start": start,
                        "end": end,
                        "partition_values": list(kwargs.get("partition_values") or []),
                    }
                )
                or pd.DataFrame(
                    {
                        "symbol": ["000001.SZ"],
                        "ann_date": ["20260430"],
                        "report_date": ["20260331"],
                        "revenue": [100.0],
                        "net_profit": [50.0],
                        "total_assets": [1000.0],
                    }
                )
            ),
        )

        result = provider.fetch_dataset(
            "fundamental",
            "cn_stock",
            date(2026, 1, 1),
            date(2026, 6, 30),
            symbols=["000001.SZ"],
            partition_values=["20260331", "20260630"],
        )

        assert captured == {
            "market": "cn_stock",
            "symbols": ["000001.SZ"],
            "fields": ["revenue", "net_profit", "total_assets"],
            "start": date(2026, 1, 1),
            "end": date(2026, 6, 30),
            "partition_values": ["20260331", "20260630"],
        }
        assert result["report_date"].tolist() == ["20260331"]

    def test_fetch_dataset_passes_partition_values_to_symbol_range(self, monkeypatch):
        provider = object.__new__(tushare_provider.TushareProvider)
        captured: dict[str, object] = {}

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "_fetch_symbol_range",
            lambda api_name, symbols, start, end, **kwargs: (
                captured.update(
                    {
                        "api_name": api_name,
                        "symbols": list(symbols),
                        "start": start,
                        "end": end,
                        "partition_values": list(kwargs.get("partition_values") or []),
                    }
                )
                or pd.DataFrame(
                    {
                        "symbol": ["000001.SZ"],
                        "date": ["20260409"],
                        "adj_factor": [1.0],
                    }
                )
            ),
        )
        monkeypatch.setattr(
            provider,
            "_normalize_dataset_frame",
            lambda _dataset, df, **_kwargs: df,
        )

        result = provider.fetch_dataset(
            "adj_factor",
            "cn_stock",
            date(2005, 1, 1),
            date(2026, 4, 9),
            symbols=["000001.SZ"],
            partition_values=["20050104", "20051230", "20260409"],
        )

        assert captured == {
            "api_name": "adj_factor",
            "symbols": ["000001.SZ"],
            "start": date(2005, 1, 1),
            "end": date(2026, 4, 9),
            "partition_values": ["20050104", "20051230", "20260409"],
        }
        assert result["date"].tolist() == ["20260409"]

    def test_fetch_symbol_range_uses_partition_values_to_shrink_year_windows(self, monkeypatch):
        provider = object.__new__(tushare_provider.TushareProvider)
        requested_ranges: list[tuple[str, str]] = []

        monkeypatch.setattr(provider, "_check_cancel_requested", lambda _cancel_check: None)
        monkeypatch.setattr(
            provider,
            "_call_dataset_api",
            lambda _api_name, **kwargs: (
                requested_ranges.append((kwargs["start_date"], kwargs["end_date"]))
                or pd.DataFrame(
                    {
                        "ts_code": [kwargs["ts_code"]],
                        "trade_date": [kwargs["end_date"]],
                        "adj_factor": [1.0],
                    }
                )
            ),
        )

        provider._fetch_symbol_range(
            "adj_factor",
            ["000001.SZ"],
            date(2005, 1, 1),
            date(2026, 4, 9),
            partition_values=["20050104", "20051230", "20260409"],
            progress_label="adj_factor",
        )

        assert requested_ranges == [
            ("20050104", "20051230"),
            ("20260409", "20260409"),
        ]

    def test_fetch_dataset_prefers_date_batch_for_adj_factor(self, monkeypatch):
        provider = object.__new__(tushare_provider.TushareProvider)
        calls: list[tuple[str, object]] = []

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "_fetch_trade_day_all",
            lambda api_name, trading_days, **_kwargs: (
                calls.append((api_name, [day.strftime("%Y%m%d") for day in trading_days]))
                or pd.DataFrame(
                    {
                        "symbol": ["000001.SZ"],
                        "date": ["20260409"],
                        "adj_factor": [1.0],
                    }
                )
            ),
        )
        monkeypatch.setattr(
            provider,
            "_fetch_symbol_range",
            lambda *_args, **_kwargs: pytest.fail("adj_factor 应优先走日期整批抓"),
        )
        monkeypatch.setattr(
            provider,
            "_normalize_dataset_frame",
            lambda _dataset, df, **_kwargs: df,
        )

        result = provider.fetch_dataset(
            "adj_factor",
            "cn_stock",
            date(2005, 1, 1),
            date(2026, 4, 9),
            symbols=["000001.SZ", "600000.SH"],
            trading_days=[date(2026, 4, 9)],
            partition_values=["20260409"],
        )

        assert calls == [("adj_factor", ["20260409"])]
        assert result["date"].tolist() == ["20260409"]

    def test_fetch_dataset_prefers_date_batch_for_weekly_when_row_limit_allows(self, monkeypatch):
        provider = object.__new__(tushare_provider.TushareProvider)
        calls: list[tuple[str, object]] = []

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "_fetch_trade_day_all",
            lambda api_name, trading_days, **_kwargs: (
                calls.append((api_name, [day.strftime("%Y%m%d") for day in trading_days]))
                or pd.DataFrame(
                    {
                        "symbol": ["000001.SZ"],
                        "date": ["20260411"],
                        "close": [10.0],
                    }
                )
            ),
        )
        monkeypatch.setattr(
            provider,
            "_fetch_symbol_range",
            lambda *_args, **_kwargs: pytest.fail("weekly 在行数上限允许时应优先走日期整批抓"),
        )
        monkeypatch.setattr(
            provider,
            "_normalize_dataset_frame",
            lambda _dataset, df, **_kwargs: df,
        )

        result = provider.fetch_dataset(
            "weekly",
            "cn_stock",
            date(2026, 4, 1),
            date(2026, 4, 11),
            symbols=["000001.SZ", "600000.SH", "300750.SZ"],
            trading_days=[date(2026, 4, 11)],
            partition_values=["20260411"],
        )

        assert calls == [("weekly", ["20260411"])]
        assert result["date"].tolist() == ["20260411"]

    def test_fetch_dataset_falls_back_to_symbol_range_when_date_batch_row_limit_too_small(self, monkeypatch):
        provider = object.__new__(tushare_provider.TushareProvider)
        calls: list[tuple[str, list[str]]] = []

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "_fetch_trade_day_all",
            lambda *_args, **_kwargs: pytest.fail("monthly 当前不应在超出单次上限时走日期整批抓"),
        )
        monkeypatch.setattr(
            provider,
            "_fetch_symbol_range",
            lambda api_name, symbols, _start, _end, **_kwargs: (
                calls.append((api_name, list(symbols)))
                or pd.DataFrame(
                    {
                        "symbol": [symbols[0]],
                        "date": ["20260430"],
                        "close": [10.0],
                    }
                )
            ),
        )
        monkeypatch.setattr(
            provider,
            "_normalize_dataset_frame",
            lambda _dataset, df, **_kwargs: df,
        )

        symbols = [f"{idx:06d}.SZ" for idx in range(4501)]
        result = provider.fetch_dataset(
            "monthly",
            "cn_stock",
            date(2026, 4, 1),
            date(2026, 4, 30),
            symbols=symbols,
            trading_days=[date(2026, 4, 30)],
            partition_values=["20260430"],
        )

        assert calls == [("monthly", symbols)]
        assert result["date"].tolist() == ["20260430"]


class TestTushareQuarterVipFetch:
    def _build_provider(self, *, points: int) -> tushare_provider.TushareProvider:
        provider = object.__new__(tushare_provider.TushareProvider)
        provider._account_points = points
        provider._extra_permissions = set()
        provider._account_rpm = 500 if points >= 5000 else 200
        provider._global_effective_rpm = 400 if points >= 5000 else 160
        return provider

    def test_quarter_statement_uses_vip_when_points_allow(self, monkeypatch):
        provider = self._build_provider(points=5000)
        calls: list[tuple[str, dict[str, object]]] = []

        monkeypatch.setattr(provider, "_check_cancel_requested", lambda _cancel_check: None)
        monkeypatch.setattr(
            provider,
            "_call_dataset_api",
            lambda api_name, **kwargs: (
                calls.append((api_name, kwargs))
                or pd.DataFrame(
                    {
                        "ts_code": ["000001.SZ"],
                        "ann_date": ["20240430"],
                        "end_date": [kwargs["period"]],
                        "revenue": [100.0],
                    }
                )
            ),
        )

        progress: list[tuple[int, int, str]] = []
        df = provider._fetch_quarter_statement_range(
            "income",
            ["000001.SZ", "600000.SH"],
            date(2024, 1, 1),
            date(2024, 6, 30),
            fields="ts_code,ann_date,end_date,revenue",
            progress_callback=lambda current, total, label: progress.append((current, total, label)),
            progress_label="fundamental",
        )

        assert len(df) == 2
        assert [name for name, _kwargs in calls] == ["income_vip", "income_vip"]
        assert calls[0][1]["period"] == "20240331"
        assert calls[1][1]["period"] == "20240630"
        assert calls[0][1]["fields"] == "ts_code,ann_date,end_date,revenue"
        assert progress[-1][1] == 2
        assert "via=income_vip" in progress[-1][2]

    def test_quarter_statement_falls_back_to_symbol_fetch_when_points_insufficient(self, monkeypatch):
        provider = self._build_provider(points=2000)
        calls: list[tuple[str, dict[str, object]]] = []

        monkeypatch.setattr(provider, "_check_cancel_requested", lambda _cancel_check: None)
        monkeypatch.setattr(
            provider,
            "_call_dataset_api",
            lambda api_name, **kwargs: (
                calls.append((api_name, kwargs))
                or pd.DataFrame(
                    {
                        "ts_code": [kwargs["ts_code"]],
                        "ann_date": ["20240430"],
                        "end_date": ["20240331"],
                    }
                )
            ),
        )

        provider._fetch_quarter_statement_range(
            "cashflow",
            ["000001.SZ", "600000.SH"],
            date(2024, 1, 1),
            date(2024, 3, 31),
            progress_label="cashflow",
        )

        assert [name for name, _kwargs in calls] == ["cashflow", "cashflow"]
        assert calls[0][1]["ts_code"] == "000001.SZ"
        assert calls[1][1]["ts_code"] == "600000.SH"
        assert calls[0][1]["start_date"] == "20240101"
        assert calls[0][1]["end_date"] == "20240331"


class TestTushareApiAccessRules:
    def _build_provider(self, *, points: int) -> tushare_provider.TushareProvider:
        provider = object.__new__(tushare_provider.TushareProvider)
        provider._account_points = points
        provider._extra_permissions = set()
        provider._account_rpm = 500 if points >= 5000 else 200
        provider._global_effective_rpm = 400 if points >= 5000 else 160
        return provider

    @pytest.mark.parametrize("api_name", ["limit_list_d", "sw_daily"])
    def test_2000_point_apis_with_documented_200rpm_cap_use_explicit_limit(
        self,
        api_name: str,
    ):
        provider = self._build_provider(points=5000)

        access = provider._describe_api_access(api_name)

        assert access["allowed"] is True
        assert access["max_rpm"] == 200
        assert access["effective_rpm"] == 160

    def test_other_2000_point_api_still_uses_account_tier_rpm(self):
        provider = self._build_provider(points=5000)

        access = provider._describe_api_access("daily_basic")

        assert access["allowed"] is True
        assert access["max_rpm"] == 500
        assert access["effective_rpm"] == 400

    @pytest.mark.parametrize("dataset", ["stk_limit", "suspend_d"])
    def test_tradability_datasets_are_registered_as_daily_trade_day_all(self, dataset: str):
        spec = get_tushare_dataset_spec(dataset)

        assert spec["fetch_mode"] == "trade_day_all"
        assert spec["partition_by"] == "date"
        assert get_tushare_dataset_update_frequency(dataset) == "daily"
        assert get_tushare_dataset_access_rule(dataset)["min_points"] == 2000
        assert get_tushare_dataset_api_doc_url(dataset)

    @pytest.mark.parametrize(
        ("dataset", "expected_row_limit"),
        [("stk_auction_o", 10000), ("stk_auction_c", 10000), ("stk_auction", 8000)],
    )
    def test_auction_datasets_are_registered_as_permission_gated_symbol_range(
        self,
        dataset: str,
        expected_row_limit: int,
    ):
        spec = get_tushare_dataset_spec(dataset)

        assert spec["fetch_mode"] == "symbol_range"
        assert spec["partition_by"] == "date"
        assert spec["date_batch_supported"] is True
        assert spec["date_batch_row_limit"] == expected_row_limit
        assert get_tushare_dataset_update_frequency(dataset) == "daily"
        access = get_tushare_dataset_access_rule(dataset)
        assert access["access"] == "permission"
        assert access["permission_key"] == "stock_minutes"
        assert get_tushare_dataset_api_doc_url(dataset)

    def test_stk_mins_is_registered_as_chunked_intraday_dataset(self):
        spec = get_tushare_dataset_spec("stk_mins")

        assert spec["fetch_mode"] == "minute_range"
        assert spec["partition_by"] == "date"
        assert spec["freq"] == "1min"
        assert spec["single_request_row_limit"] == 8000
        assert get_tushare_dataset_update_frequency("stk_mins") == "intraday"
        access = get_tushare_dataset_access_rule("stk_mins")
        assert access["access"] == "permission"
        assert access["permission_key"] == "stock_minutes"
        assert get_tushare_dataset_api_doc_url("stk_mins").endswith("doc_id=370")

    def test_stk_nineturn_is_registered_as_daily_technical_dataset(self):
        spec = get_tushare_dataset_spec("stk_nineturn")

        assert spec["fetch_mode"] == "symbol_range"
        assert spec["partition_by"] == "date"
        assert spec["date_batch_supported"] is True
        assert spec["date_batch_row_limit"] == 10000
        assert spec["date_batch_params"] == {"freq": "daily"}
        assert spec["symbol_range_params"] == {"freq": "daily"}
        assert get_tushare_dataset_update_frequency("stk_nineturn") == "daily"
        access = get_tushare_dataset_access_rule("stk_nineturn")
        assert access["access"] == "points"
        assert access["min_points"] == 6000
        assert get_tushare_dataset_api_doc_url("stk_nineturn").endswith("doc_id=364")

    @pytest.mark.parametrize("dataset", ["cyq_perf", "cyq_chips"])
    def test_cyq_datasets_are_registered_as_daily_symbol_range(self, dataset: str):
        spec = get_tushare_dataset_spec(dataset)

        assert spec["fetch_mode"] == "symbol_range"
        assert spec["partition_by"] == "date"
        assert get_tushare_dataset_update_frequency(dataset) == "daily"
        access = get_tushare_dataset_access_rule(dataset)
        assert access["access"] == "points"
        assert access["min_points"] == 5000
        assert get_tushare_dataset_api_doc_url(dataset)

    def test_quarter_statement_uses_partition_values_as_exact_quarter_gaps(self, monkeypatch):
        provider = self._build_provider(points=2000)
        calls: list[tuple[str, dict[str, object]]] = []

        monkeypatch.setattr(provider, "_check_cancel_requested", lambda _cancel_check: None)
        monkeypatch.setattr(
            provider,
            "_call_dataset_api",
            lambda api_name, **kwargs: (
                calls.append((api_name, kwargs))
                or pd.DataFrame(
                    {
                        "ts_code": [kwargs["ts_code"]],
                        "ann_date": ["20240430"],
                        "end_date": [kwargs["end_date"]],
                    }
                )
            ),
        )

        provider._fetch_quarter_statement_range(
            "cashflow",
            ["000001.SZ"],
            date(2024, 1, 1),
            date(2024, 12, 31),
            partition_values=["20240630", "20241231"],
            progress_label="cashflow",
        )

        assert [name for name, _kwargs in calls] == ["cashflow", "cashflow"]
        assert calls[0][1]["start_date"] == "20240401"
        assert calls[0][1]["end_date"] == "20240630"
        assert calls[1][1]["start_date"] == "20241001"
        assert calls[1][1]["end_date"] == "20241231"

    def test_fetch_fundamental_filters_vip_result_back_to_requested_symbols(self, monkeypatch):
        provider = self._build_provider(points=5000)

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "_fetch_quarter_statement_range",
            lambda *args, **kwargs: pd.DataFrame(
                {
                    "ts_code": ["000001.SZ", "600000.SH"],
                    "ann_date": ["20240430", "20240430"],
                    "end_date": ["20240331", "20240331"],
                    "revenue": [10.0, 20.0],
                }
            ),
        )

        result = provider.fetch_fundamental(
            "cn_stock",
            ["000001.SZ"],
            ["revenue"],
            date(2024, 1, 1),
            date(2024, 3, 31),
        )

        assert result["symbol"].tolist() == ["000001.SZ"]
        assert result["report_date"].tolist() == ["20240331"]

    @pytest.mark.parametrize(
        ("api_name", "expected_vip"),
        [("forecast", "forecast_vip"), ("express", "express_vip")],
    )
    def test_quarter_auxiliary_tables_use_vip_when_points_allow(
        self,
        monkeypatch,
        api_name: str,
        expected_vip: str,
    ):
        provider = self._build_provider(points=5000)
        calls: list[tuple[str, dict[str, object]]] = []

        monkeypatch.setattr(provider, "_check_cancel_requested", lambda _cancel_check: None)
        monkeypatch.setattr(
            provider,
            "_call_dataset_api",
            lambda called_api_name, **kwargs: (
                calls.append((called_api_name, kwargs))
                or pd.DataFrame(
                    {
                        "ts_code": ["000001.SZ"],
                        "ann_date": ["20240430"],
                        "end_date": [kwargs["period"]],
                    }
                )
            ),
        )

        provider._fetch_quarter_statement_range(
            api_name,
            ["000001.SZ"],
            date(2024, 1, 1),
            date(2024, 6, 30),
            progress_label=api_name,
        )

        assert [name for name, _kwargs in calls] == [expected_vip, expected_vip]
        assert calls[0][1]["period"] == "20240331"
        assert calls[1][1]["period"] == "20240630"


class TestTushareDateNormalization:
    def _build_provider(self) -> tushare_provider.TushareProvider:
        return object.__new__(tushare_provider.TushareProvider)

    def test_filter_by_date_range_ignores_nan_without_type_error(self):
        df = pd.DataFrame(
            {
                "date": ["20260102", float("nan"), None, "20251231", "20260401"],
                "value": [1, 2, 3, 4, 5],
            }
        )

        result = tushare_provider.TushareProvider._filter_by_date_range(
            df,
            "date",
            date(2026, 1, 1),
            date(2026, 3, 31),
        )

        assert result["date"].tolist() == ["20260102"]
        assert result["value"].tolist() == [1]

    def test_normalize_events_uses_row_level_date_fallback(self):
        provider = self._build_provider()
        raw = pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "000002.SZ", "000003.SZ"],
                "ex_date": [float("nan"), "20260203", None],
                "record_date": [None, None, None],
                "pay_date": [None, None, None],
                "ann_date": ["20260115", "20260120", "20251231"],
                "end_date": ["20251231", "20251231", "20251231"],
                "cash_div": [0.5, 1.0, 0.2],
            }
        )

        result = provider._normalize_dataset_frame(
            "events",
            raw,
            start=date(2026, 1, 1),
            end=date(2026, 2, 28),
        )

        assert result["symbol"].tolist() == ["000001.SZ", "000002.SZ"]
        assert result["date"].tolist() == ["20260115", "20260203"]

    def test_fetch_events_handles_missing_ex_date_with_fallback(self, monkeypatch):
        provider = self._build_provider()

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(provider, "_check_cancel_requested", lambda _cancel_check: None)
        monkeypatch.setattr(
            provider,
            "_call_dataset_api",
            lambda _api_name, **kwargs: pd.DataFrame(
                {
                    "ts_code": [kwargs["ts_code"]],
                    "ex_date": [float("nan")],
                    "record_date": [None],
                    "pay_date": [None],
                    "ann_date": ["20260115"],
                    "end_date": ["20251231"],
                    "cash_div": [0.5],
                }
            ),
        )

        result = provider.fetch_events(
            "cn_stock",
            ["000001.SZ"],
            date(2026, 1, 1),
            date(2026, 1, 31),
        )

        assert result["symbol"].tolist() == ["000001.SZ"]
        assert result["date"].tolist() == ["20260115"]


class TestTushareDatasetContracts:
    def _build_provider(self) -> tushare_provider.TushareProvider:
        return object.__new__(tushare_provider.TushareProvider)

    def test_fetch_index_daily_uses_index_loop_range_contract(self, monkeypatch):
        provider = self._build_provider()
        captured: dict[str, object] = {}

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "_fetch_index_loop_range",
            lambda api_name, start, end, param_name, **_kwargs: (
                captured.update(
                    {
                        "api_name": api_name,
                        "start": start,
                        "end": end,
                        "param_name": param_name,
                    }
                )
                or pd.DataFrame({"ts_code": ["000001.SH"], "trade_date": ["20260401"]})
            ),
        )
        monkeypatch.setattr(
            provider,
            "_normalize_dataset_frame",
            lambda _dataset, raw, **_kwargs: raw,
        )

        result = provider.fetch_dataset(
            "index_daily",
            "cn_stock",
            date(2026, 4, 1),
            date(2026, 4, 30),
        )

        assert captured == {
            "api_name": "index_daily",
            "start": date(2026, 4, 1),
            "end": date(2026, 4, 30),
            "param_name": "ts_code",
        }
        assert result["ts_code"].tolist() == ["000001.SH"]

    def test_fetch_index_daily_forwards_partition_values_to_index_loop_range(self, monkeypatch):
        provider = self._build_provider()
        captured: dict[str, object] = {}

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "_fetch_index_loop_range",
            lambda api_name, start, end, param_name, **kwargs: (
                captured.update(
                    {
                        "api_name": api_name,
                        "start": start,
                        "end": end,
                        "param_name": param_name,
                        "partition_values": list(kwargs.get("partition_values") or []),
                    }
                )
                or pd.DataFrame({"ts_code": ["000001.SH"], "trade_date": ["20260410"]})
            ),
        )
        monkeypatch.setattr(
            provider,
            "_normalize_dataset_frame",
            lambda _dataset, raw, **_kwargs: raw,
        )

        provider.fetch_dataset(
            "index_daily",
            "cn_stock",
            date(2026, 4, 1),
            date(2026, 4, 30),
            partition_values=["20260410"],
        )

        assert captured == {
            "api_name": "index_daily",
            "start": date(2026, 4, 1),
            "end": date(2026, 4, 30),
            "param_name": "ts_code",
            "partition_values": ["20260410"],
        }

    def test_fetch_cyq_perf_uses_symbol_range_contract(self, monkeypatch):
        provider = self._build_provider()
        captured: dict[str, object] = {}

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "_fetch_symbol_range",
            lambda api_name, symbols, start, end, **kwargs: (
                captured.update(
                    {
                        "api_name": api_name,
                        "symbols": list(symbols),
                        "start": start,
                        "end": end,
                        "partition_values": list(kwargs.get("partition_values") or []),
                    }
                )
                or pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260410"], "winner_rate": [52.3]})
            ),
        )
        monkeypatch.setattr(provider, "_normalize_dataset_frame", lambda _dataset, raw, **_kwargs: raw)

        result = provider.fetch_dataset(
            "cyq_perf",
            "cn_stock",
            date(2026, 4, 1),
            date(2026, 4, 30),
            symbols=["000001.SZ"],
            partition_values=["20260410"],
        )

        assert captured == {
            "api_name": "cyq_perf",
            "symbols": ["000001.SZ"],
            "start": date(2026, 4, 1),
            "end": date(2026, 4, 30),
            "partition_values": ["20260410"],
        }
        assert result["ts_code"].tolist() == ["000001.SZ"]

    def test_fetch_stk_auction_o_prefers_date_batch_contract(self, monkeypatch):
        provider = self._build_provider()
        captured: dict[str, object] = {}

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "_fetch_trade_day_all",
            lambda api_name, trading_days, **_kwargs: (
                captured.update(
                    {
                        "api_name": api_name,
                        "trading_days": [day.strftime("%Y%m%d") for day in trading_days],
                    }
                )
                or pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260410"], "vol": [45400.0]})
            ),
        )
        monkeypatch.setattr(
            provider,
            "_fetch_symbol_range",
            lambda *_args, **_kwargs: pytest.fail("单日竞价数据应优先走日期整批抓取"),
        )
        monkeypatch.setattr(provider, "_normalize_dataset_frame", lambda _dataset, raw, **_kwargs: raw)

        result = provider.fetch_dataset(
            "stk_auction_o",
            "cn_stock",
            date(2026, 4, 10),
            date(2026, 4, 10),
            symbols=["000001.SZ"],
            trading_days=[date(2026, 4, 10)],
            partition_values=["20260410"],
        )

        assert captured == {
            "api_name": "stk_auction_o",
            "trading_days": ["20260410"],
        }
        assert result["ts_code"].tolist() == ["000001.SZ"]


    def test_fetch_stk_auction_prefers_date_batch_without_symbols(self, monkeypatch):
        provider = self._build_provider()
        captured: dict[str, object] = {}

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "_fetch_trade_day_all",
            lambda api_name, trading_days, **_kwargs: (
                captured.update(
                    {
                        "api_name": api_name,
                        "trading_days": [day.strftime("%Y%m%d") for day in trading_days],
                    }
                )
                or pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260410"], "price": [10.0], "vol": [1000]})
            ),
        )
        monkeypatch.setattr(
            provider,
            "_fetch_symbol_range",
            lambda *_args, **_kwargs: pytest.fail("未提供 symbols 时应直接走日期整批抓取"),
        )
        monkeypatch.setattr(provider, "_normalize_dataset_frame", lambda _dataset, raw, **_kwargs: raw)

        result = provider.fetch_dataset(
            "stk_auction",
            "cn_stock",
            date(2026, 4, 10),
            date(2026, 4, 10),
            trading_days=[date(2026, 4, 10)],
            partition_values=["20260410"],
        )

        assert captured == {
            "api_name": "stk_auction",
            "trading_days": ["20260410"],
        }
        assert result["ts_code"].tolist() == ["000001.SZ"]

    def test_fetch_stk_auction_falls_back_to_symbol_range_for_small_symbol_set(self, monkeypatch):
        provider = self._build_provider()
        captured: dict[str, object] = {}

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "_fetch_trade_day_all",
            lambda *_args, **_kwargs: pytest.fail("少量股票跨多日抓取应优先走按股票区间抓取"),
        )
        monkeypatch.setattr(
            provider,
            "_fetch_symbol_range",
            lambda api_name, symbols, start, end, **kwargs: (
                captured.update(
                    {
                        "api_name": api_name,
                        "symbols": list(symbols),
                        "start": start,
                        "end": end,
                        "partition_values": list(kwargs.get("partition_values") or []),
                    }
                )
                or pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20260410"], "price": [10.0], "vol": [1000]})
            ),
        )
        monkeypatch.setattr(provider, "_normalize_dataset_frame", lambda _dataset, raw, **_kwargs: raw)

        result = provider.fetch_dataset(
            "stk_auction",
            "cn_stock",
            date(2026, 4, 10),
            date(2026, 4, 11),
            symbols=["000001.SZ"],
            trading_days=[date(2026, 4, 10), date(2026, 4, 11)],
            partition_values=["20260410", "20260411"],
        )

        assert captured == {
            "api_name": "stk_auction",
            "symbols": ["000001.SZ"],
            "start": date(2026, 4, 10),
            "end": date(2026, 4, 11),
            "partition_values": ["20260410", "20260411"],
        }
        assert result["ts_code"].tolist() == ["000001.SZ"]

    def test_index_daily_code_loader_uses_research_default_benchmark_pool(self):
        provider = self._build_provider()
        provider._fetch_index_reference = lambda _api_name: (_ for _ in ()).throw(AssertionError("should not load catalog"))

        assert provider._load_index_codes() == [
            "000001.SH",
            "000016.SH",
            "399001.SZ",
            "399006.SZ",
            "000300.SH",
            "000905.SH",
            "000852.SH",
            "000906.SH",
            "000985.CSI",
        ]

    def test_fetch_index_daily_ignores_stock_symbol_filter(self, monkeypatch):
        provider = self._build_provider()

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "_fetch_index_loop_range",
            lambda *_args, **_kwargs: pd.DataFrame(
                {
                    "ts_code": ["000300.SH"],
                    "trade_date": ["20240102"],
                    "close": [3386.3522],
                }
            ),
        )

        result = provider.fetch_dataset(
            "index_daily",
            "cn_stock",
            date(2024, 1, 2),
            date(2024, 1, 5),
            symbols=["000001.SZ"],
        )

        assert result["symbol"].tolist() == ["000300.SH"]

    def test_fetch_stock_company_uses_exchange_reference_contract(self, monkeypatch):
        provider = self._build_provider()
        captured: dict[str, object] = {}

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "_fetch_exchange_reference",
            lambda api_name, param_name, loop_values, **_kwargs: (
                captured.update(
                    {
                        "api_name": api_name,
                        "param_name": param_name,
                        "loop_values": list(loop_values),
                    }
                )
                or pd.DataFrame({"ts_code": ["000001.SZ"], "exchange": ["SZSE"]})
            ),
        )
        monkeypatch.setattr(
            provider,
            "_normalize_dataset_frame",
            lambda _dataset, raw, **_kwargs: raw,
        )

        result = provider.fetch_dataset(
            "stock_company",
            "cn_stock",
            date(2026, 4, 1),
            date(2026, 4, 30),
        )

        assert captured == {
            "api_name": "stock_company",
            "param_name": "exchange",
            "loop_values": ["SSE", "SZSE", "BSE"],
        }
        assert result["exchange"].tolist() == ["SZSE"]

    @pytest.mark.parametrize("dataset", ["ths_member", "dc_member"])
    def test_normalize_member_dataset_prefers_con_code_as_symbol(
        self,
        dataset: str,
    ):
        provider = self._build_provider()
        raw = pd.DataFrame(
            {
                "ts_code": ["885001.TI", "885002.TI"],
                "con_code": ["000001.SZ", "600000.SH"],
                "name": ["平安银行", "浦发银行"],
            }
        )

        result = provider._normalize_dataset_frame(dataset, raw)

        assert result.columns.tolist() == ["ts_code", "symbol", "name"]
        assert result["ts_code"].tolist() == ["885001.TI", "885002.TI"]
        assert result["symbol"].tolist() == ["000001.SZ", "600000.SH"]

    def test_default_datasets_exclude_high_threshold_apis_for_5000_points(self):
        datasets = get_default_tushare_datasets(points=5000, permission_keys=set())

        assert "stock_company" in datasets
        assert "st" not in datasets
        assert "limit_step" not in datasets
        assert "cyq_perf" not in datasets
        assert "cyq_chips" not in datasets

    def test_fetch_realtime_quote_uses_realtime_quote_snapshot_contract(self, monkeypatch):
        provider = self._build_provider()
        captured: dict[str, object] = {}

        monkeypatch.setattr(provider, "_check_market", lambda _market: None)
        monkeypatch.setattr(
            provider,
            "_fetch_realtime_quote_snapshot",
            lambda symbols, **_kwargs: (
                captured.update({"symbols": list(symbols)})
                or pd.DataFrame({"TS_CODE": ["000001.SZ"], "DATE": ["20260430"], "A1_P": [11.49], "A1_V": [8673]})
            ),
        )

        result = provider.fetch_dataset(
            "realtime_quote",
            "cn_stock",
            date(2026, 4, 30),
            date(2026, 4, 30),
        )

        assert captured == {"symbols": []}
        assert result["symbol"].tolist() == ["000001.SZ"]
        assert result["ask1_price"].tolist() == [11.49]
        assert result["ask1_volume"].tolist() == [8673]

    def test_normalize_realtime_quote_maps_orderbook_fields(self):
        provider = self._build_provider()
        raw = pd.DataFrame(
            {
                "TS_CODE": ["000001.SZ"],
                "DATE": ["20260430"],
                "TIME": ["09:30:00"],
                "PRICE": [11.49],
                "A1_P": [11.49],
                "A1_V": [8673],
                "B1_P": [11.48],
                "B1_V": [979],
                "VOLUME": [113924162],
                "AMOUNT": [1312828000.0],
            }
        )

        result = provider._normalize_dataset_frame("realtime_quote", raw)

        row = result.iloc[0]
        assert row["symbol"] == "000001.SZ"
        assert row["date"] == "20260430"
        assert row["time"] == "09:30:00"
        assert row["trade_time"] == "20260430 09:30:00"
        assert row["ask1_price"] == 11.49
        assert row["ask1_volume"] == 8673
        assert row["bid1_price"] == 11.48
        assert row["bid1_volume"] == 979
