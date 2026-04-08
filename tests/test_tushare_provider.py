"""Tushare Provider 运行时行为测试。"""
from __future__ import annotations

import builtins
import sys
from datetime import date

import pandas as pd
import pytest

import vortex.data.provider.tushare as tushare_provider
from vortex.data.provider.tushare_registry import get_default_tushare_datasets
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
