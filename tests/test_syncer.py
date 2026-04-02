from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd

from vortex.core.data.syncer import DataSyncer, SyncStatus


def _make_syncer(tmp_data_dir):
    ds = MagicMock()
    ds.data_dir = tmp_data_dir
    ds.pro = MagicMock()
    ds.cfg = SimpleNamespace()
    ds._api_call = MagicMock()
    ds._get_trade_dates_range = MagicMock()
    ds._get_downloaded_years = MagicMock(return_value=set())
    return DataSyncer(ds, start_year=2026), ds


def test_fund_nav_uses_nav_date_and_merges_existing_year_file(tmp_data_dir):
    out_dir = tmp_data_dir / "fund" / "fund_nav"
    out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {"ts_code": ["510300.SH"], "nav_date": ["20260102"], "unit_nav": [1.01]}
    ).to_parquet(out_dir / "2026.parquet", index=False)

    syncer, ds = _make_syncer(tmp_data_dir)
    ds._get_trade_dates_range.return_value = ["20260102", "20260103"]
    ds._api_call.return_value = pd.DataFrame(
        {"ts_code": ["510300.SH"], "nav_date": ["20260103"], "unit_nav": [1.02]}
    )

    result = syncer.sync_one("fund_nav")

    assert result.status == SyncStatus.SUCCESS
    kwargs = ds._api_call.call_args.kwargs
    assert kwargs["nav_date"] == "20260103"
    assert "trade_date" not in kwargs

    saved = pd.read_parquet(out_dir / "2026.parquet").sort_values("nav_date")
    assert saved["nav_date"].tolist() == ["20260102", "20260103"]
    assert syncer.manifest.get_watermark("fund_nav")["last_date"] == "20260103"


def test_cctv_news_uses_date_field(tmp_data_dir):
    syncer, ds = _make_syncer(tmp_data_dir)
    ds._get_trade_dates_range.return_value = ["20260102"]
    ds._api_call.return_value = pd.DataFrame(
        {"date": ["20260102"], "title": ["新闻联播"], "content": ["摘要"]}
    )

    result = syncer.sync_one("cctv_news", force=True)

    assert result.status == SyncStatus.SUCCESS
    kwargs = ds._api_call.call_args.kwargs
    assert kwargs["date"] == "20260102"
    assert "trade_date" not in kwargs
    assert (tmp_data_dir / "news" / "cctv_news" / "2026.parquet").exists()


def test_by_date_advances_watermark_on_empty_success(tmp_data_dir):
    syncer, ds = _make_syncer(tmp_data_dir)
    ds._get_trade_dates_range.return_value = ["20260102"]
    ds._api_call.return_value = pd.DataFrame()

    result = syncer.sync_one("cctv_news", force=True)

    assert result.status == SyncStatus.SKIPPED
    assert syncer.manifest.get_watermark("cctv_news")["last_date"] == "20260102"