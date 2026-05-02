from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from vortex.research.minute_downloader import (
    MinuteDownloadConfig,
    build_manifest_rollup,
    existing_symbols,
    read_symbols_file,
    run_minute_download,
    select_active_symbols,
    select_liquid_symbols,
)


def _write_bar(root: Path, day: str, rows: list[dict[str, object]]) -> None:
    path = root / "data" / "bars" / f"date={day}" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


class FakeProvider:
    def fetch_dataset(self, dataset, market, start, end, *, symbols):
        assert dataset == "stk_mins"
        assert market == "cn_stock"
        assert start == date(2026, 1, 1)
        assert end == date(2026, 12, 31)
        symbol = symbols[0]
        return pd.DataFrame(
            {
                "symbol": [symbol, symbol],
                "date": ["20260105", "20260106"],
                "minute": ["09:31:00", "09:32:00"],
                "volume": [100.0, 200.0],
            }
        )


def test_select_liquid_symbols_from_local_bars(tmp_path):
    _write_bar(
        tmp_path,
        "20260105",
        [
            {"symbol": "A", "amount": 10.0},
            {"symbol": "B", "amount": 30.0},
            {"symbol": "C", "amount": 20.0},
        ],
    )
    _write_bar(
        tmp_path,
        "20260106",
        [
            {"symbol": "A", "amount": 10.0},
            {"symbol": "B", "amount": 10.0},
            {"symbol": "C", "amount": 60.0},
        ],
    )

    assert select_liquid_symbols(tmp_path, year=2026, top_n=2) == ["C", "B"]


def test_select_active_symbols_from_local_bars(tmp_path):
    _write_bar(
        tmp_path,
        "20260105",
        [
            {"symbol": "B", "amount": 30.0},
            {"symbol": "A", "amount": 20.0},
        ],
    )
    _write_bar(
        tmp_path,
        "20260106",
        [
            {"symbol": "A", "amount": 10.0},
            {"symbol": "C", "amount": 60.0},
        ],
    )

    assert select_active_symbols(tmp_path, year=2026) == ["A", "B", "C"]


def test_read_symbols_file_supports_text_and_csv(tmp_path):
    text_path = tmp_path / "symbols.txt"
    text_path.write_text("A\nB,C\nA\n", encoding="utf-8")
    assert read_symbols_file(text_path) == ["A", "B", "C"]

    csv_path = tmp_path / "symbols.csv"
    csv_path.write_text("ts_code,name\n000001.SZ,平安银行\n600000.SH,浦发银行\n", encoding="utf-8")
    assert read_symbols_file(csv_path) == ["000001.SZ", "600000.SH"]


def test_existing_symbols_reads_parquet_stems(tmp_path):
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "000001.SZ.parquet").write_bytes(b"placeholder")

    assert existing_symbols([cache]) == {"000001.SZ"}


def test_run_minute_download_skips_existing_and_writes_manifest(tmp_path):
    _write_bar(
        tmp_path,
        "20260105",
        [
            {"symbol": "A", "amount": 30.0},
            {"symbol": "B", "amount": 20.0},
            {"symbol": "C", "amount": 10.0},
        ],
    )
    resume = tmp_path / "resume"
    resume.mkdir()
    pd.DataFrame({"symbol": ["A"], "date": ["20260105"]}).to_parquet(resume / "A.parquet", index=False)

    output = tmp_path / "research"
    result = run_minute_download(
        MinuteDownloadConfig(
            root=tmp_path,
            output_root=output,
            year=2026,
            universe="liquid3",
            top_n=3,
            max_symbols=1,
            resume_dirs=(resume,),
        ),
        provider_factory=FakeProvider,
    )

    assert result.downloaded_rows == 2
    assert (output / "minute_cache_2026_liquid3" / "B.parquet").exists()
    manifest = result.manifest_path.read_text(encoding="utf-8")
    assert '"skipped_existing_count": 1' in manifest
    assert '"symbol": "B"' in manifest


def test_run_minute_download_supports_all_active_universe(tmp_path):
    _write_bar(
        tmp_path,
        "20260105",
        [
            {"symbol": "A", "amount": 30.0},
            {"symbol": "B", "amount": 20.0},
        ],
    )

    output = tmp_path / "research"
    result = run_minute_download(
        MinuteDownloadConfig(
            root=tmp_path,
            output_root=output,
            year=2026,
            universe="all_active",
            universe_mode="all_active",
            max_symbols=1,
        ),
        provider_factory=FakeProvider,
    )

    assert result.target_symbols == ["A", "B"]
    assert result.downloaded_rows == 2
    assert (output / "minute_cache_2026_all_active" / "A.parquet").exists()


def test_build_manifest_rollup_counts_statuses(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        """
        {
          "year": 2026,
          "universe": "all_active",
          "universe_mode": "all_active",
          "status": "completed",
          "target_count": 4,
          "skipped_existing_count": 1,
          "results": [
            {"symbol": "A", "status": "downloaded", "rows": 2},
            {"symbol": "B", "status": "empty", "rows": 0},
            {"symbol": "C", "status": "failed", "rows": 0}
          ]
        }
        """,
        encoding="utf-8",
    )

    rollup = build_manifest_rollup([manifest])

    assert rollup["all_completed"] is True
    assert rollup["total_target_symbol_years"] == 4
    assert rollup["total_covered_symbol_years"] == 3
    assert rollup["total_failed_count"] == 1
    assert rollup["rows"][0]["downloaded_symbol_count"] == 1
