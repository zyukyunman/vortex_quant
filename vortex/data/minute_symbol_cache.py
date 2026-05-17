"""Internal symbol-year cache helpers for large minute-bar datasets.

This module is intentionally not a CLI surface.  ``stk_mins`` is orchestrated by
the normal data bootstrap pipeline, while this helper only handles the storage
layout that avoids loading full A-share minute bars into one in-memory frame.
"""
from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


CancelCheck = Callable[[], bool]
ProgressCallback = Callable[[int, int, str], None]


@dataclass(frozen=True)
class MinuteSymbolYearConfig:
    root: Path
    year: int
    symbols: tuple[str, ...]
    dataset: str = "stk_mins"
    universe: str = "all_active"
    start_date: date | None = None
    end_date: date | None = None
    source_cache_dirs: tuple[Path, ...] = ()
    manifest_path: Path | None = None

    @property
    def start(self) -> date:
        return self.start_date or date(self.year, 1, 1)

    @property
    def end(self) -> date:
        return self.end_date or date(self.year, 12, 31)

    @property
    def dataset_dir(self) -> Path:
        return self.root / "data" / self.dataset

    @property
    def manifest_dir(self) -> Path:
        return self.root / "state" / "manifests" / "minute_cache"

    @property
    def resolved_manifest_path(self) -> Path:
        return self.manifest_path or self.manifest_dir / f"{self.dataset}_{self.year}_{self.universe}_manifest.json"


@dataclass
class MinuteSymbolYearResult:
    manifest_path: Path
    dataset_dir: Path
    target_symbols: list[str]
    rows_written: int
    results: list[dict[str, object]] = field(default_factory=list)


def symbol_year_path(config: MinuteSymbolYearConfig, symbol: str) -> Path:
    return (
        config.dataset_dir
        / f"year={config.year}"
        / f"universe={config.universe}"
        / f"symbol={symbol}"
        / "data.parquet"
    )


def discover_legacy_minute_cache_dirs(root: Path, year: int) -> tuple[Path, ...]:
    """Find legacy research minute caches that can be migrated by bootstrap."""

    base = root / "research" / "factor-reports" / "volume-peak-ridge-valley"
    if not base.exists():
        return ()
    return tuple(sorted(path for path in base.glob(f"minute_cache_{year}_*") if path.is_dir()))


def select_active_symbols_from_bars(root: Path, year: int, fallback_symbols: Iterable[str]) -> tuple[str, ...]:
    """Resolve all locally active symbols for a year from formal daily bars."""

    frames: list[pd.DataFrame] = []
    for path in sorted((root / "data" / "bars").glob("date=*/data.parquet")):
        day = path.parent.name.split("=", 1)[1]
        if f"{year}0101" <= day <= f"{year}1231":
            frames.append(pd.read_parquet(path, columns=["symbol"]))
    if frames:
        bars = pd.concat(frames, ignore_index=True)
        symbols = sorted(str(symbol) for symbol in bars["symbol"].dropna().unique())
        return tuple(symbols)
    return tuple(str(symbol) for symbol in fallback_symbols if str(symbol).strip())


def sync_minute_symbol_year_cache(
    provider: object,
    config: MinuteSymbolYearConfig,
    *,
    market: str = "cn_stock",
    progress_callback: ProgressCallback | None = None,
    cancel_check: CancelCheck | None = None,
) -> MinuteSymbolYearResult:
    """Migrate existing symbol files or download missing symbols via data bootstrap."""

    config.manifest_dir.mkdir(parents=True, exist_ok=True)
    target_symbols = list(dict.fromkeys(config.symbols))
    manifest: dict[str, object] = {
        "schema": "vortex.data.minute_symbol_year_manifest.v1",
        "operation": "bootstrap",
        "dataset": config.dataset,
        "layout": "symbol_year",
        "year": config.year,
        "start": config.start.strftime("%Y%m%d"),
        "end": config.end.strftime("%Y%m%d"),
        "universe": config.universe,
        "target_count": len(target_symbols),
        "source_cache_dirs": [str(path) for path in config.source_cache_dirs],
        "status": "running",
        "results": [],
        "written_rows": 0,
    }
    _write_manifest(config.resolved_manifest_path, manifest)

    rows_written = 0
    rows: list[dict[str, object]] = []
    for ordinal, symbol in enumerate(target_symbols, start=1):
        if cancel_check and cancel_check():
            manifest["status"] = "cancelled"
            manifest["results"] = rows
            manifest["written_rows"] = rows_written
            _write_manifest(config.resolved_manifest_path, manifest)
            break
        row = _sync_one_symbol(provider, config, market, symbol)
        row["ordinal"] = ordinal
        rows.append(row)
        rows_written += int(row.get("rows", 0) or 0) if row.get("status") in {"migrated", "downloaded"} else 0
        manifest["results"] = rows
        manifest["written_rows"] = rows_written
        _write_manifest(config.resolved_manifest_path, manifest)
        if progress_callback is not None:
            progress_callback(ordinal, len(target_symbols), f"{config.dataset} {config.year} {symbol}: {row['status']}")

    if manifest.get("status") != "cancelled":
        manifest["status"] = "completed"
        _write_manifest(config.resolved_manifest_path, manifest)

    return MinuteSymbolYearResult(
        manifest_path=config.resolved_manifest_path,
        dataset_dir=config.dataset_dir,
        target_symbols=target_symbols,
        rows_written=rows_written,
        results=rows,
    )


def _sync_one_symbol(provider: object, config: MinuteSymbolYearConfig, market: str, symbol: str) -> dict[str, object]:
    destination = symbol_year_path(config, symbol)
    if destination.exists():
        rows = _parquet_rows(destination)
        return {"symbol": symbol, "status": "existing", "rows": rows, "path": str(destination)}

    source = _find_source_file(config.source_cache_dirs, symbol)
    if source is not None:
        frame = _filter_frame_dates(pd.read_parquet(source), config.start, config.end)
        if frame.empty:
            return {"symbol": symbol, "status": "empty", "rows": 0, "source_path": str(source)}
        _materialize_symbol_year(config, symbol, source, frame)
        row = _summarize_frame(symbol, "migrated", frame)
        row["source_path"] = str(source)
        return row

    try:
        frame = provider.fetch_dataset(  # type: ignore[attr-defined]
            config.dataset,
            market,
            config.start,
            config.end,
            symbols=[symbol],
        )
        frame = _filter_frame_dates(frame, config.start, config.end)
        if frame.empty:
            return {"symbol": symbol, "status": "empty", "rows": 0}
        _write_symbol_year(config, symbol, frame)
        return _summarize_frame(symbol, "downloaded", frame)
    except Exception as exc:  # noqa: BLE001 - manifest records per-symbol failures
        return {
            "symbol": symbol,
            "status": "failed",
            "rows": 0,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def _materialize_symbol_year(config: MinuteSymbolYearConfig, symbol: str, source: Path, filtered_frame: pd.DataFrame) -> None:
    destination = symbol_year_path(config, symbol)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if _parquet_rows(source) == len(filtered_frame):
        try:
            if destination.exists():
                destination.unlink()
            os.link(source, destination)
            return
        except OSError:
            shutil.copy2(source, destination)
            return
    _write_symbol_year(config, symbol, filtered_frame)


def _write_symbol_year(config: MinuteSymbolYearConfig, symbol: str, frame: pd.DataFrame) -> None:
    path = symbol_year_path(config, symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, path, compression="snappy", row_group_size=1_000_000)


def _filter_frame_dates(frame: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    if frame.empty or "date" not in frame.columns:
        return frame
    clean = frame.copy()
    clean["date"] = clean["date"].astype(str)
    return clean.loc[clean["date"].between(start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), inclusive="both")].copy()


def _summarize_frame(symbol: str, status: str, frame: pd.DataFrame) -> dict[str, object]:
    row: dict[str, object] = {"symbol": symbol, "status": status, "rows": int(len(frame))}
    if not frame.empty and "date" in frame.columns:
        row.update(
            {
                "date_min": str(frame["date"].min()),
                "date_max": str(frame["date"].max()),
                "dates": int(frame["date"].nunique()),
            }
        )
    return row


def _find_source_file(paths: Iterable[Path], symbol: str) -> Path | None:
    for root in paths:
        path = root.expanduser() / f"{symbol}.parquet"
        if path.exists():
            return path
    return None


def _parquet_rows(path: Path) -> int:
    try:
        return int(pq.ParquetFile(path).metadata.num_rows)
    except Exception:
        return 0


def _write_manifest(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
