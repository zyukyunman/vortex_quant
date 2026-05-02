"""可恢复的 Tushare 分钟数据下载 runner。

该模块服务于因子研究，不走正式 data pipeline 发布流程；它把 `stk_mins`
按 symbol 逐个缓存到 research workspace，并用 manifest 记录完整性。
"""
from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Callable, Iterable, Sequence

import pandas as pd

from vortex.data.provider.tushare import TushareProvider


ProviderFactory = Callable[[], object]


@dataclass(frozen=True)
class MinuteDownloadConfig:
    """分钟缓存下载配置。"""

    root: Path
    output_root: Path
    year: int
    universe: str = "liquid300"
    universe_mode: str = "liquid_top_n"
    top_n: int = 300
    symbols_file: Path | None = None
    max_symbols: int | None = None
    start_date: date | None = None
    end_date: date | None = None
    resume_dirs: tuple[Path, ...] = ()
    manifest_path: Path | None = None

    @property
    def start(self) -> date:
        return self.start_date or date(self.year, 1, 1)

    @property
    def end(self) -> date:
        return self.end_date or date(self.year, 12, 31)

    @property
    def cache_dir(self) -> Path:
        return self.output_root / f"minute_cache_{self.year}_{self.universe}"

    @property
    def resolved_manifest_path(self) -> Path:
        return self.manifest_path or self.output_root / f"minute_cache_{self.year}_{self.universe}_manifest.json"


@dataclass
class MinuteDownloadResult:
    """分钟缓存下载结果。"""

    manifest_path: Path
    cache_dir: Path
    target_symbols: list[str]
    downloaded_rows: int
    results: list[dict[str, object]] = field(default_factory=list)


def select_liquid_symbols(root: Path, *, year: int, top_n: int) -> list[str]:
    """从本地日线 bars 中按年度平均成交额选出高流动性股票。"""

    frames: list[pd.DataFrame] = []
    for path in sorted((root / "data" / "bars").glob("date=*/data.parquet")):
        day = path.parent.name.split("=", 1)[1]
        if f"{year}0101" <= day <= f"{year}1231":
            frames.append(pd.read_parquet(path, columns=["symbol", "amount"]))
    if not frames:
        raise FileNotFoundError(f"未找到 {year} 年 bars 数据: {root / 'data' / 'bars'}")
    bars = pd.concat(frames, ignore_index=True)
    return list(bars.groupby("symbol")["amount"].mean().sort_values(ascending=False).head(top_n).index)


def select_active_symbols(root: Path, *, year: int) -> list[str]:
    """从本地日线 bars 中选出当年有交易记录的全 A active 股票。"""

    frames: list[pd.DataFrame] = []
    for path in _iter_year_bar_paths(root, year):
        frames.append(pd.read_parquet(path, columns=["symbol"]))
    if not frames:
        raise FileNotFoundError(f"未找到 {year} 年 bars 数据: {root / 'data' / 'bars'}")
    bars = pd.concat(frames, ignore_index=True)
    return sorted(str(symbol) for symbol in bars["symbol"].dropna().unique())


def read_symbols_file(path: Path) -> list[str]:
    """读取外部股票清单；支持纯文本和带 symbol/ts_code 列的 CSV。"""

    resolved = path.expanduser()
    if not resolved.exists():
        raise FileNotFoundError(f"股票清单不存在: {resolved}")
    if resolved.suffix.lower() == ".csv":
        with resolved.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames:
                field = "symbol" if "symbol" in reader.fieldnames else "ts_code" if "ts_code" in reader.fieldnames else reader.fieldnames[0]
                return _dedupe_symbols(row[field] for row in reader if row.get(field))
    text = resolved.read_text(encoding="utf-8-sig")
    tokens = text.replace(",", "\n").replace(" ", "\n").splitlines()
    return _dedupe_symbols(tokens)


def select_target_symbols(config: MinuteDownloadConfig) -> list[str]:
    """按下载配置解析目标股票池。"""

    if config.symbols_file is not None:
        return read_symbols_file(config.symbols_file)
    mode = config.universe_mode.strip().lower()
    if mode in {"liquid", "liquid_top_n", "liquid-top-n"}:
        return select_liquid_symbols(config.root, year=config.year, top_n=config.top_n)
    if mode in {"all_active", "all-active", "active"}:
        return select_active_symbols(config.root, year=config.year)
    raise ValueError(f"不支持的 universe_mode: {config.universe_mode}")


def existing_symbols(paths: Iterable[Path]) -> set[str]:
    """从已有缓存目录中解析已完成 symbol。"""

    result: set[str] = set()
    for root in paths:
        if not root.exists():
            continue
        result.update(path.stem for path in root.glob("*.parquet"))
    return result


def build_manifest_rollup(manifest_paths: Sequence[Path]) -> dict[str, object]:
    """汇总多个 minute downloader manifest 的覆盖状态。"""

    rows: list[dict[str, object]] = []
    for path in manifest_paths:
        payload = json.loads(path.expanduser().read_text(encoding="utf-8"))
        results = list(payload.get("results", []))
        status_counts: dict[str, int] = {}
        downloaded_rows = 0
        for item in results:
            if not isinstance(item, dict):
                continue
            status = str(item.get("status", "unknown"))
            status_counts[status] = status_counts.get(status, 0) + 1
            downloaded_rows += int(item.get("rows", 0) or 0)
        target_count = int(payload.get("target_count", 0) or 0)
        skipped_existing_count = int(payload.get("skipped_existing_count", 0) or 0)
        covered_count = (
            skipped_existing_count
            + status_counts.get("downloaded", 0)
            + status_counts.get("empty", 0)
        )
        rows.append(
            {
                "year": payload.get("year"),
                "universe": payload.get("universe"),
                "universe_mode": payload.get("universe_mode"),
                "status": payload.get("status"),
                "target_count": target_count,
                "skipped_existing_count": skipped_existing_count,
                "downloaded_symbol_count": status_counts.get("downloaded", 0),
                "empty_symbol_count": status_counts.get("empty", 0),
                "failed_count": status_counts.get("failed", 0),
                "covered_symbol_count": covered_count,
                "downloaded_rows_manifest": downloaded_rows,
                "manifest_path": str(path),
            }
        )
    return {
        "schema": "vortex.minute_download_rollup.v1",
        "all_completed": all(row["status"] == "completed" for row in rows),
        "total_target_symbol_years": sum(int(row["target_count"]) for row in rows),
        "total_covered_symbol_years": sum(int(row["covered_symbol_count"]) for row in rows),
        "total_downloaded_rows_manifest": sum(int(row["downloaded_rows_manifest"]) for row in rows),
        "total_failed_count": sum(int(row["failed_count"]) for row in rows),
        "rows": rows,
    }


def summarize_minute_frame(symbol: str, status: str, frame: pd.DataFrame) -> dict[str, object]:
    """生成单个 symbol 的 manifest 记录。"""

    row: dict[str, object] = {
        "symbol": symbol,
        "status": status,
        "rows": int(len(frame)),
    }
    if not frame.empty:
        row.update(
            {
                "date_min": str(frame["date"].min()) if "date" in frame.columns else None,
                "date_max": str(frame["date"].max()) if "date" in frame.columns else None,
                "dates": int(frame["date"].nunique()) if "date" in frame.columns else None,
            }
        )
    return row


def run_minute_download(
    config: MinuteDownloadConfig,
    *,
    provider_factory: ProviderFactory = TushareProvider,
) -> MinuteDownloadResult:
    """按配置下载分钟数据，并持续写出 manifest。"""

    config.cache_dir.mkdir(parents=True, exist_ok=True)
    config.output_root.mkdir(parents=True, exist_ok=True)
    target_symbols = select_target_symbols(config)
    skipped = existing_symbols((*config.resume_dirs, config.cache_dir))
    pending_symbols = [symbol for symbol in target_symbols if symbol not in skipped]
    if config.max_symbols is not None:
        pending_symbols = pending_symbols[: config.max_symbols]

    manifest: dict[str, object] = {
        "schema": "vortex.minute_download_manifest.v1",
        "year": config.year,
        "start": config.start.strftime("%Y%m%d"),
        "end": config.end.strftime("%Y%m%d"),
        "universe": config.universe,
        "universe_mode": config.universe_mode,
        "top_n": config.top_n,
        "symbols_file": str(config.symbols_file) if config.symbols_file else None,
        "cache_dir": str(config.cache_dir),
        "resume_dirs": [str(path) for path in config.resume_dirs],
        "target_count": len(target_symbols),
        "skipped_existing_count": len(skipped.intersection(target_symbols)),
        "pending_count": len(pending_symbols),
        "results": [],
        "status": "running",
    }
    _write_manifest(config.resolved_manifest_path, manifest)

    provider = provider_factory()
    results: list[dict[str, object]] = []
    for idx, symbol in enumerate(pending_symbols, start=1):
        output = config.cache_dir / f"{symbol}.parquet"
        try:
            frame = provider.fetch_dataset(  # type: ignore[attr-defined]
                "stk_mins",
                "cn_stock",
                config.start,
                config.end,
                symbols=[symbol],
            )
            status = "empty" if frame.empty else "downloaded"
            if not frame.empty:
                frame.to_parquet(output, index=False)
            row = summarize_minute_frame(symbol, status, frame)
        except Exception as exc:  # noqa: BLE001 - manifest 必须显式记录失败并继续后续 symbol
            row = {
                "symbol": symbol,
                "status": "failed",
                "rows": 0,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        row["ordinal"] = idx
        results.append(row)
        manifest["results"] = results
        manifest["downloaded_rows"] = int(sum(int(item.get("rows", 0)) for item in results))
        _write_manifest(config.resolved_manifest_path, manifest)

    manifest["status"] = "completed"
    manifest["downloaded_rows"] = int(sum(int(item.get("rows", 0)) for item in results))
    _write_manifest(config.resolved_manifest_path, manifest)
    return MinuteDownloadResult(
        manifest_path=config.resolved_manifest_path,
        cache_dir=config.cache_dir,
        target_symbols=target_symbols,
        downloaded_rows=int(manifest["downloaded_rows"]),
        results=results,
    )


def _write_manifest(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _iter_year_bar_paths(root: Path, year: int) -> list[Path]:
    result: list[Path] = []
    for path in sorted((root / "data" / "bars").glob("date=*/data.parquet")):
        day = path.parent.name.split("=", 1)[1]
        if f"{year}0101" <= day <= f"{year}1231":
            result.append(path)
    return result


def _dedupe_symbols(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        symbol = str(value).strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        result.append(symbol)
    return result


def _parse_paths(values: list[str]) -> tuple[Path, ...]:
    return tuple(Path(value).expanduser() for value in values)


def _parse_cli_date(value: str) -> date:
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) != 8:
        raise argparse.ArgumentTypeError(f"日期必须是 YYYYMMDD: {value}")
    return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="下载并缓存 Tushare stk_mins 分钟数据")
    parser.add_argument("--root", type=Path, default=Path("~/Documents/vortex_workspace").expanduser())
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--universe", default="liquid300")
    parser.add_argument(
        "--universe-mode",
        default="liquid_top_n",
        choices=["liquid_top_n", "all_active"],
        help="目标股票池模式：liquid_top_n 按年度成交额 TopN，all_active 为当年本地日线有交易记录的全 A",
    )
    parser.add_argument("--top-n", type=int, default=300)
    parser.add_argument("--symbols-file", type=Path, help="外部股票清单；提供后优先于 universe-mode")
    parser.add_argument("--max-symbols", type=int)
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--resume-dir", action="append", default=[])
    parser.add_argument("--manifest-path", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    config = MinuteDownloadConfig(
        root=args.root.expanduser(),
        output_root=args.output_root.expanduser(),
        year=args.year,
        universe=args.universe,
        universe_mode=args.universe_mode,
        top_n=args.top_n,
        symbols_file=args.symbols_file.expanduser() if args.symbols_file else None,
        max_symbols=args.max_symbols,
        start_date=_parse_cli_date(args.start_date) if args.start_date else None,
        end_date=_parse_cli_date(args.end_date) if args.end_date else None,
        resume_dirs=_parse_paths(args.resume_dir),
        manifest_path=args.manifest_path.expanduser() if args.manifest_path else None,
    )
    result = run_minute_download(config)
    print(
        json.dumps(
            {
                "manifest_path": str(result.manifest_path),
                "cache_dir": str(result.cache_dir),
                "target_count": len(result.target_symbols),
                "downloaded_rows": result.downloaded_rows,
                "result_count": len(result.results),
                "pid": os.getpid(),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
