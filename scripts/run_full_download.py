#!/usr/bin/env python3
"""
run_full_download.py
增量下载全量历史数据 (2014+ 以支持 2017 起步回测)

用法:
  python scripts/run_full_download.py                    # 全量增量
  python scripts/run_full_download.py --start-year 2017  # 指定起始年
  python scripts/run_full_download.py --index-only       # 仅补齐指数数据
  python scripts/run_full_download.py --fina-only        # 仅补齐财务数据
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vortex.config.settings import Settings, setup_logging
from vortex.core.datastore import DataStore

setup_logging("INFO")


def main():
    parser = argparse.ArgumentParser(description="QuantPilot 增量数据下载")
    parser.add_argument("--start-year", type=int, default=2014,
                        help="数据起始年份 (默认: 2014, 支持2017回测需要3年前数据)")
    parser.add_argument("--index-only", action="store_true",
                        help="仅下载指数日线+行业分类")
    parser.add_argument("--fina-only", action="store_true",
                        help="仅下载财务数据 (fina_indicator/income/cashflow/balancesheet)")
    parser.add_argument("--valuation-only", action="store_true",
                        help="仅下载估值数据 (daily_basic)")
    parser.add_argument("--daily-only", action="store_true",
                        help="仅下载日线行情")
    args = parser.parse_args()

    cfg = Settings()
    cfg.validate()
    ds = DataStore(cfg)

    print("=" * 60)
    print("  QuantPilot 增量数据下载")
    print(f"  起始年份: {args.start_year}")
    print(f"  数据目录: {cfg.data_dir}")
    print("=" * 60)

    start = time.time()

    if args.index_only:
        _download_index(ds, args.start_year)
    elif args.fina_only:
        _download_fina(ds, args.start_year)
    elif args.valuation_only:
        _download_valuation(ds, args.start_year)
    elif args.daily_only:
        _download_daily(ds, args.start_year)
    else:
        status = ds.ensure_data(start_year=args.start_year)
        print("\n下载状态:")
        for k, v in status.items():
            print(f"  {k}: {v}")

    elapsed = time.time() - start
    total_size = sum(f.stat().st_size for f in cfg.data_dir.rglob("*.parquet"))
    print(f"\n完成! 耗时 {elapsed/60:.1f} 分钟, Parquet 总大小: {total_size / 1024 / 1024:.1f} MB")


def _download_index(ds, start_year):
    """下载指数日线 + 行业分类"""
    print("\n[1/3] 下载指数日线...")
    ds.download_index_daily(start_date=f"{start_year}0101")
    print("[2/3] 下载行业分类...")
    ds.download_index_classify()
    print("[3/3] 下载行业成分...")
    ds.download_index_member()


def _download_fina(ds, start_year):
    """下载全部财务表 (扩展到 start_year)"""
    from datetime import datetime
    current_year = datetime.now().year
    periods = [f"{y}1231" for y in range(start_year, current_year + 1)]
    print(f"\n报告期: {periods[0]} ~ {periods[-1]}")

    for name, fn in [
        ("财务指标", ds.download_fina_indicator),
        ("利润表", ds.download_income),
        ("现金流量表", ds.download_cashflow),
        ("资产负债表", ds.download_balancesheet),
    ]:
        print(f"\n下载 {name}...")
        fn(periods=periods)


def _download_valuation(ds, start_year):
    """下载缺失年份的估值数据"""
    from datetime import datetime
    current_year = datetime.now().year
    val_dir = ds.data_dir / "fundamental" / "valuation"
    existing = ds._get_downloaded_years(val_dir)
    today = datetime.now().strftime("%Y%m%d")

    for year in range(start_year, current_year + 1):
        if year in existing:
            print(f"  {year}: 已有, 跳过")
            continue
        y_start = f"{year}0101"
        y_end = f"{year}1231" if year < current_year else today
        print(f"  下载 {year} 年估值...")
        ds.download_daily_basic(y_start, y_end)


def _download_daily(ds, start_year):
    """下载缺失年份的日线行情"""
    from datetime import datetime
    current_year = datetime.now().year
    daily_dir = ds.data_dir / "market" / "daily"
    existing = ds._get_downloaded_years(daily_dir)
    today = datetime.now().strftime("%Y%m%d")

    for year in range(start_year, current_year + 1):
        if year in existing:
            print(f"  {year}: 已有, 跳过")
            continue
        y_start = f"{year}0101"
        y_end = f"{year}1231" if year < current_year else today
        print(f"  下载 {year} 年日线...")
        ds.download_daily(y_start, y_end)


if __name__ == "__main__":
    main()
