#!/usr/bin/env python3
"""
_run_all_downloads.py
串行执行所有缺失数据下载: 估值 → 财务 → 验证

估计总耗时: ~120 分钟 (视 Tushare API 响应速度)
"""
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vortex.config.settings import Settings, setup_logging
from vortex.core.datastore import DataStore

setup_logging("INFO")


def main():
    cfg = Settings()
    cfg.validate()
    ds = DataStore(cfg)
    start_year = 2014
    t0 = time.time()

    # ---- 1. 估值数据 (按年) ----
    print("\n" + "=" * 60)
    print("  Phase 1: 估值数据 (daily_basic)")
    print("=" * 60)
    val_dir = cfg.data_dir / "fundamental" / "valuation"
    existing_val = ds._get_downloaded_years(val_dir)
    current_year = datetime.now().year
    today = datetime.now().strftime("%Y%m%d")

    for year in range(start_year, current_year + 1):
        if year in existing_val:
            print(f"  {year}: 已有, 跳过")
            continue
        y_start = f"{year}0101"
        y_end = f"{year}1231" if year < current_year else today
        print(f"  下载 {year} 年估值...")
        ds.download_daily_basic(y_start, y_end)

    t1 = time.time()
    print(f"\n  ✅ 估值下载完成, 耗时 {(t1-t0)/60:.1f} 分钟")

    # ---- 2. 财务数据 (逐股遍历, 较慢) ----
    print("\n" + "=" * 60)
    print("  Phase 2: 财务数据 (逐股遍历, 预计 ~80 分钟)")
    print("=" * 60)
    periods = [f"{y}1231" for y in range(start_year, current_year + 1)]
    print(f"  报告期: {periods[0]} ~ {periods[-1]}")

    for name, fn in [
        ("财务指标 (fina_indicator)", ds.download_fina_indicator),
        ("利润表 (income)", ds.download_income),
        ("现金流量表 (cashflow)", ds.download_cashflow),
        ("资产负债表 (balancesheet)", ds.download_balancesheet),
    ]:
        t_start = time.time()
        print(f"\n  下载 {name}...")
        fn(periods=periods)
        t_end = time.time()
        print(f"  ✅ {name} 完成, 耗时 {(t_end-t_start)/60:.1f} 分钟")

    t2 = time.time()
    print(f"\n  ✅ 财务数据下载完成, 耗时 {(t2-t1)/60:.1f} 分钟")

    # ---- 3. 验证 ----
    print("\n" + "=" * 60)
    print("  Phase 3: 数据验证")
    print("=" * 60)
    import pandas as pd
    for f in ["fina_indicator", "cashflow", "income", "balancesheet"]:
        path = cfg.data_dir / "fundamental" / f"{f}.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            periods_found = sorted(df["end_date"].unique())
            print(f"  {f}: {len(df)} rows, {len(periods_found)} periods "
                  f"({periods_found[0]}~{periods_found[-1]})")
        else:
            print(f"  {f}: NOT FOUND!")

    val_files = sorted(val_dir.glob("*.parquet"))
    val_years = [f.stem for f in val_files]
    print(f"  valuation: {len(val_files)} years ({val_years[0]}~{val_years[-1]})")

    total = time.time() - t0
    print(f"\n{'='*60}")
    print(f"  ✅ 全部下载完成! 总耗时 {total/60:.1f} 分钟")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
