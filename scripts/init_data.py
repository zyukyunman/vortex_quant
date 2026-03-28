#!/usr/bin/env python3
"""
init_data.py
数据初始化 — 下载策略所需的全部历史数据

使用方式:
  python scripts/init_data.py                    # 默认下载 2005 年至今 (全量)
  python scripts/init_data.py --start-year 2015  # 从 2015 年开始
  python scripts/init_data.py --start-year 2017  # 仅 2017+ (快速验证)

下载内容 (增量, 跳过已有数据):
  - 股票基本信息、交易日历
  - 日线行情 + 每日估值 (按年增量)
  - 财务指标 / 利润表 / 现金流量表 / 资产负债表
  - 全量分红数据
  - 指数日线 (沪深300, 中证500, 中证红利等)
  - 申万行业分类 + 成分股
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 将项目根目录加入 sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import Settings, setup_logging
from app.core.datastore import DataStore


def main():
    parser = argparse.ArgumentParser(description="QuantPilot 数据初始化")
    parser.add_argument(
        "--start-year", type=int, default=2005,
        help="数据起始年份 (默认: 2005)",
    )
    args = parser.parse_args()

    setup_logging("INFO")

    cfg = Settings()
    cfg.validate()

    ds = DataStore(cfg)

    print("=" * 60)
    print("  QuantPilot 数据初始化")
    print(f"  起始年份: {args.start_year}")
    print(f"  数据目录: {cfg.data_dir}")
    print("  模式: 增量 (已有数据自动跳过)")
    print("=" * 60)

    status = ds.ensure_data(start_year=args.start_year)

    # ---- 完成 ----
    print("\n" + "=" * 60)
    print("  ✅ 数据初始化完成!")
    for k, v in status.items():
        print(f"    {k}: {v}")

    # 显示文件大小
    total_size = sum(f.stat().st_size for f in cfg.data_dir.rglob("*.parquet"))
    print(f"\n  Parquet 文件总大小: {total_size / 1024 / 1024:.1f} MB")
    print("=" * 60)


if __name__ == "__main__":
    main()
