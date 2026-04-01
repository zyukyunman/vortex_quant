#!/usr/bin/env python3
"""
run_sync.py
全量数据同步脚本 — 覆盖 Tushare 全部可用接口

使用方式:
  python scripts/run_sync.py                        # 全量同步 (跳过已有)
  python scripts/run_sync.py --daily                 # 每日增量同步
  python scripts/run_sync.py --category macro        # 只同步宏观数据
  python scripts/run_sync.py --category fundamental  # 只同步财务数据
  python scripts/run_sync.py --one moneyflow_hsgt    # 只同步单个接口
  python scripts/run_sync.py --force                 # 强制重新下载全部
  python scripts/run_sync.py --list                  # 列出全部任务
  python scripts/run_sync.py --list --category macro # 列出某分类任务
  python scripts/run_sync.py --points 5000           # 声明积分 (影响跳过逻辑)
  python scripts/run_sync.py --start-year 2017       # 指定起始年份
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from vortex.config.settings import Settings, setup_logging
from vortex.core.data.datastore import DataStore
from vortex.core.data.syncer import DataSyncer

setup_logging("INFO")


def main():
    parser = argparse.ArgumentParser(description="Vortex 全量数据同步")
    parser.add_argument("--daily", action="store_true",
                        help="每日增量同步模式")
    parser.add_argument("--category", type=str, default=None,
                        help="只同步某个分类 (meta/market/fundamental/reference/index/fund/moneyflow/macro/hk/us/fx/futures/bond/news)")
    parser.add_argument("--one", type=str, default=None,
                        help="只同步单个任务 (任务名)")
    parser.add_argument("--force", action="store_true",
                        help="强制重新下载 (忽略已有数据)")
    parser.add_argument("--list", action="store_true", dest="list_tasks",
                        help="列出全部已注册任务")
    parser.add_argument("--points", type=int, default=5000,
                        help="Tushare 积分 (默认2000, 影响哪些接口会被跳过)")
    parser.add_argument("--start-year", type=int, default=2000,
                        help="数据起始年份 (默认2014)")
    args = parser.parse_args()

    cfg = Settings()
    cfg.validate()
    ds = DataStore(cfg)
    syncer = DataSyncer(ds, start_year=args.start_year, user_points=args.points)

    # 列出任务模式
    if args.list_tasks:
        tasks = syncer.list_tasks(category=args.category)
        categories = syncer.list_categories()

        if args.category:
            print(f"\n分类 [{args.category}] 共 {len(tasks)} 个任务:")
        else:
            print(f"\n全部 {len(tasks)} 个任务, {len(categories)} 个分类:")
            print(f"分类: {', '.join(categories)}\n")

        print(f"{'任务名':<25} {'接口':<22} {'描述':<20} {'积分':>5} {'模式':<10} {'可用':>4}")
        print("-" * 95)
        for t in tasks:
            accessible = "✅" if t["accessible"] else "🔒"
            print(
                f"{t['name']:<25} {t['api_name']:<22} {t['description']:<20} "
                f"{t['min_points']:>5} {t['mode']:<10} {accessible:>4}"
            )
        return

    # 同步模式
    print("=" * 60)
    print("  Vortex 数据同步")
    print(f"  积分: {args.points}")
    print(f"  起始年份: {args.start_year}")
    print(f"  数据目录: {cfg.data_dir}")
    if args.daily:
        print("  模式: 每日增量")
    elif args.category:
        print(f"  模式: 分类同步 [{args.category}]")
    elif args.one:
        print(f"  模式: 单任务 [{args.one}]")
    else:
        print("  模式: 全量同步")
    print("=" * 60)

    if args.one:
        result = syncer.sync_one(args.one, force=args.force)
        print(f"\n{result.name}: {result.status.value} ({result.rows} 行, {result.elapsed:.1f}s)")
        if result.message:
            print(f"  {result.message}")
    elif args.daily:
        syncer.sync_daily()
    elif args.category:
        syncer.sync_category(args.category)
    else:
        syncer.sync_all(force=args.force)


if __name__ == "__main__":
    main()
