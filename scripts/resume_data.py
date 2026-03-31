#!/usr/bin/env python3
"""
resume_data.py
增量数据同步 — 补充缺失数据 (等同 init_data.py --start-year)

用法:
  python scripts/resume_data.py                    # 默认从 2005 开始补全
  python scripts/resume_data.py --start-year 2017  # 只补全 2017 年以后
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from vortex.config.settings import Settings, setup_logging
from vortex.core.datastore import DataStore


def main():
    parser = argparse.ArgumentParser(description="QuantPilot 增量数据同步")
    parser.add_argument(
        "--start-year", type=int, default=2005,
        help="数据起始年份 (默认: 2005)",
    )
    args = parser.parse_args()

    setup_logging("INFO")
    cfg = Settings()
    cfg.validate()
    ds = DataStore(cfg)

    print(f"增量数据同步 (start_year={args.start_year})...")
    status = ds.ensure_data(start_year=args.start_year)

    print("\n数据同步完成:")
    for k, v in status.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
