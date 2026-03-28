#!/usr/bin/env python3
"""
watch_and_run.py
监控数据下载状态，下载完成后自动运行完整管线

使用方式:
  python scripts/watch_and_run.py
  后台运行: nohup python scripts/watch_and_run.py &
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"

REQUIRED_FILES = [
    DATA_DIR / "fundamental" / "fina_indicator.parquet",
    DATA_DIR / "fundamental" / "income.parquet",
    DATA_DIR / "fundamental" / "cashflow.parquet",
    DATA_DIR / "fundamental" / "balancesheet.parquet",
    DATA_DIR / "fundamental" / "dividend.parquet",
]


def check_all_ready():
    missing = [str(f.name) for f in REQUIRED_FILES if not f.exists()]
    return len(missing) == 0, missing


def main():
    print("=" * 60)
    print("  数据监控 — 等待下载完成后自动运行管线")
    print("=" * 60)

    check_interval = 60  # 每 60 秒检查一次
    max_wait = 14400     # 最多等 4 小时

    waited = 0
    while waited < max_wait:
        ready, missing = check_all_ready()
        if ready:
            print(f"\n[{time.strftime('%H:%M:%S')}] 所有数据已就绪! 启动完整管线...")
            subprocess.run(
                [sys.executable, str(PROJECT_ROOT / "scripts" / "auto_pipeline.py")],
                cwd=str(PROJECT_ROOT),
            )
            print("\n管线执行完成!")
            return

        print(f"[{time.strftime('%H:%M:%S')}] 等待中... 缺少: {missing} ({waited}s)")
        time.sleep(check_interval)
        waited += check_interval

    print(f"超时 ({max_wait}s), 数据仍未就绪")


if __name__ == "__main__":
    main()
