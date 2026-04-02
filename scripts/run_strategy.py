#!/usr/bin/env python3
"""
run_strategy.py
月度选股执行脚本

使用方式:
  python scripts/run_strategy.py                   # 使用最近一个交易日
  python scripts/run_strategy.py --date 20260327   # 指定日期
  python scripts/run_strategy.py --top 20          # 选 Top 20

前置条件:
  - 已运行 init_data.py 下载数据
  - .env 中配置了 TUSHARE_TOKEN

输出:
  - 终端打印选股结果
  - 信号保存到 data/signal/{year}.parquet
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# 将项目根目录加入 sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from vortex.config.settings import Settings, setup_logging
from vortex.core.data.datastore import DataStore
from vortex.core.factorhub import FactorHub
from vortex.core.signalbus import SignalBus
from vortex.strategy.dividend import (
    DEFAULT_WEIGHTS,
    DividendQualityFCFStrategy,
)
from vortex.utils.date_utils import get_recent_trade_dates, today_str


def parse_args():
    parser = argparse.ArgumentParser(description="红利质量现金流复合策略 — 月度选股")
    parser.add_argument(
        "--date", type=str, default=None,
        help="选股基准日期 YYYYMMDD (默认: 最近一个有数据的交易日)",
    )
    parser.add_argument(
        "--top", type=int, default=None,
        help="选股数量 (默认: 配置文件的 top_n=30)",
    )
    parser.add_argument(
        "--weights-file", type=str, default=None,
        help="加载研究阶段产出的权重配置 JSON (默认: data/reports/strategy_weights_config.json)",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="显示详细日志",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    log_level = "DEBUG" if args.verbose else "INFO"
    setup_logging(log_level)

    # ---- 配置 ----
    cfg = Settings()
    cfg.validate()

    if args.top:
        cfg.top_n = args.top

    # ---- 初始化各层 ----
    ds = DataStore(cfg)
    fh = FactorHub(ds)
    fh.register_all_defaults()

    bus = SignalBus(cfg.data_dir)

    # ---- 确定选股日期 ----
    if args.date:
        run_date = args.date
    else:
        # 使用最近一个有数据的交易日
        recent = get_recent_trade_dates(today_str(), cfg.data_dir, n=1)
        if recent:
            run_date = recent[-1]
        else:
            # 降级: 直接用今天
            run_date = today_str()

    # ---- 加载权重 (研究阶段已确定) ----
    print("\n" + "=" * 60)
    print("  QuantPilot — 红利质量现金流复合策略")
    print(f"  选股日期: {run_date}")
    print(f"  目标数量: Top {cfg.top_n}")

    default_cfg = cfg.data_dir / "reports" / "strategy_weights_config.json"
    fallback_weights = cfg.data_dir / "reports" / "weights_optimal.json"

    weights_file = Path(args.weights_file) if args.weights_file else default_cfg
    if weights_file.exists():
        import json
        saved = json.loads(weights_file.read_text(encoding="utf-8"))
        # 兼容两种格式:
        # 1) strategy_weights_config.json (推荐)
        # 2) weights_optimal.json
        weights = saved.get("weights", DEFAULT_WEIGHTS)
        weight_label = saved.get("method", "configured")
        print(f"  加载权重: {weights_file.name} ({weight_label})")
    elif fallback_weights.exists():
        import json
        saved = json.loads(fallback_weights.read_text(encoding="utf-8"))
        weights = saved.get("weights", DEFAULT_WEIGHTS)
        weight_label = saved.get("method", "optimal")
        print(f"  加载权重: {fallback_weights.name} ({weight_label})")
    else:
        weights = DEFAULT_WEIGHTS.copy()
        weight_label = "default"
        print("  权重: 默认固定权重")

    print("=" * 60)

    # ---- 执行策略 ----
    strategy = DividendQualityFCFStrategy(ds, fh, bus, weights=weights)
    result = strategy.run(run_date)

    # ---- 信号持久化 ----
    bus.flush(run_date)

    # ---- 打印结果 ----
    print("\n" + result.summary())

    # ---- 打印统计信息 ----
    if result.signals:
        print("\n📊 持仓统计:")

        # 行业分布
        industries = {}
        for sig in result.signals:
            ind = sig.metadata.get("industry", "未知")
            industries[ind] = industries.get(ind, 0) + sig.weight

        print("  行业分布:")
        for ind, w in sorted(industries.items(), key=lambda x: -x[1]):
            print(f"    {ind}: {w:.1%}")

        # 总权重
        total_w = sum(s.weight for s in result.signals)
        print(f"\n  总权重: {total_w:.1%}")
        print(f"  平均得分: {sum(s.score for s in result.signals) / len(result.signals):.4f}")

        # 股息率分布
        dy_values = []
        for sig in result.signals:
            if "股息率=" in sig.reason:
                for part in sig.reason.split(" | "):
                    if part.startswith("股息率="):
                        val_str = part.replace("股息率=", "").replace("%", "")
                        try:
                            dy_values.append(float(val_str))
                        except ValueError:
                            pass
        if dy_values:
            print(f"  股息率范围: {min(dy_values):.1%} ~ {max(dy_values):.1%}")
            print(f"  股息率中位: {sorted(dy_values)[len(dy_values)//2]:.1%}")

    print("\n" + "=" * 60)
    print("  完成! 信号已保存到 data/signal/ 目录")
    print("=" * 60)


if __name__ == "__main__":
    main()
