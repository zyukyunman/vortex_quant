#!/usr/bin/env python3
"""
run_factor_test.py
因子有效性测试 + 权重优化 + 多组权重回测对比

流程:
  1. IC 分析 (最近 36 个月，每月末计算因期 Spearman IC)
  2. 计算 IC 加权 & ICIR 加权最优权重
  3. 三组权重 (默认/IC最优/ICIR最优) SA回测对比
  4. 选出最优权重 → 跑完整多段回测
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from vortex.config.settings import Settings, StrategyConfig, setup_logging

setup_logging("INFO")
logger = logging.getLogger("factor_test")

# ================================================================
SCORING_FACTORS = ["dividend_yield", "fcf_yield", "roe_ttm", "delta_roe", "opcfd", "ep"]

FACTOR_CN = {
    "dividend_yield": "股息率",
    "fcf_yield": "FCF收益率",
    "roe_ttm": "ROE",
    "delta_roe": "ΔROE",
    "opcfd": "现金流占比",
    "ep": "E/P(盈利收益率)",
}

DEFAULT_WEIGHTS = {
    "dividend_yield": 0.30,
    "fcf_yield": 0.25,
    "ep": 0.20,
    "roe_ttm": 0.10,
    "delta_roe": 0.08,
    "opcfd": 0.07,
}

BENCHMARK_CODES = ["000300.SH", "000905.SH", "000922.CSI"]


# ================================================================
#  IC 分析
# ================================================================

def get_monthly_end_dates(cal_list: list, start: str, end: str, n_months: int = 36) -> list:
    """获取区间内每月末交易日 (最近 n_months 个)"""
    by_month = {}
    for d in cal_list:
        if start <= d <= end:
            by_month[d[:6]] = d  # 每月最后一天
    months = sorted(by_month.keys())[-n_months:]
    return [by_month[m] for m in months]


def run_ic_analysis(ds, fh, cal_list: list, lookback_dates: list, forward_days: int = 20):
    """
    计算各因子的 IC (Spearman rank correlation with forward returns)

    Returns: {factor_name: {ic_mean, ic_std, ic_ir, n_periods, positive_pct, ics: list}}
    """
    ic_results = {}

    for factor_name in SCORING_FACTORS:
        ics = []
        for d in lookback_dates:
            try:
                factor_val = fh.compute(factor_name, d)
                if factor_val.empty or len(factor_val) < 30:
                    continue

                # 找 forward_days 后的交易日
                idx = None
                for i, c in enumerate(cal_list):
                    if c >= d:
                        idx = i
                        break
                if idx is None or idx + forward_days >= len(cal_list):
                    continue
                future_date = cal_list[idx + forward_days]

                # 获取价格
                df_d = ds.get_daily(trade_date=d)
                df_f = ds.get_daily(trade_date=future_date)
                if df_d.empty or df_f.empty:
                    continue
                price_d = df_d.set_index("ts_code")["close"]
                price_f = df_f.set_index("ts_code")["close"]
                common = factor_val.index.intersection(price_d.index).intersection(price_f.index)
                if len(common) < 30:
                    continue

                ret = (price_f.reindex(common) / price_d.reindex(common)) - 1
                ic = factor_val.reindex(common).corr(ret, method="spearman")
                if not np.isnan(ic):
                    ics.append({"date": d, "ic": ic})
            except Exception as e:
                continue

        if ics:
            ic_arr = np.array([x["ic"] for x in ics])
            ic_mean = ic_arr.mean()
            ic_std = ic_arr.std()
            ic_ir = ic_mean / ic_std if ic_std > 0 else 0
            ic_results[factor_name] = {
                "ic_mean": float(ic_mean),
                "ic_std": float(ic_std),
                "ic_ir": float(ic_ir),
                "n_periods": len(ics),
                "positive_pct": float((ic_arr > 0).mean()),
                "ics": ics,
            }
        else:
            ic_results[factor_name] = {
                "ic_mean": 0, "ic_std": 0, "ic_ir": 0,
                "n_periods": 0, "positive_pct": 0, "ics": [],
            }

    return ic_results


# ================================================================
#  权重优化
# ================================================================

def compute_optimal_weights(ic_results: dict):
    """
    根据 IC 分析结果计算 3 组权重:
    1. IC 加权 (正IC因子按绝对值加权)
    2. ICIR 加权 (正ICIR因子按绝对值加权)
    3. 默认权重
    """
    all_weights = {"默认权重": DEFAULT_WEIGHTS.copy()}

    # IC 加权
    ic_w = {}
    for name, info in ic_results.items():
        if info["ic_mean"] > 0 and info["positive_pct"] > 0.45 and info["n_periods"] >= 6:
            ic_w[name] = abs(info["ic_mean"])
    if len(ic_w) >= 3:
        total = sum(ic_w.values())
        w = {k: v / total for k, v in ic_w.items()}
        for f in SCORING_FACTORS:
            w.setdefault(f, 0.0)
        all_weights["IC加权"] = w

    # ICIR 加权
    icir_w = {}
    for name, info in ic_results.items():
        if info["ic_ir"] > 0.1 and info["positive_pct"] > 0.45 and info["n_periods"] >= 6:
            icir_w[name] = abs(info["ic_ir"])
    if len(icir_w) >= 3:
        total = sum(icir_w.values())
        w = {k: v / total for k, v in icir_w.items()}
        for f in SCORING_FACTORS:
            w.setdefault(f, 0.0)
        all_weights["ICIR加权"] = w

    # 等权
    all_weights["等权"] = {f: 1.0 / len(SCORING_FACTORS) for f in SCORING_FACTORS}

    return all_weights


# ================================================================
#  回测对比
# ================================================================

def run_backtest_compare(ds, fh, weight_sets: dict, start: str, end: str, freq: str = "SA"):
    """对多组权重做回测比较"""
    from vortex.core.signalbus import SignalBus
    from vortex.core.weight_optimizer import FixedWeightOptimizer
    from vortex.executor.backtest import BacktestEngine
    from vortex.strategy.dividend import DividendQualityFCFStrategy

    results = {}
    for label, weights in weight_sets.items():
        logger.info("回测 [%s] freq=%s  %s ~ %s", label, freq, start, end)
        active = {k: v for k, v in weights.items() if v > 0.001}
        logger.info("  权重: %s", {k: f"{v:.1%}" for k, v in sorted(active.items(), key=lambda x: -x[1])})

        bus = SignalBus(ds.data_dir)
        optimizer = FixedWeightOptimizer(weights)
        scfg = StrategyConfig(top_n=30)
        strategy = DividendQualityFCFStrategy(ds, fh, bus, weight_optimizer=optimizer, strategy_config=scfg)
        engine = BacktestEngine(ds)

        try:
            result = engine.run(strategy, start, end, freq=freq, benchmark_codes=BENCHMARK_CODES)
            m = result.metrics
            results[label] = result
            logger.info("  → 总收益: %.2f%%, 年化: %.2f%%, 夏普: %.3f, 回撤: %.2f%%, Calmar: %.3f",
                        m.get("total_return", 0) * 100,
                        m.get("annual_return", 0) * 100,
                        m.get("sharpe_ratio", 0),
                        m.get("max_drawdown", 0) * 100,
                        m.get("calmar_ratio", 0))
        except Exception as e:
            logger.error("  → 回测失败: %s", e)
            import traceback
            traceback.print_exc()

    return results


# ================================================================
#  报告输出
# ================================================================

def print_ic_report(ic_results: dict):
    """打印 IC 分析报告"""
    print("\n" + "=" * 80)
    print("  因子 IC 分析报告 (Spearman Rank IC, 20日远期收益)")
    print("=" * 80)
    print(f"{'因子':<20} {'IC均值':>10} {'IC标准差':>10} {'ICIR':>10} {'正IC占比':>10} {'样本数':>8}")
    print("-" * 80)

    sorted_factors = sorted(ic_results.items(), key=lambda x: -x[1]["ic_ir"])
    for name, info in sorted_factors:
        cn = FACTOR_CN.get(name, name)
        ic_mean = info["ic_mean"]
        ic_std = info["ic_std"]
        ic_ir = info["ic_ir"]
        pos = info["positive_pct"]
        n = info["n_periods"]

        # 标记有效性
        if ic_ir > 0.5 and pos > 0.55:
            mark = "⭐⭐⭐"
        elif ic_ir > 0.3 and pos > 0.5:
            mark = "⭐⭐"
        elif ic_ir > 0.1 and pos > 0.45:
            mark = "⭐"
        elif ic_mean <= 0:
            mark = "❌"
        else:
            mark = "⚠️"

        print(f"{cn:<18} {ic_mean:>10.4f} {ic_std:>10.4f} {ic_ir:>10.3f} {pos:>9.0%} {n:>8d}  {mark}")

    print("-" * 80)
    print("评级: ⭐⭐⭐=强因子(ICIR>0.5) ⭐⭐=中等(ICIR>0.3) ⭐=弱(ICIR>0.1) ⚠️=待观察 ❌=无效")
    print()


def print_weight_comparison(weight_sets: dict):
    """打印权重对比表"""
    print("\n" + "=" * 80)
    print("  因子权重方案对比")
    print("=" * 80)
    labels = list(weight_sets.keys())
    header = f"{'因子':<20}" + "".join(f"{l:>14}" for l in labels)
    print(header)
    print("-" * 80)
    for f in SCORING_FACTORS:
        cn = FACTOR_CN.get(f, f)
        row = f"{cn:<18}"
        for l in labels:
            w = weight_sets[l].get(f, 0)
            row += f"{w:>13.1%}" if w > 0.001 else f"{'—':>14}"
        print(row)
    print()


def print_backtest_comparison(bt_results: dict):
    """打印回测对比表"""
    print("\n" + "=" * 80)
    print("  回测绩效对比")
    print("=" * 80)
    print(f"{'方案':<16} {'总收益':>10} {'年化':>10} {'夏普':>8} {'最大回撤':>10} {'Calmar':>8} {'Sortino':>8} {'换手率':>8}")
    print("-" * 100)

    best_sharpe = max(r.metrics.get("sharpe_ratio", -999) for r in bt_results.values())
    for label, result in bt_results.items():
        m = result.metrics
        is_best = m.get("sharpe_ratio", 0) == best_sharpe
        marker = " ← 最优" if is_best else ""
        print(f"{label:<14} {m.get('total_return',0):>9.2%} {m.get('annual_return',0):>9.2%} "
              f"{m.get('sharpe_ratio',0):>7.3f} {m.get('max_drawdown',0):>9.2%} "
              f"{m.get('calmar_ratio',0):>7.3f} {m.get('sortino_ratio',0):>7.3f} "
              f"{m.get('avg_turnover',0):>7.2%}{marker}")

    # 基准
    first_result = list(bt_results.values())[0]
    bench_m = first_result.calc_benchmark_metrics()
    for bname, bm in bench_m.items():
        print(f"  {bname:<12} {bm['total_return']:>9.2%} {bm['annual_return']:>9.2%} "
              f"{bm['sharpe_ratio']:>7.3f} {bm['max_drawdown']:>9.2%} "
              f"{bm['calmar_ratio']:>7.3f} {bm['sortino_ratio']:>7.3f} {'—':>8}")
    print()


# ================================================================
#  主流程
# ================================================================

def main():
    parser = argparse.ArgumentParser(description="QuantPilot 因子测试 + 权重优化 + 回测对比")
    parser.add_argument("--ic-start", default="20190101", help="IC分析起始日 (默认: 20190101)")
    parser.add_argument("--ic-end", default="20260327", help="IC分析截止日")
    parser.add_argument("--ic-months", type=int, default=60, help="IC回看月数 (默认: 60)")
    parser.add_argument("--forward-days", type=int, default=20, help="远期收益天数 (默认: 20)")
    parser.add_argument("--bt-start", default="20170103", help="回测起始日 (默认: 20170103)")
    parser.add_argument("--bt-end", default="20260327", help="回测截止日")
    parser.add_argument("--freq", default="SA", choices=["M", "Q", "SA"], help="调仓频率")
    parser.add_argument("--top-n", type=int, default=30, help="选股数量")
    parser.add_argument("--skip-ic", action="store_true", help="跳过IC分析，直接用默认权重回测")
    args = parser.parse_args()

    t0 = time.time()

    print("=" * 80)
    print("  QuantPilot 因子测试 + 权重优化 + 回测对比")
    print("=" * 80)
    print(f"  IC 分析区间: {args.ic_start} ~ {args.ic_end} (回看 {args.ic_months} 个月)")
    print(f"  回测区间:    {args.bt_start} ~ {args.bt_end} (调仓: {args.freq})")
    print(f"  选股数量:    Top {args.top_n}")
    print("=" * 80)

    cfg = Settings()
    cfg.validate()

    from vortex.core.data.datastore import DataStore
    from vortex.core.factorhub import FactorHub
    from vortex.utils.date_utils import load_trade_cal

    ds = DataStore(cfg)
    fh = FactorHub(ds)
    fh.register_all_defaults()

    cal = sorted([d.strftime("%Y%m%d") for d in load_trade_cal(ds.data_dir)])

    # ============ Step 1: IC 分析 ============
    if not args.skip_ic:
        print(f"\n{'='*60}")
        print("  Step 1: IC 分析")
        print(f"{'='*60}")

        lookback_dates = get_monthly_end_dates(cal, args.ic_start, args.ic_end, n_months=args.ic_months)
        logger.info("IC 回看日期: %d 个月 (%s ~ %s)", len(lookback_dates),
                     lookback_dates[0] if lookback_dates else "N/A",
                     lookback_dates[-1] if lookback_dates else "N/A")

        ic_results = run_ic_analysis(ds, fh, cal, lookback_dates, forward_days=args.forward_days)
        print_ic_report(ic_results)
    else:
        ic_results = {}
        print("\n跳过 IC 分析，使用默认权重")

    # ============ Step 2: 计算最优权重 ============
    print(f"\n{'='*60}")
    print("  Step 2: 权重优化")
    print(f"{'='*60}")

    weight_sets = compute_optimal_weights(ic_results)
    print_weight_comparison(weight_sets)

    # ============ Step 3: 回测对比 ============
    print(f"\n{'='*60}")
    print(f"  Step 3: 回测对比 ({args.bt_start} ~ {args.bt_end}, {args.freq}调仓)")
    print(f"{'='*60}")

    bt_results = run_backtest_compare(ds, fh, weight_sets, args.bt_start, args.bt_end, freq=args.freq)
    if bt_results:
        print_backtest_comparison(bt_results)

    # ============ Step 4: 选最优 → 完整报告 ============
    best_label = None
    best_sharpe = -999
    for label, result in bt_results.items():
        s = result.metrics.get("sharpe_ratio", -999)
        if s > best_sharpe:
            best_sharpe = s
            best_label = label

    if best_label:
        print(f"\n★ 最优方案: {best_label} (夏普={best_sharpe:.3f})")
        final_weights = weight_sets[best_label]
    else:
        print("\n★ 无有效回测结果，使用默认权重")
        final_weights = DEFAULT_WEIGHTS
        best_label = "默认权重"

    # 保存最优权重
    report_dir = cfg.data_dir / "reports"
    report_dir.mkdir(exist_ok=True)
    weights_file = report_dir / "weights_optimal.json"
    weights_file.write_text(json.dumps({
        "method": best_label,
        "weights": {k: round(v, 4) for k, v in final_weights.items()},
        "ic_results": {k: {kk: vv for kk, vv in v.items() if kk != "ics"} for k, v in ic_results.items()},
        "backtest_comparison": {k: v.metrics for k, v in bt_results.items()},
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n最优权重已保存: {weights_file}")

    elapsed = time.time() - t0
    print(f"\n总耗时: {elapsed/60:.1f} 分钟")


if __name__ == "__main__":
    main()
