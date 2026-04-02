#!/usr/bin/env python3
"""
run_factor_test.py
因子评测 + 权重优化 + 回测对比 (基于 vortex.evaluation)

流程:
    1. 从策略对象获取 eval_specs → EvalPipeline 评测
    2. WeightTuner 多方案权重优化
    3. 多组权重回测对比
    4. 生成 HTML 报告 + 结构化输出文件夹
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import unicodedata
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from vortex.config.settings import Settings, StrategyConfig, setup_logging
from vortex.evaluation import (
    EvalPipeline,
    FactorRole,
    WeightTuner,
    generate_eval_html,
    generate_weight_html,
)
from vortex.evaluation.horizon_policy import (
    apply_scoring_horizon_policy,
    collect_scoring_horizons,
    collect_scoring_ls_horizons,
    infer_factor_family,
    recommend_weight_horizon,
)
from vortex.strategy.dividend import SCORING_FACTORS, DEFAULT_WEIGHTS

setup_logging("INFO")
logger = logging.getLogger("factor_test")

FACTOR_CN = {
    "dividend_yield": "股息率",
    "fcf_yield": "FCF收益率",
    "roe_ttm": "ROE",
    "delta_roe": "ΔROE",
    "opcfd": "现金流占比",
    "ep": "E/P(盈利收益率)",
}

BENCHMARK_CODES = ["000300.SH", "000905.SH", "000922.CSI"]


# ================================================================
#  工具函数
# ================================================================

def get_monthly_end_dates(cal_list: list, start: str, end: str, n_months: int = 36) -> list:
    """获取区间内每月末交易日 (最近 n_months 个)"""
    by_month = {}
    for d in cal_list:
        if start <= d <= end:
            by_month[d[:6]] = d
    months = sorted(by_month.keys())[-n_months:]
    return [by_month[m] for m in months]


def parse_forward_days_list(text: str) -> list[int]:
    """解析 '1,5,20' 形式的多周期参数。"""
    days = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        day = int(part)
        if day <= 0:
            raise ValueError("forward days 必须为正整数")
        days.append(day)
    return sorted(set(days))


def _display_width(text: str) -> int:
    """按终端显示宽度计算字符串长度（中日韩宽字符按2列）。"""
    width = 0
    for ch in text:
        width += 2 if unicodedata.east_asian_width(ch) in ("F", "W") else 1
    return width


def _truncate_display(text: str, max_width: int) -> str:
    """按显示宽度截断字符串，避免中英混排错列。"""
    if _display_width(text) <= max_width:
        return text

    out = []
    width = 0
    for ch in text:
        ch_width = 2 if unicodedata.east_asian_width(ch) in ("F", "W") else 1
        if width + ch_width > max_width - 3:
            break
        out.append(ch)
        width += ch_width
    return "".join(out) + "..."


def _pad_display(text: str, width: int) -> str:
    """按显示宽度补空格。"""
    return text + " " * max(0, width - _display_width(text))


def print_scoring_horizon_plan(specs: list) -> None:
    """打印打分因子的 horizon 规划，便于核对自动规则是否符合预期。"""
    scoring_specs = [spec for spec in specs if spec.role == FactorRole.SCORING]
    if not scoring_specs:
        return

    print("  打分因子 horizon 规划:")
    for spec in scoring_specs:
        cn = FACTOR_CN.get(spec.factor_name, spec.factor_name)
        family = spec.factor_family or infer_factor_family(spec.factor_name)
        print(f"    {cn:<16} family={family:<9} IC={list(spec.horizons)}  LS={spec.ls_horizon}d")


# ================================================================
#  终端报告 (保留简洁版)
# ================================================================

def print_admission_table(results, specs):
    """打印准入判断表"""
    spec_map = {s.factor_name: s for s in specs}

    columns = [
        ("因子", 22),
        ("角色", 6),
        ("准入", 8),
        ("描述", 28),
        ("原因", 52),
    ]
    sep = " | "
    table_width = sum(w for _, w in columns) + len(sep) * (len(columns) - 1)

    print("\n" + "=" * table_width)
    print("  因子准入判断")
    print("=" * table_width)
    header = sep.join(_pad_display(name, width) for name, width in columns)
    print(header)
    print("-" * table_width)

    for r in results:
        cn = FACTOR_CN.get(r.factor_name, r.factor_name)
        status = "✓ 通过" if r.passed else "✗ 未通过"
        role_cn = {"scoring": "打分", "filter": "过滤", "risk": "风险"}.get(r.role.value, r.role.value)
        spec = spec_map.get(r.factor_name)
        desc = spec.description if spec else "—"

        row_values = [
            _truncate_display(cn, columns[0][1]),
            _truncate_display(role_cn, columns[1][1]),
            _truncate_display(status, columns[2][1]),
            _truncate_display(desc, columns[3][1]),
            _truncate_display(r.reason, columns[4][1]),
        ]
        line = sep.join(
            _pad_display(val, columns[idx][1])
            for idx, val in enumerate(row_values)
        )
        print(line)

    print()


def print_weight_comparison(comparison: pd.DataFrame):
    """打印权重对比表"""
    if comparison.empty:
        return
    print("\n" + "=" * 80)
    print("  因子权重方案对比")
    print("=" * 80)
    cols = [c for c in comparison.columns if c != "factor"]
    header = f"{'因子':<18}" + "".join(f"{c:>14}" for c in cols)
    print(header)
    print("-" * 80)
    for _, row in comparison.iterrows():
        cn = FACTOR_CN.get(row["factor"], row["factor"])
        line = f"{cn:<16}"
        for c in cols:
            w = row[c]
            line += f"{w:>13.1%}" if w > 0.001 else f"{'—':>14}"
        print(line)
    print()


def print_backtest_comparison(bt_results: dict):
    """打印回测对比表"""
    if not bt_results:
        return
    print("\n" + "=" * 100)
    print("  回测绩效对比")
    print("=" * 100)
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

    first_result = list(bt_results.values())[0]
    bench_m = first_result.calc_benchmark_metrics()
    for bname, bm in bench_m.items():
        print(f"  {bname:<12} {bm['total_return']:>9.2%} {bm['annual_return']:>9.2%} "
              f"{bm['sharpe_ratio']:>7.3f} {bm['max_drawdown']:>9.2%} "
              f"{bm['calmar_ratio']:>7.3f} {bm['sortino_ratio']:>7.3f} {'—':>8}")
    print()


# ================================================================
#  回测对比
# ================================================================

def run_backtest_compare(
    ds, fh, weight_sets: dict, start: str, end: str,
    freq: str = "SA", top_n: int = 30,
):
    """对多组权重做回测比较"""
    from vortex.core.signalbus import SignalBus
    from vortex.executor.backtest import BacktestEngine
    from vortex.strategy.dividend import DividendQualityFCFStrategy

    results = {}
    for label, weights in weight_sets.items():
        logger.info("回测 [%s] freq=%s  %s ~ %s", label, freq, start, end)
        active = {k: v for k, v in weights.items() if v > 0.001}
        logger.info("  权重: %s", {k: f"{v:.1%}" for k, v in sorted(active.items(), key=lambda x: -x[1])})

        bus = SignalBus(ds.data_dir)
        scfg = StrategyConfig(top_n=top_n)
        strategy = DividendQualityFCFStrategy(ds, fh, bus, weights=weights, strategy_config=scfg)
        engine = BacktestEngine(ds)

        try:
            result = engine.run(strategy, start, end, freq=freq, benchmark_codes=BENCHMARK_CODES)
            m = result.metrics
            results[label] = result
            logger.info("  → 总收益: %.2f%%, 年化: %.2f%%, 夏普: %.3f, 回撤: %.2f%%",
                        m.get("total_return", 0) * 100,
                        m.get("annual_return", 0) * 100,
                        m.get("sharpe_ratio", 0),
                        m.get("max_drawdown", 0) * 100)
        except Exception as e:
            logger.error("  → 回测失败: %s", e)
            import traceback
            traceback.print_exc()

    return results


# ================================================================
#  主流程
# ================================================================

def main():
    parser = argparse.ArgumentParser(description="QuantPilot 因子评测 + 权重优化 + 回测对比")
    parser.add_argument("--ic-start", default="20190101", help="IC分析起始日 (默认: 20190101)")
    parser.add_argument("--ic-end", default="20260327", help="IC分析截止日")
    parser.add_argument("--ic-months", type=int, default=60, help="IC回看月数 (默认: 60)")
    parser.add_argument("--forward-days-list", default="", help="手动指定多周期前瞻收益天数，如 1,5,20；留空则按调仓频率和因子性质自动推荐")
    parser.add_argument("--ls-horizon", type=int, default=0, help="手动指定多空组合使用的远期收益天数；0表示自动")
    parser.add_argument("--weight-horizon", type=int, default=0, help="手动指定权重优化使用的 horizon；0表示按调仓频率自动推荐")
    parser.add_argument("--bt-start", default="20170103", help="回测起始日 (默认: 20170103)")
    parser.add_argument("--bt-end", default="20260327", help="回测截止日")
    parser.add_argument("--freq", default="SA", choices=["M", "Q", "SA"], help="调仓频率")
    parser.add_argument("--top-n", type=int, default=30, help="选股数量")
    parser.add_argument("--skip-ic", action="store_true", help="跳过IC分析，直接用默认权重回测")
    parser.add_argument("--skip-bt", action="store_true", help="跳过回测，只做因子评测和权重优化")
    parser.add_argument("--output-dir", default="", help="输出目录 (默认: data/reports/eval_{timestamp})")
    args = parser.parse_args()

    t0 = time.time()
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    cfg = Settings()
    cfg.validate()

    from vortex.core.data.datastore import DataStore
    from vortex.core.factorhub import FactorHub
    from vortex.core.signalbus import SignalBus
    from vortex.analysis.analyzer import FactorAnalyzer
    from vortex.utils.date_utils import load_trade_cal

    ds = DataStore(cfg)
    fh = FactorHub(ds)
    fh.register_all_defaults()
    analyzer = FactorAnalyzer(ds, fh)

    bus = SignalBus(ds.data_dir)
    from vortex.strategy.dividend import DividendQualityFCFStrategy

    strategy = DividendQualityFCFStrategy(ds, fh, bus)
    base_specs = strategy.eval_specs()
    manual_forward_days_list = parse_forward_days_list(args.forward_days_list) if args.forward_days_list.strip() else None
    manual_ls_horizon = args.ls_horizon if args.ls_horizon > 0 else None
    resolved_weight_horizon = args.weight_horizon if args.weight_horizon > 0 else recommend_weight_horizon(args.freq)
    specs = apply_scoring_horizon_policy(
        base_specs,
        freq=args.freq,
        forward_days_list=manual_forward_days_list,
        ls_horizon=manual_ls_horizon,
    )
    resolved_forward_days_list = collect_scoring_horizons(specs)
    resolved_ls_horizons = collect_scoring_ls_horizons(specs)
    comparison_horizons = sorted(set(resolved_forward_days_list + [resolved_weight_horizon]))

    print("=" * 80)
    print("  QuantPilot 因子评测 + 权重优化 + 回测对比")
    print("=" * 80)
    print(f"  IC 分析区间: {args.ic_start} ~ {args.ic_end} (回看 {args.ic_months} 个月)")
    print(f"  IC 周期:      {resolved_forward_days_list}")
    print(f"  多空周期:     {resolved_ls_horizons}")
    print(f"  配权周期:     {resolved_weight_horizon}d")
    print(f"  回测区间:    {args.bt_start} ~ {args.bt_end} (调仓: {args.freq})")
    print(f"  选股数量:    Top {args.top_n}")
    print("=" * 80)
    print_scoring_horizon_plan(specs)
    print("=" * 80)

    cal = sorted([d.strftime("%Y%m%d") for d in load_trade_cal(ds.data_dir)])

    # 输出目录
    if args.output_dir:
        out_dir = Path(args.output_dir)
    else:
        out_dir = cfg.data_dir / "reports" / f"eval_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    run_config = {
        "timestamp": timestamp,
        "ic_start": args.ic_start,
        "ic_end": args.ic_end,
        "ic_months": args.ic_months,
        "forward_days_list": resolved_forward_days_list,
        "ls_horizons": resolved_ls_horizons,
        "weight_horizon": resolved_weight_horizon,
        "comparison_horizons": comparison_horizons,
        "bt_start": args.bt_start,
        "bt_end": args.bt_end,
        "freq": args.freq,
        "top_n": args.top_n,
        "scoring_specs": {
            spec.factor_name: {
                "factor_family": spec.factor_family or infer_factor_family(spec.factor_name),
                "horizons": list(spec.horizons),
                "ls_horizon": spec.ls_horizon,
            }
            for spec in specs
            if spec.role == FactorRole.SCORING
        },
    }

    # ============ Step 1: 因子评测 ============
    eval_results = []
    if not args.skip_ic:
        print(f"\n{'='*60}")
        print("  Step 1: 因子评测 (EvalPipeline)")
        print(f"{'='*60}")

        lookback_dates = get_monthly_end_dates(
            cal, args.ic_start, args.ic_end, n_months=args.ic_months,
        )
        logger.info("评测截面: %d 个月 (%s ~ %s)", len(lookback_dates),
                     lookback_dates[0] if lookback_dates else "N/A",
                     lookback_dates[-1] if lookback_dates else "N/A")

        # 运行评测管线
        pipeline = EvalPipeline(analyzer)
        pipeline.add_many(specs)
        eval_results = pipeline.run(lookback_dates)

        # 终端输出
        print_admission_table(eval_results, specs)

        # 保存结构化结果
        pipeline.save_report(eval_results, out_dir / "evaluation", run_config=run_config)

        # 生成因子评测 HTML
        generate_eval_html(
            eval_results, specs,
            output_path=out_dir / "factor_evaluation.html",
            title=f"因子评测报告 ({args.ic_start}~{args.ic_end})",
        )
        print(f"因子评测 HTML: {out_dir / 'factor_evaluation.html'}")
    else:
        print("\n跳过因子评测，使用默认权重")

    # ============ Step 2: 权重优化 ============
    print(f"\n{'='*60}")
    print("  Step 2: 权重优化 (WeightTuner)")
    print(f"{'='*60}")

    lookback_dates_wt = get_monthly_end_dates(
        cal, args.ic_start, args.ic_end, n_months=args.ic_months,
    ) if not args.skip_ic else []

    # 收集通过准入的打分因子
    admitted_scoring = [
        r.factor_name for r in eval_results
        if r.role == FactorRole.SCORING and r.passed
    ] if eval_results else SCORING_FACTORS

    tuner = WeightTuner(analyzer)

    if lookback_dates_wt and admitted_scoring:
        comparison = tuner.compare(
            admitted_scoring, lookback_dates_wt,
            horizons=comparison_horizons,
            methods=["ic", "icir", "equal"],
        )
        print_weight_comparison(comparison)

        # 最优权重 (ICIR)
        final_weights = tuner.optimize(
            admitted_scoring, lookback_dates_wt,
            horizon=resolved_weight_horizon, method="icir",
        )
    else:
        comparison = pd.DataFrame()
        final_weights = DEFAULT_WEIGHTS.copy()

    # 保存权重报告
    tuner.save_report(
        final_weights, out_dir / "weights",
        method="icir", horizon=resolved_weight_horizon,
        comparison=comparison, run_config=run_config,
    )

    # 生成权重 HTML
    generate_weight_html(
        final_weights,
        comparison=comparison,
        output_path=out_dir / "weight_optimization.html",
        title=f"权重优化报告 ({resolved_weight_horizon}d)",
        method="icir",
        horizon=resolved_weight_horizon,
    )
    print(f"权重优化 HTML: {out_dir / 'weight_optimization.html'}")

    # ============ Step 3: 回测对比 ============
    bt_results = {}
    if not args.skip_bt:
        print(f"\n{'='*60}")
        print(f"  Step 3: 回测对比 ({args.bt_start} ~ {args.bt_end}, {args.freq}调仓)")
        print(f"{'='*60}")

        # 构建回测权重方案
        weight_sets = {"默认权重": DEFAULT_WEIGHTS.copy()}
        if final_weights != DEFAULT_WEIGHTS:
            weight_sets["ICIR最优"] = final_weights
        if comparison is not None and not comparison.empty:
            for col in comparison.columns:
                if col == "factor":
                    continue
                w = dict(zip(comparison["factor"], comparison[col]))
                # 补齐不在 admitted 中的因子
                for f in SCORING_FACTORS:
                    w.setdefault(f, 0.0)
                weight_sets[col] = w

        bt_results = run_backtest_compare(
            ds, fh, weight_sets, args.bt_start, args.bt_end,
            freq=args.freq, top_n=args.top_n,
        )
        if bt_results:
            print_backtest_comparison(bt_results)

    # ============ Step 4: 保存最终结果 ============
    best_label = None
    best_sharpe = -999
    for label, result in bt_results.items():
        s = result.metrics.get("sharpe_ratio", -999)
        if s > best_sharpe:
            best_sharpe = s
            best_label = label

    if best_label:
        print(f"\n★ 最优方案: {best_label} (夏普={best_sharpe:.3f})")
    else:
        best_label = "ICIR最优" if final_weights != DEFAULT_WEIGHTS else "默认权重"

    # 保存最优权重到标准位置
    report_dir = cfg.data_dir / "reports"
    report_dir.mkdir(exist_ok=True)
    weights_file = report_dir / "weights_optimal.json"
    weights_file.write_text(json.dumps({
        "method": best_label,
        "run_config": run_config,
        "weights": {k: round(v, 4) for k, v in final_weights.items()},
        "backtest_comparison": {k: v.metrics for k, v in bt_results.items()} if bt_results else {},
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"最优权重: {weights_file}")

    runtime_cfg_file = report_dir / "strategy_weights_config.json"
    runtime_cfg_file.write_text(json.dumps({
        "description": "运行阶段固定读取的权重配置（研究阶段产出）",
        "updated_at": timestamp,
        "method": best_label,
        "weights_file": "weights_optimal.json",
        "weights": {k: round(v, 4) for k, v in final_weights.items()},
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"运行阶段权重配置: {runtime_cfg_file}")

    elapsed = time.time() - t0
    print(f"\n输出目录: {out_dir}")
    print(f"总耗时: {elapsed/60:.1f} 分钟")


if __name__ == "__main__":
    main()
