#!/usr/bin/env python3
"""
auto_pipeline.py
全自动运行管线: 数据同步 → 因子计算 → IC分析 → 权重调优 → 回测 → 选股 → 推送

使用方式:
  python scripts/auto_pipeline.py              # 使用现有数据运行
  python scripts/auto_pipeline.py --sync       # 先增量同步数据再运行
  python scripts/auto_pipeline.py --start-year 2005 --sync  # 指定起始年份
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import traceback
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from vortex.config.settings import Settings, setup_logging

setup_logging("INFO")
logger = logging.getLogger("auto_pipeline")

# ================================================================
#  辅助函数
# ================================================================

def check_data_ready(data_dir: Path) -> dict:
    """检查各类数据是否就绪"""
    status = {}
    checks = {
        "stock_basic": data_dir / "meta" / "stock_basic.parquet",
        "trade_cal": data_dir / "meta" / "trade_cal.parquet",
        "daily": data_dir / "market" / "daily",
        "valuation": data_dir / "fundamental" / "valuation",
        "fina_indicator": data_dir / "fundamental" / "fina_indicator.parquet",
        "income": data_dir / "fundamental" / "income.parquet",
        "cashflow": data_dir / "fundamental" / "cashflow.parquet",
        "balancesheet": data_dir / "fundamental" / "balancesheet.parquet",
        "dividend": data_dir / "fundamental" / "dividend.parquet",
    }
    for name, path in checks.items():
        if path.is_dir():
            parquets = list(path.glob("*.parquet"))
            status[name] = len(parquets) > 0
        else:
            status[name] = path.exists()
    return status


def wait_for_data(data_dir: Path, max_wait: int = 7200, interval: int = 60):
    """等待关键数据就绪，最多等 max_wait 秒"""
    # 核心数据: 没有这些就无法运行
    core = ["stock_basic", "trade_cal", "daily", "valuation"]
    # 增强数据: 有了更好，没有也能降级运行
    enhanced = ["fina_indicator", "income", "cashflow", "balancesheet", "dividend"]

    waited = 0
    while waited < max_wait:
        status = check_data_ready(data_dir)
        core_ready = all(status.get(k, False) for k in core)
        enhanced_ready = all(status.get(k, False) for k in enhanced)

        if not core_ready:
            logger.error("核心数据缺失: %s", [k for k in core if not status.get(k)])
            return False, False

        if enhanced_ready:
            logger.info("全部数据就绪!")
            return True, True

        missing = [k for k in enhanced if not status.get(k)]
        logger.info("等待增强数据 (%d/%d): 缺少 %s (已等 %ds)",
                     sum(status.get(k, False) for k in enhanced), len(enhanced),
                     missing, waited)

        # 等 5 分钟后开始降级运行
        if waited >= 300:
            logger.warning("增强数据等待超时, 使用降级模式运行 (仅核心数据)")
            return True, False

        time.sleep(interval)
        waited += interval

    logger.warning("数据等待超时 (%ds), 使用当前可用数据运行", max_wait)
    return True, False


def run_ic_analysis(ds, fh, date: str, lookback_dates: list):
    """运行 IC 分析，返回各因子的 IC 均值和 IR"""
    import numpy as np
    import pandas as pd

    scoring_factors = ["dividend_yield", "fcf_yield", "roe_ttm", "delta_roe", "opcfd", "ep"]

    ic_results = {}
    for factor_name in scoring_factors:
        ics = []
        for d in lookback_dates:
            try:
                factor_val = fh.compute(factor_name, d)
                if factor_val.empty or len(factor_val) < 30:
                    continue

                # 获取 20 天后收益率
                from vortex.utils.date_utils import load_trade_cal
                cal = sorted([dt.strftime("%Y%m%d") for dt in load_trade_cal(ds.data_dir)])
                idx = None
                for i, c in enumerate(cal):
                    if c >= d:
                        idx = i
                        break
                if idx is None or idx + 20 >= len(cal):
                    continue
                future_date = cal[idx + 20]

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
                    ics.append(ic)
            except Exception:
                continue

        if ics:
            ic_arr = np.array(ics)
            ic_mean = ic_arr.mean()
            ic_std = ic_arr.std()
            ic_ir = ic_mean / ic_std if ic_std > 0 else 0
            ic_results[factor_name] = {
                "ic_mean": ic_mean,
                "ic_std": ic_std,
                "ic_ir": ic_ir,
                "n_periods": len(ics),
                "positive_pct": (ic_arr > 0).mean(),
            }
            logger.info("  %s: IC_mean=%.4f, IC_IR=%.4f, positive=%.0f%%, n=%d",
                        factor_name, ic_mean, ic_ir, (ic_arr > 0).mean() * 100, len(ics))
        else:
            logger.warning("  %s: 无有效 IC 数据", factor_name)

    return ic_results


def compute_optimal_weights(ic_results: dict):
    """根据 IC 分析结果计算最优权重"""
    import numpy as np

    # 默认权重 (降级方案)
    default_weights = {
        "dividend_yield": 0.30,
        "fcf_yield": 0.25,
        "roe_ttm": 0.10,
        "delta_roe": 0.08,
        "opcfd": 0.07,
        "ep": 0.20,
    }

    if not ic_results:
        logger.warning("无 IC 数据，使用默认权重")
        return default_weights, "fixed_default"

    # 方案 A: IC 加权 — 按 IC 均值的绝对值加权
    ic_weights = {}
    for name, info in ic_results.items():
        # 只使用显著为正的因子 (positive_pct > 50% 且 IC_mean > 0)
        if info["ic_mean"] > 0 and info["positive_pct"] > 0.5:
            ic_weights[name] = abs(info["ic_mean"])

    # 方案 B: ICIR 加权 — 按 IC/IC_std 加权，更稳健
    icir_weights = {}
    for name, info in ic_results.items():
        if info["ic_ir"] > 0.2 and info["positive_pct"] > 0.5:
            icir_weights[name] = abs(info["ic_ir"])

    # 选择方案: 若 ICIR 有效因子 >= 3，用 ICIR；否则用 IC；否则用默认
    if len(icir_weights) >= 3:
        total = sum(icir_weights.values())
        weights = {k: v / total for k, v in icir_weights.items()}
        # 补充缺失因子权重为 0
        for f in default_weights:
            weights.setdefault(f, 0.0)
        method = "icir_optimal"
        logger.info("使用 ICIR 最优权重 (%d 个有效因子)", len(icir_weights))
    elif len(ic_weights) >= 3:
        total = sum(ic_weights.values())
        weights = {k: v / total for k, v in ic_weights.items()}
        for f in default_weights:
            weights.setdefault(f, 0.0)
        method = "ic_optimal"
        logger.info("使用 IC 最优权重 (%d 个有效因子)", len(ic_weights))
    else:
        weights = default_weights
        method = "fixed_default"
        logger.warning("有效因子不足, 使用默认权重")

    return weights, method


def run_backtest_comparison(ds, fh, weight_sets: dict, start: str, end: str, benchmark_codes: list = None):
    """对多组权重做回测比较"""
    from vortex.core.signalbus import SignalBus
    from vortex.executor.backtest import BacktestEngine
    from vortex.strategy.dividend import DividendQualityFCFStrategy

    engine = BacktestEngine(ds)
    results = {}

    for label, weights in weight_sets.items():
        logger.info("\n回测方案: %s", label)
        logger.info("权重: %s", {k: f"{v:.2%}" for k, v in weights.items() if v > 0})

        bus = SignalBus(ds.data_dir)
        strategy = DividendQualityFCFStrategy(ds, fh, bus, weights=weights)

        try:
            bt_result = engine.run(
                strategy, start, end, freq="M",
                benchmark_codes=benchmark_codes,
            )
            results[label] = bt_result
            logger.info("  → 年化: %.2f%%, 夏普: %.3f, 最大回撤: %.2f%%",
                        bt_result.metrics.get("annual_return", 0) * 100,
                        bt_result.metrics.get("sharpe_ratio", 0),
                        bt_result.metrics.get("max_drawdown", 0) * 100)
        except Exception as e:
            logger.error("  → 回测失败: %s", e)
            traceback.print_exc()

    return results


def select_best_result(bt_results: dict):
    """根据 Sharpe ratio 选出最佳方案"""
    best_label = None
    best_sharpe = -999
    for label, result in bt_results.items():
        sharpe = result.metrics.get("sharpe_ratio", -999)
        if sharpe > best_sharpe:
            best_sharpe = sharpe
            best_label = label
    return best_label


def format_final_report(
    ic_results: dict,
    optimal_weights: dict,
    weight_method: str,
    bt_results: dict,
    best_label: str,
    latest_selection,
    data_status: dict,
):
    """格式化最终推送报告 (Markdown)"""
    lines = ["# QuantPilot 自动调优报告\n"]

    # 数据状态
    lines.append("## 数据状态")
    for k, v in data_status.items():
        lines.append(f"- {k}: {'OK' if v else 'MISSING'}")
    lines.append("")

    # IC 分析
    if ic_results:
        lines.append("## IC 分析结果")
        lines.append("| 因子 | IC均值 | IC_IR | 正IC占比 | 样本数 |")
        lines.append("|------|--------|-------|----------|--------|")
        for name, info in sorted(ic_results.items(), key=lambda x: -x[1]["ic_ir"]):
            lines.append(
                f"| {name} | {info['ic_mean']:.4f} | {info['ic_ir']:.3f} | "
                f"{info['positive_pct']:.0%} | {info['n_periods']} |"
            )
        lines.append("")

    # 最优权重
    lines.append(f"## 最优权重方案 ({weight_method})")
    for name, w in sorted(optimal_weights.items(), key=lambda x: -x[1]):
        if w > 0.001:
            lines.append(f"- {name}: {w:.1%}")
    lines.append("")

    # 回测对比
    if bt_results:
        lines.append("## 回测对比")
        lines.append("| 方案 | 年化收益 | 夏普 | 最大回撤 | Calmar | 换手率 |")
        lines.append("|------|----------|------|----------|--------|--------|")
        for label, result in bt_results.items():
            m = result.metrics
            marker = " **" if label == best_label else ""
            lines.append(
                f"| {label}{marker} | {m.get('annual_return', 0):.2%} | "
                f"{m.get('sharpe_ratio', 0):.3f} | {m.get('max_drawdown', 0):.2%} | "
                f"{m.get('calmar_ratio', 0):.3f} | {m.get('avg_turnover', 0):.2%} |"
            )
        lines.append(f"\n**推荐方案: {best_label}**\n")

    # 最新选股
    if latest_selection and latest_selection.signals:
        lines.append(f"## 最新选股 ({latest_selection.date})")
        lines.append(f"策略: {latest_selection.strategy}, Top {latest_selection.top_n}")
        lines.append("| # | 代码 | 名称 | 权重 | 得分 | 行业 |")
        lines.append("|---|------|------|------|------|------|")
        for i, sig in enumerate(latest_selection.signals[:30], 1):
            name = sig.name or sig.metadata.get("name", "")
            industry = sig.metadata.get("industry", "")
            lines.append(
                f"| {i} | {sig.ts_code} | {name} | {sig.weight:.1%} | "
                f"{sig.score:.4f} | {industry} |"
            )
        lines.append("")

    return "\n".join(lines)


# ================================================================
#  主流程
# ================================================================

def main():
    parser = argparse.ArgumentParser(description="QuantPilot 全自动管线")
    parser.add_argument(
        "--sync", action="store_true",
        help="启动前增量同步数据 (只下载缺失部分)",
    )
    parser.add_argument(
        "--start-year", type=int, default=2005,
        help="数据起始年份 (配合 --sync 使用, 默认: 2005)",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("  QuantPilot 全自动管线启动")
    logger.info("=" * 60)

    cfg = Settings()
    cfg.validate()

    from vortex.core.data.datastore import DataStore
    from vortex.core.factorhub import FactorHub
    from vortex.core.signalbus import SignalBus
    from vortex.core.weight_optimizer import FixedWeightOptimizer
    from vortex.executor.backtest import BacktestEngine
    from vortex.notify.serverchan import send_serverchan
    from vortex.strategy.dividend import DEFAULT_WEIGHTS, DividendQualityFCFStrategy
    from vortex.utils.date_utils import get_recent_trade_dates, load_trade_cal, today_str

    # ---- Step 0: 数据同步 (可选) ----
    ds = DataStore(cfg)
    if args.sync:
        logger.info("\n[Step 0] 增量同步数据 (start_year=%d)...", args.start_year)
        sync_status = ds.ensure_data(start_year=args.start_year)
        logger.info("同步结果: %s", sync_status)

    # ---- Step 1: 检查数据就绪 ----
    logger.info("\n[Step 1] 检查数据就绪状态...")
    core_ready, enhanced_ready = wait_for_data(cfg.data_dir, max_wait=600, interval=30)
    if not core_ready:
        logger.error("核心数据未就绪，退出")
        send_serverchan(cfg.serverchan_key, "QuantPilot 失败", "核心数据未就绪，管线终止")
        return

    data_status = check_data_ready(cfg.data_dir)
    logger.info("数据状态: %s", data_status)

    # ---- 初始化 ----
    fh = FactorHub(ds)
    fh.register_all_defaults()

    # ---- 确定日期 ----
    recent = get_recent_trade_dates(today_str(), cfg.data_dir, n=1)
    run_date = recent[-1] if recent else today_str()
    logger.info("选股基准日: %s", run_date)

    # 获取回测和 IC 分析用的历史日期
    cal = sorted([d.strftime("%Y%m%d") for d in load_trade_cal(cfg.data_dir)])
    # 最近 12 个月末日期 (用于 IC)
    monthly_dates = []
    by_month = {}
    for d in cal:
        if d <= run_date:
            by_month[d[:6]] = d
    months = sorted(by_month.keys())[-13:-1]  # 过去 12 个月
    lookback_dates = [by_month[m] for m in months if m in by_month]

    # ---- Step 2: IC 分析 ----
    logger.info("\n[Step 2] IC 分析 (%d 个回看日期)...", len(lookback_dates))
    ic_results = {}
    if len(lookback_dates) >= 3 and enhanced_ready:
        ic_results = run_ic_analysis(ds, fh, run_date, lookback_dates)
    else:
        logger.warning("数据不足或增强数据未就绪，跳过 IC 分析")

    # ---- Step 3: 计算最优权重 ----
    logger.info("\n[Step 3] 计算最优权重...")
    optimal_weights, weight_method = compute_optimal_weights(ic_results)
    logger.info("最优权重方案: %s", weight_method)
    for k, v in sorted(optimal_weights.items(), key=lambda x: -x[1]):
        if v > 0.001:
            logger.info("  %s: %.1f%%", k, v * 100)

    # ---- Step 4: 回测对比 ----
    logger.info("\n[Step 4] 回测对比...")
    # 回测区间: 最近 1 年
    bt_end = run_date
    # 回测区间: 多段回测 (近1年 + 近3年 + 近5年)
    # 回测区间: 多段回测
    bt_end = run_date
    bt_start_candidates = [d for d in cal if d <= bt_end]

    # 构建多段回测区间 (固定起点 + 相对区间)
    bt_segments = {}

    # 固定起点段: 2017-至今
    fixed_starts = [("2017-至今", "20170101")]
    for seg_name, seg_start in fixed_starts:
        if bt_start_candidates and bt_start_candidates[0] <= seg_start:
            bt_segments[seg_name] = seg_start

    # 相对区间段
    segment_defs = [
        ("近1年", 252),
        ("近3年", 756),
        ("近5年", 1260),
    ]
    for seg_name, n_days in segment_defs:
        if len(bt_start_candidates) > n_days:
            bt_segments[seg_name] = bt_start_candidates[-n_days]
        elif len(bt_start_candidates) > 60:
            bt_segments[seg_name] = bt_start_candidates[0]

    # 默认用最长可用区间
    if bt_segments:
        main_segment = list(bt_segments.keys())[-1]  # 最长区间
        bt_start = bt_segments[main_segment]
    elif len(bt_start_candidates) > 60:
        bt_start = bt_start_candidates[0]
    else:
        bt_start = bt_start_candidates[0] if bt_start_candidates else bt_end

    # 基准指数列表
    BENCHMARK_CODES = ["000300.SH", "000905.SH", "000922.CSI"]

    weight_sets = {
        "默认权重": DEFAULT_WEIGHTS,
        "等权": {f: 1/6 for f in DEFAULT_WEIGHTS},
    }
    if weight_method != "fixed_default":
        weight_sets["最优权重"] = optimal_weights

    bt_results = {}
    if bt_start < bt_end:
        bt_results = run_backtest_comparison(
            ds, fh, weight_sets, bt_start, bt_end,
            benchmark_codes=BENCHMARK_CODES,
        )

    # 多段回测 (用最优/默认权重)
    segment_results = {}
    if len(bt_segments) > 1:
        final_w = weight_sets.get("最优权重", DEFAULT_WEIGHTS)
        for seg_name, seg_start in bt_segments.items():
            if seg_start >= bt_end:
                continue
            logger.info("多段回测 [%s]: %s ~ %s", seg_name, seg_start, bt_end)
            try:
                bus_seg = SignalBus(cfg.data_dir)
                strat_seg = DividendQualityFCFStrategy(ds, fh, bus_seg, weights=final_w)
                engine_seg = BacktestEngine(ds)
                seg_result = engine_seg.run(
                    strat_seg, seg_start, bt_end, freq="M",
                    benchmark_codes=BENCHMARK_CODES,
                )
                segment_results[seg_name] = seg_result
                m = seg_result.metrics
                logger.info("  %s → 年化: %.2f%%, 夏普: %.3f, 最大回撤: %.2f%%",
                            seg_name, m.get("annual_return", 0) * 100,
                            m.get("sharpe_ratio", 0), m.get("max_drawdown", 0) * 100)
            except Exception as e:
                logger.warning("  %s 回测失败: %s", seg_name, e)

    best_label = select_best_result(bt_results) if bt_results else "默认权重"

    # ---- Step 5: 使用最佳权重执行最新选股 ----
    logger.info("\n[Step 5] 使用最佳方案 '%s' 执行选股...", best_label)

    # 选定最终权重
    if best_label and best_label in weight_sets:
        final_weights = weight_sets[best_label]
    else:
        final_weights = DEFAULT_WEIGHTS

    bus = SignalBus(cfg.data_dir)
    strategy = DividendQualityFCFStrategy(ds, fh, bus, weights=final_weights)

    latest_selection = None
    try:
        latest_selection = strategy.run(run_date)
        bus.flush(run_date)
        logger.info("\n选股完成: %d 只", len(latest_selection.signals))
        print(latest_selection.summary())
    except Exception as e:
        logger.error("选股失败: %s", e)
        traceback.print_exc()

    # ---- Step 6: 推送最终报告 ----
    logger.info("\n[Step 6] 推送最终报告...")
    report = format_final_report(
        ic_results, final_weights, weight_method,
        bt_results, best_label, latest_selection, data_status,
    )

    # 构建标题
    if latest_selection and latest_selection.signals:
        n = len(latest_selection.signals)
        title = f"QuantPilot 选股完成 — {run_date} Top{n} ({best_label})"
    else:
        title = f"QuantPilot 报告 — {run_date}"

    ok = send_serverchan(cfg.serverchan_key, title, report)
    if ok:
        logger.info("推送成功!")
    else:
        logger.error("推送失败!")

    # 保存报告到文件
    report_path = cfg.data_dir / "reports"
    report_path.mkdir(exist_ok=True)
    report_file = report_path / f"report_{run_date}.md"
    report_file.write_text(report, encoding="utf-8")
    logger.info("报告已保存: %s", report_file)

    # 保存最优权重到 JSON
    weights_file = report_path / f"weights_{run_date}.json"
    weights_file.write_text(json.dumps({
        "date": run_date,
        "method": weight_method,
        "weights": final_weights,
        "best_backtest": best_label,
        "metrics": {k: v.metrics for k, v in bt_results.items()} if bt_results else {},
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("权重已保存: %s", weights_file)

    # ---- Step 7: 生成回测图表 ----
    logger.info("\n[Step 7] 生成回测图表...")
    if bt_results:
        # 方案A: quantstats 专业报告 (推荐)
        try:
            from scripts.gen_qs_report import generate_from_backtest_result
            best_bt = bt_results.get(best_label)
            if best_bt:
                qs_path = generate_from_backtest_result(
                    best_bt,
                    benchmark_name="沪深300",
                    output_dir=report_path,
                )
                if qs_path:
                    logger.info("quantstats 报告: %s", qs_path)

                    # 多段回测也生成 quantstats
                    for seg_name, seg_result in segment_results.items():
                        try:
                            seg_path = report_path / f"quantstats_{seg_name}_{run_date}.html"
                            generate_from_backtest_result(
                                seg_result,
                                benchmark_name="沪深300",
                                output_dir=report_path,
                            )
                        except Exception:
                            pass
        except Exception as e:
            logger.warning("quantstats 报告生成失败 (%s), 降级为 HTML 图表", e)

        # 方案B: HTML 交互式图表 (作为备用)
        try:
            chart_data = {
                "run_date": run_date,
                "weights": final_weights,
                "results": {
                    label: {
                        "nav": {k: float(v) for k, v in r.nav_series.items()},
                        "returns": {k: float(v) for k, v in r.returns_series.items()},
                        "metrics": r.metrics,
                    }
                    for label, r in bt_results.items()
                },
            }
            # 保存数据 JSON (可复用)
            data_json = report_path / f"_chart_data.json"
            data_json.write_text(json.dumps(chart_data, ensure_ascii=False))

            from scripts.gen_html_report import generate_html
            html = generate_html(chart_data)
            html_file = report_path / f"backtest_report_{run_date}.html"
            html_file.write_text(html, encoding="utf-8")
            logger.info("HTML 图表报告: %s", html_file)
        except Exception as e:
            logger.error("HTML 图表生成失败: %s", e)
    else:
        logger.warning("无回测结果，跳过图表生成")

    logger.info("\n" + "=" * 60)
    logger.info("  全自动管线完成!")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
