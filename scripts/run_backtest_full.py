#!/usr/bin/env python3
"""
run_backtest_full.py
多段回测 + 全量回测 → 综合 HTML 报告 (含基准对比)

A股历史阶段划分 (2017-2026):
  ① 2017-01 ~ 2018-01  蓝筹白马行情 (供给侧改革尾声, 价值风格)
  ② 2018-02 ~ 2019-01  贸易战+去杠杆熊市
  ③ 2019-01 ~ 2021-02  核心资产牛市 (宽信用+疫情流动性)
  ④ 2021-02 ~ 2022-04  结构分化+赛道崩塌
  ⑤ 2022-04 ~ 2024-09  系统性熊市 (地产危机+弱复苏+中特估)
  ⑥ 2024-09 ~ Now       924行情+政策转向

用法:
  python scripts/run_backtest_full.py                       # 全量 2017~now, SA
  python scripts/run_backtest_full.py --freq Q              # 季度调仓
  python scripts/run_backtest_full.py --start 20190101      # 自定义起始
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from vortex.config.settings import Settings, StrategyConfig, setup_logging

setup_logging("INFO")

from vortex.core.data.datastore import DataStore
from vortex.core.factorhub import FactorHub
from vortex.core.signalbus import SignalBus
from vortex.core.weight_optimizer import FixedWeightOptimizer
from vortex.executor.backtest import BacktestEngine, BacktestResult
from vortex.strategy.dividend import DEFAULT_WEIGHTS, DividendQualityFCFStrategy


# ================================================================
#  A股历史阶段定义
# ================================================================

MARKET_PHASES: List[Tuple[str, str, str, str]] = [
    # (start, end, name, description)
    ("20170103", "20180131", "蓝筹白马", "供给侧改革尾声, 核心资产价值回归"),
    ("20180201", "20190131", "贸易摩擦熊市", "中美贸易战+金融去杠杆, 全面杀估值"),
    ("20190201", "20210219", "核心资产牛市", "宽信用+疫情流动性宽松, 抱团行情"),
    ("20210222", "20220429", "赛道分化", "茅台见顶, 新能源先涨后跌, 风格剧变"),
    ("20220502", "20240923", "深度调整", "地产出清+疫情尾声+AI概念+中特估"),
    ("20240924", "20260328", "924反转", "政策转向, 强力V反, 宽基修复"),
]

FREQ_LABEL = {"M": "月度", "Q": "季度", "SA": "半年"}
BENCHMARK_CODES = ["000300.SH", "000905.SH", "000922.CSI"]
BENCHMARK_NAMES = {"000300.SH": "沪深300", "000905.SH": "中证500", "000922.CSI": "中证红利"}


# ================================================================
#  核心: 执行一段回测
# ================================================================

def run_one_segment(
    engine: BacktestEngine,
    strategy,
    start: str,
    end: str,
    freq: str,
    label: str,
) -> Optional[BacktestResult]:
    """安全执行一段回测, 失败返回 None"""
    try:
        result = engine.run(
            strategy, start, end, freq=freq,
            benchmark_codes=BENCHMARK_CODES,
        )
        m = result.metrics
        n_pos = sum(1 for p in result.positions_history if p["n_stocks"] > 0)
        print(f"  ✅ {label}: 总收益={m.get('total_return',0):.2%}, "
              f"年化={m.get('annual_return',0):.2%}, "
              f"夏普={m.get('sharpe_ratio',0):.3f}, "
              f"回撤={m.get('max_drawdown',0):.2%}, "
              f"有效调仓={n_pos}/{m.get('n_rebalance',0)}")
        return result
    except Exception as e:
        print(f"  ❌ {label}: {e}")
        return None


# ================================================================
#  HTML 报告: 全量 + 多段对比
# ================================================================

def build_full_report(
    full_result: BacktestResult,
    segment_results: List[Tuple[str, str, BacktestResult]],
    weights: dict,
    freq: str,
    label: str = "红利质量FCF",
) -> str:
    """生成综合 HTML: 全量净值+回撤+基准 + 分段对比表 + 分段净值"""
    m = full_result.metrics
    nav = full_result.nav_series

    # ---- 基准绩效指标 ----
    bench_metrics = full_result.calc_benchmark_metrics()
    rets = full_result.returns_series

    # ---- 全量净值 (归一化) ----
    dates = sorted(nav.index.tolist())
    first_val = nav.iloc[0]
    nav_norm = [{"x": _fmt(d), "y": round(nav[d] / first_val, 4)} for d in dates]

    # ---- 回撤 ----
    nav_arr = np.array([nav[d] for d in dates])
    cummax = np.maximum.accumulate(nav_arr)
    dd_pct = -((cummax - nav_arr) / cummax) * 100
    dd_data = [{"x": _fmt(d), "y": round(float(v), 2)} for d, v in zip(dates, dd_pct)]

    # ---- 基准颜色 (回撤/净值/分段共用) ----
    bench_colors = ["#9467bd", "#8c564b", "#e377c2"]

    # ---- 基准回撤 ----
    bench_dd_datasets_js = ""
    if full_result.benchmark_returns:
        for i, (bname, bret) in enumerate(full_result.benchmark_returns.items()):
            b_dates = sorted(bret.index.tolist())
            b_nav_v = 1.0
            b_navs = []
            for bd in b_dates:
                b_nav_v *= (1 + bret[bd])
                b_navs.append(b_nav_v)
            b_arr = np.array(b_navs)
            b_cummax = np.maximum.accumulate(b_arr)
            b_dd = -((b_cummax - b_arr) / b_cummax) * 100
            b_dd_pts = [{"x": _fmt(bd), "y": round(float(v), 2)} for bd, v in zip(b_dates, b_dd)]
            color = bench_colors[i % len(bench_colors)]
            bench_dd_datasets_js += f"""{{
              label: '{bname}',
              data: {json.dumps(b_dd_pts)},
              borderColor: '{color}',
              fill: false, pointRadius: 0, borderWidth: 1.2, borderDash: [4,3]
            }},"""

    # ---- 基准净值 ----
    bench_datasets_js = ""
    bench_table_rows = ""
    if full_result.benchmark_returns:
        for i, (bname, bret) in enumerate(full_result.benchmark_returns.items()):
            b_dates = sorted(bret.index.tolist())
            b_nav = 1.0
            b_points = []
            for bd in b_dates:
                b_nav *= (1 + bret[bd])
                b_points.append({"x": _fmt(bd), "y": round(b_nav, 4)})
            color = bench_colors[i % len(bench_colors)]
            total_ret = b_nav - 1
            ann_ret = (b_nav ** (252 / max(len(b_dates), 1))) - 1 if len(b_dates) > 0 else 0
            bench_datasets_js += f"""{{
              label: '{bname} (年化{ann_ret*100:.1f}%)',
              data: {json.dumps(b_points)},
              borderColor: '{color}',
              fill: false, pointRadius: 0, borderWidth: 1.5, borderDash: [5,3]
            }},"""
            bench_table_rows += f"<tr><td>{bname}</td><td>{total_ret:.2%}</td><td>{ann_ret:.2%}</td></tr>"

    # ---- 月度/年度收益 ----
    monthly = {}
    for d in sorted(rets.index):
        ym = d[:6]
        monthly[ym] = monthly.get(ym, 0) + rets[d]
    m_labels = sorted(monthly.keys())
    m_vals = [round(monthly[k] * 100, 2) for k in m_labels]
    m_colors = ["#2ca02c" if v >= 0 else "#d62728" for v in m_vals]
    m_display = [f"{k[:4]}-{k[4:]}" for k in m_labels]

    yearly = {}
    for d in sorted(rets.index):
        y = d[:4]
        yearly[y] = yearly.get(y, 0) + rets[d]
    y_labels = sorted(yearly.keys())
    y_vals = [round(yearly[k] * 100, 2) for k in y_labels]
    y_colors = ["#1565c0" if v >= 0 else "#c62828" for v in y_vals]

    # ---- 分段对比表 ----
    seg_rows = ""
    seg_chart_datasets = []
    seg_colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f"]
    for idx, (seg_name, seg_desc, seg_result) in enumerate(segment_results):
        sm = seg_result.metrics
        n_pos = sum(1 for p in seg_result.positions_history if p["n_stocks"] > 0)
        # 分段基准收益 (紧跟策略收益)
        seg_bench_cells = ""
        seg_bm = seg_result.calc_benchmark_metrics()
        for bname in (bench_metrics or {}):
            if bname in seg_bm:
                br = seg_bm[bname]['total_return']
                seg_bench_cells += f"<td class=\"{'positive' if br>=0 else 'negative'}\">{br:.2%}</td>"
            else:
                seg_bench_cells += "<td>-</td>"
        seg_rows += f"""<tr>
          <td style="text-align:left"><strong>{seg_name}</strong><br><small>{seg_desc}</small></td>
          <td>{_fmt(sm.get('start_date',''))}</td>
          <td>{_fmt(sm.get('end_date',''))}</td>
          <td class="{'positive' if sm.get('total_return',0)>=0 else 'negative'}" style="font-weight:700">{sm.get('total_return',0):.2%}</td>
          {seg_bench_cells}
          <td>{sm.get('sharpe_ratio',0):.3f}</td>
          <td class="negative">{sm.get('max_drawdown',0):.2%}</td>
          <td>{sm.get('calmar_ratio',0):.3f}</td>
          <td>{sm.get('sortino_ratio',0):.3f}</td>
          <td>{sm.get('avg_turnover',0):.2%}</td>
          <td>{n_pos}/{sm.get('n_rebalance',0)}</td>
        </tr>"""

        # 分段净值数据 (归一化) — 策略 + 基准
        seg_nav = seg_result.nav_series
        seg_dates = sorted(seg_nav.index.tolist())
        if seg_dates:
            seg_first = seg_nav.iloc[0]
            seg_points = [{"x": _fmt(d), "y": round(seg_nav[d] / seg_first, 4)} for d in seg_dates]
            seg_chart_datasets.append({
                "label": seg_name,
                "data": seg_points,
                "borderColor": seg_colors[idx % len(seg_colors)],
                "fill": False,
                "pointRadius": 0,
                "borderWidth": 2,
            })
            # 全部基准净值归一化
            if seg_result.benchmark_returns:
                for bi, (bname_seg, bret) in enumerate(seg_result.benchmark_returns.items()):
                    b_dates = sorted([d for d in bret.index if seg_dates[0] <= d <= seg_dates[-1]])
                    if b_dates:
                        b_nav_val = 1.0
                        b_pts = []
                        for bd in b_dates:
                            b_nav_val *= (1 + bret[bd])
                            b_pts.append({"x": _fmt(bd), "y": round(b_nav_val, 4)})
                        seg_chart_datasets.append({
                            "label": f"{seg_name}-{bname_seg}",
                            "data": b_pts,
                            "borderColor": bench_colors[bi % len(bench_colors)],
                            "fill": False,
                            "pointRadius": 0,
                            "borderWidth": 1,
                            "borderDash": [4, 3],
                            "_benchGroup": bname_seg,
                        })

    # ---- 持仓明细 ----
    stock_basic = {}
    try:
        sb_path = Path("data/meta/stock_basic.parquet")
        if sb_path.exists():
            sb = pd.read_parquet(sb_path)
            stock_basic = dict(zip(sb["ts_code"], sb["name"]))
    except Exception:
        pass

    holdings_html = ""
    for pos in full_result.positions_history:
        d = pos["date"]
        n = pos["n_stocks"]
        w_dict = pos["weights"]
        rows = ""
        for i, (code, wt) in enumerate(sorted(w_dict.items(), key=lambda x: -x[1]), 1):
            name = stock_basic.get(code, "")
            rows += f"<tr><td>{i}</td><td>{code}</td><td>{name}</td><td>{wt:.2%}</td></tr>\n"
        holdings_html += f"""
        <details class="holdings-block">
          <summary><strong>{d[:4]}-{d[4:6]}-{d[6:]}</strong> 调仓 — {n} 只股票</summary>
          <table class="holdings-table">
            <thead><tr><th>#</th><th>代码</th><th>名称</th><th>权重</th></tr></thead>
            <tbody>{rows}</tbody>
          </table>
        </details>"""

    freq_cn = FREQ_LABEL.get(freq, freq)
    start_d = m.get("start_date", "N/A")
    end_d = m.get("end_date", "N/A")

    # ---- 年度对比: 策略 vs 各基准 ----
    yearly_compare_rows = ""
    for y in y_labels:
        row = f"<td><strong>{y}</strong></td>"
        row += f"<td class=\"{'positive' if yearly.get(y,0)>=0 else 'negative'}\">{yearly.get(y,0)*100:.2f}%</td>"
        if full_result.benchmark_returns:
            for bname, bret in full_result.benchmark_returns.items():
                by_ret = sum(bret[d] for d in bret.index if d[:4] == y)
                row += f"<td class=\"{'positive' if by_ret>=0 else 'negative'}\">{by_ret*100:.2f}%</td>"
        yearly_compare_rows += f"<tr>{row}</tr>"

    bench_header_cols = ""
    if full_result.benchmark_returns:
        for bname in full_result.benchmark_returns:
            bench_header_cols += f"<th>{bname}</th>"

    # ---- 基准指标对比表 HTML ----
    bench_compare_html = ""
    if bench_metrics:
        bench_rows = f"""<tr style="background:#e3f2fd;font-weight:600">
          <td style="text-align:left">{label} (策略)</td>
          <td class="{'positive' if m.get('total_return',0)>=0 else 'negative'}">{m.get('total_return',0):.2%}</td>
          <td class="{'positive' if m.get('annual_return',0)>=0 else 'negative'}">{m.get('annual_return',0):.2%}</td>
          <td>{m.get('annual_volatility',0):.2%}</td>
          <td>{m.get('sharpe_ratio',0):.3f}</td>
          <td>{m.get('sortino_ratio',0):.3f}</td>
          <td class="negative">{m.get('max_drawdown',0):.2%}</td>
          <td>{m.get('calmar_ratio',0):.3f}</td>
        </tr>"""
        for bname, bm in bench_metrics.items():
            bench_rows += f"""<tr>
          <td style="text-align:left">{bname}</td>
          <td class="{'positive' if bm['total_return']>=0 else 'negative'}">{bm['total_return']:.2%}</td>
          <td class="{'positive' if bm['annual_return']>=0 else 'negative'}">{bm['annual_return']:.2%}</td>
          <td>{bm['annual_volatility']:.2%}</td>
          <td>{bm['sharpe_ratio']:.3f}</td>
          <td>{bm['sortino_ratio']:.3f}</td>
          <td class="negative">{bm['max_drawdown']:.2%}</td>
          <td>{bm['calmar_ratio']:.3f}</td>
        </tr>"""
        bench_compare_html = f"""
<div class="card">
  <h2>策略 vs 基准绩效对比</h2>
  <table>
    <thead><tr>
      <th style="text-align:left">名称</th><th>总收益</th><th>年化</th><th>波动率</th>
      <th>夏普</th><th>Sortino</th><th>最大回撤</th><th>Calmar</th>
    </tr></thead>
    <tbody>{bench_rows}</tbody>
  </table>
</div>"""

    # ---- 分段基准超额收益 ----
    seg_bench_headers = ""
    full_bench_cells = ""
    for bname in (bench_metrics or {}):
        seg_bench_headers += f"<th>{bname}收益</th>"
        br = bench_metrics[bname]['total_return']
        full_bench_cells += f"<td class=\"{'positive' if br>=0 else 'negative'}\">{br:.2%}</td>"

    # ---- 组装 HTML ----
    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>QuantPilot 综合回测报告 — {label}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "PingFang SC", "Microsoft YaHei", sans-serif;
         background: #f0f2f5; color: #1a1a2e; padding: 20px; }}
  .container {{ max-width: 1400px; margin: 0 auto; }}
  h1 {{ text-align: center; margin: 24px 0 8px; font-size: 26px; }}
  h1 small {{ display: block; font-size: 14px; color: #666; font-weight: normal; margin-top: 6px; }}
  .card {{ background: #fff; border-radius: 12px; padding: 24px; margin: 16px 0; box-shadow: 0 1px 6px rgba(0,0,0,0.06); }}
  .card h2 {{ font-size: 17px; margin-bottom: 16px; color: #16213e; border-left: 4px solid #3498db; padding-left: 12px; }}
  .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  @media (max-width: 900px) {{ .grid-2 {{ grid-template-columns: 1fr; }} }}

  .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; }}
  .kpi {{ text-align: center; padding: 16px 8px; background: #f8f9fc; border-radius: 10px; }}
  .kpi .value {{ font-size: 26px; font-weight: 700; }}
  .kpi .label {{ font-size: 12px; color: #666; margin-top: 4px; }}

  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th, td {{ padding: 8px 10px; text-align: center; border-bottom: 1px solid #eee; }}
  th {{ background: #34495e; color: #fff; font-weight: 600; font-size: 12px; position: sticky; top: 0; }}
  tr:hover {{ background: #f5f7fa; }}
  .positive {{ color: #c62828; font-weight: 600; }}
  .negative {{ color: #2e7d32; font-weight: 600; }}
  .neutral {{ color: #1565c0; }}

  .holdings-block {{ margin: 8px 0; }}
  .holdings-block summary {{ cursor: pointer; padding: 10px 14px; background: #f8f9fc; border-radius: 8px; font-size: 14px; }}
  .holdings-block summary:hover {{ background: #eef1f7; }}
  .holdings-table {{ margin-top: 8px; }}
  .holdings-table th {{ background: #546e7a; font-size: 12px; }}

  .chart-container {{ position: relative; width: 100%; }}
  .badge {{ display: inline-block; padding: 3px 10px; border-radius: 16px; font-size: 12px; font-weight: 600; }}
  .badge-blue {{ background: #e3f2fd; color: #1565c0; }}
  .badge-green {{ background: #e8f5e9; color: #2e7d32; }}
  .footer {{ text-align: center; color: #999; font-size: 12px; margin: 24px 0 8px; }}
  .seg-desc {{ font-size: 11px; color: #888; }}
</style>
</head>
<body>
<div class="container">

<h1>QuantPilot 综合回测报告
  <small>{label} | <span class="badge badge-blue">{freq_cn}调仓</span> | {_fmt(start_d)} ~ {_fmt(end_d)}</small>
</h1>

<!-- ======== KPI 卡片 ======== -->
<div class="card">
  <h2>全量回测绩效 ({_fmt(start_d)} ~ {_fmt(end_d)})</h2>
  <div class="kpi-grid">
    <div class="kpi"><div class="value {'positive' if m.get('total_return',0)>=0 else 'negative'}">{m.get('total_return',0):.2%}</div><div class="label">总收益率</div></div>
    <div class="kpi"><div class="value {'positive' if m.get('annual_return',0)>=0 else 'negative'}">{m.get('annual_return',0):.2%}</div><div class="label">年化收益率</div></div>
    <div class="kpi"><div class="value neutral">{m.get('sharpe_ratio',0):.3f}</div><div class="label">夏普比率</div></div>
    <div class="kpi"><div class="value negative">{m.get('max_drawdown',0):.2%}</div><div class="label">最大回撤</div></div>
    <div class="kpi"><div class="value neutral">{m.get('calmar_ratio',0):.3f}</div><div class="label">Calmar比率</div></div>
    <div class="kpi"><div class="value neutral">{m.get('sortino_ratio',0):.3f}</div><div class="label">Sortino比率</div></div>
    <div class="kpi"><div class="value neutral">{m.get('annual_volatility',0):.2%}</div><div class="label">年化波动率</div></div>
    <div class="kpi"><div class="value neutral">{m.get('avg_turnover',0):.2%}</div><div class="label">平均换手率</div></div>
  </div>
</div>

{bench_compare_html}

<!-- ======== 全量净值曲线 (含基准) ======== -->
<div class="card">
  <h2>全量净值曲线 (含基准对比)</h2>
  <div class="chart-container"><canvas id="navChart" height="80"></canvas></div>
</div>

<!-- ======== 回撤曲线 (全宽) ======== -->
<div class="card">
  <h2>回撤曲线 (含基准)</h2>
  <div class="chart-container"><canvas id="ddChart" height="80"></canvas></div>
</div>

<!-- ======== 因子权重 ======== -->
<div class="card">
  <h2>因子权重 (ICIR加权优化)</h2>
  <div class="kpi-grid">
    {_build_weight_kpis(weights)}
  </div>
</div>

<!-- ======== 分段回测对比 ======== -->
<div class="card">
  <h2>A股历史阶段回测对比</h2>
  <div style="overflow-x:auto">
  <table>
    <thead><tr>
      <th style="text-align:left;min-width:140px">阶段</th><th>起始</th><th>结束</th>
      <th>策略收益</th>{seg_bench_headers}<th>夏普</th><th>最大回撤</th><th>Calmar</th><th>Sortino</th><th>换手率</th><th>调仓</th>
    </tr></thead>
    <tbody>
      {seg_rows}
      <tr style="background:#e3f2fd;font-weight:600">
        <td style="text-align:left">📊 全量合计</td>
        <td>{_fmt(start_d)}</td><td>{_fmt(end_d)}</td>
        <td class="{'positive' if m.get('total_return',0)>=0 else 'negative'}" style="font-weight:700">{m.get('total_return',0):.2%}</td>
        {full_bench_cells}
        <td>{m.get('sharpe_ratio',0):.3f}</td>
        <td class="negative">{m.get('max_drawdown',0):.2%}</td>
        <td>{m.get('calmar_ratio',0):.3f}</td>
        <td>{m.get('sortino_ratio',0):.3f}</td>
        <td>{m.get('avg_turnover',0):.2%}</td>
        <td>{m.get('n_rebalance',0)}</td>
      </tr>
    </tbody>
  </table>
  </div>
</div>

<!-- ======== 年度收益对比 ======== -->
<div class="card">
  <h2>年度收益对比: 策略 vs 基准</h2>
  <table>
    <thead><tr><th>年度</th><th>{label}</th>{bench_header_cols}</tr></thead>
    <tbody>{yearly_compare_rows}</tbody>
  </table>
</div>

<!-- ======== 月度/年度柱状图 ======== -->
<div class="grid-2">
  <div class="card">
    <h2>月度收益率 (%)</h2>
    <div class="chart-container"><canvas id="monthlyChart" height="140"></canvas></div>
  </div>
  <div class="card">
    <h2>年度收益率 (%)</h2>
    <div class="chart-container"><canvas id="yearlyChart" height="140"></canvas></div>
  </div>
</div>

<!-- ======== 分段净值叠加图 ======== -->
<div class="card">
  <h2>各阶段净值走势 (归一化)</h2>
  <div class="chart-container"><canvas id="segChart" height="80"></canvas></div>
</div>

<!-- ======== 调仓持仓明细 ======== -->
<div class="card">
  <h2>调仓持仓明细 (点击展开)</h2>
  {holdings_html}
</div>

<p class="footer">QuantPilot 综合回测报告 — {label} | {freq_cn}调仓 | 生成于 {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
</div>

<script>
// === 全量净值 ===
new Chart(document.getElementById('navChart'), {{
  type: 'line',
  data: {{ datasets: [
    {{ label: '{label} (年化{m.get("annual_return",0)*100:.1f}%, 夏普{m.get("sharpe_ratio",0):.2f})',
       data: {json.dumps(nav_norm)}, borderColor: '#1f77b4',
       fill: false, pointRadius: 0, borderWidth: 2 }},
    {bench_datasets_js}
  ] }},
  options: {{
    responsive: true,
    interaction: {{ mode: 'index', intersect: false }},
    scales: {{
      x: {{ type: 'time', time: {{ unit: 'month', displayFormats: {{ month: 'yyyy-MM' }} }} }},
      y: {{ title: {{ display: true, text: '净值 (归一化=1.0)' }} }}
    }},
    plugins: {{ tooltip: {{ callbacks: {{ label: function(ctx) {{ return ctx.dataset.label.split(' (')[0] + ': ' + ctx.parsed.y.toFixed(4); }} }} }} }}
  }}
}});

// === 回撤 (含基准) ===
new Chart(document.getElementById('ddChart'), {{
  type: 'line',
  data: {{ datasets: [
    {{ label: '策略回撤', data: {json.dumps(dd_data)},
      borderColor: '#d62728', backgroundColor: 'rgba(214,39,40,0.12)', fill: true, pointRadius: 0, borderWidth: 1.5 }},
    {bench_dd_datasets_js}
  ] }},
  options: {{
    responsive: true,
    interaction: {{ mode: 'index', intersect: false }},
    plugins: {{ legend: {{ display: true, position: 'top' }} }},
    scales: {{
      x: {{ type: 'time', time: {{ unit: 'month', displayFormats: {{ month: 'yyyy-MM' }} }} }},
      y: {{ title: {{ display: true, text: '回撤 (%)' }} }}
    }}
  }}
}});

// === 月度柱状 ===
new Chart(document.getElementById('monthlyChart'), {{
  type: 'bar',
  data: {{ labels: {json.dumps(m_display)}, datasets: [{{ data: {json.dumps(m_vals)}, backgroundColor: {json.dumps(m_colors)} }}] }},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ title: {{ display: true, text: '%' }} }} }} }}
}});

// === 年度柱状 ===
new Chart(document.getElementById('yearlyChart'), {{
  type: 'bar',
  data: {{ labels: {json.dumps(y_labels)}, datasets: [{{ data: {json.dumps(y_vals)}, backgroundColor: {json.dumps(y_colors)} }}] }},
  options: {{ responsive: true, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ title: {{ display: true, text: '%' }} }} }} }}
}});

// === 分段净值叠加 (基准全局联动开关) ===
(function() {{
  var segDS = {json.dumps(seg_chart_datasets, ensure_ascii=False)};
  new Chart(document.getElementById('segChart'), {{
    type: 'line',
    data: {{ datasets: segDS }},
    options: {{
      responsive: true,
      interaction: {{ mode: 'index', intersect: false }},
      scales: {{
        x: {{ type: 'time', time: {{ unit: 'month', displayFormats: {{ month: 'yyyy-MM' }} }} }},
        y: {{ title: {{ display: true, text: '归一化净值' }} }}
      }},
      plugins: {{
        legend: {{
          labels: {{
            generateLabels: function(chart) {{
              var items = [];
              var benchSeen = {{}};
              chart.data.datasets.forEach(function(ds, i) {{
                if (ds._benchGroup) {{
                  if (!benchSeen[ds._benchGroup]) {{
                    benchSeen[ds._benchGroup] = true;
                    items.push({{
                      text: ds._benchGroup,
                      fillStyle: ds.borderColor,
                      strokeStyle: ds.borderColor,
                      lineWidth: 1,
                      lineDash: [4, 3],
                      hidden: !chart.isDatasetVisible(i),
                      _benchGroup: ds._benchGroup,
                      datasetIndex: i
                    }});
                  }}
                }} else {{
                  items.push({{
                    text: ds.label,
                    fillStyle: ds.borderColor,
                    strokeStyle: ds.borderColor,
                    lineWidth: 2,
                    hidden: !chart.isDatasetVisible(i),
                    datasetIndex: i
                  }});
                }}
              }});
              return items;
            }}
          }},
          onClick: function(evt, item, legend) {{
            var chart = legend.chart;
            if (item._benchGroup) {{
              var grp = item._benchGroup;
              var anyVis = chart.data.datasets.some(function(ds, i) {{
                return ds._benchGroup === grp && chart.isDatasetVisible(i);
              }});
              chart.data.datasets.forEach(function(ds, i) {{
                if (ds._benchGroup === grp) chart.setDatasetVisibility(i, !anyVis);
              }});
            }} else {{
              var idx = item.datasetIndex;
              chart.setDatasetVisibility(idx, !chart.isDatasetVisible(idx));
            }}
            chart.update();
          }}
        }}
      }}
    }}
  }});
}})();
</script>
</body>
</html>"""
    return html


def _fmt(d: str) -> str:
    """YYYYMMDD → YYYY-MM-DD"""
    if isinstance(d, str) and len(d) == 8:
        return f"{d[:4]}-{d[4:6]}-{d[6:]}"
    return str(d)


def _build_weight_kpis(weights: dict) -> str:
    """因子权重 → KPI 卡片 HTML"""
    name_map = {"dividend_yield": "股息率", "fcf_yield": "FCF收益率", "roe_ttm": "ROE",
                "delta_roe": "ΔROE", "opcfd": "现金流占比", "ep": "E/P(盈利收益率)"}
    html = ""
    for k, v in weights.items():
        cname = name_map.get(k, k)
        pct = f"{v*100:.0f}%" if v > 0.001 else "0%"
        color = "neutral" if v > 0.001 else "negative"
        html += f'<div class="kpi"><div class="value {color}">{pct}</div><div class="label">{cname}</div></div>\n'
    return html


# ================================================================
#  主流程
# ================================================================

def main():
    parser = argparse.ArgumentParser(description="QuantPilot 多段回测 → 综合 HTML 报告")
    parser.add_argument("--start", default="20170103", help="全量回测起始日 (默认: 20170103)")
    parser.add_argument("--end", default=datetime.now().strftime("%Y%m%d"),
                        help="全量回测结束日 (默认: today)")
    parser.add_argument("--freq", default="SA", choices=["M", "Q", "SA"],
                        help="调仓频率 (默认: SA)")
    parser.add_argument("--top-n", type=int, default=30, help="选股数量 (默认: 30)")
    args = parser.parse_args()

    t0 = time.time()
    cfg = Settings()
    cfg.validate()

    # 策略配置 (与 Settings 解耦)
    scfg = StrategyConfig(top_n=args.top_n)

    ds = DataStore(cfg)
    fh = FactorHub(ds)
    fh.register_all_defaults()
    bus = SignalBus(ds.data_dir)
    optimizer = FixedWeightOptimizer(DEFAULT_WEIGHTS)
    strategy = DividendQualityFCFStrategy(ds, fh, bus,
                                          weight_optimizer=optimizer,
                                          strategy_config=scfg)
    engine = BacktestEngine(ds)

    freq_cn = FREQ_LABEL.get(args.freq, args.freq)

    # ---- 1. 全量回测 ----
    print("=" * 70)
    print(f"  全量回测: {args.start} ~ {args.end}, {freq_cn}调仓")
    print("=" * 70)

    full_result = run_one_segment(
        engine, strategy, args.start, args.end, args.freq, "全量"
    )
    if full_result is None:
        print("全量回测失败, 退出")
        return
    print(full_result.summary())

    # ---- 2. 分段回测 ----
    print("\n" + "=" * 70)
    print("  分段回测: A股历史各阶段")
    print("=" * 70)

    # 过滤出有效的分段 (在全量回测范围内)
    valid_phases = [
        (s, min(e, args.end), n, d)
        for s, e, n, d in MARKET_PHASES
        if s >= args.start and s < args.end
    ]

    segment_results = []
    for seg_start, seg_end, seg_name, seg_desc in valid_phases:
        # 每个分段需要新的 FactorHub 缓存
        fh_seg = FactorHub(ds)
        fh_seg.register_all_defaults()
        bus_seg = SignalBus(ds.data_dir)
        strategy_seg = DividendQualityFCFStrategy(
            ds, fh_seg, bus_seg,
            weight_optimizer=optimizer,
            strategy_config=scfg,
        )
        result = run_one_segment(
            engine, strategy_seg, seg_start, seg_end, args.freq,
            f"{seg_name} ({seg_start[:4]}-{seg_end[:4]})"
        )
        if result is not None:
            segment_results.append((seg_name, seg_desc, result))

    # ---- 3. 生成报告 ----
    print("\n生成综合 HTML 报告...")
    report_dir = cfg.data_dir / "reports"
    report_dir.mkdir(exist_ok=True)

    html = build_full_report(
        full_result, segment_results, DEFAULT_WEIGHTS, args.freq,
    )
    end_d = full_result.metrics.get("end_date", args.end)
    html_file = report_dir / f"backtest_full_{args.start}_{end_d}.html"
    html_file.write_text(html, encoding="utf-8")

    # JSON 汇总
    json_data = {
        "type": "multi_segment_backtest",
        "start": args.start, "end": args.end, "freq": args.freq,
        "full_metrics": full_result.metrics,
        "segments": [
            {"name": n, "desc": d, "metrics": r.metrics}
            for n, d, r in segment_results
        ],
    }
    json_file = report_dir / f"backtest_full_{args.start}_{end_d}.json"
    json_file.write_text(json.dumps(json_data, ensure_ascii=False, indent=2, default=str))

    elapsed = time.time() - t0
    print(f"\n{'='*70}")
    print(f"  ✅ 总耗时: {elapsed/60:.1f} 分钟")
    print(f"  📊 HTML 报告: {html_file}")
    print(f"     浏览器打开: file://{html_file.resolve()}")
    print(f"  📋 JSON 数据: {json_file}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
