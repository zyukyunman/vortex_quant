#!/usr/bin/env python3
"""
run_backtest_sa.py
执行 2023-2026 半年调仓完整回测 → 自动输出 HTML 报告

用法:
  python scripts/run_backtest_sa.py
  python scripts/run_backtest_sa.py --start 20200101 --end 20260328 --freq Q
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from vortex.config.settings import Settings, setup_logging

setup_logging("INFO")

from vortex.core.data.datastore import DataStore
from vortex.core.factorhub import FactorHub
from vortex.core.signalbus import SignalBus
from vortex.core.weight_optimizer import FixedWeightOptimizer
from vortex.executor.backtest import BacktestEngine
from vortex.strategy.dividend import DEFAULT_WEIGHTS, DividendQualityFCFStrategy


# ================================================================
#  HTML 报告生成 (自包含, Chart.js CDN)
# ================================================================

FREQ_LABEL = {"M": "月度", "Q": "季度", "SA": "半年", "D": "每日"}


def _build_html_report(result, weights: dict, freq: str, label: str = "红利质量FCF") -> str:
    """从 BacktestResult 直接生成完整 HTML 报告"""
    m = result.metrics
    nav = result.nav_series
    rets = result.returns_series

    # 1) 净值曲线 (归一化到 1.0)
    dates = sorted(nav.index.tolist())
    first_val = nav.iloc[0]
    nav_norm = [{"x": f"{d[:4]}-{d[4:6]}-{d[6:]}", "y": round(nav[d] / first_val, 4)} for d in dates]

    # 2) 回撤曲线
    import numpy as np
    nav_arr = np.array([nav[d] for d in dates])
    cummax = np.maximum.accumulate(nav_arr)
    dd_pct = -((cummax - nav_arr) / cummax) * 100
    dd_data = [{"x": f"{d[:4]}-{d[4:6]}-{d[6:]}", "y": round(float(v), 2)} for d, v in zip(dates, dd_pct)]

    # 3) 月度收益
    monthly = {}
    for d in sorted(rets.index):
        ym = d[:6]
        monthly[ym] = monthly.get(ym, 0) + rets[d]
    m_labels = sorted(monthly.keys())
    m_vals = [round(monthly[k] * 100, 2) for k in m_labels]
    m_colors = ["#2ca02c" if v >= 0 else "#d62728" for v in m_vals]
    m_display = [f"{k[:4]}-{k[4:]}" for k in m_labels]

    # 4) 年度收益
    yearly = {}
    for d in sorted(rets.index):
        y = d[:4]
        yearly[y] = yearly.get(y, 0) + rets[d]
    y_labels = sorted(yearly.keys())
    y_vals = [round(yearly[k] * 100, 2) for k in y_labels]
    y_colors = ["#1565c0" if v >= 0 else "#c62828" for v in y_vals]

    # 5) 因子权重
    name_map = {"dividend_yield": "股息率", "fcf_yield": "FCF收益率", "roe_ttm": "ROE",
                "delta_roe": "ΔROE", "opcfd": "现金流占比", "ep": "E/P(盈利收益率)"}
    w_items = {k: v for k, v in weights.items() if v > 0.001}
    pie_labels = [name_map.get(k, k) for k in w_items]
    pie_values = [round(v * 100, 1) for v in w_items.values()]
    pie_colors = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948", "#b07aa1", "#ff9da7"]

    # 6) 持仓表格
    stock_basic = {}
    try:
        import pandas as pd
        sb_path = Path("data/meta/stock_basic.parquet")
        if sb_path.exists():
            sb = pd.read_parquet(sb_path)
            stock_basic = dict(zip(sb["ts_code"], sb["name"]))
    except Exception:
        pass

    holdings_html = ""
    for pos in result.positions_history:
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

    # 7) 再平衡日期标注线
    rb_annotations = {}
    for i, d in enumerate(result.rebalance_dates):
        # 只标注有持仓的
        has_pos = any(p["date"] == d for p in result.positions_history)
        if has_pos:
            rb_annotations[f"rb{i}"] = {
                "type": "line",
                "xMin": f"{d[:4]}-{d[4:6]}-{d[6:]}",
                "xMax": f"{d[:4]}-{d[4:6]}-{d[6:]}",
                "borderColor": "rgba(52,152,219,0.4)",
                "borderWidth": 1,
                "borderDash": [4, 4],
            }

    # ---- 组装 HTML ----
    freq_cn = FREQ_LABEL.get(freq, freq)
    start_d = m.get("start_date", "N/A")
    end_d = m.get("end_date", "N/A")
    fmt_date = lambda s: f"{s[:4]}-{s[4:6]}-{s[6:]}" if isinstance(s, str) and len(s) == 8 else s

    # 基准数据 (如有)
    bench_datasets_js = ""
    bench_legend_rows = ""
    if result.benchmark_returns:
        bench_colors = ["#9467bd", "#8c564b", "#e377c2"]
        for i, (bname, bret) in enumerate(result.benchmark_returns.items()):
            # 计算基准净值
            b_dates = sorted(bret.index.tolist())
            b_nav = 1.0
            b_points = []
            for bd in b_dates:
                b_nav *= (1 + bret[bd])
                b_points.append({"x": f"{bd[:4]}-{bd[4:6]}-{bd[6:]}", "y": round(b_nav, 4)})
            color = bench_colors[i % len(bench_colors)]
            total_ret = b_nav - 1
            ann_ret = (b_nav ** (252 / max(len(b_dates), 1))) - 1
            bench_datasets_js += f"""{{
              label: '{bname} (年化{ann_ret*100:.1f}%)',
              data: {json.dumps(b_points)},
              borderColor: '{color}',
              fill: false, pointRadius: 0, borderWidth: 1.5, borderDash: [5,3]
            }},"""
            bench_legend_rows += f"<tr><td>{bname}</td><td>{total_ret:.2%}</td><td>{ann_ret:.2%}</td></tr>"

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>QuantPilot 回测报告 — {label} {freq_cn}调仓</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "PingFang SC", "Microsoft YaHei", sans-serif; background: #f0f2f5; color: #1a1a2e; padding: 20px; }}
  .container {{ max-width: 1280px; margin: 0 auto; }}
  h1 {{ text-align: center; margin: 24px 0 8px; font-size: 26px; }}
  h1 small {{ display: block; font-size: 14px; color: #666; font-weight: normal; margin-top: 6px; }}
  .card {{ background: #fff; border-radius: 12px; padding: 24px; margin: 16px 0; box-shadow: 0 1px 6px rgba(0,0,0,0.06); }}
  .card h2 {{ font-size: 17px; margin-bottom: 16px; color: #16213e; border-left: 4px solid #3498db; padding-left: 12px; }}
  .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  .grid-3 {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }}
  @media (max-width: 900px) {{ .grid-2, .grid-3 {{ grid-template-columns: 1fr; }} }}

  /* KPI 卡片 */
  .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); gap: 12px; margin-bottom: 8px; }}
  .kpi {{ text-align: center; padding: 16px 8px; background: #f8f9fc; border-radius: 10px; }}
  .kpi .value {{ font-size: 26px; font-weight: 700; }}
  .kpi .label {{ font-size: 12px; color: #666; margin-top: 4px; }}
  .kpi .positive {{ color: #c62828; }}
  .kpi .negative {{ color: #2e7d32; }}
  .kpi .neutral {{ color: #1565c0; }}

  /* 表格 */
  table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
  th, td {{ padding: 8px 12px; text-align: center; border-bottom: 1px solid #eee; }}
  th {{ background: #34495e; color: #fff; font-weight: 600; font-size: 13px; }}
  tr:hover {{ background: #f5f7fa; }}
  td:first-child {{ text-align: left; }}

  /* 持仓 */
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
</style>
</head>
<body>
<div class="container">

<h1>QuantPilot 回测报告
<small>{label} | <span class="badge badge-blue">{freq_cn}调仓</span> | {fmt_date(start_d)} ~ {fmt_date(end_d)}</small>
</h1>

<!-- KPI 卡片 -->
<div class="card">
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

<!-- 净值曲线 -->
<div class="card">
  <h2>净值曲线 (归一化)</h2>
  <div class="chart-container"><canvas id="navChart" height="75"></canvas></div>
</div>

<!-- 回撤 + 因子权重 -->
<div class="grid-2">
  <div class="card">
    <h2>回撤曲线</h2>
    <div class="chart-container"><canvas id="ddChart" height="140"></canvas></div>
  </div>
  <div class="card">
    <h2>因子权重分布</h2>
    <div class="chart-container" style="max-width:340px;margin:0 auto"><canvas id="pieChart" height="200"></canvas></div>
  </div>
</div>

<!-- 月度 + 年度收益 -->
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

<!-- 详细指标 -->
<div class="card">
  <h2>详细绩效指标</h2>
  <table>
    <thead><tr><th style="text-align:left">指标</th><th>值</th></tr></thead>
    <tbody>
      <tr><td>回测区间</td><td>{fmt_date(start_d)} ~ {fmt_date(end_d)}</td></tr>
      <tr><td>再平衡次数</td><td>{m.get('n_rebalance',0)}</td></tr>
      <tr><td>总收益率</td><td>{m.get('total_return',0):.2%}</td></tr>
      <tr><td>年化收益率</td><td>{m.get('annual_return',0):.2%}</td></tr>
      <tr><td>年化波动率</td><td>{m.get('annual_volatility',0):.2%}</td></tr>
      <tr><td>夏普比率</td><td>{m.get('sharpe_ratio',0):.3f}</td></tr>
      <tr><td>Sortino比率</td><td>{m.get('sortino_ratio',0):.3f}</td></tr>
      <tr><td>最大回撤</td><td>{m.get('max_drawdown',0):.2%}</td></tr>
      <tr><td>最大回撤天数</td><td>{m.get('max_dd_days',0)}</td></tr>
      <tr><td>Calmar比率</td><td>{m.get('calmar_ratio',0):.3f}</td></tr>
      <tr><td>盈亏比 (Profit Factor)</td><td>{m.get('profit_factor',0):.2f}</td></tr>
      <tr><td>平均换手率</td><td>{m.get('avg_turnover',0):.2%}</td></tr>
      <tr><td>日胜率</td><td>{m.get('win_rate',0):.2%}</td></tr>
    </tbody>
  </table>
</div>

<!-- 调仓持仓明细 -->
<div class="card">
  <h2>调仓持仓明细 (点击展开)</h2>
  {holdings_html}
</div>

<p class="footer">QuantPilot — 生成于 {end_d} | 策略: {label} | 调仓频率: {freq_cn}</p>
</div>

<script>
// === 净值曲线 ===
new Chart(document.getElementById('navChart'), {{
  type: 'line',
  data: {{ datasets: [
    {{
      label: '{label} (年化{m.get("annual_return",0)*100:.1f}%, 夏普{m.get("sharpe_ratio",0):.2f})',
      data: {json.dumps(nav_norm)},
      borderColor: '#1f77b4',
      fill: false, pointRadius: 0, borderWidth: 2
    }},
    {bench_datasets_js}
  ] }},
  options: {{
    responsive: true,
    interaction: {{ mode: 'index', intersect: false }},
    scales: {{
      x: {{ type: 'time', time: {{ unit: 'month', displayFormats: {{ month: 'yyyy-MM' }} }} }},
      y: {{ title: {{ display: true, text: '净值 (归一化=1.0)' }} }}
    }},
    plugins: {{
      tooltip: {{ callbacks: {{ label: function(ctx) {{ return ctx.dataset.label.split(' (')[0] + ': ' + ctx.parsed.y.toFixed(4); }} }} }}
    }}
  }}
}});

// === 回撤 ===
new Chart(document.getElementById('ddChart'), {{
  type: 'line',
  data: {{ datasets: [{{ label: '回撤 (%)', data: {json.dumps(dd_data)},
    borderColor: '#d62728', backgroundColor: 'rgba(214,39,40,0.12)', fill: true, pointRadius: 0, borderWidth: 1.5
  }}] }},
  options: {{
    responsive: true,
    scales: {{
      x: {{ type: 'time', time: {{ unit: 'month', displayFormats: {{ month: 'yyyy-MM' }} }} }},
      y: {{ title: {{ display: true, text: '回撤 (%)' }} }}
    }}
  }}
}});

// === 因子权重 ===
new Chart(document.getElementById('pieChart'), {{
  type: 'doughnut',
  data: {{
    labels: {json.dumps(pie_labels, ensure_ascii=False)},
    datasets: [{{ data: {json.dumps(pie_values)}, backgroundColor: {json.dumps(pie_colors[:len(pie_values)])} }}]
  }},
  options: {{
    responsive: true,
    plugins: {{
      legend: {{ position: 'bottom', labels: {{ font: {{ size: 12 }} }} }},
      tooltip: {{ callbacks: {{ label: function(ctx) {{ return ctx.label + ': ' + ctx.parsed + '%'; }} }} }}
    }}
  }}
}});

// === 月度柱状 ===
new Chart(document.getElementById('monthlyChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(m_display)},
    datasets: [{{ label: '月度收益 (%)', data: {json.dumps(m_vals)}, backgroundColor: {json.dumps(m_colors)} }}]
  }},
  options: {{ responsive: true, scales: {{ y: {{ title: {{ display: true, text: '%' }} }} }},
    plugins: {{ tooltip: {{ callbacks: {{ label: function(ctx) {{ return ctx.parsed.y.toFixed(2) + '%'; }} }} }}, legend: {{ display: false }} }} }}
}});

// === 年度柱状 ===
new Chart(document.getElementById('yearlyChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(y_labels)},
    datasets: [{{ label: '年度收益 (%)', data: {json.dumps(y_vals)}, backgroundColor: {json.dumps(y_colors)} }}]
  }},
  options: {{ responsive: true, scales: {{ y: {{ title: {{ display: true, text: '%' }} }} }},
    plugins: {{ tooltip: {{ callbacks: {{ label: function(ctx) {{ return ctx.parsed.y.toFixed(2) + '%'; }} }} }}, legend: {{ display: false }} }} }}
}});
</script>
</body>
</html>"""
    return html


# ================================================================
#  主流程
# ================================================================

def main():
    parser = argparse.ArgumentParser(description="QuantPilot 回测 → HTML 报告")
    parser.add_argument("--start", default="20230101", help="回测起始日 (默认: 20230101)")
    parser.add_argument("--end", default="20260328", help="回测结束日 (默认: 20260328)")
    parser.add_argument("--freq", default="SA", choices=["M", "Q", "SA"], help="调仓频率 M/Q/SA (默认: SA)")
    parser.add_argument("--top-n", type=int, default=30, help="选股数量 (默认: 30)")
    args = parser.parse_args()

    cfg = Settings()
    cfg.validate()
    ds = DataStore(cfg)
    fh = FactorHub(ds)
    fh.register_all_defaults()

    freq_cn = FREQ_LABEL.get(args.freq, args.freq)
    print("=" * 60)
    print(f"  完整回测: {args.start[:4]}-{args.end[:4]}, {freq_cn}调仓")
    print(f"  区间: {args.start} ~ {args.end}")
    print("=" * 60)

    bus = SignalBus(ds.data_dir)
    optimizer = FixedWeightOptimizer(DEFAULT_WEIGHTS)
    strategy = DividendQualityFCFStrategy(ds, fh, bus, weight_optimizer=optimizer)
    engine = BacktestEngine(ds)

    result = engine.run(
        strategy, args.start, args.end, freq=args.freq,
        benchmark_codes=["000300.SH", "000905.SH", "000922.CSI"],
    )

    print(result.summary())

    # ---- 生成 HTML 报告 ----
    report_dir = cfg.data_dir / "reports"
    report_dir.mkdir(exist_ok=True)

    html = _build_html_report(result, DEFAULT_WEIGHTS, args.freq, label="红利质量FCF")
    end_d = result.metrics.get("end_date", args.end)
    html_file = report_dir / f"backtest_{args.freq}_{args.start}_{end_d}.html"
    html_file.write_text(html, encoding="utf-8")
    print(f"\n✅ HTML 报告: {html_file}")
    print(f"   浏览器打开: file://{html_file.resolve()}")

    # ---- 也保存 JSON ----
    result_data = {
        "backtest": f"{args.start[:4]}-{args.end[:4]} {freq_cn}调仓",
        "start": args.start, "end": args.end, "freq": args.freq,
        "metrics": result.metrics,
        "rebalance_dates": result.rebalance_dates,
        "nav_last": float(result.nav_series.iloc[-1]) if len(result.nav_series) > 0 else None,
    }
    json_file = report_dir / f"backtest_{args.freq}_{args.start}_{end_d}.json"
    json_file.write_text(json.dumps(result_data, ensure_ascii=False, indent=2, default=str))
    print(f"   JSON 数据:  {json_file}")


if __name__ == "__main__":
    main()
