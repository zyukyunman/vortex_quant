#!/usr/bin/env python3
"""
gen_html_report.py
从回测数据 JSON 生成 HTML 交互式图表报告 (无需 matplotlib)
使用 Chart.js CDN，零额外依赖
"""
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = PROJECT_ROOT / "data" / "reports" / "_chart_data.json"


def generate_html(data: dict) -> str:
    run_date = data["run_date"]
    weights = data["weights"]
    results = data["results"]

    # 准备净值数据
    nav_datasets = []
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]
    for i, (label, rd) in enumerate(results.items()):
        nav = rd["nav"]
        dates = sorted(nav.keys())
        nav_base = rd["metrics"].get("initial_capital", nav[dates[0]])
        nav_norm = [nav[d] / nav_base for d in dates]
        m = rd["metrics"]
        nav_datasets.append({
            "label": f'{label} (年化{m["annual_return"]*100:.1f}%, 夏普{m["sharpe_ratio"]:.2f})',
            "data": [{"x": f'{d[:4]}-{d[4:6]}-{d[6:]}', "y": round(v, 4)} for d, v in zip(dates, nav_norm)],
            "borderColor": colors[i % len(colors)],
            "fill": False,
            "pointRadius": 0,
            "borderWidth": 2,
        })

    # 回撤数据 (最佳方案)
    best_label = max(results, key=lambda k: results[k]["metrics"].get("sharpe_ratio", -999))
    best_nav = results[best_label]["nav"]
    dates = sorted(best_nav.keys())
    nav_vals = [best_nav[d] for d in dates]
    cummax = []
    mx = 0
    for v in nav_vals:
        mx = max(mx, v)
        cummax.append(mx)
    dd = [-(cm - v) / cm * 100 if cm > 0 else 0 for cm, v in zip(cummax, nav_vals)]
    dd_data = [{"x": f'{d[:4]}-{d[4:6]}-{d[6:]}', "y": round(v, 2)} for d, v in zip(dates, dd)]

    # 月度收益
    best_rets = results[best_label]["returns"]
    monthly = {}
    for d, r in sorted(best_rets.items()):
        ym = d[:6]
        monthly[ym] = monthly.get(ym, 0) + r
    monthly_labels = sorted(monthly.keys())
    monthly_vals = [round(monthly[k] * 100, 2) for k in monthly_labels]
    monthly_colors = ["#2ca02c" if v >= 0 else "#d62728" for v in monthly_vals]
    monthly_display = [f'{k[:4]}-{k[4:]}' for k in monthly_labels]

    # 权重饼图
    name_map = {"dividend_yield": "股息率", "fcf_yield": "FCF收益率", "roe_ttm": "ROE",
                "delta_roe": "ΔROE", "opcfd": "OPCFD", "ep": "E/P"}
    w_items = {k: v for k, v in weights.items() if v > 0.001}
    pie_labels = [name_map.get(k, k) for k in w_items]
    pie_values = [round(v * 100, 1) for v in w_items.values()]
    pie_colors = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948"]

    # 绩效指标表格
    metric_rows = ""
    metric_defs = [
        ("start_date", "回测起始", False, False),
        ("end_date", "回测结束", False, False),
        ("n_rebalance", "再平衡次数", False, False),
        ("total_return", "总收益率", True, False),
        ("annual_return", "年化收益率", True, False),
        ("annual_volatility", "年化波动率", True, True),
        ("sharpe_ratio", "夏普比率", False, False),
        ("max_drawdown", "最大回撤", True, True),
        ("calmar_ratio", "Calmar比率", False, False),
        ("avg_turnover", "平均换手率", True, True),
        ("win_rate", "日胜率", True, False),
    ]
    for key, cn, is_pct, lower_better in metric_defs:
        row = f"<tr><td><strong>{cn}</strong></td>"
        vals_for_compare = []
        for label in results:
            v = results[label]["metrics"].get(key, 0)
            vals_for_compare.append((label, v))

        for label in results:
            v = results[label]["metrics"].get(key, 0)
            if isinstance(v, str):
                cell = v
            elif is_pct:
                cell = f"{v:.2%}"
            elif isinstance(v, float):
                cell = f"{v:.3f}"
            else:
                cell = str(v)

            # 高亮最佳值
            if len(vals_for_compare) > 1 and isinstance(v, (int, float)):
                best_v = min(vals_for_compare, key=lambda x: x[1])[1] if lower_better else max(vals_for_compare, key=lambda x: x[1])[1]
                if v == best_v and key not in ("start_date", "end_date", "n_rebalance"):
                    cell = f'<span style="color:#2ca02c;font-weight:bold">{cell}</span>'

            row += f"<td>{cell}</td>"
        row += "</tr>"
        metric_rows += row

    headers = "".join(f"<th>{lb}</th>" for lb in results)

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>QuantPilot 回测报告 — {run_date}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f5f6fa; color: #2c3e50; padding: 20px; }}
  .container {{ max-width: 1200px; margin: 0 auto; }}
  h1 {{ text-align: center; margin: 20px 0; font-size: 28px; color: #2c3e50; }}
  h1 small {{ display: block; font-size: 14px; color: #7f8c8d; font-weight: normal; margin-top: 5px; }}
  .card {{ background: white; border-radius: 12px; padding: 24px; margin: 16px 0; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
  .card h2 {{ font-size: 18px; margin-bottom: 16px; color: #34495e; border-left: 4px solid #3498db; padding-left: 12px; }}
  .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
  @media (max-width: 800px) {{ .grid-2 {{ grid-template-columns: 1fr; }} }}
  table {{ width: 100%; border-collapse: collapse; }}
  th, td {{ padding: 10px 14px; text-align: center; border-bottom: 1px solid #ecf0f1; }}
  th {{ background: #34495e; color: white; font-weight: 600; }}
  tr:hover {{ background: #f8f9fa; }}
  td:first-child {{ text-align: left; }}
  .chart-container {{ position: relative; width: 100%; }}
  .badge {{ display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: 600; }}
  .badge-green {{ background: #e8f5e9; color: #2e7d32; }}
  .badge-blue {{ background: #e3f2fd; color: #1565c0; }}
</style>
</head>
<body>
<div class="container">
<h1>QuantPilot 回测报告
<small>基准日: {run_date[:4]}-{run_date[4:6]}-{run_date[6:]} | 推荐方案: <span class="badge badge-blue">{best_label}</span></small>
</h1>

<div class="card">
<h2>净值曲线对比</h2>
<div class="chart-container"><canvas id="navChart" height="80"></canvas></div>
</div>

<div class="grid-2">
<div class="card">
<h2>回撤曲线 — {best_label}</h2>
<div class="chart-container"><canvas id="ddChart" height="120"></canvas></div>
</div>
<div class="card">
<h2>因子权重分布</h2>
<div class="chart-container"><canvas id="pieChart" height="120"></canvas></div>
</div>
</div>

<div class="card">
<h2>月度收益率 (%)</h2>
<div class="chart-container"><canvas id="monthlyChart" height="60"></canvas></div>
</div>

<div class="card">
<h2>绩效指标对比</h2>
<table>
<thead><tr><th>指标</th>{headers}</tr></thead>
<tbody>{metric_rows}</tbody>
</table>
</div>

</div>

<script>
// 净值曲线
new Chart(document.getElementById('navChart'), {{
  type: 'line',
  data: {{ datasets: {json.dumps(nav_datasets, ensure_ascii=False)} }},
  options: {{
    responsive: true,
    interaction: {{ mode: 'index', intersect: false }},
    scales: {{
      x: {{ type: 'time', time: {{ unit: 'month', displayFormats: {{ month: 'yyyy-MM' }} }} }},
      y: {{ title: {{ display: true, text: '归一化净值' }} }}
    }},
    plugins: {{ tooltip: {{ callbacks: {{ label: function(ctx) {{ return ctx.dataset.label.split(' (')[0] + ': ' + ctx.parsed.y.toFixed(4); }} }} }} }}
  }}
}});

// 回撤
new Chart(document.getElementById('ddChart'), {{
  type: 'line',
  data: {{
    datasets: [{{
      label: '回撤 (%)',
      data: {json.dumps(dd_data)},
      borderColor: '#d62728',
      backgroundColor: 'rgba(214,39,40,0.15)',
      fill: true,
      pointRadius: 0,
      borderWidth: 1.5
    }}]
  }},
  options: {{
    responsive: true,
    scales: {{
      x: {{ type: 'time', time: {{ unit: 'month', displayFormats: {{ month: 'yyyy-MM' }} }} }},
      y: {{ title: {{ display: true, text: '回撤 (%)' }} }}
    }}
  }}
}});

// 因子权重饼图
new Chart(document.getElementById('pieChart'), {{
  type: 'doughnut',
  data: {{
    labels: {json.dumps(pie_labels, ensure_ascii=False)},
    datasets: [{{
      data: {json.dumps(pie_values)},
      backgroundColor: {json.dumps(pie_colors[:len(pie_values)])}
    }}]
  }},
  options: {{
    responsive: true,
    plugins: {{
      legend: {{ position: 'right' }},
      tooltip: {{ callbacks: {{ label: function(ctx) {{ return ctx.label + ': ' + ctx.parsed + '%'; }} }} }}
    }}
  }}
}});

// 月度收益柱状图
new Chart(document.getElementById('monthlyChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(monthly_display)},
    datasets: [{{
      label: '月度收益 (%)',
      data: {json.dumps(monthly_vals)},
      backgroundColor: {json.dumps(monthly_colors)}
    }}]
  }},
  options: {{
    responsive: true,
    scales: {{
      y: {{ title: {{ display: true, text: '%' }} }}
    }},
    plugins: {{
      tooltip: {{ callbacks: {{ label: function(ctx) {{ return ctx.parsed.y.toFixed(2) + '%'; }} }} }}
    }}
  }}
}});
</script>
</body>
</html>"""
    return html


def main():
    if len(sys.argv) > 1:
        data_file = Path(sys.argv[1])
    else:
        data_file = DATA_FILE

    if not data_file.exists():
        print(f"数据文件不存在: {data_file}")
        print("请先运行 gen_charts.py --backtest 生成回测数据")
        sys.exit(1)

    data = json.loads(data_file.read_text())
    html = generate_html(data)

    run_date = data["run_date"]
    out = data_file.parent / f"backtest_report_{run_date}.html"
    out.write_text(html, encoding="utf-8")
    print(f"HTML 报告已生成: {out}")
    print(f"在浏览器中打开: file://{out.resolve()}")


if __name__ == "__main__":
    main()
