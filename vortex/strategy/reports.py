"""策略报告与多 run 对比工具。"""
from __future__ import annotations

import json
from html import escape
from pathlib import Path

from vortex.strategy.backtest import BacktestResult


def write_backtest_report_json(
    result: BacktestResult,
    path: str | Path,
    *,
    strategy_name: str,
    metadata: dict[str, object] | None = None,
) -> Path:
    """写出标准 `backtest_report.json`。"""

    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": "vortex.backtest_report.v1",
        "strategy_name": strategy_name,
        "metadata": metadata or {},
        **result.to_dict(),
    }
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output


def write_backtest_report_html(
    result: BacktestResult,
    path: str | Path,
    *,
    strategy_name: str,
) -> Path:
    """写出轻量回测 HTML。"""

    metrics = result.metrics
    rows = {
        "年化收益": f"{metrics.annual_return:.2%}",
        "总收益": f"{metrics.total_return:.2%}",
        "最大回撤": f"{metrics.max_drawdown:.2%}",
        "Sharpe": f"{metrics.sharpe:.2f}",
        "Calmar": f"{metrics.calmar:.2f}",
        "平均换手": f"{metrics.turnover:.2%}",
        "目标审查": result.goal_review.status,
    }
    table_rows = "\n".join(
        f"<tr><td>{escape(key)}</td><td>{escape(value)}</td></tr>"
        for key, value in rows.items()
    )
    html = f"""<!doctype html>
<html lang="zh-CN"><meta charset="utf-8"><title>{escape(strategy_name)} 回测报告</title>
<body>
<h1>{escape(strategy_name)} 回测报告</h1>
<table border="1"><tbody>{table_rows}</tbody></table>
</body></html>"""
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
    return output


def compare_backtest_reports(reports: dict[str, BacktestResult]) -> list[dict[str, object]]:
    """对比多个回测 run，按 Calmar 和 Sharpe 排序。"""

    rows = []
    for name, result in reports.items():
        metrics = result.metrics
        rows.append(
            {
                "name": name,
                "annual_return": metrics.annual_return,
                "max_drawdown": metrics.max_drawdown,
                "sharpe": metrics.sharpe,
                "calmar": metrics.calmar,
                "turnover": metrics.turnover,
                "goal_status": result.goal_review.status,
            }
        )
    return sorted(rows, key=lambda item: (float(item["calmar"]), float(item["sharpe"])), reverse=True)
