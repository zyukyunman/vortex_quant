"""研究报告与 signal snapshot 写出工具。

这些函数把纯计算结果固化为 JSON/HTML artifact，供策略回测、审计
和后续审计复用。报告层不重新计算指标，只负责写出事实和元信息。
"""
from __future__ import annotations

import json
from html import escape
from pathlib import Path

import pandas as pd

from vortex.research.evaluation import FactorCandidate, FactorEvaluationResult


def write_factor_report_json(
    result: FactorEvaluationResult,
    path: str | Path,
    *,
    factor_name: str,
    metadata: dict[str, object] | None = None,
) -> Path:
    """写出标准 `research_report.json`。"""

    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema": "vortex.research_report.v1",
        "factor_name": factor_name,
        "metadata": metadata or {},
        "result": result.to_dict(),
    }
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output


def write_factor_tear_sheet_html(
    result: FactorEvaluationResult,
    path: str | Path,
    *,
    factor_name: str,
) -> Path:
    """写出轻量 HTML tear sheet。"""

    rows = "\n".join(
        "<tr>"
        f"<td>{horizon}</td>"
        f"<td>{stats.ic_mean:.4f}</td>"
        f"<td>{stats.icir:.4f}</td>"
        f"<td>{stats.positive_rate:.2%}</td>"
        f"<td>{stats.count}</td>"
        "</tr>"
        for horizon, stats in result.ic_stats.items()
    )
    html = f"""<!doctype html>
<html lang="zh-CN"><meta charset="utf-8"><title>{escape(factor_name)} 因子评测</title>
<body>
<h1>{escape(factor_name)} 因子评测</h1>
<table border="1">
<thead><tr><th>周期</th><th>IC 均值</th><th>ICIR</th><th>正 IC 占比</th><th>样本数</th></tr></thead>
<tbody>{rows}</tbody>
</table>
<h2>多空组合</h2>
<p>周期：{result.long_short.horizon}；多空均值：{result.long_short.long_short_mean:.4%}；Sharpe：{result.long_short.sharpe:.4f}</p>
</body></html>"""
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html, encoding="utf-8")
    return output


def publish_signal_snapshot(
    signal: pd.DataFrame,
    path: str | Path,
    *,
    signal_name: str,
    metadata: dict[str, object] | None = None,
) -> Path:
    """发布不可变 signal snapshot。

    这里采用 JSON lines，避免早期阶段引入专门表结构；后续 promoted 时可
    替换为 parquet，但 schema 字段应保持兼容。
    """

    if signal.empty:
        raise ValueError("signal 不能为空")
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    records = []
    for date, row in signal.sort_index().iterrows():
        for symbol, value in row.dropna().items():
            records.append({"date": str(date), "symbol": str(symbol), "value": float(value)})
    payload = {
        "schema": "vortex.signal_snapshot.v1",
        "signal_name": signal_name,
        "metadata": metadata or {},
        "records": records,
    }
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output


def candidates_to_dict(candidates: list[FactorCandidate]) -> list[dict[str, object]]:
    """把候选池转为 JSON 可写结构。"""

    return [candidate.to_dict() for candidate in candidates]
