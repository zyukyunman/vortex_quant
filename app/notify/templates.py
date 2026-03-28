"""
templates.py
消息模板 — 格式化 Signal/SelectionResult 为可读文本

所有模板输出 Markdown 格式 (Server酱支持 Markdown)
"""
from __future__ import annotations

from typing import Dict, List

from app.models import SelectionResult, Signal


def format_selection_result(result: SelectionResult) -> tuple[str, str]:
    """
    格式化选股结果为 Server酱 title + desp

    Returns
    -------
    tuple[str, str]
        (title, desp_markdown)
    """
    title = f"📊 {result.strategy} 选股 {result.date}"

    lines = [
        f"# {result.strategy} 选股结果",
        f"**日期**: {result.date}",
        f"**样本空间**: {result.universe_size} → 通过筛选: {result.after_filter_size}"
        f" → 入选: {result.top_n}",
        "",
        "| # | 代码 | 简称 | 权重 | 得分 | 理由 |",
        "|---|------|------|------|------|------|",
    ]

    for i, sig in enumerate(result.signals, 1):
        lines.append(
            f"| {i} | {sig.ts_code} | {sig.name} | "
            f"{sig.weight:.1%} | {sig.score:.4f} | {sig.reason} |"
        )

    # 行业分布
    industries: Dict[str, float] = {}
    for sig in result.signals:
        ind = sig.metadata.get("industry", "未知")
        industries[ind] = industries.get(ind, 0) + sig.weight

    if industries:
        lines.append("")
        lines.append("## 行业分布")
        lines.append("| 行业 | 权重 |")
        lines.append("|------|------|")
        for ind, w in sorted(industries.items(), key=lambda x: -x[1]):
            lines.append(f"| {ind} | {w:.1%} |")

    # 权重方法
    wm = result.metadata.get("weight_method", "unknown")
    lines.append(f"\n**权重方法**: {wm}")

    return title, "\n".join(lines)


def format_risk_alert(level: str, message: str, details: str = "") -> tuple[str, str]:
    """格式化风控告警"""
    emoji = {"P0": "🚨", "P1": "⚠️", "P2": "📋"}.get(level, "📌")
    title = f"{emoji} [{level}] {message[:24]}"
    desp = f"# {level} 风控告警\n\n{message}\n\n{details}"
    return title, desp


def format_daily_summary(
    date: str,
    results: List[SelectionResult],
    errors: List[str],
) -> tuple[str, str]:
    """格式化每日运行摘要"""
    title = f"📈 QuantPilot 日报 {date}"
    lines = [f"# QuantPilot 每日摘要 {date}", ""]

    for r in results:
        lines.append(f"## {r.strategy}")
        lines.append(f"- 样本空间: {r.universe_size}")
        lines.append(f"- 通过筛选: {r.after_filter_size}")
        lines.append(f"- 最终入选: {r.top_n}")
        if r.signals:
            top3 = r.signals[:3]
            lines.append("- Top 3:")
            for s in top3:
                lines.append(f"  - {s.ts_code} {s.name} (权重={s.weight:.1%})")
        lines.append("")

    if errors:
        lines.append("## ⚠️ 错误")
        for e in errors:
            lines.append(f"- {e}")

    return title, "\n".join(lines)


def format_data_update(date: str, stats: Dict) -> tuple[str, str]:
    """格式化数据更新通知"""
    title = f"📦 数据更新完成 {date}"
    lines = [f"# 数据更新 {date}", ""]
    for k, v in stats.items():
        lines.append(f"- **{k}**: {v}")
    return title, "\n".join(lines)
