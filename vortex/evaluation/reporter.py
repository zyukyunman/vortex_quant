"""
reporter.py
评测报告 HTML 生成

生成轻量 HTML 报告，避免 matplotlib 在低内存机器上的 OOM 风险。
"""
from __future__ import annotations

import logging
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from vortex.evaluation.spec import FactorRole

logger = logging.getLogger(__name__)

# 因子中文名映射
FACTOR_CN = {
    "dividend_yield": "股息率",
    "fcf_yield": "FCF收益率",
    "roe_ttm": "ROE",
    "delta_roe": "ΔROE",
    "opcfd": "现金流/负债",
    "ep": "E/P(盈利收益率)",
    "consecutive_div_years": "连续分红年数",
    "fcf_ttm": "近一年自由现金流",
    "payout_ratio_3y": "三年平均分红比例",
    "debt_to_assets": "资产负债率",
    "roe_stability": "ROE稳定性",
    "dividend_yield_3y": "三年平均股息率",
    "ocf_3y_positive": "三年OCF全正",
    "roe_over_pb": "ROE/PB",
    "netprofit_yoy": "扣非净利润同比",
}

# 因子含义映射 (用于权重报告)
FACTOR_DESC = {
    "dividend_yield": "静态股息率(最近完整年度)",
    "fcf_yield": "自由现金流收益率 FCF/EV",
    "roe_ttm": "最新年报ROE",
    "delta_roe": "年报ROE同比变化",
    "opcfd": "经营现金流/总负债",
    "ep": "盈利收益率 E/P",
    "consecutive_div_years": "连续分红年数",
    "fcf_ttm": "最近完整年报自由现金流>0",
    "payout_ratio_3y": "三年平均股利支付率",
    "debt_to_assets": "资产负债率<=70%",
    "netprofit_yoy": "扣非净利润同比>=-10%",
    "roe_stability": "ROE稳定性(越小越好)",
    "dividend_yield_3y": "三年平均静态股息率(观察尾部风险)",
}


def _get_cn(name: str) -> str:
    return FACTOR_CN.get(name, name)


def _get_desc(name: str) -> str:
    return FACTOR_DESC.get(name, "")


def _role_cn(role: FactorRole) -> str:
    return {
        FactorRole.SCORING: "打分",
        FactorRole.FILTER: "过滤",
        FactorRole.RISK: "风险",
        FactorRole.TIMING: "择时",
    }.get(role, role.value)


def _fmt_float(value, digits: int = 4) -> str:
    if value is None:
        return "—"
    try:
        if pd.isna(value):
            return "—"
        return f"{float(value):.{digits}f}"
    except Exception:
        return "—"


def _fmt_pct(value) -> str:
    if value is None:
        return "—"
    try:
        if pd.isna(value):
            return "—"
        return f"{float(value):.1%}"
    except Exception:
        return "—"


def _status_badge(passed: bool) -> str:
    if passed:
        return "<span class='badge pass'>通过</span>"
    return "<span class='badge fail'>未通过</span>"


def _factor_label(factor_name: str) -> str:
    cn = escape(_get_cn(factor_name))
    en = escape(factor_name)
    return f"<strong>{cn}</strong><div class='code'>{en}</div>"


def _render_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "<p class='muted'>无数据</p>"
    return df.to_html(index=False, escape=False, classes="report-table", border=0)


def _style_block() -> str:
    return """
<style>
body {
    font-family: "PingFang SC", "Microsoft YaHei", sans-serif;
    background: #f6f8fb;
    color: #1f2937;
    margin: 0;
    padding: 24px;
}
.container {
    max-width: 1400px;
    margin: 0 auto;
}
h1, h2, h3 {
    margin: 0;
}
h1 {
    font-size: 28px;
    margin-bottom: 8px;
}
h2 {
    font-size: 20px;
    margin: 28px 0 12px 0;
}
h3 {
    font-size: 16px;
    margin: 18px 0 8px 0;
}
.meta {
    color: #6b7280;
    margin-bottom: 16px;
}
.summary {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
}
.card {
    background: white;
    border-radius: 10px;
    padding: 12px 16px;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
}
.card .k {
    font-size: 12px;
    color: #6b7280;
}
.card .v {
    font-size: 20px;
    font-weight: 600;
    margin-top: 4px;
}
.report-table {
    width: 100%;
    border-collapse: collapse;
    background: #ffffff;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
    border-radius: 10px;
    overflow: hidden;
    margin-bottom: 12px;
}
.report-table th {
    background: #1f4e79;
    color: #fff;
    padding: 10px 8px;
    text-align: left;
    font-weight: 600;
    font-size: 13px;
}
.report-table td {
    padding: 9px 8px;
    border-top: 1px solid #e5e7eb;
    font-size: 13px;
    vertical-align: top;
}
.report-table tr:nth-child(even) {
    background: #fbfdff;
}
.badge {
    display: inline-block;
    border-radius: 999px;
    padding: 2px 8px;
    font-size: 12px;
    font-weight: 600;
}
.badge.pass {
    background: #dcfce7;
    color: #166534;
}
.badge.fail {
    background: #fee2e2;
    color: #991b1b;
}
.code {
    font-family: "SFMono-Regular", Menlo, Consolas, monospace;
    font-size: 11px;
    color: #6b7280;
    margin-top: 2px;
}
.muted {
    color: #6b7280;
}
.section {
    margin-top: 18px;
}
.toolbar {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin: 14px 0 8px 0;
}
.btn {
    background: #1f4e79;
    color: #ffffff;
    border: 0;
    border-radius: 8px;
    padding: 6px 10px;
    font-size: 12px;
    cursor: pointer;
}
.btn.secondary {
    background: #475569;
}
.collapsible {
    background: #ffffff;
    border-radius: 10px;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.08);
    margin-top: 12px;
    overflow: hidden;
}
.collapsible > summary {
    cursor: pointer;
    list-style: none;
    padding: 12px 14px;
    font-size: 16px;
    font-weight: 600;
    background: #f8fafc;
    border-bottom: 1px solid #e5e7eb;
}
.collapsible > summary::-webkit-details-marker {
    display: none;
}
.collapsible > summary::after {
    content: "+";
    float: right;
    color: #64748b;
}
.collapsible[open] > summary::after {
    content: "-";
}
.collapsible-body {
    padding: 12px 14px;
}
</style>
"""


def _script_block() -> str:
    return """
<script>
function setAllDetails(openState) {
    document.querySelectorAll("details.collapsible").forEach(function(item) {
        item.open = openState;
    });
}
</script>
"""


def _render_collapsible(title: str, content: str, open_by_default: bool = False) -> str:
    """渲染可折叠区块。"""
    open_attr = " open" if open_by_default else ""
    return (
        f"<details class='collapsible'{open_attr}>"
        f"<summary>{escape(title)}</summary>"
        f"<div class='collapsible-body'>{content}</div>"
        "</details>"
    )


def generate_eval_html(
    results: List,
    specs: List,
    output_path: str | Path,
    title: str = "因子评测报告",
) -> Path:
    """生成因子评测 HTML 报告。"""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    spec_map = {s.factor_name: s for s in specs}

    n_total = len(results)
    n_pass = sum(1 for r in results if r.passed)
    n_scoring = sum(1 for r in results if r.role == FactorRole.SCORING)
    n_filter = sum(1 for r in results if r.role == FactorRole.FILTER)
    n_risk = sum(1 for r in results if r.role == FactorRole.RISK)

    # 准入总表
    overview_rows = []
    for r in results:
        spec = spec_map.get(r.factor_name)
        overview_rows.append(
            {
                "因子": _factor_label(r.factor_name),
                "角色": escape(_role_cn(r.role)),
                "描述": escape(spec.description if spec else _get_desc(r.factor_name) or "—"),
                "数据源": escape(spec.data_source if spec else "—"),
                "准入": _status_badge(r.passed),
                "结论": escape(r.reason),
            }
        )
    overview_df = pd.DataFrame(overview_rows)

    # 打分因子详情
    scoring_results = [r for r in results if r.role == FactorRole.SCORING]
    scoring_horizons = sorted(
        {
            int(str(k).split("_")[-1][:-1])
            for r in scoring_results
            for k in r.metrics.keys()
            if str(k).startswith("mean_ic_") and str(k).endswith("d") and str(k).split("_")[-1][:-1].isdigit()
        }
    )
    scoring_rows = []
    for r in scoring_results:
        spec = spec_map.get(r.factor_name)
        row = {
            "因子": _factor_label(r.factor_name),
            "描述": escape(spec.description if spec else _get_desc(r.factor_name) or "—"),
            "准入": _status_badge(r.passed),
            "结论": escape(r.reason),
        }
        for h in scoring_horizons:
            row[f"IC_{h}d"] = _fmt_float(r.metrics.get(f"mean_ic_{h}d"), 4)
            row[f"ICIR_{h}d"] = _fmt_float(r.metrics.get(f"icir_{h}d"), 3)
            row[f"正IC率_{h}d"] = _fmt_pct(r.metrics.get(f"positive_rate_{h}d"))
        ls_h = spec.ls_horizon if spec else 5
        row[f"多空_{ls_h}d"] = _fmt_pct(r.metrics.get(f"long_short_{ls_h}d"))
        row["LS Sharpe"] = _fmt_float(r.metrics.get("ls_sharpe"), 3)
        scoring_rows.append(row)
    scoring_df = pd.DataFrame(scoring_rows)

    # 过滤因子详情
    filter_results = [r for r in results if r.role == FactorRole.FILTER]
    filter_rows = []
    for r in filter_results:
        spec = spec_map.get(r.factor_name)
        filter_rows.append(
            {
                "因子": _factor_label(r.factor_name),
                "描述": escape(spec.description if spec else _get_desc(r.factor_name) or "—"),
                "覆盖度": _fmt_pct(r.metrics.get("coverage")),
                "通过率": _fmt_pct(r.metrics.get("pass_rate")),
                "平均通过数": _fmt_float(r.metrics.get("avg_pass_count"), 1),
                "敏感度(-20%)": _fmt_pct(r.metrics.get("sensitivity_lo")),
                "敏感度(+20%)": _fmt_pct(r.metrics.get("sensitivity_hi")),
                "准入": _status_badge(r.passed),
                "结论": escape(r.reason),
            }
        )
    filter_df = pd.DataFrame(filter_rows)

    # 风险因子详情
    risk_results = [r for r in results if r.role == FactorRole.RISK]
    risk_rows = []
    for r in risk_results:
        spec = spec_map.get(r.factor_name)
        risk_rows.append(
            {
                "因子": _factor_label(r.factor_name),
                "描述": escape(spec.description if spec else _get_desc(r.factor_name) or "—"),
                "覆盖度": _fmt_pct(r.metrics.get("coverage")),
                "均值": _fmt_float(r.metrics.get("mean"), 4),
                "中位数": _fmt_float(r.metrics.get("median"), 4),
                "标准差": _fmt_float(r.metrics.get("std"), 4),
                "高尾占比": _fmt_pct(r.metrics.get("tail_high_ratio")),
                "低尾占比": _fmt_pct(r.metrics.get("tail_low_ratio")),
                "结论": escape(r.reason),
            }
        )
    risk_df = pd.DataFrame(risk_rows)

    # IC 时序明细
    detail_parts = []
    for r in scoring_results:
        spec = spec_map.get(r.factor_name)
        if r.detail is None or r.detail.empty:
            continue
        detail_df = r.detail.copy()
        if "date" in detail_df.columns:
            detail_df["date"] = detail_df["date"].astype(str)
        if "ic" in detail_df.columns:
            detail_df["ic"] = detail_df["ic"].map(lambda x: _fmt_float(x, 4))
        if len(detail_df) > 24:
            detail_df = detail_df.tail(24)

        desc = spec.description if spec and spec.description else _get_desc(r.factor_name)
        detail_parts.append(
            _render_collapsible(
                f"{_get_cn(r.factor_name)} ({r.factor_name})",
                "\n".join(
                    [
                        f"<p class='muted'>{escape(desc or '—')}</p>",
                        _render_table(detail_df),
                    ]
                ),
                open_by_default=False,
            )
        )

    sections_html = "\n".join(
        [
            _render_collapsible("1. 因子准入总览", _render_table(overview_df), open_by_default=True),
            _render_collapsible("2. 打分因子评测", _render_table(scoring_df), open_by_default=True),
            _render_collapsible("3. 过滤因子评测", _render_table(filter_df), open_by_default=False),
            _render_collapsible("4. 风险因子评测", _render_table(risk_df), open_by_default=False),
            _render_collapsible(
                "5. IC 时序明细 (最近 24 期)",
                "\n".join(detail_parts) if detail_parts else "<p class='muted'>无 IC 明细可展示</p>",
                open_by_default=False,
            ),
        ]
    )

    html = "\n".join(
        [
            "<!doctype html>",
            "<html lang='zh-CN'>",
            "<head>",
            "<meta charset='utf-8' />",
            f"<title>{escape(title)}</title>",
            _style_block(),
            _script_block(),
            "</head>",
            "<body>",
            "<div class='container'>",
            f"<h1>{escape(title)}</h1>",
            f"<div class='meta'>生成时间: {escape(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}</div>",
            "<div class='summary'>",
            f"<div class='card'><div class='k'>评测因子数</div><div class='v'>{n_total}</div></div>",
            f"<div class='card'><div class='k'>通过因子数</div><div class='v'>{n_pass}</div></div>",
            f"<div class='card'><div class='k'>打分因子</div><div class='v'>{n_scoring}</div></div>",
            f"<div class='card'><div class='k'>过滤因子</div><div class='v'>{n_filter}</div></div>",
            f"<div class='card'><div class='k'>风险因子</div><div class='v'>{n_risk}</div></div>",
            "</div>",
            "<div class='toolbar'>",
            "<button class='btn' type='button' onclick='setAllDetails(true)'>全部展开</button>",
            "<button class='btn secondary' type='button' onclick='setAllDetails(false)'>全部收起</button>",
            "</div>",
            sections_html,
            "</div>",
            "</body>",
            "</html>",
        ]
    )

    output_path.write_text(html, encoding="utf-8")
    logger.info("因子评测 HTML 已生成: %s", output_path)
    return output_path


def generate_weight_html(
    weights: Dict[str, float],
    comparison: Optional[pd.DataFrame] = None,
    output_path: str | Path = "weight_report.html",
    title: str = "因子权重优化报告",
    method: str = "",
    horizon: int = 0,
) -> Path:
    """生成权重优化 HTML 报告。"""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    active = [(k, float(v)) for k, v in weights.items() if float(v) > 0.001]
    active.sort(key=lambda x: -x[1])

    final_rows = [
        {
            "因子": _factor_label(name),
            "描述": escape(_get_desc(name) or "—"),
            "权重": _fmt_pct(weight),
        }
        for name, weight in active
    ]
    final_df = pd.DataFrame(final_rows)

    comparison_df = pd.DataFrame()
    if comparison is not None and not comparison.empty and "factor" in comparison.columns:
        rows = []
        for _, row in comparison.iterrows():
            name = str(row["factor"])
            item = {
                "因子": _factor_label(name),
                "描述": escape(_get_desc(name) or "—"),
            }
            for col in comparison.columns:
                if col == "factor":
                    continue
                item[str(col)] = _fmt_pct(row[col])
            rows.append(item)
        comparison_df = pd.DataFrame(rows)

    method_txt = method if method else "—"
    horizon_txt = f"{horizon}d" if horizon else "—"

    sections_html = "\n".join(
        [
            _render_collapsible("1. 最终权重", _render_table(final_df), open_by_default=True),
            _render_collapsible("2. 多方案权重对比", _render_table(comparison_df), open_by_default=False),
        ]
    )

    html = "\n".join(
        [
            "<!doctype html>",
            "<html lang='zh-CN'>",
            "<head>",
            "<meta charset='utf-8' />",
            f"<title>{escape(title)}</title>",
            _style_block(),
            _script_block(),
            "</head>",
            "<body>",
            "<div class='container'>",
            f"<h1>{escape(title)}</h1>",
            f"<div class='meta'>生成时间: {escape(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))}</div>",
            "<div class='summary'>",
            f"<div class='card'><div class='k'>配权方法</div><div class='v'>{escape(method_txt)}</div></div>",
            f"<div class='card'><div class='k'>配权周期</div><div class='v'>{escape(horizon_txt)}</div></div>",
            f"<div class='card'><div class='k'>有效因子数</div><div class='v'>{len(active)}</div></div>",
            "</div>",
            "<div class='toolbar'>",
            "<button class='btn' type='button' onclick='setAllDetails(true)'>全部展开</button>",
            "<button class='btn secondary' type='button' onclick='setAllDetails(false)'>全部收起</button>",
            "</div>",
            sections_html,
            "</div>",
            "</body>",
            "</html>",
        ]
    )

    output_path.write_text(html, encoding="utf-8")
    logger.info("权重优化 HTML 已生成: %s", output_path)
    return output_path
