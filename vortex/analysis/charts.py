"""
charts.py
回测图表生成模块

职责:
  - 生成净值曲线图 (策略 vs 基准)
  - 生成回撤曲线图
  - 生成月度收益热力图
  - 生成权重分布饼图
  - 支持保存为 PNG 文件
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import matplotlib
matplotlib.use("Agg")  # 无头模式，不依赖 GUI

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

plt.rcParams["font.sans-serif"] = ["Arial Unicode MS", "PingFang SC", "SimHei", "sans-serif"]
plt.rcParams["axes.unicode_minus"] = False
plt.rcParams["figure.dpi"] = 150

logger = logging.getLogger(__name__)


def plot_backtest_summary(
    bt_results: Dict,
    weights: Dict[str, float],
    save_dir: Path,
    run_date: str = "",
) -> List[Path]:
    """
    生成完整回测图表集

    Parameters
    ----------
    bt_results : dict
        {label: BacktestResult} 回测结果字典
    weights : dict
        {factor_name: weight} 最终采用的因子权重
    save_dir : Path
        图表保存目录
    run_date : str
        运行日期标识

    Returns
    -------
    list[Path]
        生成的图表文件路径列表
    """
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"_{run_date}" if run_date else ""
    saved = []

    # 1) 净值曲线对比图
    path = save_dir / f"nav_curve{suffix}.png"
    _plot_nav_curves(bt_results, path)
    saved.append(path)

    # 2) 回撤曲线图 (仅画最佳方案)
    best_label = _pick_best(bt_results)
    if best_label:
        path = save_dir / f"drawdown{suffix}.png"
        _plot_drawdown(bt_results[best_label], best_label, path)
        saved.append(path)

        # 3) 月度收益热力图
        path = save_dir / f"monthly_returns{suffix}.png"
        _plot_monthly_heatmap(bt_results[best_label], best_label, path)
        saved.append(path)

    # 4) 因子权重饼图
    if weights:
        path = save_dir / f"weight_pie{suffix}.png"
        _plot_weight_pie(weights, path)
        saved.append(path)

    # 5) 综合仪表盘 (四合一)
    path = save_dir / f"dashboard{suffix}.png"
    _plot_dashboard(bt_results, weights, best_label, path)
    saved.append(path)

    logger.info("图表已生成 %d 张: %s", len(saved), [p.name for p in saved])
    return saved


# ================================================================
#  各子图绘制
# ================================================================

def _pick_best(bt_results: Dict) -> Optional[str]:
    """选 Sharpe 最高的方案"""
    best, best_sharpe = None, -999
    for label, r in bt_results.items():
        s = r.metrics.get("sharpe_ratio", -999)
        if s > best_sharpe:
            best_sharpe = s
            best = label
    return best


def _parse_dates(date_index):
    """把 YYYYMMDD 字符串索引转为 datetime"""
    return pd.to_datetime(date_index, format="%Y%m%d")


def _plot_nav_curves(bt_results: Dict, save_path: Path):
    """净值曲线对比图"""
    fig, ax = plt.subplots(figsize=(12, 5))

    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]
    for i, (label, result) in enumerate(bt_results.items()):
        nav = result.nav_series
        dates = _parse_dates(nav.index)
        # 归一化到 1
        nav_norm = nav / nav.iloc[0]
        c = colors[i % len(colors)]
        m = result.metrics
        lbl = f"{label} (年化{m.get('annual_return', 0):.1%}, 夏普{m.get('sharpe_ratio', 0):.2f})"
        ax.plot(dates, nav_norm, label=lbl, color=c, linewidth=1.5)

    ax.set_title("策略净值曲线对比", fontsize=14, fontweight="bold")
    ax.set_ylabel("归一化净值")
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(str(save_path))
    plt.close(fig)
    logger.info("净值曲线图: %s", save_path)


def _plot_drawdown(result, label: str, save_path: Path):
    """回撤曲线图"""
    nav = result.nav_series
    dates = _parse_dates(nav.index)
    cummax = nav.cummax()
    drawdown = (cummax - nav) / cummax

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 6), height_ratios=[2, 1], sharex=True)

    # 上半部: 净值
    nav_norm = nav / nav.iloc[0]
    ax1.plot(dates, nav_norm, color="#1f77b4", linewidth=1.5)
    ax1.fill_between(dates, nav_norm, alpha=0.1, color="#1f77b4")
    ax1.set_ylabel("归一化净值")
    ax1.set_title(f"{label} — 净值与回撤", fontsize=14, fontweight="bold")
    ax1.grid(True, alpha=0.3)

    # 下半部: 回撤
    ax2.fill_between(dates, -drawdown * 100, color="#d62728", alpha=0.6)
    ax2.set_ylabel("回撤 (%)")
    ax2.set_xlabel("")
    ax2.grid(True, alpha=0.3)

    max_dd = drawdown.max()
    max_dd_date = drawdown.idxmax()
    ax2.annotate(
        f"最大回撤 {max_dd:.2%}\n{max_dd_date}",
        xy=(_parse_dates([max_dd_date])[0], -max_dd * 100),
        xytext=(10, -20), textcoords="offset points",
        fontsize=8, color="#d62728",
        arrowprops=dict(arrowstyle="->", color="#d62728"),
    )

    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(str(save_path))
    plt.close(fig)
    logger.info("回撤图: %s", save_path)


def _plot_monthly_heatmap(result, label: str, save_path: Path):
    """月度收益热力图"""
    returns = result.returns_series.copy()
    returns.index = _parse_dates(returns.index)

    # 按月聚合
    monthly = returns.groupby([returns.index.year, returns.index.month]).sum()
    monthly.index.names = ["year", "month"]
    df_monthly = monthly.reset_index()
    df_monthly.columns = ["year", "month", "return"]

    pivot = df_monthly.pivot(index="year", columns="month", values="return")
    pivot.columns = [f"{m}月" for m in pivot.columns]

    fig, ax = plt.subplots(figsize=(10, max(3, len(pivot) * 0.8 + 1)))

    data = pivot.values * 100  # 转为百分比
    im = ax.imshow(data, cmap="RdYlGn", aspect="auto", vmin=-10, vmax=10)

    ax.set_xticks(range(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, fontsize=9)
    ax.set_yticks(range(len(pivot.index)))
    ax.set_yticklabels(pivot.index, fontsize=9)

    # 在格子里标注数值
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            val = data[i, j]
            if not np.isnan(val):
                color = "white" if abs(val) > 5 else "black"
                ax.text(j, i, f"{val:.1f}%", ha="center", va="center",
                        fontsize=8, color=color)

    ax.set_title(f"{label} — 月度收益率 (%)", fontsize=14, fontweight="bold")
    fig.colorbar(im, ax=ax, shrink=0.8, label="%")
    fig.tight_layout()
    fig.savefig(str(save_path))
    plt.close(fig)
    logger.info("月度收益热力图: %s", save_path)


def _plot_weight_pie(weights: Dict[str, float], save_path: Path):
    """因子权重饼图"""
    # 过滤掉 0 权重
    w = {k: v for k, v in weights.items() if v > 0.001}
    if not w:
        return

    labels = list(w.keys())
    values = list(w.values())

    # 中文因子名映射
    name_map = {
        "dividend_yield": "股息率",
        "fcf_yield": "自由现金流收益率",
        "roe_ttm": "ROE",
        "delta_roe": "ROE变化",
        "opcfd": "经营现金流/负债",
        "ep": "E/P (盈利收益率)",
    }
    display_labels = [f"{name_map.get(l, l)}\n{v:.0%}" for l, v in zip(labels, values)]

    colors = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948"]
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.pie(
        values,
        labels=display_labels,
        colors=colors[:len(values)],
        startangle=90,
        textprops={"fontsize": 10},
    )
    ax.set_title("因子权重分布", fontsize=14, fontweight="bold")
    fig.tight_layout()
    fig.savefig(str(save_path))
    plt.close(fig)
    logger.info("权重饼图: %s", save_path)


def _plot_dashboard(bt_results: Dict, weights: Dict, best_label: Optional[str], save_path: Path):
    """综合仪表盘 — 四合一大图"""
    fig = plt.figure(figsize=(16, 10))

    gs = fig.add_gridspec(2, 2, hspace=0.35, wspace=0.3)

    # ---- 左上: 净值曲线 ----
    ax1 = fig.add_subplot(gs[0, 0])
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]
    for i, (label, result) in enumerate(bt_results.items()):
        nav = result.nav_series
        dates = _parse_dates(nav.index)
        nav_norm = nav / nav.iloc[0]
        ax1.plot(dates, nav_norm, label=label, color=colors[i % len(colors)], linewidth=1.2)
    ax1.set_title("净值曲线", fontsize=12, fontweight="bold")
    ax1.legend(fontsize=8, loc="upper left")
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    for tick in ax1.xaxis.get_major_ticks():
        tick.label1.set_fontsize(8)
        tick.label1.set_rotation(30)

    # ---- 右上: 回撤 ----
    ax2 = fig.add_subplot(gs[0, 1])
    if best_label and best_label in bt_results:
        nav = bt_results[best_label].nav_series
        dates = _parse_dates(nav.index)
        cummax = nav.cummax()
        dd = (cummax - nav) / cummax
        ax2.fill_between(dates, -dd * 100, color="#d62728", alpha=0.5)
        ax2.set_title(f"回撤 — {best_label}", fontsize=12, fontweight="bold")
        ax2.set_ylabel("%")
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        for tick in ax2.xaxis.get_major_ticks():
            tick.label1.set_fontsize(8)
            tick.label1.set_rotation(30)

    # ---- 左下: 绩效指标表 ----
    ax3 = fig.add_subplot(gs[1, 0])
    ax3.axis("off")
    if bt_results:
        col_labels = ["指标"] + list(bt_results.keys())
        row_data = []
        metric_names = [
            ("annual_return", "年化收益", True),
            ("sharpe_ratio", "夏普比率", False),
            ("max_drawdown", "最大回撤", True),
            ("calmar_ratio", "Calmar比率", False),
            ("annual_volatility", "年化波动", True),
            ("avg_turnover", "平均换手", True),
            ("win_rate", "日胜率", True),
        ]
        for key, cn, is_pct in metric_names:
            row = [cn]
            for label in bt_results:
                v = bt_results[label].metrics.get(key, 0)
                row.append(f"{v:.2%}" if is_pct else f"{v:.3f}")
            row_data.append(row)

        table = ax3.table(
            cellText=row_data,
            colLabels=col_labels,
            cellLoc="center",
            loc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1, 1.5)

        # 表头加粗
        for j in range(len(col_labels)):
            table[(0, j)].set_text_props(fontweight="bold")
            table[(0, j)].set_facecolor("#4e79a7")
            table[(0, j)].set_text_props(color="white", fontweight="bold")

        ax3.set_title("绩效指标", fontsize=12, fontweight="bold", y=0.95)

    # ---- 右下: 权重饼图 ----
    ax4 = fig.add_subplot(gs[1, 1])
    w = {k: v for k, v in weights.items() if v > 0.001}
    if w:
        name_map = {
            "dividend_yield": "股息率",
            "fcf_yield": "FCF收益率",
            "roe_ttm": "ROE",
            "delta_roe": "ΔROE",
            "opcfd": "经营现金流/负债",
            "ep": "E/P",
        }
        labels = [name_map.get(k, k) for k in w]
        pie_colors = ["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948"]
        ax4.pie(
            list(w.values()), labels=labels,
            autopct="%1.0f%%", colors=pie_colors[:len(w)],
            startangle=90, textprops={"fontsize": 9},
        )
        ax4.set_title("因子权重", fontsize=12, fontweight="bold")

    fig.suptitle("QuantPilot 回测仪表盘", fontsize=16, fontweight="bold", y=0.98)
    fig.savefig(str(save_path))
    plt.close(fig)
    logger.info("综合仪表盘: %s", save_path)
