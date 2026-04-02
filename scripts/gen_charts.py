#!/usr/bin/env python3
"""
轻量回测图表生成 — 分两阶段运行，隔离内存
阶段1: 回测 → 存 JSON (回测引擎占用内存在子进程结束后释放)
阶段2: 从 JSON 读数据 → matplotlib 绘图
"""
import gc
import json
import os
import subprocess
import sys
from pathlib import Path

os.environ["MPLBACKEND"] = "Agg"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def run_backtest_phase():
    """阶段1: 回测，保存数据到 JSON"""
    from vortex.config.settings import Settings, setup_logging
    setup_logging("INFO")

    cfg = Settings()
    cfg.validate()

    from vortex.core.data.datastore import DataStore
    from vortex.core.factorhub import FactorHub
    from vortex.core.signalbus import SignalBus
    from vortex.executor.backtest import BacktestEngine
    from vortex.strategy.dividend import DEFAULT_WEIGHTS, DividendQualityFCFStrategy
    from vortex.utils.date_utils import get_recent_trade_dates, load_trade_cal, today_str

    ds = DataStore(cfg)
    fh = FactorHub(ds)
    fh.register_all_defaults()

    recent = get_recent_trade_dates(today_str(), cfg.data_dir, n=1)
    run_date = recent[-1] if recent else today_str()
    cal = sorted([d.strftime("%Y%m%d") for d in load_trade_cal(cfg.data_dir)])
    bt_end = run_date
    cands = [d for d in cal if d <= bt_end]
    bt_start = cands[-252] if len(cands) > 252 else cands[0]

    weights_file = cfg.data_dir / "reports" / f"weights_{run_date}.json"
    if weights_file.exists():
        saved = json.loads(weights_file.read_text())
        final_weights = saved["weights"]
    else:
        final_weights = DEFAULT_WEIGHTS

    weight_sets = {
        "默认权重": DEFAULT_WEIGHTS,
        "等权": {f: 1 / 6 for f in DEFAULT_WEIGHTS},
    }

    engine = BacktestEngine(ds)
    all_data = {}

    for label, weights in weight_sets.items():
        print(f"回测: {label}", flush=True)
        bus = SignalBus(cfg.data_dir)
        strat = DividendQualityFCFStrategy(ds, fh, bus, weights=weights)
        result = engine.run(strat, bt_start, bt_end, freq="M")
        all_data[label] = {
            "nav": {k: float(v) for k, v in result.nav_series.items()},
            "returns": {k: float(v) for k, v in result.returns_series.items()},
            "metrics": result.metrics,
        }
        m = result.metrics
        print(f"  年化: {m['annual_return']:.2%}, 夏普: {m['sharpe_ratio']:.3f}", flush=True)
        del result, strat, bus, optimizer
        gc.collect()

    tmp = cfg.data_dir / "reports" / "_chart_data.json"
    tmp.write_text(json.dumps({
        "run_date": run_date,
        "weights": final_weights,
        "results": all_data,
    }, ensure_ascii=False))
    print(f"DATA_FILE={tmp}", flush=True)


def plot_phase(data_file: str):
    """阶段2: 从 JSON 绘图"""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import numpy as np
    import pandas as pd

    plt.rcParams["font.sans-serif"] = ["Arial Unicode MS", "PingFang SC", "SimHei"]
    plt.rcParams["axes.unicode_minus"] = False
    plt.rcParams["figure.dpi"] = 150

    data = json.loads(Path(data_file).read_text())
    run_date = data["run_date"]
    weights = data["weights"]
    results = data["results"]
    save_dir = Path(data_file).parent
    sfx = f"_{run_date}"
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]

    def pdates(keys):
        return pd.to_datetime(list(keys), format="%Y%m%d")

    best_label = max(results, key=lambda k: results[k]["metrics"].get("sharpe_ratio", -999))

    # -- 1) 净值曲线 --
    fig, ax = plt.subplots(figsize=(12, 5))
    for i, (lb, rd) in enumerate(results.items()):
        nav = pd.Series(rd["nav"])
        dt = pdates(nav.index)
        nav_base = rd["metrics"].get("initial_capital", float(nav.iloc[0]))
        nav_n = nav / nav_base
        m = rd["metrics"]
        ax.plot(dt, nav_n, label=f"{lb} (年化{m['annual_return']:.1%}, 夏普{m['sharpe_ratio']:.2f})",
                color=colors[i % len(colors)], linewidth=1.5)
    ax.set_title("策略净值曲线对比", fontsize=14, fontweight="bold")
    ax.set_ylabel("归一化净值")
    ax.legend(loc="upper left", fontsize=9)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate(); fig.tight_layout()
    p1 = save_dir / f"nav_curve{sfx}.png"
    fig.savefig(p1); plt.close(fig)
    print(f"[1/5] {p1.name}", flush=True)

    # -- 2) 回撤 --
    nav = pd.Series(results[best_label]["nav"])
    dates = pdates(nav.index)
    cmx = nav.cummax()
    dd = (cmx - nav) / cmx

    fig, (a1, a2) = plt.subplots(2, 1, figsize=(12, 6), height_ratios=[2, 1], sharex=True)
    nav_base = results[best_label]["metrics"].get("initial_capital", float(nav.iloc[0]))
    nav_n = nav / nav_base
    a1.plot(dates, nav_n, color="#1f77b4", linewidth=1.5)
    a1.fill_between(dates, nav_n, alpha=0.1, color="#1f77b4")
    a1.set_ylabel("归一化净值")
    a1.set_title(f"{best_label} — 净值与回撤", fontsize=14, fontweight="bold")
    a1.grid(True, alpha=0.3)

    a2.fill_between(dates, -dd * 100, color="#d62728", alpha=0.6)
    a2.set_ylabel("回撤 (%)")
    a2.grid(True, alpha=0.3)
    mdd = dd.max(); mdd_d = dd.idxmax()
    mdd_dt = pd.to_datetime(mdd_d, format="%Y%m%d")
    a2.annotate(f"最大回撤 {mdd:.2%}\n{mdd_d}", xy=(mdd_dt, -mdd * 100),
                xytext=(10, -20), textcoords="offset points", fontsize=8, color="#d62728",
                arrowprops=dict(arrowstyle="->", color="#d62728"))
    a2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    fig.autofmt_xdate(); fig.tight_layout()
    p2 = save_dir / f"drawdown{sfx}.png"
    fig.savefig(p2); plt.close(fig)
    print(f"[2/5] {p2.name}", flush=True)

    # -- 3) 月度热力图 --
    rets = pd.Series(results[best_label]["returns"])
    rets.index = pdates(rets.index)
    monthly = rets.groupby([rets.index.year, rets.index.month]).sum()
    monthly.index.names = ["year", "month"]
    df_m = monthly.reset_index(); df_m.columns = ["year", "month", "return"]
    piv = df_m.pivot(index="year", columns="month", values="return")
    piv.columns = [f"{m}月" for m in piv.columns]

    fig, ax = plt.subplots(figsize=(10, max(3, len(piv) * 0.8 + 1)))
    vals = piv.values * 100
    im = ax.imshow(vals, cmap="RdYlGn", aspect="auto", vmin=-10, vmax=10)
    ax.set_xticks(range(len(piv.columns))); ax.set_xticklabels(piv.columns, fontsize=9)
    ax.set_yticks(range(len(piv.index))); ax.set_yticklabels(piv.index, fontsize=9)
    for ri in range(vals.shape[0]):
        for ci in range(vals.shape[1]):
            v = vals[ri, ci]
            if not np.isnan(v):
                ax.text(ci, ri, f"{v:.1f}%", ha="center", va="center", fontsize=8,
                        color="white" if abs(v) > 5 else "black")
    ax.set_title(f"{best_label} — 月度收益率 (%)", fontsize=14, fontweight="bold")
    fig.colorbar(im, ax=ax, shrink=0.8, label="%")
    fig.tight_layout()
    p3 = save_dir / f"monthly_returns{sfx}.png"
    fig.savefig(p3); plt.close(fig)
    print(f"[3/5] {p3.name}", flush=True)

    # -- 4) 因子权重饼图 --
    w = {k: v for k, v in weights.items() if v > 0.001}
    nm = {"dividend_yield": "股息率", "fcf_yield": "FCF收益率", "roe_ttm": "ROE",
          "delta_roe": "ΔROE", "opcfd": "OPCFD", "ep": "E/P"}
    fig, ax = plt.subplots(figsize=(7, 7))
    ax.pie(list(w.values()), labels=[f"{nm.get(k, k)}\n{v:.0%}" for k, v in w.items()],
           colors=["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948"][:len(w)],
           startangle=90, textprops={"fontsize": 10})
    ax.set_title("因子权重分布", fontsize=14, fontweight="bold")
    fig.tight_layout()
    p4 = save_dir / f"weight_pie{sfx}.png"
    fig.savefig(p4); plt.close(fig)
    print(f"[4/5] {p4.name}", flush=True)

    # -- 5) 综合仪表盘 --
    fig = plt.figure(figsize=(16, 10))
    gs = fig.add_gridspec(2, 2, hspace=0.35, wspace=0.3)

    ax1 = fig.add_subplot(gs[0, 0])
    for i, (lb, rd) in enumerate(results.items()):
        ns = pd.Series(rd["nav"])
        nav_base = rd["metrics"].get("initial_capital", float(ns.iloc[0]))
        ax1.plot(pdates(ns.index), ns / nav_base, label=lb, color=colors[i % len(colors)], lw=1.2)
    ax1.set_title("净值曲线", fontsize=12, fontweight="bold")
    ax1.legend(fontsize=8); ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    for t in ax1.xaxis.get_major_ticks(): t.label1.set_fontsize(8); t.label1.set_rotation(30)

    ax2 = fig.add_subplot(gs[0, 1])
    ax2.fill_between(dates, -dd * 100, color="#d62728", alpha=0.5)
    ax2.set_title(f"回撤 — {best_label}", fontsize=12, fontweight="bold")
    ax2.set_ylabel("%"); ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    for t in ax2.xaxis.get_major_ticks(): t.label1.set_fontsize(8); t.label1.set_rotation(30)

    ax3 = fig.add_subplot(gs[1, 0]); ax3.axis("off")
    col_labels = ["指标"] + list(results.keys())
    mns = [("annual_return", "年化收益", True), ("sharpe_ratio", "夏普比率", False),
           ("max_drawdown", "最大回撤", True), ("calmar_ratio", "Calmar", False),
           ("annual_volatility", "年化波动", True), ("avg_turnover", "平均换手", True),
           ("win_rate", "日胜率", True)]
    rows = []
    for key, cn, ip in mns:
        row = [cn]
        for lb in results:
            v = results[lb]["metrics"].get(key, 0)
            row.append(f"{v:.2%}" if ip else f"{v:.3f}")
        rows.append(row)
    tbl = ax3.table(cellText=rows, colLabels=col_labels, cellLoc="center", loc="center")
    tbl.auto_set_font_size(False); tbl.set_fontsize(9); tbl.scale(1, 1.5)
    for j in range(len(col_labels)):
        tbl[(0, j)].set_facecolor("#4e79a7")
        tbl[(0, j)].set_text_props(color="white", fontweight="bold")
    ax3.set_title("绩效指标", fontsize=12, fontweight="bold", y=0.95)

    ax4 = fig.add_subplot(gs[1, 1])
    ax4.pie(list(w.values()), labels=[nm.get(k, k) for k in w], autopct="%1.0f%%",
            colors=["#4e79a7", "#f28e2b", "#e15759", "#76b7b2", "#59a14f", "#edc948"][:len(w)],
            startangle=90, textprops={"fontsize": 9})
    ax4.set_title("因子权重", fontsize=12, fontweight="bold")

    fig.suptitle("QuantPilot 回测仪表盘", fontsize=16, fontweight="bold", y=0.98)
    p5 = save_dir / f"dashboard{sfx}.png"
    fig.savefig(p5); plt.close(fig)
    print(f"[5/5] {p5.name}", flush=True)

    # 清理
    Path(data_file).unlink(missing_ok=True)
    print("\n全部图表生成完成!")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--plot":
        plot_phase(sys.argv[2])
    elif len(sys.argv) > 1 and sys.argv[1] == "--backtest":
        run_backtest_phase()
    else:
        # 分两个子进程顺序执行，隔离内存
        print("=== 阶段1: 回测 ===", flush=True)
        r1 = subprocess.run(
            [sys.executable, __file__, "--backtest"],
            capture_output=True, text=True, cwd=str(PROJECT_ROOT),
        )
        print(r1.stdout)
        if r1.returncode != 0:
            print(f"回测失败 (code {r1.returncode}):\n{r1.stderr}")
            sys.exit(1)

        data_file = None
        for line in r1.stdout.strip().split("\n"):
            if line.startswith("DATA_FILE="):
                data_file = line.split("=", 1)[1]
        if not data_file:
            print("错误: 未获取到数据文件")
            sys.exit(1)

        print(f"\n=== 阶段2: 绘图 ===", flush=True)
        r2 = subprocess.run(
            [sys.executable, __file__, "--plot", data_file],
            capture_output=True, text=True, cwd=str(PROJECT_ROOT),
        )
        print(r2.stdout)
        if r2.returncode != 0:
            print(f"绘图失败 (code {r2.returncode}):\n{r2.stderr}")
            sys.exit(1)
