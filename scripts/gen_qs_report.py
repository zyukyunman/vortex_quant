#!/usr/bin/env python3
"""
gen_qs_report.py
使用 quantstats 生成专业级回测报告

用法:
  1. 独立运行 (读取 _chart_data.json):
     python scripts/gen_qs_report.py

  2. 被 auto_pipeline 调用:
     from scripts.gen_qs_report import generate_quantstats_report
     generate_quantstats_report(bt_result, benchmark_code="000300.SH", output_dir=...)
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logger = logging.getLogger(__name__)

BENCHMARK_NAMES = {
    "000300.SH": "沪深300",
    "000905.SH": "中证500",
    "000852.SH": "中证1000",
    "932000.CSI": "中证2000",
    "000922.CSI": "中证红利",
    "000016.SH": "上证50",
    "000985.CSI": "中证全指",
}


def _to_datetime_index(series: pd.Series) -> pd.Series:
    """将 YYYYMMDD 字符串 index 转为 DatetimeIndex"""
    new_idx = pd.to_datetime(series.index, format="%Y%m%d")
    series.index = new_idx
    return series


def generate_quantstats_report(
    returns: pd.Series,
    benchmark: pd.Series | None = None,
    title: str = "QuantPilot 回测报告",
    output_path: str | Path | None = None,
) -> Path | None:
    """
    使用 quantstats 生成 HTML 回测报告

    Parameters
    ----------
    returns : pd.Series
        策略日收益率(index=YYYYMMDD 或 DatetimeIndex)
    benchmark : pd.Series, optional
        基准日收益率
    title : str
        报告标题
    output_path : str or Path, optional
        输出文件路径, 不传则自动生成

    Returns
    -------
    Path or None
        生成的文件路径
    """
    try:
        import quantstats as qs
    except ImportError:
        logger.error("quantstats 未安装, 请执行: pip install quantstats")
        return None

    # 确保 DatetimeIndex
    if not isinstance(returns.index, pd.DatetimeIndex):
        returns = _to_datetime_index(returns)
    if benchmark is not None and not isinstance(benchmark.index, pd.DatetimeIndex):
        benchmark = _to_datetime_index(benchmark)

    # 对齐
    if benchmark is not None:
        common = returns.index.intersection(benchmark.index)
        returns = returns.loc[common]
        benchmark = benchmark.loc[common]

    # 输出路径
    if output_path is None:
        report_dir = PROJECT_ROOT / "data" / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        output_path = report_dir / "quantstats_report.html"
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 生成报告
    logger.info("生成 quantstats 报告 → %s", output_path)
    qs.reports.html(
        returns,
        benchmark=benchmark,
        title=title,
        output=str(output_path),
    )
    logger.info("quantstats 报告生成完成: %s", output_path)
    return output_path


def generate_from_backtest_result(bt_result, benchmark_name: str = "沪深300", output_dir: Path | None = None) -> Path | None:
    """
    从 BacktestResult 对象直接生成 quantstats 报告

    Parameters
    ----------
    bt_result : BacktestResult
        回测结果
    benchmark_name : str
        基准名称 (需在 bt_result.benchmark_returns 中存在)
    output_dir : Path
        输出目录
    """
    returns = bt_result.returns_series

    benchmark = None
    if bt_result.benchmark_returns and benchmark_name in bt_result.benchmark_returns:
        benchmark = bt_result.benchmark_returns[benchmark_name]

    if output_dir is None:
        output_dir = PROJECT_ROOT / "data" / "reports"

    run_date = bt_result.metrics.get("end_date", "unknown")
    output_path = output_dir / f"quantstats_{run_date}.html"

    return generate_quantstats_report(
        returns=returns,
        benchmark=benchmark,
        title=f"QuantPilot 回测报告 — {run_date}",
        output_path=output_path,
    )


def main():
    """从 _chart_data.json 读取数据并生成 quantstats 报告"""
    data_file = PROJECT_ROOT / "data" / "reports" / "_chart_data.json"
    if not data_file.exists():
        print(f"未找到数据文件: {data_file}")
        print("请先执行 auto_pipeline.py 生成回测数据")
        sys.exit(1)

    with open(data_file) as f:
        data = json.load(f)

    run_date = data["run_date"]
    results = data["results"]

    # 选最佳方案
    best_label = max(results, key=lambda k: results[k]["metrics"].get("sharpe_ratio", -999))
    best = results[best_label]

    # 构建收益率序列
    returns = pd.Series(best["returns"], dtype=float)
    returns = _to_datetime_index(returns)

    # 尝试加载基准
    benchmark = None
    try:
        from vortex.config.settings import Settings
        from vortex.core.datastore import DataStore
        cfg = Settings()
        ds = DataStore(cfg)
        start_date = min(best["returns"].keys())
        end_date = max(best["returns"].keys())
        df_idx = ds.get_index_daily(ts_code="000300.SH", start_date=start_date, end_date=end_date)
        if not df_idx.empty:
            df_idx = df_idx.sort_values("trade_date")
            benchmark = df_idx.set_index("trade_date")["pct_chg"] / 100.0
            benchmark = _to_datetime_index(benchmark)
            logger.info("已加载沪深300基准: %d 日", len(benchmark))
    except Exception as e:
        logger.warning("加载基准失败: %s, 将生成无基准报告", e)

    output_path = PROJECT_ROOT / "data" / "reports" / f"quantstats_{run_date}.html"
    generate_quantstats_report(
        returns=returns,
        benchmark=benchmark,
        title=f"QuantPilot {best_label} — {run_date}",
        output_path=output_path,
    )
    print(f"报告已生成: {output_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
    main()
