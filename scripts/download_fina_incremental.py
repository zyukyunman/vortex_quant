#!/usr/bin/env python3
"""
download_fina_incremental.py
增量下载财务数据 — 仅下载缺失报告期, 与已有数据合并

优势: 不会覆盖已下载的 2021-2025 数据, 只补充 2014-2020
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from vortex.config.settings import Settings, setup_logging
setup_logging("INFO")

from vortex.core.datastore import DataStore

import logging
logger = logging.getLogger(__name__)


def download_incremental(ds: DataStore, table_name: str, api_func_name: str,
                         fields: str, target_periods: list,
                         dedup_cols: list):
    """增量下载一个财务表"""
    path = ds.data_dir / "fundamental" / f"{table_name}.parquet"

    # 读已有数据
    existing = pd.DataFrame()
    existing_periods = set()
    if path.exists():
        existing = pd.read_parquet(path)
        existing_periods = set(existing["end_date"].unique())
        logger.info("[%s] 已有报告期: %s", table_name, sorted(existing_periods))

    missing = sorted(set(target_periods) - existing_periods)
    if not missing:
        logger.info("[%s] 无缺失报告期, 跳过", table_name)
        return

    logger.info("[%s] 缺失报告期: %s, 开始下载...", table_name, missing)

    stocks = ds._load_stock_list()
    all_dfs = []
    t0 = time.time()

    for i, code in enumerate(stocks):
        df = ds._api_call(api_func_name, ts_code=code, fields=fields)
        if not df.empty:
            df = df[df["end_date"].isin(missing)]
            if not df.empty:
                all_dfs.append(df)
        if (i + 1) % 500 == 0:
            elapsed = time.time() - t0
            eta = elapsed / (i + 1) * (len(stocks) - i - 1)
            logger.info("  [%s] 进度: %d/%d (%.0fs elapsed, ETA %.0fs)",
                       table_name, i + 1, len(stocks), elapsed, eta)

    if not all_dfs:
        logger.warning("[%s] 无新数据", table_name)
        return

    new_data = pd.concat(all_dfs, ignore_index=True)
    logger.info("[%s] 新增 %d 行", table_name, len(new_data))

    # 合并
    if not existing.empty:
        combined = pd.concat([existing, new_data], ignore_index=True)
    else:
        combined = new_data

    combined = combined.drop_duplicates(subset=dedup_cols, keep="first")
    combined.to_parquet(path, index=False)
    logger.info("[%s] 合并后 %d 行 → %s", table_name, len(combined), path)


def main():
    cfg = Settings()
    cfg.validate()
    ds = DataStore(cfg)

    # 目标: 2014-2025 所有年报期
    target_periods = [f"{y}1231" for y in range(2014, 2026)]

    print("=" * 60)
    print("  增量财务数据下载")
    print(f"  目标报告期: {target_periods[0]} ~ {target_periods[-1]}")
    print("=" * 60)

    t0 = time.time()

    # 1. 财务指标
    download_incremental(
        ds, "fina_indicator", "fina_indicator",
        "ts_code,ann_date,end_date,"
        "roe,roe_dt,roe_waa,grossprofit_margin,"
        "profit_dedt,netprofit_yoy,or_yoy,q_profit_yoy,"
        "equity_yoy,debt_to_assets,op_yoy,ocfps,cfps",
        target_periods,
        ["ts_code", "end_date"],
    )

    # 2. 利润表
    download_incremental(
        ds, "income", "income",
        "ts_code,ann_date,end_date,report_type,"
        "revenue,operate_profit,n_income,n_income_attr_p,"
        "total_profit,ebit",
        target_periods,
        ["ts_code", "end_date", "report_type"],
    )

    # 3. 现金流量表
    download_incremental(
        ds, "cashflow", "cashflow",
        "ts_code,ann_date,end_date,report_type,"
        "n_cashflow_act,c_pay_acq_const_fiolta,"
        "n_cashflow_inv_act,n_cash_flows_fnc_act,free_cashflow",
        target_periods,
        ["ts_code", "end_date", "report_type"],
    )

    # 4. 资产负债表
    download_incremental(
        ds, "balancesheet", "balancesheet",
        "ts_code,ann_date,end_date,report_type,"
        "total_assets,total_hldr_eqy_exc_min_int,total_liab,"
        "total_cur_assets,total_cur_liab,total_share",
        target_periods,
        ["ts_code", "end_date", "report_type"],
    )

    elapsed = time.time() - t0
    print(f"\n完成! 总耗时 {elapsed/60:.1f} 分钟")


if __name__ == "__main__":
    main()
