"""
datastore.py
L1 数据层 — 数据采集、清洗、存储、查询

职责:
  - 通过 Tushare Pro 下载 A 股数据
  - Parquet 分类存储
  - DuckDB SQL 查询接口
  - 增量更新 + 防重复

依赖: 无业务模块依赖，只依赖 config
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import pandas as pd
import tushare as ts

from config.settings import Settings

logger = logging.getLogger(__name__)


class DataStore:
    """共享数据中心 — 唯一的数据写入者"""

    def __init__(self, settings: Settings):
        self.cfg = settings
        self.data_dir = settings.data_dir
        self.pro = ts.pro_api(settings.tushare_token)
        self.conn = duckdb.connect()

        # 创建子目录
        for sub in [
            "market/daily", "market/adj_factor",
            "fundamental", "fundamental/valuation",
            "meta", "factor", "signal",
            "portfolio", "risk", "execution",
        ]:
            (self.data_dir / sub).mkdir(parents=True, exist_ok=True)

    # ================================================================
    #  Tushare API 封装 (带重试 + 限频)
    # ================================================================
    def _api_call(self, func_name: str, **kwargs) -> pd.DataFrame:
        """统一 Tushare API 调用，自动重试 + 限频"""
        for attempt in range(1, self.cfg.tushare_retry + 1):
            try:
                time.sleep(self.cfg.tushare_pause)
                df = getattr(self.pro, func_name)(**kwargs)
                return df if df is not None else pd.DataFrame()
            except Exception as e:
                logger.warning(
                    "Tushare %s 第%d次失败: %s, kwargs=%s",
                    func_name, attempt, e, kwargs,
                )
                if attempt < self.cfg.tushare_retry:
                    time.sleep(2 ** attempt)
        logger.error("Tushare %s 最终失败, kwargs=%s", func_name, kwargs)
        return pd.DataFrame()

    # ================================================================
    #  下载方法 — 写入 Parquet
    # ================================================================

    def download_stock_basic(self) -> pd.DataFrame:
        """下载股票基本信息 → meta/stock_basic.parquet"""
        logger.info("下载股票基本信息...")
        df = self._api_call(
            "stock_basic",
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,area,industry,market,"
                   "list_date,is_hs,curr_type,act_name,act_ent_type",
        )
        if df.empty:
            return df
        path = self.data_dir / "meta" / "stock_basic.parquet"
        df.to_parquet(path, index=False)
        logger.info("股票基本信息: %d 只 → %s", len(df), path)
        return df

    def download_trade_cal(
        self, start_date: str = "20100101", end_date: str = "20271231"
    ) -> pd.DataFrame:
        """下载交易日历 → meta/trade_cal.parquet"""
        logger.info("下载交易日历 %s ~ %s ...", start_date, end_date)
        df = self._api_call(
            "trade_cal",
            exchange="SSE",
            start_date=start_date,
            end_date=end_date,
            fields="cal_date,is_open",
        )
        if df.empty:
            return df
        path = self.data_dir / "meta" / "trade_cal.parquet"
        df.to_parquet(path, index=False)
        logger.info("交易日历: %d 天 → %s", len(df), path)
        return df

    def download_daily(
        self, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        下载日线行情(按交易日遍历) → market/daily/{year}.parquet

        包含字段: ts_code, trade_date, open, high, low, close, vol, amount, pct_chg
        """
        logger.info("下载日线行情 %s ~ %s ...", start_date, end_date)
        trade_dates = self._get_trade_dates_range(start_date, end_date)
        all_dfs = []
        for i, td in enumerate(trade_dates):
            df = self._api_call("daily", trade_date=td)
            if not df.empty:
                all_dfs.append(df)
            if (i + 1) % 100 == 0:
                logger.info("  日线进度: %d / %d", i + 1, len(trade_dates))

        if not all_dfs:
            return pd.DataFrame()
        result = pd.concat(all_dfs, ignore_index=True)
        # 按年分片存储
        self._save_by_year(result, "trade_date", self.data_dir / "market" / "daily")
        logger.info("日线行情: %d 行 已保存", len(result))
        return result

    def download_daily_basic(
        self, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        下载每日估值指标 → fundamental/valuation/{year}.parquet

        包含字段: ts_code, trade_date, pe_ttm, pb, ps_ttm, dv_ratio, dv_ttm,
                  total_mv, circ_mv, turnover_rate_f
        """
        logger.info("下载每日估值 %s ~ %s ...", start_date, end_date)
        trade_dates = self._get_trade_dates_range(start_date, end_date)
        all_dfs = []
        for i, td in enumerate(trade_dates):
            df = self._api_call(
                "daily_basic",
                trade_date=td,
                fields="ts_code,trade_date,pe_ttm,pb,ps_ttm,"
                       "dv_ratio,dv_ttm,total_mv,circ_mv,turnover_rate_f",
            )
            if not df.empty:
                all_dfs.append(df)
            if (i + 1) % 100 == 0:
                logger.info("  估值进度: %d / %d", i + 1, len(trade_dates))

        if not all_dfs:
            return pd.DataFrame()
        result = pd.concat(all_dfs, ignore_index=True)
        self._save_by_year(
            result, "trade_date", self.data_dir / "fundamental" / "valuation"
        )
        logger.info("每日估值: %d 行 已保存", len(result))
        return result

    def download_dividend(self) -> pd.DataFrame:
        """
        下载全部股票分红数据 → fundamental/dividend.parquet

        逐股票请求，数据量不大但调用次数多。
        字段: ts_code, end_date, ann_date, div_proc, cash_div, cash_div_tax,
              stk_div, record_date, ex_date, pay_date
        """
        logger.info("下载分红数据(逐股遍历)...")
        stocks = self._load_stock_list()
        all_dfs = []
        for i, code in enumerate(stocks):
            df = self._api_call(
                "dividend",
                ts_code=code,
                fields="ts_code,end_date,ann_date,div_proc,"
                       "cash_div,cash_div_tax,stk_div,stk_bo_rate,stk_co_rate,"
                       "record_date,ex_date,pay_date,base_share",
            )
            if not df.empty:
                all_dfs.append(df)
            if (i + 1) % 500 == 0:
                logger.info("  分红进度: %d / %d", i + 1, len(stocks))

        if not all_dfs:
            return pd.DataFrame()
        result = pd.concat(all_dfs, ignore_index=True)
        path = self.data_dir / "fundamental" / "dividend.parquet"
        result.to_parquet(path, index=False)
        logger.info("分红数据: %d 行 → %s", len(result), path)
        return result

    def download_fina_indicator(self, periods: Optional[List[str]] = None) -> pd.DataFrame:
        """
        下载财务指标 → fundamental/fina_indicator.parquet

        逐股票请求 (低权限 Tushare 账号不支持按 period 批量查询)。
        """
        if periods is None:
            periods = self._recent_periods(n_years=5)
        logger.info("下载财务指标(逐股遍历), 报告期: %s", periods)
        stocks = self._load_stock_list()
        all_dfs = []
        for i, code in enumerate(stocks):
            df = self._api_call(
                "fina_indicator",
                ts_code=code,
                fields="ts_code,ann_date,end_date,"
                       "roe,roe_dt,roe_waa,grossprofit_margin,"
                       "profit_dedt,netprofit_yoy,or_yoy,q_profit_yoy,"
                       "equity_yoy,debt_to_assets,"
                       "op_yoy,ocfps,cfps",
            )
            if not df.empty:
                # 只保留目标报告期
                df = df[df["end_date"].isin(periods)]
                if not df.empty:
                    all_dfs.append(df)
            if (i + 1) % 500 == 0:
                logger.info("  财务指标进度: %d / %d", i + 1, len(stocks))
        if not all_dfs:
            return pd.DataFrame()
        result = pd.concat(all_dfs, ignore_index=True).drop_duplicates(
            subset=["ts_code", "end_date"], keep="first"
        )
        path = self.data_dir / "fundamental" / "fina_indicator.parquet"
        result.to_parquet(path, index=False)
        logger.info("财务指标: %d 行 → %s", len(result), path)
        return result

    def download_income(self, periods: Optional[List[str]] = None) -> pd.DataFrame:
        """下载利润表 → fundamental/income.parquet (逐股遍历)"""
        if periods is None:
            periods = self._recent_periods(n_years=5)
        logger.info("下载利润表(逐股遍历), 报告期: %s", periods)
        stocks = self._load_stock_list()
        all_dfs = []
        for i, code in enumerate(stocks):
            df = self._api_call(
                "income",
                ts_code=code,
                fields="ts_code,ann_date,end_date,report_type,"
                       "revenue,operate_profit,n_income,n_income_attr_p,"
                       "total_profit,ebit",
            )
            if not df.empty:
                df = df[df["end_date"].isin(periods)]
                if not df.empty:
                    all_dfs.append(df)
            if (i + 1) % 500 == 0:
                logger.info("  利润表进度: %d / %d", i + 1, len(stocks))
        if not all_dfs:
            return pd.DataFrame()
        result = pd.concat(all_dfs, ignore_index=True).drop_duplicates(
            subset=["ts_code", "end_date", "report_type"], keep="first"
        )
        path = self.data_dir / "fundamental" / "income.parquet"
        result.to_parquet(path, index=False)
        logger.info("利润表: %d 行 → %s", len(result), path)
        return result

    def download_cashflow(self, periods: Optional[List[str]] = None) -> pd.DataFrame:
        """下载现金流量表 → fundamental/cashflow.parquet (逐股遍历)"""
        if periods is None:
            periods = self._recent_periods(n_years=5)
        logger.info("下载现金流量表(逐股遍历), 报告期: %s", periods)
        stocks = self._load_stock_list()
        all_dfs = []
        for i, code in enumerate(stocks):
            df = self._api_call(
                "cashflow",
                ts_code=code,
                fields="ts_code,ann_date,end_date,report_type,"
                       "n_cashflow_act,c_pay_acq_const_fiolta,"
                       "n_cashflow_inv_act,n_cash_flows_fnc_act,"
                       "free_cashflow",
            )
            if not df.empty:
                df = df[df["end_date"].isin(periods)]
                if not df.empty:
                    all_dfs.append(df)
            if (i + 1) % 500 == 0:
                logger.info("  现金流量表进度: %d / %d", i + 1, len(stocks))
        if not all_dfs:
            return pd.DataFrame()
        result = pd.concat(all_dfs, ignore_index=True).drop_duplicates(
            subset=["ts_code", "end_date", "report_type"], keep="first"
        )
        path = self.data_dir / "fundamental" / "cashflow.parquet"
        result.to_parquet(path, index=False)
        logger.info("现金流量表: %d 行 → %s", len(result), path)
        return result

    def download_balancesheet(self, periods: Optional[List[str]] = None) -> pd.DataFrame:
        """下载资产负债表 → fundamental/balancesheet.parquet (逐股遍历)"""
        if periods is None:
            periods = self._recent_periods(n_years=5)
        logger.info("下载资产负债表(逐股遍历), 报告期: %s", periods)
        stocks = self._load_stock_list()
        all_dfs = []
        for i, code in enumerate(stocks):
            df = self._api_call(
                "balancesheet",
                ts_code=code,
                fields="ts_code,ann_date,end_date,report_type,"
                       "total_assets,total_liab,"
                       "total_hldr_eqy_inc_min,total_hldr_eqy_exc_min,"
                       "money_cap,notes_receiv,accounts_receiv,"
                       "total_cur_assets,total_cur_liab",
            )
            if not df.empty:
                df = df[df["end_date"].isin(periods)]
                if not df.empty:
                    all_dfs.append(df)
            if (i + 1) % 500 == 0:
                logger.info("  资产负债表进度: %d / %d", i + 1, len(stocks))
        if not all_dfs:
            return pd.DataFrame()
        result = pd.concat(all_dfs, ignore_index=True).drop_duplicates(
            subset=["ts_code", "end_date", "report_type"], keep="first"
        )
        path = self.data_dir / "fundamental" / "balancesheet.parquet"
        result.to_parquet(path, index=False)
        logger.info("资产负债表: %d 行 → %s", len(result), path)
        return result

    def download_index_daily(
        self,
        index_codes: Optional[List[str]] = None,
        start_date: str = "20100101",
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        下载指数日线行情 → market/index_daily/{ts_code}.parquet

        默认下载: 沪深300, 中证500, 中证1000, 中证2000, 中证红利, 上证50, 中证全指
        """
        if index_codes is None:
            index_codes = [
                "000300.SH",  # 沪深300
                "000905.SH",  # 中证500
                "000852.SH",  # 中证1000
                "932000.CSI", # 中证2000
                "000922.CSI", # 中证红利
                "000016.SH",  # 上证50
                "000985.CSI", # 中证全指
            ]
        if end_date is None:
            from datetime import datetime
            end_date = datetime.now().strftime("%Y%m%d")

        out_dir = self.data_dir / "market" / "index_daily"
        out_dir.mkdir(parents=True, exist_ok=True)

        logger.info("下载指数日线 %s ~ %s, %d 只指数...", start_date, end_date, len(index_codes))
        all_dfs = []
        for code in index_codes:
            df = self._api_call(
                "index_daily",
                ts_code=code,
                start_date=start_date,
                end_date=end_date,
                fields="ts_code,trade_date,open,high,low,close,vol,amount,pct_chg",
            )
            if not df.empty:
                path = out_dir / f"{code.replace('.', '_')}.parquet"
                df.to_parquet(path, index=False)
                all_dfs.append(df)
                logger.info("  %s: %d 行", code, len(df))
            time.sleep(self.cfg.tushare_pause)

        if not all_dfs:
            return pd.DataFrame()
        result = pd.concat(all_dfs, ignore_index=True)
        logger.info("指数日线: 共 %d 行", len(result))
        return result

    def download_index_classify(self) -> pd.DataFrame:
        """
        下载申万行业分类 → meta/index_classify.parquet

        字段: index_code, industry_name, level (L1/L2/L3), src (SW=申万)
        """
        logger.info("下载申万行业分类...")
        all_dfs = []
        for level in ["L1", "L2"]:
            df = self._api_call(
                "index_classify",
                level=level,
                src="SW",
            )
            if not df.empty:
                df["level"] = level
                all_dfs.append(df)
                logger.info("  申万%s: %d 个行业", level, len(df))

        if not all_dfs:
            return pd.DataFrame()
        result = pd.concat(all_dfs, ignore_index=True)
        path = self.data_dir / "meta" / "index_classify.parquet"
        result.to_parquet(path, index=False)
        logger.info("申万行业分类: %d 行 → %s", len(result), path)
        return result

    def download_index_member(self, index_codes: Optional[List[str]] = None) -> pd.DataFrame:
        """
        下载申万行业成分股 → meta/index_member.parquet

        逐行业请求每个行业的成分股列表。
        """
        logger.info("下载申万行业成分股...")
        # 先获取行业分类
        classify = self.get_index_classify()
        if classify.empty:
            classify = self.download_index_classify()
        if classify.empty:
            logger.error("无行业分类数据")
            return pd.DataFrame()

        if index_codes is None:
            index_codes = classify["index_code"].tolist()

        all_dfs = []
        for i, code in enumerate(index_codes):
            df = self._api_call("index_member", index_code=code)
            if not df.empty:
                all_dfs.append(df)
            if (i + 1) % 50 == 0:
                logger.info("  行业成分进度: %d / %d", i + 1, len(index_codes))

        if not all_dfs:
            return pd.DataFrame()
        result = pd.concat(all_dfs, ignore_index=True)
        path = self.data_dir / "meta" / "index_member.parquet"
        result.to_parquet(path, index=False)
        logger.info("行业成分股: %d 行 → %s", len(result), path)
        return result

    # ================================================================
    #  查询方法 — DuckDB SQL on Parquet
    # ================================================================

    def query(self, sql: str) -> pd.DataFrame:
        """直接 SQL 查询 Parquet 文件，路径用 {data_dir} 占位"""
        sql = sql.replace("{data_dir}", str(self.data_dir))
        return self.conn.execute(sql).df()

    def get_stock_basic(self) -> pd.DataFrame:
        """获取股票基本信息"""
        path = self.data_dir / "meta" / "stock_basic.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def get_daily(
        self,
        trade_date: Optional[str] = None,
        ts_codes: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """查询日线数据"""
        pattern = str(self.data_dir / "market" / "daily" / "*.parquet")
        sql = f"SELECT * FROM '{pattern}' WHERE 1=1"
        if trade_date:
            sql += f" AND trade_date = '{trade_date}'"
        if start_date:
            sql += f" AND trade_date >= '{start_date}'"
        if end_date:
            sql += f" AND trade_date <= '{end_date}'"
        if ts_codes:
            codes_str = ",".join(f"'{c}'" for c in ts_codes)
            sql += f" AND ts_code IN ({codes_str})"
        return self.conn.execute(sql).df()

    def get_valuation(
        self,
        trade_date: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """查询每日估值数据 (PE, PB, MV, 股息率)"""
        pattern = str(self.data_dir / "fundamental" / "valuation" / "*.parquet")
        sql = f"SELECT * FROM '{pattern}' WHERE 1=1"
        if trade_date:
            sql += f" AND trade_date = '{trade_date}'"
        if start_date:
            sql += f" AND trade_date >= '{start_date}'"
        if end_date:
            sql += f" AND trade_date <= '{end_date}'"
        return self.conn.execute(sql).df()

    def get_dividend(self) -> pd.DataFrame:
        """获取全量分红数据"""
        path = self.data_dir / "fundamental" / "dividend.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def get_fina_indicator(self) -> pd.DataFrame:
        """获取财务指标"""
        path = self.data_dir / "fundamental" / "fina_indicator.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def get_income(self) -> pd.DataFrame:
        """获取利润表"""
        path = self.data_dir / "fundamental" / "income.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def get_cashflow(self) -> pd.DataFrame:
        """获取现金流量表"""
        path = self.data_dir / "fundamental" / "cashflow.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def get_balancesheet(self) -> pd.DataFrame:
        """获取资产负债表"""
        path = self.data_dir / "fundamental" / "balancesheet.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def get_index_daily(
        self,
        ts_code: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        查询指数日线行情

        Parameters
        ----------
        ts_code : str, optional
            指数代码, 如 "000300.SH"
        start_date, end_date : str, optional
            YYYYMMDD 格式日期范围
        """
        idx_dir = self.data_dir / "market" / "index_daily"
        if not idx_dir.exists():
            return pd.DataFrame()

        if ts_code:
            path = idx_dir / f"{ts_code.replace('.', '_')}.parquet"
            if not path.exists():
                return pd.DataFrame()
            df = pd.read_parquet(path)
        else:
            parquets = list(idx_dir.glob("*.parquet"))
            if not parquets:
                return pd.DataFrame()
            dfs = [pd.read_parquet(p) for p in parquets]
            df = pd.concat(dfs, ignore_index=True)

        if start_date:
            df = df[df["trade_date"] >= start_date]
        if end_date:
            df = df[df["trade_date"] <= end_date]
        return df

    def get_index_classify(self, level: Optional[str] = None) -> pd.DataFrame:
        """
        查询申万行业分类

        Parameters
        ----------
        level : str, optional
            "L1" 或 "L2", 不传返回全部
        """
        path = self.data_dir / "meta" / "index_classify.parquet"
        if not path.exists():
            return pd.DataFrame()
        df = pd.read_parquet(path)
        if level and "level" in df.columns:
            df = df[df["level"] == level]
        return df

    def get_index_member(self, index_code: Optional[str] = None) -> pd.DataFrame:
        """
        查询申万行业成分股

        Parameters
        ----------
        index_code : str, optional
            行业指数代码, 不传返回全部
        """
        path = self.data_dir / "meta" / "index_member.parquet"
        if not path.exists():
            return pd.DataFrame()
        df = pd.read_parquet(path)
        if index_code:
            df = df[df["index_code"] == index_code]
        return df

    def get_stock_industry_map(self) -> pd.DataFrame:
        """
        获取股票→申万行业的映射表

        Returns
        -------
        pd.DataFrame
            columns: ts_code (con_code), index_code, industry_name
            只包含当前有效成分 (out_date 为空或 > 今天)
        """
        member = self.get_index_member()
        if member.empty:
            return pd.DataFrame()

        classify = self.get_index_classify()

        # 过滤当前有效成分
        from datetime import datetime
        today = datetime.now().strftime("%Y%m%d")
        if "out_date" in member.columns:
            member = member[
                member["out_date"].isna() | (member["out_date"] > today)
            ]

        # 合并行业名称
        if not classify.empty and "index_code" in classify.columns:
            member = member.merge(
                classify[["index_code", "industry_name", "level"]],
                on="index_code",
                how="left",
            )

        return member

    # ================================================================
    #  内部辅助
    # ================================================================

    def _load_stock_list(self) -> List[str]:
        """加载已缓存的股票代码列表"""
        df = self.get_stock_basic()
        if df.empty:
            df = self.download_stock_basic()
        return df["ts_code"].tolist()

    def _get_trade_dates_range(self, start: str, end: str) -> List[str]:
        """获取一段时间内的交易日列表"""
        cal_path = self.data_dir / "meta" / "trade_cal.parquet"
        if not cal_path.exists():
            self.download_trade_cal()
        df = pd.read_parquet(cal_path)
        mask = (
            (df["is_open"] == 1)
            & (df["cal_date"] >= start)
            & (df["cal_date"] <= end)
        )
        return sorted(df.loc[mask, "cal_date"].tolist())

    def _save_by_year(
        self, df: pd.DataFrame, date_col: str, out_dir: Path
    ):
        """按年份分片保存 Parquet"""
        out_dir.mkdir(parents=True, exist_ok=True)
        df["_year"] = df[date_col].str[:4]
        for year, group in df.groupby("_year"):
            path = out_dir / f"{year}.parquet"
            group.drop(columns=["_year"]).to_parquet(path, index=False)
        df.drop(columns=["_year"], inplace=True)

    @staticmethod
    def _recent_periods(n_years: int = 5) -> List[str]:
        """最近 n 年的年报期 (12月31日)"""
        from datetime import datetime
        current_year = datetime.now().year
        return [f"{current_year - i}1231" for i in range(1, n_years + 1)]

    @staticmethod
    def _all_report_periods(start_year: int = 2005) -> List[str]:
        """从 start_year 到当前年的所有年报报告期 (12月31日)"""
        from datetime import datetime
        current_year = datetime.now().year
        return [f"{y}1231" for y in range(start_year, current_year + 1)]

    def _get_downloaded_years(self, directory: Path) -> set:
        """获取目录中已下载的年份集合 (基于 {year}.parquet 文件名)"""
        if not directory.exists():
            return set()
        return {int(f.stem) for f in directory.glob("*.parquet") if f.stem.isdigit()}

    # ================================================================
    #  数据完整性保障
    # ================================================================

    def ensure_data(self, start_year: int = 2005) -> dict:
        """
        确保数据完整性 — 检查并增量下载缺失数据

        日线/估值按年增量下载 (跳过已有年份);
        财务数据按报告期覆盖范围判断是否需要重新下载;
        指数/行业数据按文件存在性判断。

        Parameters
        ----------
        start_year : int
            数据起始年份, 默认 2005

        Returns
        -------
        dict : 各数据类型的下载状态
        """
        from datetime import datetime

        today = datetime.now().strftime("%Y%m%d")
        current_year = datetime.now().year
        status = {}

        # ---- 1. 元数据 ----
        logger.info("[ensure_data] 检查元数据...")
        if not (self.data_dir / "meta" / "stock_basic.parquet").exists():
            self.download_stock_basic()
            status["stock_basic"] = "downloaded"
        else:
            status["stock_basic"] = "exists"

        # 交易日历 — 确保覆盖 start_year
        cal_path = self.data_dir / "meta" / "trade_cal.parquet"
        need_cal = True
        if cal_path.exists():
            df_cal = pd.read_parquet(cal_path)
            if df_cal["cal_date"].min() <= f"{start_year}0101":
                need_cal = False
        if need_cal:
            self.download_trade_cal(start_date=f"{start_year}0101")
            status["trade_cal"] = "downloaded"
        else:
            status["trade_cal"] = "exists"

        # ---- 2. 日线行情 — 按年增量 ----
        logger.info("[ensure_data] 检查日线行情...")
        daily_dir = self.data_dir / "market" / "daily"
        existing_years = self._get_downloaded_years(daily_dir)
        need_years = set(range(start_year, current_year + 1))
        missing_daily = sorted(need_years - existing_years)

        if missing_daily:
            logger.info("日线行情缺失 %d 个年份: %s", len(missing_daily), missing_daily)
            for year in missing_daily:
                y_start = f"{year}0101"
                y_end = f"{year}1231" if year < current_year else today
                logger.info("  下载 %d 年日线...", year)
                self.download_daily(y_start, y_end)
            status["daily"] = f"downloaded {len(missing_daily)} years"
        else:
            status["daily"] = "complete"

        # ---- 3. 估值数据 — 按年增量 ----
        logger.info("[ensure_data] 检查估值数据...")
        val_dir = self.data_dir / "fundamental" / "valuation"
        existing_years = self._get_downloaded_years(val_dir)
        missing_val = sorted(need_years - existing_years)

        if missing_val:
            logger.info("估值数据缺失 %d 个年份: %s", len(missing_val), missing_val)
            for year in missing_val:
                y_start = f"{year}0101"
                y_end = f"{year}1231" if year < current_year else today
                logger.info("  下载 %d 年估值...", year)
                self.download_daily_basic(y_start, y_end)
            status["valuation"] = f"downloaded {len(missing_val)} years"
        else:
            status["valuation"] = "complete"

        # ---- 4. 财务数据 — 检查期间覆盖范围 ----
        logger.info("[ensure_data] 检查财务数据...")
        full_periods = self._all_report_periods(start_year)

        fina_tables = [
            ("fina_indicator", self.download_fina_indicator),
            ("income", self.download_income),
            ("cashflow", self.download_cashflow),
            ("balancesheet", self.download_balancesheet),
        ]
        for name, fn in fina_tables:
            path = self.data_dir / "fundamental" / f"{name}.parquet"
            need_download = False
            if not path.exists():
                need_download = True
            else:
                df_existing = pd.read_parquet(path)
                existing_periods = set(df_existing["end_date"].unique())
                target_earliest = f"{start_year}1231"
                if target_earliest not in existing_periods:
                    need_download = True

            if need_download:
                logger.info("下载 %s (报告期 %s ~ %s)...",
                            name, full_periods[0], full_periods[-1])
                fn(periods=full_periods)
                status[name] = "downloaded"
            else:
                status[name] = "exists"

        # ---- 5. 分红数据 ----
        div_path = self.data_dir / "fundamental" / "dividend.parquet"
        if not div_path.exists():
            self.download_dividend()
            status["dividend"] = "downloaded"
        else:
            status["dividend"] = "exists"

        # ---- 6. 指数日线 ----
        logger.info("[ensure_data] 检查指数日线...")
        idx_dir = self.data_dir / "market" / "index_daily"
        idx_files = list(idx_dir.glob("*.parquet")) if idx_dir.exists() else []
        if len(idx_files) < 5:
            self.download_index_daily(start_date=f"{start_year}0101")
            status["index_daily"] = "downloaded"
        else:
            status["index_daily"] = "exists"

        # ---- 7. 行业分类 ----
        logger.info("[ensure_data] 检查行业分类...")
        if not (self.data_dir / "meta" / "index_classify.parquet").exists():
            self.download_index_classify()
            status["index_classify"] = "downloaded"
        else:
            status["index_classify"] = "exists"

        if not (self.data_dir / "meta" / "index_member.parquet").exists():
            self.download_index_member()
            status["index_member"] = "downloaded"
        else:
            status["index_member"] = "exists"

        logger.info("[ensure_data] 完成: %s", status)
        return status
