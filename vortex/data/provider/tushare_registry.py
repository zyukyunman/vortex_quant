"""Tushare 数据集注册表。

这层只负责回答 3 个问题：

1. Vortex 把哪些 Tushare 接口视为可落盘 dataset
2. 每个 dataset 应如何抓取（fetch_mode）和如何分区（partition_by）
3. 哪些名字是历史兼容别名，哪些 dataset 默认进入“全量拉取”

注意：
- Vortex 的 dataset 名不一定和 Tushare API 名完全一致。
- `bars` / `fundamental` / `events` / `valuation` 是当前仓库的稳定内部名，
  分别映射到 `daily` / `income` / `dividend` / `daily_basic`。
"""
from __future__ import annotations

import os
from typing import Any

TUSHARE_INDEX_MARKETS = ["MS", "SSE", "SZSE", "CSI", "CICC", "SW", "OTH"]
TUSHARE_FUND_MARKETS = ["E", "O"]

# 当前仓库默认按用户现有 Tushare 档位建模；仍可通过环境变量覆盖。
DEFAULT_TUSHARE_POINTS = 5000

# 积分档位 -> 每分钟频次（参考 Tushare doc_id=290）。
TUSHARE_POINT_TIERS: tuple[tuple[int, int], ...] = (
    (15000, 500),
    (10000, 500),
    (5000, 500),
    (2000, 200),
    (120, 50),
    (0, 0),
)

# API 访问规则：普通积分接口按 min_points 判断；独立权限接口按 permission_key 判断。
TUSHARE_API_ACCESS_RULES: dict[str, dict[str, Any]] = {
    "stock_basic": {"access": "points", "min_points": 120},
    "trade_cal": {"access": "points", "min_points": 120},
    "daily": {"access": "points", "min_points": 120},
    "shibor_lpr": {"access": "points", "min_points": 120},
    "index_global": {"access": "points", "min_points": 120},
    "us_tycr": {"access": "points", "min_points": 120},
    "weekly": {"access": "points", "min_points": 2000},
    "monthly": {"access": "points", "min_points": 2000},
    "pro_bar": {"access": "points", "min_points": 2000},
    "daily_basic": {"access": "points", "min_points": 2000},
    "top_list": {"access": "points", "min_points": 2000},
    "top_inst": {"access": "points", "min_points": 2000},
    "moneyflow": {"access": "points", "min_points": 2000},
    "income": {"access": "points", "min_points": 2000},
    "balancesheet": {"access": "points", "min_points": 2000},
    "cashflow": {"access": "points", "min_points": 2000},
    "forecast": {"access": "points", "min_points": 2000},
    "express": {"access": "points", "min_points": 2000},
    "dividend": {"access": "points", "min_points": 2000},
    "fina_indicator": {"access": "points", "min_points": 2000},
    "disclosure_date": {"access": "points", "min_points": 2000},
    "fund_basic": {"access": "points", "min_points": 2000},
    "index_basic": {"access": "points", "min_points": 2000},
    "index_daily": {"access": "points", "min_points": 2000},
    "index_classify": {"access": "points", "min_points": 2000},
    "index_member_all": {"access": "points", "min_points": 2000},
    "index_weight": {"access": "points", "min_points": 2000},
    "moneyflow_hsgt": {"access": "points", "min_points": 2000},
    "hsgt_top10": {"access": "points", "min_points": 2000},
    "moneyflow_ind_dc": {"access": "points", "min_points": 2000},
    "moneyflow_mkt_dc": {"access": "points", "min_points": 2000},
    "limit_list_d": {"access": "points", "min_points": 2000},
    "limit_step": {"access": "points", "min_points": 2000},
    "kpl_list": {"access": "points", "min_points": 2000},
    "dc_hot": {"access": "points", "min_points": 2000},
    "ths_hot": {"access": "points", "min_points": 2000},
    "adj_factor": {"access": "points", "min_points": 2000},
    "namechange": {"access": "points", "min_points": 2000},
    "stock_company": {"access": "points", "min_points": 2000},
    "stock_st": {"access": "points", "min_points": 2000},
    "st": {"access": "points", "min_points": 2000},
    "sw_daily": {"access": "points", "min_points": 2000},
    "ths_index": {"access": "points", "min_points": 2000},
    "ths_member": {"access": "points", "min_points": 2000},
    "dc_index": {"access": "points", "min_points": 2000},
    "dc_member": {"access": "points", "min_points": 2000},
    "cn_cpi": {"access": "points", "min_points": 2000},
    "cn_ppi": {"access": "points", "min_points": 2000},
    "cn_pmi": {"access": "points", "min_points": 2000},
    "cn_gdp": {"access": "points", "min_points": 2000},
    "cn_m": {"access": "points", "min_points": 2000},
    "sf_month": {"access": "points", "min_points": 2000},
    "shibor": {"access": "points", "min_points": 2000},
    "anns_d": {"access": "permission", "permission_key": "announcements", "rpm": 500},
    "news": {"access": "permission", "permission_key": "news", "rpm": 400},
    "major_news": {"access": "permission", "permission_key": "news", "rpm": 400},
    "research_report": {"access": "permission", "permission_key": "research_report", "rpm": 500},
    "npr": {"access": "permission", "permission_key": "policy", "rpm": 500},
    "irm_qa_sh": {"access": "permission", "permission_key": "investor_qa", "rpm": 500},
    "irm_qa_sz": {"access": "permission", "permission_key": "investor_qa", "rpm": 500},
    "us_daily": {"access": "permission", "permission_key": "us_daily", "rpm": 500},
    "hk_daily": {"access": "permission", "permission_key": "hk_daily", "rpm": 500},
    "stk_mins": {"access": "permission", "permission_key": "stock_minutes", "rpm": 500},
    "rt_k": {"access": "permission", "permission_key": "realtime_daily", "rpm": 50},
}

TUSHARE_DATASET_ALIASES: dict[str, str] = {
    "stock_basic": "instruments",
    "trade_cal": "calendar",
    "daily": "bars",
    "income": "fundamental",
    "dividend": "events",
    "daily_basic": "valuation",
}

DEFAULT_TUSHARE_PRIORITY_DATASETS = [
    "instruments",
    "calendar",
    "bars",
    "valuation",
    "fundamental",
]

TUSHARE_DATASET_REGISTRY: dict[str, dict[str, Any]] = {
    # ------------------------------------------------------------------
    # 核心基础数据（内部稳定名）
    # ------------------------------------------------------------------
    "instruments": {
        "api": "stock_basic",
        "description": "A 股标的列表",
        "phase": "1A",
        "fetch_mode": "stock_reference",
        "partition_by": None,
        "default_enabled": True,
    },
    "calendar": {
        "api": "trade_cal",
        "description": "交易日历",
        "phase": "1A",
        "fetch_mode": "calendar",
        "partition_by": None,
        "default_enabled": True,
    },
    "bars": {
        "api": "daily",
        "description": "A 股日线行情 (OHLCV)",
        "phase": "1A",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
        "quality_check": True,
    },
    "fundamental": {
        "api": "income",
        "description": "利润表（PIT 对齐）",
        "phase": "1A",
        "fetch_mode": "symbol_quarter_range",
        "partition_by": "report_date",
        "default_enabled": True,
        "pit_required": True,
    },
    "events": {
        "api": "dividend",
        "description": "分红事件",
        "phase": "1A",
        "fetch_mode": "symbol_once",
        "partition_by": "date",
        "default_enabled": True,
    },
    "valuation": {
        "api": "daily_basic",
        "description": "估值与市值指标",
        "phase": "1B",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    # ------------------------------------------------------------------
    # A 股扩展
    # ------------------------------------------------------------------
    "adj_factor": {
        "api": "adj_factor",
        "description": "复权因子",
        "phase": "1B",
        "fetch_mode": "symbol_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "namechange": {
        "api": "namechange",
        "description": "股票曾用名 / ST 历史",
        "phase": "1B",
        "fetch_mode": "symbol_once",
        "partition_by": "date",
        "default_enabled": True,
    },
    "stock_company": {
        "api": "stock_company",
        "description": "上市公司基本信息",
        "phase": "1B",
        "fetch_mode": "symbol_once",
        "partition_by": None,
        "default_enabled": True,
    },
    "stock_st": {
        "api": "stock_st",
        "description": "历史 ST 列表",
        "phase": "2",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "st": {
        "api": "st",
        "description": "ST 风险警示板股票",
        "phase": "2",
        "fetch_mode": "reference_once",
        "partition_by": None,
        "default_enabled": True,
    },
    "weekly": {
        "api": "weekly",
        "description": "A 股周线行情",
        "phase": "2",
        "fetch_mode": "symbol_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "monthly": {
        "api": "monthly",
        "description": "A 股月线行情",
        "phase": "2",
        "fetch_mode": "symbol_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "balancesheet": {
        "api": "balancesheet",
        "description": "资产负债表",
        "phase": "1B",
        "fetch_mode": "symbol_quarter_range",
        "partition_by": "report_date",
        "default_enabled": True,
        "pit_required": True,
    },
    "cashflow": {
        "api": "cashflow",
        "description": "现金流量表",
        "phase": "1B",
        "fetch_mode": "symbol_quarter_range",
        "partition_by": "report_date",
        "default_enabled": True,
        "pit_required": True,
    },
    "fina_indicator": {
        "api": "fina_indicator",
        "description": "财务指标（ROE/毛利率/净利率等）",
        "phase": "1B",
        "fetch_mode": "symbol_quarter_range",
        "partition_by": "report_date",
        "default_enabled": True,
        "pit_required": True,
    },
    "forecast": {
        "api": "forecast",
        "description": "业绩预告",
        "phase": "2",
        "fetch_mode": "symbol_quarter_range",
        "partition_by": "report_date",
        "default_enabled": True,
    },
    "express": {
        "api": "express",
        "description": "业绩快报",
        "phase": "2",
        "fetch_mode": "symbol_quarter_range",
        "partition_by": "report_date",
        "default_enabled": True,
    },
    "disclosure_date": {
        "api": "disclosure_date",
        "description": "财报披露计划",
        "phase": "2",
        "fetch_mode": "symbol_quarter_range",
        "partition_by": "report_date",
        "default_enabled": True,
    },
    "moneyflow": {
        "api": "moneyflow",
        "description": "个股资金流向",
        "phase": "1B",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "moneyflow_hsgt": {
        "api": "moneyflow_hsgt",
        "description": "沪深港通资金流向",
        "phase": "1B",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "hsgt_top10": {
        "api": "hsgt_top10",
        "description": "沪深港通十大成交股",
        "phase": "2",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "top_list": {
        "api": "top_list",
        "description": "龙虎榜每日统计",
        "phase": "2",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "top_inst": {
        "api": "top_inst",
        "description": "龙虎榜机构交易",
        "phase": "2",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "moneyflow_ind_dc": {
        "api": "moneyflow_ind_dc",
        "description": "东方财富板块资金流向",
        "phase": "2",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "moneyflow_mkt_dc": {
        "api": "moneyflow_mkt_dc",
        "description": "东方财富大盘资金流向",
        "phase": "2",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "limit_list_d": {
        "api": "limit_list_d",
        "description": "涨跌停明细",
        "phase": "2",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "limit_step": {
        "api": "limit_step",
        "description": "涨停连板梯队",
        "phase": "2",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "kpl_list": {
        "api": "kpl_list",
        "description": "开盘啦榜单数据",
        "phase": "2",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "dc_hot": {
        "api": "dc_hot",
        "description": "东方财富热榜",
        "phase": "2",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "ths_hot": {
        "api": "ths_hot",
        "description": "同花顺热榜",
        "phase": "2",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    # ------------------------------------------------------------------
    # 指数 / 板块 / 主题
    # ------------------------------------------------------------------
    "fund_basic": {
        "api": "fund_basic",
        "description": "基金列表",
        "phase": "2",
        "fetch_mode": "fund_reference",
        "partition_by": None,
        "default_enabled": True,
    },
    "index_basic": {
        "api": "index_basic",
        "description": "指数基本信息",
        "phase": "1B",
        "fetch_mode": "index_reference",
        "partition_by": None,
        "default_enabled": True,
    },
    "index_daily": {
        "api": "index_daily",
        "description": "指数日线行情",
        "phase": "1B",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "index_classify": {
        "api": "index_classify",
        "description": "行业分类目录",
        "phase": "1B",
        "fetch_mode": "reference_once",
        "partition_by": None,
        "default_enabled": True,
    },
    "index_member_all": {
        "api": "index_member_all",
        "description": "申万行业成分（全量）",
        "phase": "1B",
        "fetch_mode": "reference_once",
        "partition_by": None,
        "default_enabled": True,
    },
    "index_weight": {
        "api": "index_weight",
        "description": "指数成分股权重",
        "phase": "1B",
        "fetch_mode": "index_loop_range",
        "partition_by": "date",
        "default_enabled": True,
        "loop_source": "index_basic",
        "param_name": "index_code",
    },
    "sw_daily": {
        "api": "sw_daily",
        "description": "申万行业日线",
        "phase": "1B",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "ths_index": {
        "api": "ths_index",
        "description": "同花顺概念/行业目录",
        "phase": "2",
        "fetch_mode": "reference_once",
        "partition_by": None,
        "default_enabled": True,
    },
    "ths_member": {
        "api": "ths_member",
        "description": "同花顺概念成分股",
        "phase": "2",
        "fetch_mode": "member_loop",
        "partition_by": None,
        "default_enabled": True,
        "loop_source": "ths_index",
        "param_name": "ts_code",
    },
    "dc_index": {
        "api": "dc_index",
        "description": "东方财富主题目录",
        "phase": "2",
        "fetch_mode": "reference_once",
        "partition_by": None,
        "default_enabled": True,
    },
    "dc_member": {
        "api": "dc_member",
        "description": "东方财富主题成分股",
        "phase": "2",
        "fetch_mode": "member_loop",
        "partition_by": None,
        "default_enabled": True,
        "loop_source": "dc_index",
        "param_name": "ts_code",
    },
    # ------------------------------------------------------------------
    # 新闻 / 公告 / 语料
    # ------------------------------------------------------------------
    "anns_d": {
        "api": "anns_d",
        "description": "上市公司公告",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "news": {
        "api": "news",
        "description": "新闻快讯",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "major_news": {
        "api": "major_news",
        "description": "新闻通讯（长篇）",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "research_report": {
        "api": "research_report",
        "description": "券商研究报告",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "npr": {
        "api": "npr",
        "description": "国家政策库",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "irm_qa_sh": {
        "api": "irm_qa_sh",
        "description": "上证 e 互动问答",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "irm_qa_sz": {
        "api": "irm_qa_sz",
        "description": "深证易互动问答",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    # ------------------------------------------------------------------
    # 宏观 / 跨市场
    # ------------------------------------------------------------------
    "cn_cpi": {
        "api": "cn_cpi",
        "description": "居民消费价格指数",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "cn_ppi": {
        "api": "cn_ppi",
        "description": "工业生产者价格指数",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "cn_pmi": {
        "api": "cn_pmi",
        "description": "采购经理指数",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "cn_gdp": {
        "api": "cn_gdp",
        "description": "国内生产总值",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "cn_m": {
        "api": "cn_m",
        "description": "货币供应量",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "sf_month": {
        "api": "sf_month",
        "description": "社融增量（月度）",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "shibor": {
        "api": "shibor",
        "description": "Shibor 利率",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "shibor_lpr": {
        "api": "shibor_lpr",
        "description": "LPR 贷款基础利率",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "us_tycr": {
        "api": "us_tycr",
        "description": "美国国债收益率曲线",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "us_daily": {
        "api": "us_daily",
        "description": "美股日线行情",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "hk_daily": {
        "api": "hk_daily",
        "description": "港股日线行情",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    "index_global": {
        "api": "index_global",
        "description": "国际主要指数",
        "phase": "3A",
        "fetch_mode": "date_range",
        "partition_by": "date",
        "default_enabled": True,
    },
    # ------------------------------------------------------------------
    # 非默认：更高频 / 非 bootstrap 语义
    # ------------------------------------------------------------------
    "pro_bar": {
        "api": "pro_bar",
        "description": "复权行情（top-level helper）",
        "phase": "3B",
        "fetch_mode": "pro_bar",
        "partition_by": "date",
        "default_enabled": False,
    },
    "stk_mins": {
        "api": "stk_mins",
        "description": "股票分钟行情",
        "phase": "3B",
        "fetch_mode": "minute_range",
        "partition_by": "date",
        "default_enabled": False,
    },
    "rt_k": {
        "api": "rt_k",
        "description": "实时行情快照",
        "phase": "3B",
        "fetch_mode": "realtime_snapshot",
        "partition_by": "date",
        "default_enabled": False,
    },
}


def resolve_tushare_dataset_name(name: str) -> str:
    """把历史别名解析为当前 canonical dataset 名。"""
    return TUSHARE_DATASET_ALIASES.get(name, name)


def get_tushare_dataset_spec(name: str) -> dict[str, Any]:
    """获取 canonical dataset 元信息。"""
    canonical = resolve_tushare_dataset_name(name)
    if canonical not in TUSHARE_DATASET_REGISTRY:
        raise KeyError(f"未知 Tushare dataset: {name}")
    return TUSHARE_DATASET_REGISTRY[canonical]


def parse_tushare_points(value: str | None, default: int = DEFAULT_TUSHARE_POINTS) -> int:
    """解析 Tushare 积分档位。默认按当前用户 5000 积分建模。"""
    if value is None or not value.strip():
        return default
    try:
        return max(int(value.strip()), 0)
    except ValueError:
        return default


def parse_tushare_permission_keys(value: str | None) -> set[str]:
    """解析额外独立权限集合，格式: a,b,c。"""
    if value is None or not value.strip():
        return set()
    return {
        part.strip()
        for part in value.split(",")
        if part.strip()
    }


def resolve_tushare_points_rpm(points: int) -> int:
    """根据积分档位推导普通积分接口的每分钟频次。"""
    for min_points, rpm in TUSHARE_POINT_TIERS:
        if points >= min_points:
            return rpm
    return 0


def get_tushare_api_access_rule(api_name: str) -> dict[str, Any]:
    """按 API 名获取访问规则；未显式列出的接口默认按 2000 积分普通接口处理。"""
    return TUSHARE_API_ACCESS_RULES.get(
        api_name,
        {"access": "points", "min_points": 2000},
    )


def get_tushare_dataset_access_rule(name: str) -> dict[str, Any]:
    """按 dataset 名获取访问规则。"""
    spec = get_tushare_dataset_spec(name)
    api_name = str(spec.get("api") or name)
    return get_tushare_api_access_rule(api_name)


def get_default_tushare_datasets(
    points: int | None = None,
    permission_keys: set[str] | None = None,
) -> list[str]:
    """返回默认进入“全量拉取”的 canonical dataset 列表。

    默认行为不是“把 registry 里所有 default_enabled 都塞进去”，而是先按当前账号
    的积分档位 / 独立权限过滤，只保留当前账号理论上可访问的数据集。
    """
    resolved_points = parse_tushare_points(
        os.environ.get("TUSHARE_POINTS"),
    ) if points is None else points
    resolved_permissions = parse_tushare_permission_keys(
        os.environ.get("TUSHARE_EXTRA_PERMISSIONS")
    ) if permission_keys is None else permission_keys

    return [
        name
        for name, meta in TUSHARE_DATASET_REGISTRY.items()
        if bool(meta.get("default_enabled", True))
        and (
            (
                get_tushare_api_access_rule(str(meta.get("api") or name)).get("access") == "points"
                and resolved_points >= int(
                    get_tushare_api_access_rule(str(meta.get("api") or name)).get("min_points", 0)
                )
            )
            or (
                get_tushare_api_access_rule(str(meta.get("api") or name)).get("access") == "permission"
                and str(
                    get_tushare_api_access_rule(str(meta.get("api") or name)).get("permission_key", "")
                ) in resolved_permissions
            )
        )
    ]
