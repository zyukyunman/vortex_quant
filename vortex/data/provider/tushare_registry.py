"""Tushare 数据集注册表。

这层只负责回答 3 个问题：

1. Vortex 把哪些 Tushare 接口视为可落盘 dataset
2. 每个 dataset 应如何抓取（fetch_mode）、如何分区（partition_by），以及是否存在更高效的候选抓法
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
TUSHARE_INDEX_DAILY_MARKETS = ["SSE", "SZSE", "CSI"]
DEFAULT_TUSHARE_INDEX_DAILY_CODES = tuple(
    item.strip()
    for item in os.getenv(
        "VORTEX_TUSHARE_INDEX_DAILY_CODES",
        "000001.SH,000016.SH,399001.SZ,399006.SZ,000300.SH,000905.SH,000852.SH,000906.SH,000985.CSI",
    ).split(",")
    if item.strip()
)
TUSHARE_FUND_MARKETS = ["E", "O"]
TUSHARE_STOCK_EXCHANGES = ["SSE", "SZSE", "BSE"]

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
    "income_vip": {"access": "points", "min_points": 5000},
    "balancesheet_vip": {"access": "points", "min_points": 5000},
    "cashflow_vip": {"access": "points", "min_points": 5000},
    "fina_indicator_vip": {"access": "points", "min_points": 5000},
    "forecast_vip": {"access": "points", "min_points": 5000},
    "express_vip": {"access": "points", "min_points": 5000},
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
    # limit_list_d 和 sw_daily 虽然积分门槛是 2000，但实际频控上限长期表现为 200 次/分钟；
    # 不能直接沿用账户档位（例如 5000 积分账户的 500 rpm），否则会持续触发限频。
    "limit_list_d": {"access": "points", "min_points": 2000, "rpm": 200},
    "stk_limit": {"access": "points", "min_points": 2000},
    "suspend_d": {"access": "points", "min_points": 2000},
    "cyq_perf": {"access": "points", "min_points": 5000, "rpm": 200},
    "cyq_chips": {"access": "points", "min_points": 5000, "rpm": 200},
    "limit_step": {"access": "points", "min_points": 8000},
    "kpl_list": {"access": "points", "min_points": 2000},
    "dc_hot": {"access": "points", "min_points": 2000},
    "ths_hot": {"access": "points", "min_points": 2000},
    "adj_factor": {"access": "points", "min_points": 2000},
    "namechange": {"access": "points", "min_points": 2000},
    "stock_company": {"access": "points", "min_points": 120},
    "stock_st": {"access": "points", "min_points": 2000},
    "st": {"access": "points", "min_points": 6000},
    "sw_daily": {"access": "points", "min_points": 2000, "rpm": 200},
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
    "stk_auction_o": {"access": "permission", "permission_key": "stock_minutes", "rpm": 500},
    "stk_auction_c": {"access": "permission", "permission_key": "stock_minutes", "rpm": 500},
    "stk_auction": {"access": "permission", "permission_key": "stock_minutes", "rpm": 500},
    "stk_nineturn": {"access": "points", "min_points": 6000},
    "rt_k": {"access": "permission", "permission_key": "realtime_daily", "rpm": 50},
    "realtime_quote": {"access": "points", "min_points": 2000, "rpm": 50},
}

TUSHARE_DATASET_ALIASES: dict[str, str] = {
    "stock_basic": "instruments",
    "trade_cal": "calendar",
    "daily": "bars",
    "income": "fundamental",
    "dividend": "events",
    "daily_basic": "valuation",
}

TUSHARE_API_DOC_URLS: dict[str, str] = {
    "fund_basic": "https://tushare.pro/wctapi/documents/19.md",
    "stock_basic": "https://tushare.pro/wctapi/documents/25.md",
    "daily": "https://tushare.pro/wctapi/documents/27.md",
    "adj_factor": "https://tushare.pro/wctapi/documents/28.md",
    "daily_basic": "https://tushare.pro/wctapi/documents/32.md",
    "income": "https://tushare.pro/wctapi/documents/33.md",
    "balancesheet": "https://tushare.pro/wctapi/documents/36.md",
    "cashflow": "https://tushare.pro/wctapi/documents/44.md",
    "forecast": "https://tushare.pro/wctapi/documents/45.md",
    "express": "https://tushare.pro/wctapi/documents/46.md",
    "moneyflow_hsgt": "https://tushare.pro/wctapi/documents/47.md",
    "hsgt_top10": "https://tushare.pro/wctapi/documents/48.md",
    "fina_indicator": "https://tushare.pro/wctapi/documents/79.md",
    "index_basic": "https://tushare.pro/wctapi/documents/94.md",
    "index_daily": "https://tushare.pro/wctapi/documents/95.md",
    "index_weight": "https://tushare.pro/wctapi/documents/96.md",
    "namechange": "https://tushare.pro/wctapi/documents/100.md",
    "dividend": "https://tushare.pro/wctapi/documents/103.md",
    "top_list": "https://tushare.pro/wctapi/documents/106.md",
    "top_inst": "https://tushare.pro/wctapi/documents/107.md",
    "stock_company": "https://tushare.pro/wctapi/documents/112.md",
    "trade_cal": "https://tushare.pro/wctapi/documents/137.md",
    "news": "https://tushare.pro/wctapi/documents/143.md",
    "weekly": "https://tushare.pro/wctapi/documents/144.md",
    "monthly": "https://tushare.pro/wctapi/documents/145.md",
    "pro_bar": "https://tushare.pro/wctapi/documents/146.md",
    "shibor": "https://tushare.pro/wctapi/documents/149.md",
    "shibor_lpr": "https://tushare.pro/wctapi/documents/151.md",
    "disclosure_date": "https://tushare.pro/wctapi/documents/162.md",
    "moneyflow": "https://tushare.pro/wctapi/documents/170.md",
    "anns_d": "https://tushare.pro/wctapi/documents/176.md",
    "index_classify": "https://tushare.pro/wctapi/documents/181.md",
    "hk_daily": "https://tushare.pro/wctapi/documents/192.md",
    "major_news": "https://tushare.pro/wctapi/documents/195.md",
    "index_global": "https://tushare.pro/wctapi/documents/211.md",
    "us_tycr": "https://tushare.pro/wctapi/documents/219.md",
    "cn_gdp": "https://tushare.pro/wctapi/documents/227.md",
    "cn_cpi": "https://tushare.pro/wctapi/documents/228.md",
    "cn_m": "https://tushare.pro/wctapi/documents/242.md",
    "cn_ppi": "https://tushare.pro/wctapi/documents/245.md",
    "us_daily": "https://tushare.pro/wctapi/documents/254.md",
    "ths_index": "https://tushare.pro/wctapi/documents/259.md",
    "ths_member": "https://tushare.pro/wctapi/documents/261.md",
    "cyq_perf": "https://tushare.pro/document/2?doc_id=293",
    "cyq_chips": "https://tushare.pro/document/2?doc_id=294",
    "limit_list_d": "https://tushare.pro/wctapi/documents/298.md",
    "stk_limit": "https://tushare.pro/wctapi/documents/183.md",
    "suspend_d": "https://tushare.pro/wctapi/documents/214.md",
    "sf_month": "https://tushare.pro/wctapi/documents/310.md",
    "ths_hot": "https://tushare.pro/wctapi/documents/320.md",
    "dc_hot": "https://tushare.pro/wctapi/documents/321.md",
    "cn_pmi": "https://tushare.pro/wctapi/documents/325.md",
    "sw_daily": "https://tushare.pro/wctapi/documents/327.md",
    "index_member_all": "https://tushare.pro/wctapi/documents/335.md",
    "moneyflow_ind_dc": "https://tushare.pro/wctapi/documents/344.md",
    "moneyflow_mkt_dc": "https://tushare.pro/wctapi/documents/345.md",
    "kpl_list": "https://tushare.pro/wctapi/documents/347.md",
    "limit_step": "https://tushare.pro/wctapi/documents/356.md",
    "dc_index": "https://tushare.pro/wctapi/documents/362.md",
    "dc_member": "https://tushare.pro/wctapi/documents/363.md",
    "irm_qa_sh": "https://tushare.pro/wctapi/documents/366.md",
    "irm_qa_sz": "https://tushare.pro/wctapi/documents/367.md",
    "rt_k": "https://tushare.pro/wctapi/documents/372.md",
    "stk_auction_o": "https://tushare.pro/document/2?doc_id=353",
    "stk_auction_c": "https://tushare.pro/document/2?doc_id=354",
    "stk_auction": "https://tushare.pro/document/2?doc_id=369",
    "stk_mins": "https://tushare.pro/document/2?doc_id=370",
    "stk_nineturn": "https://tushare.pro/document/2?doc_id=364",
    "realtime_quote": "https://tushare.pro/document/2?doc_id=315",
    "stock_st": "https://tushare.pro/wctapi/documents/397.md",
    "npr": "https://tushare.pro/wctapi/documents/406.md",
    "research_report": "https://tushare.pro/wctapi/documents/415.md",
    "st": "https://tushare.pro/wctapi/documents/423.md",
}

TUSHARE_COMMON_FIELD_DOCS: dict[str, str] = {
    "symbol": "统一证券代码，A 股形如 600519.SH / 000001.SZ。",
    "name": "证券或实体名称。",
    "date": "统一日期字段，默认使用 YYYYMMDD；若是分区表，通常也是分区键。",
    "report_date": "报告期，对应财报所属季度/年度，通常来自 Tushare 的 end_date。",
    "ann_date": "公告日期，即财报或事件正式披露日期。",
    "list_date": "上市日期。",
    "delist_date": "退市日期；为空通常表示仍在上市。",
    "open": "开盘价。",
    "high": "最高价。",
    "low": "最低价。",
    "close": "收盘价。",
    "pre_close": "前收盘价。",
    "change": "涨跌额。",
    "pct_chg": "涨跌幅，单位通常为百分比。",
    "volume": "成交量；A 股日线口径下单位为手。",
    "amount": "成交额；A 股日线口径下单位为千元。",
    "turnover_rate": "换手率，单位为百分比。",
    "turnover_rate_f": "自由流通换手率，单位为百分比。",
    "volume_ratio": "量比。",
    "pe": "市盈率（PE）。",
    "pe_ttm": "滚动市盈率（PE TTM）。",
    "pb": "市净率（PB）。",
    "ps": "市销率（PS）。",
    "ps_ttm": "滚动市销率（PS TTM）。",
    "dv_ratio": "股息率，单位为百分比。",
    "dv_ttm": "滚动股息率，单位为百分比。",
    "total_mv": "总市值，通常单位为万元。",
    "circ_mv": "流通市值，通常单位为万元。",
    "is_open": "交易所日历是否开市；1 表示开市，0 表示休市。",
    "pretrade_date": "上一交易日。",
    "exchange": "交易所代码，如 SSE / SZSE。",
    "cal_date": "交易日历日期。",
    "adj_factor": "复权因子，用于前复权/后复权换算。",
}

TUSHARE_DATASET_NOTES: dict[str, str] = {
    "bars": "A 股不复权日线行情；一行对应一个 symbol + date。当前来自 Tushare daily，volume 单位为手，amount 单位为千元。",
    "fundamental": "利润表数据，report_date 表示报告期，ann_date 表示公告日；该表会经过 PIT 对齐，只允许在当前 as_of 下可见的数据落盘。",
    "events": "分红事件稀疏表；统一事件日期 date 采用 ex_date -> record_date -> pay_date -> ann_date -> end_date 的优先级回退，不再依赖单一原始日期列。",
    "valuation": "估值与市值指标表；常用于估值因子、风格分析与横向比较，按交易日分区。",
    "calendar": "交易日历基准表；用于判断某天是否开市，以及 PIT 对齐和区间切片。",
    "instruments": "当前证券主数据；默认 universe 以当前仍在上市的标的为主，不等于历史所有曾上市证券。",
    "adj_factor": "复权因子表；通常与 bars 联合使用，把不复权价格换算成前复权/后复权口径。",
    "realtime_quote": "Tushare 实时盘口快照；可获取最新价与买卖五档，但只能代表抓取当下，不能替代历史盘口回放。",
    "cyq_perf": "每日筹码及胜率表；按股票和交易日记录筹码成本分位与 winner_rate，适合做筹码结构研究。",
    "cyq_chips": "每日筹码分布明细；同一股票同一天会有多行 price-percent 价格分布，不适合作为默认全量 bootstrap。",
    "stk_auction_o": "开盘集合竞价结果表；支持按交易日整批抓，默认优先按日期下载，必要时回退到按股票区间补抓。",
    "stk_auction_c": "收盘集合竞价结果表；支持按交易日整批抓，默认优先按日期下载，必要时回退到按股票区间补抓。",
    "stk_auction": "当日集合竞价成交表；包含 price/turnover_rate/volume_ratio，适合做开盘竞价活跃度和容量压力研究。",
}

TUSHARE_DATASET_FIELD_DOCS: dict[str, dict[str, str]] = {
    "instruments": {
        "symbol": "证券代码。",
        "name": "证券简称。",
        "area": "所属地域。",
        "industry": "所属行业。",
        "market": "市场板块，如主板/创业板/科创板。",
        "exchange": "交易所代码。",
        "list_status": "上市状态；L=上市，D=退市，P=暂停上市。",
        "list_date": "上市日期。",
        "delist_date": "退市日期。",
        "is_hs": "是否沪深港通标的。",
    },
    "calendar": {
        "exchange": "交易所代码。",
        "cal_date": "日历日期。",
        "is_open": "是否开市；1 开市，0 休市。",
        "pretrade_date": "前一个交易日。",
    },
    "bars": {
        "symbol": "证券代码。",
        "date": "交易日期；也是 bars 的分区键。",
        "open": "开盘价。",
        "high": "最高价。",
        "low": "最低价。",
        "close": "收盘价。",
        "volume": "成交量，单位为手。",
        "amount": "成交额，单位为千元。",
    },
    "fundamental": {
        "symbol": "证券代码。",
        "ann_date": "公告日期；PIT 对齐时的关键时间字段。",
        "report_date": "报告期，对应财报所属季度/年度。",
        "total_revenue": "营业总收入。",
        "revenue": "营业收入。",
        "operate_profit": "营业利润。",
        "n_income": "净利润。",
        "n_income_attr_p": "归属于母公司股东的净利润（归母净利润）。",
        "basic_eps": "基本每股收益。",
        "diluted_eps": "稀释每股收益。",
    },
    "events": {
        "symbol": "证券代码。",
        "date": "统一事件日期；按 ex_date -> record_date -> pay_date -> ann_date -> end_date 回退生成。",
        "ann_date": "分红方案公告日。",
        "end_date": "分红对应的报告期。",
        "record_date": "股权登记日。",
        "ex_date": "除权除息日。",
        "pay_date": "派息日。",
        "div_listdate": "送转股份上市日。",
        "cash_div": "每股现金分红（税前）。",
        "stk_div": "每股送股比例。",
        "stk_bo_rate": "每股转增比例。",
        "div_proc": "分红实施进度。",
    },
    "valuation": {
        "symbol": "证券代码。",
        "date": "交易日期；也是 valuation 的分区键。",
        "close": "当日收盘价。",
        "turnover_rate": "换手率（%）。",
        "turnover_rate_f": "自由流通换手率（%）。",
        "volume_ratio": "量比。",
        "pe": "市盈率（PE）。",
        "pe_ttm": "滚动市盈率（PE TTM）。",
        "pb": "市净率（PB）。",
        "ps": "市销率（PS）。",
        "ps_ttm": "滚动市销率（PS TTM）。",
        "dv_ratio": "股息率（%）。",
        "dv_ttm": "滚动股息率（%）。",
        "total_mv": "总市值，通常单位为万元。",
        "circ_mv": "流通市值，通常单位为万元。",
    },
    "adj_factor": {
        "symbol": "证券代码。",
        "date": "交易日期；也是 adj_factor 的分区键。",
        "adj_factor": "复权因子。",
    },
    "realtime_quote": {
        "symbol": "证券代码。",
        "date": "快照日期。",
        "time": "快照时间，通常为 HH:MM:SS。",
        "name": "证券简称。",
        "open": "当日开盘价。",
        "pre_close": "前收盘价。",
        "price": "最新成交价。",
        "high": "当日最高价。",
        "low": "当日最低价。",
        "bid": "委买参考价。",
        "ask": "委卖参考价。",
        "volume": "成交量。",
        "amount": "成交额。",
        "bid1_volume": "买一挂单量。",
        "bid1_price": "买一价格。",
        "ask1_volume": "卖一挂单量。",
        "ask1_price": "卖一价格。",
        "trade_time": "date + time 组合后的快照时间戳。",
    },
    "cyq_perf": {
        "symbol": "证券代码。",
        "date": "交易日期。",
        "his_low": "历史最低价。",
        "his_high": "历史最高价。",
        "cost_5pct": "5 分位成本。",
        "cost_15pct": "15 分位成本。",
        "cost_50pct": "50 分位成本。",
        "cost_85pct": "85 分位成本。",
        "cost_95pct": "95 分位成本。",
        "weight_avg": "加权平均成本。",
        "winner_rate": "胜率。",
    },
    "cyq_chips": {
        "symbol": "证券代码。",
        "date": "交易日期。",
        "price": "成本价格。",
        "percent": "该价格对应的筹码占比（%）。",
    },
    "stk_auction_o": {
        "symbol": "证券代码。",
        "date": "交易日期。",
        "open": "开盘集合竞价窗口首个撮合价。",
        "high": "开盘集合竞价最高价。",
        "low": "开盘集合竞价最低价。",
        "close": "开盘集合竞价最终成交价；最接近 9:30 实际开盘价。",
        "volume": "开盘集合竞价成交量（股）。",
        "amount": "开盘集合竞价成交额（元）。",
        "vwap": "开盘集合竞价均价。",
    },
    "stk_auction_c": {
        "symbol": "证券代码。",
        "date": "交易日期。",
        "open": "收盘集合竞价窗口首个撮合价。",
        "high": "收盘集合竞价最高价。",
        "low": "收盘集合竞价最低价。",
        "close": "收盘集合竞价最终成交价。",
        "volume": "收盘集合竞价成交量（股）。",
        "amount": "收盘集合竞价成交额（元）。",
        "vwap": "收盘集合竞价均价。",
    },
    "stk_auction": {
        "symbol": "证券代码。",
        "date": "交易日期。",
        "volume": "集合竞价成交量（股）。",
        "price": "集合竞价成交均价；对单价撮合场景可近似视作开盘成交价。",
        "amount": "集合竞价成交金额（元）。",
        "pre_close": "昨收价（元）。",
        "turnover_rate": "集合竞价换手率（%）。",
        "volume_ratio": "集合竞价量比。",
        "float_share": "流通股本（万股）。",
    },
    "stk_mins": {
        "symbol": "证券代码。",
        "date": "交易日期，由 trade_time 派生。",
        "trade_time": "分钟 K 线时间戳。",
        "minute": "日内分钟时间，格式 HH:MM:SS。",
        "freq": "分钟频度：1min/5min/15min/30min/60min。",
        "open": "分钟开盘价。",
        "high": "分钟最高价。",
        "low": "分钟最低价。",
        "close": "分钟收盘价。",
        "volume": "分钟成交量（股）。",
        "amount": "分钟成交额（元）。",
    },
    "stk_nineturn": {
        "symbol": "证券代码。",
        "date": "交易日期。",
        "freq": "频率，默认 daily。",
        "open": "开盘价。",
        "high": "最高价。",
        "low": "最低价。",
        "close": "收盘价。",
        "volume": "成交量。",
        "amount": "成交额。",
        "up_count": "上九转计数。",
        "down_count": "下九转计数。",
        "nine_up_turn": "是否上九转；+9 表示上九转。",
        "nine_down_turn": "是否下九转；-9 表示下九转。",
    },
}

DEFAULT_TUSHARE_PRIORITY_DATASETS = [
    "instruments",
    "calendar",
    "bars",
    "valuation",
    "fundamental",
]

# 运行频率口径：决定默认调度顺序，也用于按频率裁剪 dataset 子集。
# 这里说的“频率”是产品层的建议更新节奏，不等于底层 API 的技术 fetch_mode。
TUSHARE_UPDATE_FREQUENCY_ORDER: tuple[str, ...] = (
    "daily",
    "weekly",
    "monthly",
    "quarterly",
    "other",
    "intraday",
)

TUSHARE_UPDATE_FREQUENCY_ALIASES: dict[str, str] = {
    "hourly": "intraday",
    "realtime": "intraday",
}

TUSHARE_DATASET_UPDATE_FREQUENCIES: dict[str, str] = {
    "instruments": "other",
    "calendar": "daily",
    "bars": "daily",
    "fundamental": "quarterly",
    "events": "other",
    "valuation": "daily",
    "adj_factor": "daily",
    "namechange": "other",
    "stock_company": "other",
    "stock_st": "daily",
    "st": "other",
    "weekly": "weekly",
    "monthly": "monthly",
    "balancesheet": "quarterly",
    "cashflow": "quarterly",
    "fina_indicator": "quarterly",
    "forecast": "quarterly",
    "express": "quarterly",
    "disclosure_date": "quarterly",
    "moneyflow": "daily",
    "moneyflow_hsgt": "daily",
    "hsgt_top10": "daily",
    "top_list": "daily",
    "top_inst": "daily",
    "moneyflow_ind_dc": "daily",
    "moneyflow_mkt_dc": "daily",
    "cyq_perf": "daily",
    "cyq_chips": "daily",
    "limit_list_d": "daily",
    "stk_limit": "daily",
    "suspend_d": "daily",
    "stk_auction_o": "daily",
    "stk_auction_c": "daily",
    "stk_auction": "daily",
    "stk_nineturn": "daily",
    "limit_step": "daily",
    "kpl_list": "daily",
    "dc_hot": "daily",
    "ths_hot": "daily",
    "fund_basic": "other",
    "index_basic": "other",
    "index_daily": "daily",
    "index_classify": "other",
    "index_member_all": "other",
    "index_weight": "weekly",
    "sw_daily": "daily",
    "ths_index": "other",
    "ths_member": "other",
    "dc_index": "other",
    "dc_member": "other",
    "anns_d": "daily",
    "news": "daily",
    "major_news": "daily",
    "research_report": "daily",
    "npr": "daily",
    "irm_qa_sh": "daily",
    "irm_qa_sz": "daily",
    "cn_cpi": "monthly",
    "cn_ppi": "monthly",
    "cn_pmi": "monthly",
    "cn_gdp": "quarterly",
    "cn_m": "monthly",
    "sf_month": "monthly",
    "shibor": "daily",
    "shibor_lpr": "monthly",
    "us_tycr": "daily",
    "us_daily": "daily",
    "hk_daily": "daily",
    "index_global": "daily",
    "pro_bar": "daily",
    "stk_mins": "intraday",
    "rt_k": "intraday",
    "realtime_quote": "intraday",
}

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
        "date_field_priority": ["ex_date", "record_date", "pay_date", "ann_date", "end_date"],
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
        "date_partition_mode": "trade_day",
        "date_batch_supported": True,
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
        "fetch_mode": "exchange_reference",
        "partition_by": None,
        "default_enabled": True,
        "param_name": "exchange",
        "loop_values": TUSHARE_STOCK_EXCHANGES,
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
        "date_partition_mode": "week_end",
        "date_batch_supported": True,
        "date_batch_row_limit": 6000,
        "default_enabled": True,
    },
    "monthly": {
        "api": "monthly",
        "description": "A 股月线行情",
        "phase": "2",
        "fetch_mode": "symbol_range",
        "partition_by": "date",
        "date_partition_mode": "month_end",
        "date_batch_supported": True,
        "date_batch_row_limit": 4500,
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
    "cyq_perf": {
        "api": "cyq_perf",
        "description": "每日筹码及胜率",
        "phase": "2",
        "fetch_mode": "symbol_range",
        "partition_by": "date",
        "default_enabled": False,
    },
    "cyq_chips": {
        "api": "cyq_chips",
        "description": "每日筹码分布",
        "phase": "2",
        "fetch_mode": "symbol_range",
        "partition_by": "date",
        "default_enabled": False,
    },
    "limit_list_d": {
        "api": "limit_list_d",
        "description": "涨跌停明细",
        "phase": "2",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "stk_limit": {
        "api": "stk_limit",
        "description": "每日涨跌停价格",
        "phase": "2",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "suspend_d": {
        "api": "suspend_d",
        "description": "每日停复牌信息",
        "phase": "2",
        "fetch_mode": "trade_day_all",
        "partition_by": "date",
        "default_enabled": True,
    },
    "stk_auction_o": {
        "api": "stk_auction_o",
        "description": "股票开盘集合竞价数据",
        "phase": "2",
        "fetch_mode": "symbol_range",
        "partition_by": "date",
        "date_batch_supported": True,
        "date_batch_row_limit": 10000,
        "default_enabled": True,
    },
    "stk_auction_c": {
        "api": "stk_auction_c",
        "description": "股票收盘集合竞价数据",
        "phase": "2",
        "fetch_mode": "symbol_range",
        "partition_by": "date",
        "date_batch_supported": True,
        "date_batch_row_limit": 10000,
        "default_enabled": True,
    },
    "stk_auction": {
        "api": "stk_auction",
        "description": "当日集合竞价成交数据",
        "phase": "2",
        "fetch_mode": "symbol_range",
        "partition_by": "date",
        "date_batch_supported": True,
        "date_batch_row_limit": 8000,
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
        "fetch_mode": "index_loop_range",
        "partition_by": "date",
        # 该接口在实时更新窗口内可能存在发布延迟，最近几个交易日即便曾记录 source_empty
        # 也应继续重试，避免把“迟到数据”永久误判为历史空分区。
        "source_empty_retry_recent_days": 5,
        "reuse_source_empty_coverage": False,
        "default_enabled": True,
        "param_name": "ts_code",
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
        # 指数权重本质上是“按调仓日期生效”的低频数据，
        # 用周末分区可以避免在日更里反复把整周都当作新分区重扫。
        "date_partition_mode": "week_end",
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
        "symbol_field_priority": ["con_code", "ts_code"],
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
        "symbol_field_priority": ["con_code", "ts_code"],
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
        "freq": "1min",
        "single_request_row_limit": 8000,
        "date_field_priority": ("date",),
        "default_enabled": False,
    },
    "stk_nineturn": {
        "api": "stk_nineturn",
        "description": "神奇九转指标",
        "phase": "3B",
        "fetch_mode": "symbol_range",
        "partition_by": "date",
        "date_batch_supported": True,
        "date_batch_row_limit": 10000,
        "date_batch_params": {"freq": "daily"},
        "symbol_range_params": {"freq": "daily"},
        "default_enabled": False,
    },
    "realtime_quote": {
        "api": "realtime_quote",
        "description": "实时盘口快照（含买卖五档）",
        "phase": "3B",
        "fetch_mode": "realtime_quote_snapshot",
        "partition_by": "date",
        "default_enabled": False,
        "date_field_priority": ("date", "DATE"),
        "symbol_field_priority": ("symbol", "ts_code", "TS_CODE", "code", "CODE"),
        "rename_map": {
            "NAME": "name",
            "TIME": "time",
            "OPEN": "open",
            "PRE_CLOSE": "pre_close",
            "PRICE": "price",
            "HIGH": "high",
            "LOW": "low",
            "BID": "bid",
            "ASK": "ask",
            "VOLUME": "volume",
            "AMOUNT": "amount",
            "B1_V": "bid1_volume",
            "B1_P": "bid1_price",
            "B2_V": "bid2_volume",
            "B2_P": "bid2_price",
            "B3_V": "bid3_volume",
            "B3_P": "bid3_price",
            "B4_V": "bid4_volume",
            "B4_P": "bid4_price",
            "B5_V": "bid5_volume",
            "B5_P": "bid5_price",
            "A1_V": "ask1_volume",
            "A1_P": "ask1_price",
            "A2_V": "ask2_volume",
            "A2_P": "ask2_price",
            "A3_V": "ask3_volume",
            "A3_P": "ask3_price",
            "A4_V": "ask4_volume",
            "A4_P": "ask4_price",
            "A5_V": "ask5_volume",
            "A5_P": "ask5_price",
        },
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

for _dataset_name, _frequency in TUSHARE_DATASET_UPDATE_FREQUENCIES.items():
    if _dataset_name in TUSHARE_DATASET_REGISTRY:
        TUSHARE_DATASET_REGISTRY[_dataset_name].setdefault(
            "update_frequency",
            _frequency,
        )


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


def get_tushare_api_doc_url(api_name: str) -> str | None:
    """按 API 名返回官方接口文档地址。"""
    url = TUSHARE_API_DOC_URLS.get(api_name)
    if not url:
        return None

    legacy_prefix = "https://tushare.pro/wctapi/documents/"
    if url.startswith(legacy_prefix) and url.endswith(".md"):
        doc_id = url[len(legacy_prefix) : -len(".md")]
        if doc_id.isdigit():
            return f"https://tushare.pro/document/2?doc_id={doc_id}"
    return url


def get_tushare_dataset_api_name(name: str) -> str:
    """按 dataset 名返回底层 Tushare API 名。"""
    spec = get_tushare_dataset_spec(name)
    return str(spec.get("api") or resolve_tushare_dataset_name(name))


def get_tushare_dataset_api_doc_url(name: str) -> str | None:
    """按 dataset 名返回底层 Tushare API 的官方文档地址。"""
    return get_tushare_api_doc_url(get_tushare_dataset_api_name(name))


def get_tushare_dataset_access_rule(name: str) -> dict[str, Any]:
    """按 dataset 名获取访问规则。"""
    spec = get_tushare_dataset_spec(name)
    api_name = str(spec.get("api") or name)
    return get_tushare_api_access_rule(api_name)


def normalize_tushare_update_frequencies(
    update_frequencies: list[str] | tuple[str, ...] | set[str] | None,
) -> list[str]:
    """规范化更新频率列表，并按统一优先级排序。"""
    if not update_frequencies:
        return []

    normalized: list[str] = []
    seen: set[str] = set()
    for raw in update_frequencies:
        token = str(raw).strip().lower()
        if not token:
            continue
        canonical = TUSHARE_UPDATE_FREQUENCY_ALIASES.get(token, token)
        if canonical not in TUSHARE_UPDATE_FREQUENCY_ORDER:
            allowed = ", ".join(TUSHARE_UPDATE_FREQUENCY_ORDER)
            raise ValueError(
                f"未知更新频率: {raw}；可选值: {allowed}"
            )
        if canonical not in seen:
            normalized.append(canonical)
            seen.add(canonical)

    normalized.sort(key=TUSHARE_UPDATE_FREQUENCY_ORDER.index)
    return normalized


def get_tushare_dataset_update_frequency(name: str) -> str:
    """按 dataset 名返回建议更新频率。"""
    canonical = resolve_tushare_dataset_name(name)
    return TUSHARE_DATASET_UPDATE_FREQUENCIES.get(canonical, "other")


def filter_tushare_datasets_by_update_frequency(
    datasets: list[str],
    update_frequencies: list[str] | tuple[str, ...] | set[str] | None,
) -> list[str]:
    """按更新频率过滤 dataset，保留输入顺序。"""
    normalized = normalize_tushare_update_frequencies(update_frequencies)
    if not normalized:
        return list(datasets)

    allowed = set(normalized)
    return [
        dataset
        for dataset in datasets
        if get_tushare_dataset_update_frequency(dataset) in allowed
    ]


def get_tushare_dataset_note(name: str) -> str | None:
    """返回 dataset 的表级备注。"""
    canonical = resolve_tushare_dataset_name(name)
    return TUSHARE_DATASET_NOTES.get(canonical)


def get_tushare_dataset_field_docs(name: str) -> dict[str, str]:
    """返回 dataset 的字段说明；dataset 级说明优先，未命中时回退到通用字段说明。"""
    canonical = resolve_tushare_dataset_name(name)
    merged = dict(TUSHARE_COMMON_FIELD_DOCS)
    merged.update(TUSHARE_DATASET_FIELD_DOCS.get(canonical, {}))
    return merged


def get_default_tushare_datasets(
    points: int | None = None,
    permission_keys: set[str] | None = None,
    update_frequencies: list[str] | tuple[str, ...] | set[str] | None = None,
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

    datasets = [
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
    return filter_tushare_datasets_by_update_frequency(
        datasets,
        update_frequencies,
    )
