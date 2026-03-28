# Tushare Pro API 完整参考手册

> 数据来源: <https://tushare.pro/document/2>  
> 更新日期: 2026-03-28  
> HTTP端点: POST <http://api.tushare.pro>

---

## 目录

1. [调用方式](#调用方式)
2. [积分权限体系](#积分权限体系)
3. [股票数据 — 基础数据](#一-股票数据--基础数据)
4. [股票数据 — 行情数据](#二-股票数据--行情数据)
5. [股票数据 — 财务数据](#三-股票数据--财务数据)
6. [股票数据 — 参考数据](#四-股票数据--参考数据)
7. [股票数据 — 特色数据](#五-股票数据--特色数据)
8. [股票数据 — 两融/资金流向/打板](#六-股票数据--两融资金流向打板)
9. [ETF专题](#七-etf专题)
10. [指数专题](#八-指数专题)
11. [公募基金](#九-公募基金)
12. [期货/现货/期权](#十-期货现货期权)
13. [债券专题](#十一-债券专题)
14. [外汇/港股/美股](#十二-外汇港股美股)
15. [宏观经济](#十三-宏观经济)
16. [行业经济](#十四-行业经济)
17. [大模型语料/资讯](#十五-大模型语料资讯)
18. [低积分账户实战指南](#低积分账户实战指南)

---

## 调用方式

### Python SDK

```python
import tushare as ts
ts.set_token('your_token')
pro = ts.pro_api()

# 方式1: 直接调用
df = pro.daily(ts_code='000001.SZ', start_date='20240101', end_date='20240301')

# 方式2: query方式
df = pro.query('daily', ts_code='000001.SZ', start_date='20240101')
```

### HTTP (语言无关)

```bash
curl -X POST -d '{
  "api_name": "daily",
  "token": "xxx",
  "params": {"ts_code":"000001.SZ","start_date":"20240101"},
  "fields": "ts_code,trade_date,open,high,low,close,vol"
}' http://api.tushare.pro
```

### 代码规范

| 交易所 | 代码 | 后缀 | 示例 |
|--------|------|------|------|
| 上交所 | SSE | .SH | 600000.SH |
| 深交所 | SZSE | .SZ | 000001.SZ |
| 北交所 | BSE | .BJ | 9xxxxx.BJ |
| 港交所 | HKEX | .HK | 00001.HK |

---

## 积分权限体系

| 积分等级 | 可用范围 | 典型限制 |
|----------|----------|----------|
| **120** (注册) | 极少接口 | 基本无法使用 |
| **2000** | 大部分基础接口 | 财务数据只能按**单只股票**查历史 |
| **5000** | VIP接口 + 高频 | `_vip`接口可按**季度**批量拉全市场 |
| **10000+** | 分钟行情/实时数据 | 高频Tick、实时分钟 |

---

## 一. 股票数据 — 基础数据

| 接口 | 函数名 | 描述 | 积分 | 限量 | 关键字段 |
|------|--------|------|------|------|----------|
| 股票列表 | `stock_basic` | 获取股票基础信息 | 2000 | 6000条/次, 50次/分 | ts_code, name, industry, list_date, list_status, is_hs, act_name, act_ent_type |
| 交易日历 | `trade_cal` | 各交易所交易日历 | 基础 | — | exchange, cal_date, is_open, pretrade_date |
| 每日股本(盘前) | `stk_factor_pro` | 盘前股本数据 | 2000 | — | ts_code, trade_date |
| ST股票列表 | `st_list` | ST/\*ST股票 | 2000 | — | ts_code, name |
| 沪深港通股票列表 | `hs_const` | 港通标的列表 | 2000 | — | ts_code, hs_type |
| 股票曾用名 | `namechange` | 历史名称变更 | 2000 | — | ts_code, name, start_date |
| 上市公司基本信息 | `stock_company` | 公司注册/管理信息 | 2000 | — | ts_code, chairman, manager |
| 上市公司管理层 | `stk_managers` | 高管信息 | 2000 | — | ts_code, name, title |
| 管理层薪酬和持股 | `stk_rewards` | 薪酬持股 | 2000 | — | ts_code, name, reward |
| IPO新股上市 | `new_share` | 新股信息 | 2000 | — | ts_code, sub_code, ipo_date |
| 股票历史列表 | `bak_basic` | 含退市历史 | 2000 | — | ts_code |

---

## 二. 股票数据 — 行情数据

| 接口 | 函数名 | 描述 | 积分 | 限量/频率 | 关键输出字段 |
|------|--------|------|------|-----------|-------------|
| **历史日线** | `daily` | A股未复权日线 | 基础 | 6000条/次, 500次/分 | ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount |
| 实时日线 | `quotes` | 实时行情 | 5000 | — | — |
| 历史分钟 | `stk_mins` | 1/5/15/30/60分钟 | 5000+ | — | — |
| 实时分钟 | `stk_mins_rt` | 实时分钟 | 10000 | — | — |
| 周线行情 | `weekly` | 周线 | 基础 | — | 同daily |
| 月线行情 | `monthly` | 月线 | 基础 | — | 同daily |
| 复权行情 | `pro_bar` (adj) | 前/后复权 | 基础 | — | 同daily + adj_factor |
| 周/月线行情(每日更新) | `stk_weekly`/`stk_monthly` | 每日更新 | 2000 | — | — |
| 复权因子 | `adj_factor` | 复权因子 | 基础 | — | ts_code, trade_date, adj_factor |
| **每日指标** | `daily_basic` | PE/PB/换手率/市值等 | **2000** (5000无限量) | 6000条/次 | ts_code, trade_date, close, turnover_rate, turnover_rate_f, volume_ratio, pe, pe_ttm, pb, ps, ps_ttm, **dv_ratio**, **dv_ttm**, total_share, float_share, free_share, **total_mv**, **circ_mv** |
| 通用行情接口 | `pro_bar` | 多功能行情 | 基础 | — | — |
| 每日涨跌停价格 | `stk_limit` | 涨跌停板价 | 2000 | — | ts_code, up_limit, down_limit |
| 每日停复牌信息 | `suspend_d` | 停复牌 | 2000 | — | ts_code, suspend_type |
| 沪深股通十大成交股 | `hsgt_top10` | 北向十大成交 | 2000 | — | — |
| 港股通十大成交股 | `ggt_top10` | 南向十大成交 | 2000 | — | — |
| 港股通每日成交统计 | `ggt_daily` | 南向每日 | 2000 | — | — |
| 港股通每月成交统计 | `ggt_monthly` | 南向每月 | 2000 | — | — |
| 备用行情 | `bak_daily` | 备用 | 基础 | — | — |

---

## 三. 股票数据 — 财务数据

### ⚠️ 关键限制：2000积分只能按**单只股票**查询，5000积分可用 `_vip` 接口按季度批量查

| 接口 | 函数名 | 描述 | 积分 | VIP接口 | 关键输出字段 |
|------|--------|------|------|---------|-------------|
| **利润表** | `income` | 上市公司利润表 | **2000** | `income_vip`(5000) | ts_code, ann_date, f_ann_date, end_date, report_type, **basic_eps**, **total_revenue**, **revenue**, total_cogs, oper_cost, sell_exp, admin_exp, fin_exp, **operate_profit**, **total_profit**, income_tax, **n_income**, **n_income_attr_p**, ebit, ebitda, rd_exp |
| **资产负债表** | `balancesheet` | 资产负债表 | **2000** | `balancesheet_vip`(5000) | ts_code, end_date, report_type, **total_assets**, **total_liab**, **total_hldr_eqy_exc_min_int**, money_cap, accounts_receiv, inventories, fix_assets, **total_cur_assets**, **total_nca**, lt_borr, st_borr, **total_share**, cap_rese |
| **现金流量表** | `cashflow` | 现金流量表 | **2000** | `cashflow_vip`(5000) | ts_code, end_date, report_type, **n_cashflow_act**(经营), **n_cashflow_inv_act**(投资), **n_cash_flows_fnc_act**(筹资), **c_fr_sale_sg**, c_paid_goods_s, **free_cashflow**, n_incr_cash_cash_equ |
| **财务指标** | `fina_indicator` | 综合财务指标(100条/次) | **2000** | `fina_indicator_vip`(5000) | ts_code, end_date, **eps**, **roe**, **roe_waa**, **roe_dt**, roa, **roic**, bps, **ocfps**, **fcff**, **fcfe**, **grossprofit_margin**, **netprofit_margin**, **debt_to_assets**, current_ratio, **netprofit_yoy**, **or_yoy**, **roe_yoy**, **equity_yoy**, rd_exp |
| 业绩预告 | `forecast` | 业绩预告 | 2000 | `forecast_vip`(5000) | ts_code, type, p_change_min/max |
| 业绩快报 | `express` | 业绩快报 | 2000 | `express_vip`(5000) | ts_code, revenue, n_income, total_assets |
| **分红送股** | `dividend` | 分红送股历史 | **2000** | — | ts_code, end_date, ann_date, **div_proc**(实施进度), **stk_div**(每股送转), stk_bo_rate, stk_co_rate, **cash_div**(每股分红税后), **cash_div_tax**(税前), **record_date**, **ex_date**, pay_date, imp_ann_date, base_share |
| 财务审计意见 | `fina_audit` | 审计意见 | 2000 | — | ts_code, audit_result |
| 主营业务构成 | `fina_mainbz` | 主营构成 | 2000 | — | ts_code, bz_item, bz_sales |
| 财报披露日期表 | `disclosure_date` | 披露计划 | 2000 | — | ts_code, end_date, pre_date, actual_date |

### fina_indicator 核心字段分类

| 类别 | 字段 |
|------|------|
| **每股指标** | eps, dt_eps, bps, ocfps, cfps, fcff_ps, fcfe_ps, retainedps |
| **盈利能力** | roe, roe_waa, roe_dt, roa, roic, netprofit_margin, grossprofit_margin |
| **现金流** | fcff, fcfe, ocf_to_or, ocf_to_opincome, salescash_to_or |
| **偿债能力** | current_ratio, quick_ratio, debt_to_assets, debt_to_eqt |
| **运营效率** | assets_turn, ca_turn, fa_turn, inv_turn, ar_turn, turn_days |
| **成长性** | netprofit_yoy, or_yoy, roe_yoy, equity_yoy, tr_yoy, basic_eps_yoy |
| **单季度** | q_roe, q_npta, q_eps, q_netprofit_margin, q_gsprofit_margin, q_sales_yoy |
| **杜邦分析** | roa_dp, dp_assets_to_eqt |
| **研发** | rd_exp |

---

## 四. 股票数据 — 参考数据

| 接口 | 函数名 | 描述 | 积分 |
|------|--------|------|------|
| 前十大股东 | `top10_holders` | 前十大股东 | 2000 |
| 前十大流通股东 | `top10_floatholders` | 流通股东 | 2000 |
| 股权质押统计 | `pledge_stat` | 质押统计 | 2000 |
| 股权质押明细 | `pledge_detail` | 质押明细 | 2000 |
| 股票回购 | `repurchase` | 回购 | 2000 |
| 限售股解禁 | `share_float` | 解禁 | 2000 |
| 大宗交易 | `block_trade` | 大宗 | 2000 |
| 股东人数 | `stk_holdernumber` | 股东户数 | 2000 |
| 股东增减持 | `stk_holdertrade` | 增减持 | 2000 |
| 个股异常波动 | `stk_surv` | 异常波动 | 2000 |

---

## 五. 股票数据 — 特色数据

| 接口 | 函数名 | 描述 | 积分 |
|------|--------|------|------|
| 券商盈利预测 | `report_rc` | 盈利预测 | 5000+ |
| 每日筹码及胜率 | `cyq_perf` | 筹码分析 | 5000+ |
| 每日筹码分布 | `cyq_chips` | 筹码分布 | 5000+ |
| 股票技术面因子 | `stk_factor` | 技术因子(专业版) | 5000+ |
| 中央结算系统持股 | `hk_hold` | CCASS | 2000 |
| 沪深股通持股明细 | `ggt_hold` | 北向明细 | 2000 |
| 股票开盘集合竞价 | `stk_auction_o` | 集合竞价(开盘) | 5000 |
| 股票收盘集合竞价 | `stk_auction_c` | 集合竞价(收盘) | 5000 |
| AH股比价 | `ah_comparelist` | AH比价 | 2000 |
| 机构调研 | `stk_surv` | 调研 | 2000 |
| 券商月度金股 | `broker_recommend` | 金股 | 2000 |

---

## 六. 股票数据 — 两融/资金流向/打板

| 接口 | 函数名 | 描述 | 积分 |
|------|--------|------|------|
| 融资融券交易汇总 | `margin` | 两融汇总 | 2000 |
| 融资融券交易明细 | `margin_detail` | 两融明细 | 2000 |
| 融资融券标的(盘前) | `margin_target` | 盘前标的 | 2000 |
| 转融资交易汇总 | `slb_len_mm` | 转融资 | 2000 |
| 个股资金流向 | `moneyflow` | 资金流 | 2000 |
| 个股资金流向(THS) | `moneyflow_ths` | 同花顺口径 | 5000 |
| 个股资金流向(DC) | `moneyflow_dc` | 东财口径 | 5000 |
| 沪深港通资金流向 | `moneyflow_hsgt` | 北向资金 | 2000 |
| 龙虎榜统计 | `top_list` | 龙虎榜 | 2000 |
| 龙虎榜机构 | `top_inst` | 机构席位 | 2000 |
| 涨跌停和炸板 | `limit_list_d` | 涨停板 | 2000 |
| 同花顺概念板块 | `ths_index` | THS概念 | 2000 |
| 同花顺概念成分 | `ths_member` | THS成分 | 2000 |
| 东方财富概念板块 | `dc_index` | DC概念 | 2000 |

---

## 七. ETF专题

| 接口 | 函数名 | 描述 | 积分 |
|------|--------|------|------|
| ETF基本信息 | `fund_basic` | ETF列表 | 2000 |
| ETF基准指数 | `fund_adj` | 基准 | 2000 |
| ETF日线行情 | `fund_daily` | ETF日线 | 2000 |
| ETF复权因子 | `fund_adj` | 复权因子 | 2000 |
| ETF份额规模 | `fund_share` | 份额 | 2000 |
| ETF历史分钟 | `ft_mins` | 分钟线 | 5000 |
| ETF实时日线 | `fund_daily_rt` | 实时 | 5000 |

---

## 八. 指数专题

| 接口 | 函数名 | 描述 | 积分 | 限量 | 关键输出字段 |
|------|--------|------|------|------|-------------|
| 指数基本信息 | `index_basic` | 指数列表 | 2000 | — | ts_code, name, market, publisher, category |
| **指数日线行情** | `index_daily` | 指数日线 | **2000** (5000高频) | 8000条/次 | ts_code, trade_date, close, open, high, low, pre_close, change, pct_chg, vol, amount |
| 指数实时日线 | `index_daily_rt` | 实时 | 5000 | — | — |
| 指数周线 | `index_weekly` | 周线 | 2000 | — | 同日线 |
| 指数月线 | `index_monthly` | 月线 | 2000 | — | 同日线 |
| 指数历史分钟 | `index_mins` | 分钟 | 5000 | — | — |
| **指数成分和权重** | `index_weight` | 成分股权重(月度) | **2000** | — | **index_code**, **con_code**, trade_date, **weight** |
| 大盘指数每日指标 | `index_dailybasic` | PE/PB等 | 2000 | — | ts_code, trade_date, pe, pb |
| **申万行业分类** | `index_classify` | SW行业分类 | **2000** | — | **index_code**, **industry_name**, parent_code, **level**(L1/L2/L3), is_pub, **src**(SW2014/SW2021) |
| 申万行业成分(分级) | `index_member` | SW成分 | 2000 | — | index_code, con_code |
| 申万行业指数日行情 | `sw_daily` | SW日线 | **5000** | — | — |
| 中信行业成分 | `ci_cons` | CITIC成分 | 2000 | — | — |
| 中信行业指数日行情 | `ci_daily` | CITIC日线 | 5000 | — | — |
| 国际主要指数 | `index_global` | 全球指数 | 2000 | — | — |
| 沪深市场每日交易统计 | `daily_info` | 市场统计 | 2000 | — | — |

### index_classify 申万2021版行业数量

- 一级行业: 31个
- 二级行业: 134个
- 三级行业: 346个

---

## 九. 公募基金

| 接口 | 函数名 | 描述 | 积分 |
|------|--------|------|------|
| 基金列表 | `fund_basic` | 基金列表 | 2000 |
| 基金管理人 | `fund_company` | 基金公司 | 2000 |
| 基金经理 | `fund_manager` | 基金经理 | 2000 |
| 基金规模 | `fund_share` | 规模 | 2000 |
| 基金净值 | `fund_nav` | 净值 | 2000 |
| 基金分红 | `fund_div` | 分红 | 2000 |
| 基金持仓 | `fund_portfolio` | 持仓 | 2000 |

---

## 十. 期货/现货/期权

### 期货

| 接口 | 函数名 | 描述 | 积分 |
|------|--------|------|------|
| 合约信息 | `fut_basic` | 期货合约 | 2000 |
| 交易日历 | `trade_cal` | 期货日历 | 基础 |
| 日线行情 | `fut_daily` | 期货日线 | 2000 |
| 仓单日报 | `fut_wsr` | 仓单 | 2000 |
| 每日结算参数 | `fut_settle` | 结算 | 2000 |
| 每日持仓排名 | `fut_holding` | 持仓排名 | 2000 |
| 南华期货指数 | `index_nhf` | 南华指数 | 2000 |
| 期货主力连续 | `fut_mapping` | 主力合约 | 2000 |

### 期权

| 接口 | 函数名 | 描述 | 积分 |
|------|--------|------|------|
| 期权合约信息 | `opt_basic` | 合约列表 | 2000 |
| 期权日线行情 | `opt_daily` | 日线 | 2000 |

---

## 十一. 债券专题

| 接口 | 函数名 | 描述 | 积分 |
|------|--------|------|------|
| 可转债基础信息 | `cb_basic` | 转债列表 | 2000 |
| 可转债发行 | `cb_issue` | 发行 | 2000 |
| 可转债行情 | `cb_daily` | 日线 | 2000 |
| 可转债转股价变动 | `cb_price_chg` | 转股价 | 2000 |
| 国债收益率曲线 | `yc_cb` | 国债曲线 | 2000 |

---

## 十二. 外汇/港股/美股

### 港股

| 接口 | 函数名 | 描述 | 积分 |
|------|--------|------|------|
| 港股基础信息 | `hk_basic` | 港股列表 | 2000 |
| 港股日线行情 | `hk_daily` | 港股日线 | 2000 |
| 港股复权行情 | `hk_adj_daily` | 复权 | 2000 |
| 港股利润表 | `hk_income` | 利润表 | 5000 |
| 港股资产负债表 | `hk_balancesheet` | 资产负债 | 5000 |
| 港股现金流量表 | `hk_cashflow` | 现金流 | 5000 |
| 港股财务指标 | `hk_fina_indicator` | 财务指标 | 5000 |

### 美股

| 接口 | 函数名 | 描述 | 积分 |
|------|--------|------|------|
| 美股基础信息 | `us_basic` | 美股列表 | 2000 |
| 美股日线行情 | `us_daily` | 美股日线 | 2000 |
| 美股利润表 | `us_income` | 利润表 | 5000 |

### 外汇

| 接口 | 函数名 | 描述 | 积分 |
|------|--------|------|------|
| 外汇基础信息 | `fx_obasic` | 外汇列表 | 2000 |
| 外汇日线行情 | `fx_daily` | 外汇日线 | 2000 |

---

## 十三. 宏观经济

| 接口 | 函数名 | 描述 | 积分 |
|------|--------|------|------|
| Shibor利率 | `shibor` | Shibor | 基础 |
| Shibor报价 | `shibor_quote` | 报价 | 基础 |
| LPR贷款基础利率 | `shibor_lpr` | LPR | 基础 |
| Libor利率 | `libor` | Libor | 基础 |
| GDP | `cn_gdp` | 国内生产总值 | 2000 |
| CPI | `cn_cpi` | 消费者价格指数 | 2000 |
| PPI | `cn_ppi` | 工业品出厂价格 | 2000 |
| 货币供应量 | `cn_m` | M0/M1/M2 | 2000 |
| 社会融资 | `sf_month` | 社融 | 2000 |
| PMI | `cn_pmi` | 采购经理指数 | 2000 |

---

## 十四. 行业经济

| 接口 | 函数名 | 描述 | 积分 |
|------|--------|------|------|
| 台湾电子产业月营收 | `bo_monthly` | 台电子 | 2000 |
| 电影票房(日/周/月) | `bo_cinema` | 票房 | 2000 |

---

## 十五. 大模型语料/资讯

| 接口 | 函数名 | 描述 | 积分 |
|------|--------|------|------|
| 新闻快讯 | `news` | 短讯 | 5000 |
| 新闻通讯 | `major_news` | 长篇 | 5000 |
| 新闻联播文字稿 | `cctv_news` | 新闻联播 | 2000 |
| 上市公司公告 | `anns` | 公告 | 5000 |
| 国家政策库 | `gov_policy` | 政策 | 5000 |
| 券商研究报告 | `report` | 研报 | 5000 |

---

## 低积分账户实战指南

### 2000积分可用的核心接口（QuantPilot项目使用）

```
stock_basic      → 股票列表（调一次即可）
trade_cal        → 交易日历（调一次即可）
daily            → A股日线（500次/分，6000条/次）
daily_basic      → 每日指标（PE/PB/股息率/市值）
adj_factor       → 复权因子
income           → 利润表（⚠️ 只能按单只股票查）
balancesheet     → 资产负债表（⚠️ 同上）
cashflow         → 现金流量表（⚠️ 同上）
fina_indicator   → 财务指标（⚠️ 同上，且100条/次）
dividend         → 分红送股
index_daily      → 指数日线
index_weight     → 指数成分权重
index_classify   → 申万行业分类
```

### ⚠️ 关键Gotchas（坑点）

#### 1. 财务数据只能逐股查询（2000积分）

```python
# ❌ 错误：不能批量查
df = pro.fina_indicator(period='20231231')  # 报错 

# ✅ 正确：逐股遍历
for code in stock_list:
    df = pro.fina_indicator(ts_code=code)
    time.sleep(0.15)  # 控制频率
```

#### 2. fina_indicator 每次最多100条

```python
# 一个股票最多返回100个季度(25年历史)，一般够用
# 如需更多数据，用start_date/end_date分段
```

#### 3. daily_basic 的 dv_ratio vs dv_ttm

```python
# dv_ratio = 近12个月股息率（基于最近一期年报分红）
# dv_ttm   = TTM股息率（基于滚动12个月分红）
# 对于高股息策略，建议用 dv_ttm
```

#### 4. dividend 接口的 div_proc 字段

```python
# div_proc 取值：预案 / 实施 / 不分配
# 高股息策略只关心 div_proc == '实施' 的记录
# cash_div 是税后每股分红，cash_div_tax 是税前
```

#### 5. 频率控制

```python
# 基础积分：大部分接口 1分钟最多50次
# daily 接口比较宽松：500次/分
# 建议统一加 time.sleep(0.2)
```

#### 6. 报表类型 report_type

```python
# 1=合并报表(默认)  2=单季合并  4=调整合并(上年同期)
# 5=调整前合并(原始)  6=母公司
# 回测时注意：同一期可能有多条，需按 f_ann_date 去重
```

#### 7. index_weight 是月度数据

```python
# 建议用当月首尾日期
df = pro.index_weight(
    index_code='399300.SZ', 
    start_date='20240901', 
    end_date='20240930'
)
```

#### 8. 日期格式统一 YYYYMMDD

```python
# 所有日期参数必须是字符串 '20240101'，不是 datetime 对象
```

### 数据下载策略（低积分优化）

```python
# 策略：本地Parquet缓存 + 增量更新
# 1. stock_basic/trade_cal → 全量拉一次
# 2. daily/daily_basic → 按日期全量拉(trade_date参数)
# 3. 财务数据 → 逐股遍历，本地缓存，增量更新
# 4. dividend → 逐股查询，缓存到本地

# 批量下载财务数据的最佳实践：
stocks = pro.stock_basic(list_status='L')['ts_code'].tolist()
for i, code in enumerate(stocks):
    try:
        df = pro.fina_indicator(ts_code=code)
        df.to_parquet(f'data/fundamental/fina_indicator/{code}.parquet')
    except Exception as e:
        print(f'{code} failed: {e}')
    if i % 5 == 0:
        time.sleep(1)  # 每5只股票暂停1秒
```

---

## 常用指数代码速查

| 指数 | 代码 | 发布方 |
|------|------|--------|
| 上证综指 | 000001.SH | SSE |
| 深证成指 | 399001.SZ | SZSE |
| 沪深300 | 000300.SH / 399300.SZ | CSI |
| 中证500 | 000905.SH | CSI |
| 中证1000 | 000852.SH | CSI |
| 创业板指 | 399006.SZ | SZSE |
| 科创50 | 000688.SH | SSE |
| 上证50 | 000016.SH | SSE |
| 中证红利 | 000922.SH | CSI |
| 上证红利 | 000015.SH | SSE |
| 深证红利 | 399324.SZ | SZSE |
| 深证A指 | 399107.SZ | SZSE |
