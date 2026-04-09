# Tushare 全量数据接口参考表

> 目标：Vortex 数据域的长期目标是抓取 Tushare 全量数据。本文档列出 Tushare 全部公开接口，按业务分类组织，供数据域开发与数据治理参考。
>
> 来源：tushare.pro 官方接口文档（截至 2025 年）。

---

## 0. Vortex 当前的高效抓法口径

### 0.1 先看 6 类抓法

| 抓法类型 | 典型接口 | “完整分区”示例 | 什么时候优先用 |
|------|------|------|------|
| 日期整批抓 | `daily` / `daily_basic` / `moneyflow` | 单个 `trade_date` 的全市场快照 | 一个日期就能拿到该分区的大部分或全部数据，而且单次返回上限装得下当前 universe |
| 周/月末整批抓 | `weekly` / `monthly` / `stk_weekly_monthly` | 单个 `week_end` / `month_end` 快照 | 周/月线接口支持按锚点日期整批抓，且单次上限足够 |
| 季度整批抓 | `income_vip` / `balancesheet_vip` 等 | 单个季度末财报快照 | 财务类接口支持按 `period` 批量抓，远优于逐股票逐季度循环 |
| 交易所 / 市场整批抓 | `stock_company` / `fund_basic` / `index_basic` | 一个 `exchange` 或 `market` 的整表 | 参考表、名录表天然按市场维度拆分 |
| 整表缓存 / 目录复用 | `stock_basic` / `index_classify` / `ths_index` | 整表或目录缓存 | 变化频率低，不该每次同步都重抓 |
| 股票历史抓 | `adj_factor` / `weekly` / `monthly` / `index_daily` | 单只证券在一个时间窗口内的历史 | 只有在整批抓不安全、上限不够或根本不支持时才回退 |

### 0.2 当前已接入 dataset 能力矩阵（直接影响当前代码）

| 数据集 | 底层接口 | 资产范围 | 当前主抓法 | 可选更优抓法 | 完整分区 | 关键限制 | 当前结论 |
|------|------|------|------|------|------|------|------|
| `bars` | `daily` | 股票 | 日期整批抓 | 股票历史抓（不推荐） | 单个 `trade_date` | `trade_date` 路线天然对应日分区 | 保持按日期整批抓 |
| `valuation` | `daily_basic` | 股票 | 日期整批抓 | 股票历史抓（不推荐） | 单个 `trade_date` | 单次 6000 行，当前股票 universe 可装下 | 保持按日期整批抓 |
| `adj_factor` | `adj_factor` | 股票 | **自动比较：日期整批抓 vs 股票历史抓** | 同左 | 单个 `trade_date` | 官方明确支持“单日全部股票复权因子”；缺口很少时日期路径请求数显著更小 | 已切到自动选路 |
| `weekly` | `weekly` | 股票 | **自动比较：周末整批抓 vs 股票历史抓** | 同左 | 单个 `week_end` | 官方单次上限 6000，当前股票 universe 大体可装下 | 已切到自动选路 |
| `monthly` | `monthly` | 股票 | 自动比较后通常回退股票历史抓 | 月末整批抓（仅候选） | 单个 `month_end` | 官方单次上限 4500，当前股票 universe 约 5499，常会超上限 | 当前仍以股票历史抓为主 |
| `stock_st` | `stock_st` | 股票 | 日期整批抓 | 无 | 单个 `trade_date` | ST 名单天然是按日快照 | 保持按日期整批抓 |
| `moneyflow` | `moneyflow` | 股票 | 日期整批抓 | 股票历史抓（不推荐） | 单个 `trade_date` | 缺口续跑按日最直接 | 保持按日期整批抓 |
| `moneyflow_hsgt` | `moneyflow_hsgt` | 股票 / 市场 | 日期整批抓 | 无 | 单个 `trade_date` | 北向 / 南向资金本身就是日快照 | 保持按日期整批抓 |
| `hsgt_top10` | `hsgt_top10` | 股票 | 日期整批抓 | 无 | 单个 `trade_date` | 每日 TOP10 榜单 | 保持按日期整批抓 |
| `top_list` | `top_list` | 股票 | 日期整批抓 | 无 | 单个 `trade_date` | 龙虎榜天然按日 | 保持按日期整批抓 |
| `top_inst` | `top_inst` | 股票 | 日期整批抓 | 无 | 单个 `trade_date` | 龙虎榜机构成交天然按日 | 保持按日期整批抓 |
| `moneyflow_ind_dc` | `moneyflow_ind_dc` | 行业 / 主题 | 日期整批抓 | 无 | 单个 `trade_date` | 板块资金流本身是按日快照 | 保持按日期整批抓 |
| `moneyflow_mkt_dc` | `moneyflow_mkt_dc` | 市场 | 日期整批抓 | 无 | 单个 `trade_date` | 大盘资金流本身是按日快照 | 保持按日期整批抓 |
| `limit_list_d` | `limit_list_d` | 股票 | 日期整批抓 | 无 | 单个 `trade_date` | 涨跌停明细天然按日 | 保持按日期整批抓 |
| `limit_step` | `limit_step` | 股票 | 日期整批抓 | 无 | 单个 `trade_date` | 连板梯队天然按日 | 保持按日期整批抓 |
| `kpl_list` | `kpl_list` | 股票 | 日期整批抓 | 无 | 单个 `trade_date` | 榜单类天然按日 | 保持按日期整批抓 |
| `dc_hot` | `dc_hot` | 多市场热榜 | 日期整批抓 | 无 | 单个 `trade_date` | 热榜天然按日 | 保持按日期整批抓 |
| `ths_hot` | `ths_hot` | 多市场热榜 | 日期整批抓 | 无 | 单个 `trade_date` | 热榜天然按日 | 保持按日期整批抓 |
| `sw_daily` | `sw_daily` | 行业指数 | 日期整批抓 | 指数历史抓（不推荐） | 单个 `trade_date` | 当前申万日线更适合按日抓 | 保持按日期整批抓 |
| `fundamental` / `balancesheet` / `cashflow` / `fina_indicator` / `forecast` / `express` / `disclosure_date` | 季频财务接口 | 股票 | 季度整批抓 | 股票逐季抓（仅回退） | 单个季度末 `report_date` / `period` | 5000 分档位优先 `*_vip(period=季度末)`；积分不足才回退 | 保持季度整批抓优先 |
| `stock_company` | `stock_company` | 股票参考表 | 交易所整批抓 | 整表单次抓（不适用） | 单个 `exchange` | 当前已按 `SSE/SZSE/BSE` 拆分 | 保持交易所整批抓 |
| `instruments` | `stock_basic` | 股票参考表 | 整表缓存 / refresh | 无 | 整表 | 变化频率低 | 本地有缓存时优先复用 |
| `calendar` | `trade_cal` | 市场参考表 | 整表缓存 / refresh | 无 | 交易日历范围 | 作为其它表的基准依赖 | 本地有缓存时优先复用 |
| `fund_basic` | `fund_basic` | 基金参考表 | 市场整批抓 + 缓存 | 无 | 单个 `market` 或整表 | 当前基金接入主要是列表，不是时间序列 | 先按参考表优化 |
| `index_basic` | `index_basic` | 指数参考表 | 市场整批抓 + 缓存 | 无 | 单个 `market` | 当前已按 market 维度抓 | 保持市场整批抓 |
| `index_classify` / `index_member_all` / `ths_index` / `dc_index` / `st` | 目录 / 参考接口 | 指数 / 行业 / 主题 / 风险名单 | 整表缓存 / refresh | 无 | 整表或目录 | 变化频率低 | 保持缓存优先 |
| `ths_member` / `dc_member` | 成员接口 | 行业 / 主题 | 成员循环 | 无 | 单个板块 / 主题 | 必须先有目录，再逐板块抓成分 | 保持成员循环 |
| `index_weight` | `index_weight` | 指数 | 指数循环 + 日期范围 | 无 | 单个指数在一个时间窗口 | 权重天然依赖 index_code | 保持指数循环 |
| `index_daily` | `index_daily` | 指数 | 指数循环 + 日期范围 | 指数按日整批抓（未来可评估） | 单个指数在一个时间窗口 | 当前官方接入和本地 schema 更适合按指数代码抓 | 保持指数循环 |
| `events` | `dividend` | 股票事件 | 稀疏事件扫描 | 重叠窗口 / exact-range coverage | 一个扫描范围，不是单个交易日快照 | 事件存在补发 / 日期列回退 / 稀疏分布 | 不纳入按日期 vs 按股票的简单比较 |
| `namechange` | `namechange` | 股票事件 | 稀疏事件扫描 | 重叠窗口 | 一个扫描范围 | 名称 / ST 历史有修订风险 | 不纳入按日期 vs 按股票的简单比较 |

### 0.3 已确认的未来候选接口（本轮能力表要一起盘点）

| 接口 | 资产范围 | 潜在高效抓法 | 完整分区 | 当前已知限制 | 当前建议 |
|------|------|------|------|------|------|
| `fund_daily` | 基金 / ETF | 日期整批抓 | 单个 `trade_date` | 需确认基金 universe、单次上限和本地 schema | 作为基金行情首选候选 |
| `fund_adj` | 基金 / ETF | 日期整批抓或基金历史抓 | 单个 `trade_date` | 需确认单次上限和是否适合全市场快照 | 作为基金复权候选 |
| `fund_nav` | 基金 | 日期窗口抓 | 净值披露日期窗口 | 场外基金净值更新频率不等于交易日 | 倾向窗口抓，不强行套 `trade_date` |
| `fund_div` | 基金 | 事件扫描 | 分红事件范围 | 与股票 `events` 类似，存在稀疏 / 补发风险 | 不直接套日期整批抓 |
| `fund_portfolio` | 基金 | 季度整批抓 | 单个季度 | 季度披露，不是日频 | 走季度路线 |
| `ths_daily` | 行业 / 主题 | 日期整批抓 | 单个 `trade_date` | 需确认权限、单次上限和字段稳定性 | 适合作为板块日线候选 |
| `dc_daily` | 行业 / 主题 | 日期整批抓 | 单个 `trade_date` | 同上 | 适合作为板块日线候选 |
| `index_weekly` | 指数 | 周末整批抓或指数历史抓 | 单个 `week_end` | 需确认单次上限与指数 universe | 可仿照股票 `weekly` 评估 |
| `index_monthly` | 指数 | 月末整批抓或指数历史抓 | 单个 `month_end` | 需确认单次上限与指数 universe | 可仿照股票 `monthly` 评估 |
| `index_dailybasic` | 指数 | 日期整批抓 | 单个 `trade_date` | 需确认是否覆盖当前目标指数 universe | 适合作为指数日指标候选 |
| `suspend_d` | 股票 | 日期整批抓 | 单个 `trade_date` | 返回的是停复牌名单，不是全量行情 | 适合按日整批抓 |
| `stk_limit` | 股票 / 基金 | 日期整批抓 | 单个 `trade_date` | 单次上限约 5800，需跟当前 universe 持续对比 | 适合作为候选日快照接口 |
| `bak_daily` | 股票 | 日期整批抓 | 单个 `trade_date` | 需确认字段口径与主行情的定位差异 | 更适合作为补充表，不替代 `daily` |
| `stk_weekly_monthly` | 股票 | 周/月末整批抓 | 单个 `week_end` / `month_end` | 是“每日更新”的周/月线，不完全等于经典 `weekly/monthly` 语义 | 后续重点评估，用来改善 `monthly` 路径 |

---

## 一、股票数据

### 1.1 基础数据

| 接口 | 标题 | 说明 |
|------|------|------|
| `stock_basic` | 股票列表 | 股票代码、名称、上市日期、退市日期等 |
| `trade_cal` | 交易日历 | 沪深交易所交易日历 |
| `namechange` | 股票曾用名 | 历史名称变更记录（含 ST 状态） |
| `stock_company` | 上市公司基本信息 | 公司注册信息 |
| `stk_managers` | 上市公司管理层 | 管理层成员 |
| `stk_rewards` | 管理层薪酬和持股 | 薪酬与持股 |
| `new_share` | IPO 新股上市 | 新股上市列表 |
| `stk_premarket` | 每日股本(盘前) | 总股本/流通股本/涨跌停价 |
| `bak_basic` | 股票历史列表 | 备用基础列表（2016 起） |
| `stock_hsgt` | 沪深港通股票列表 | 沪深港通标的 |
| `stock_st` | ST 股票列表 | 按交易日获取历史 ST 列表 |
| `st` | ST 风险警示板股票 | 风险警示板股票列表 |
| `bse_mapping` | 北交所新旧代码对照 | 代码变更映射表 |

### 1.2 行情数据

| 接口 | 标题 | 说明 |
|------|------|------|
| `daily` | 历史日线 | 日线行情（含前后复权） |
| `pro_bar` | 通用行情接口 / 复权行情 | 支持复权的通用行情 |
| `weekly` | 周线行情 | 每周最后一个交易日更新 |
| `monthly` | 月线行情 | 月线数据 |
| `stk_weekly_monthly` | 周月线行情(每日更新) | 日度更新的周/月线 |
| `stk_week_month_adj` | 周月线复权行情(每日更新) | 复权版周/月线 |
| `stk_mins` | 历史分钟 | 1/5/15/30/60min 分钟线 |
| `rt_k` | 实时日线 | 实时日 K 线 |
| `rt_min` | 实时分钟 | 实时分钟 K 线 |
| `daily_basic` | 每日指标 | PE/PB/PS/市值/换手率等 |
| `adj_factor` | 复权因子 | 前后复权因子 |
| `suspend_d` | 每日停复牌信息 | 按日获取停复牌 |
| `stk_limit` | 每日涨跌停价格 | 全市场涨跌停价 |
| `bak_daily` | 备用行情 | 含特定指标的备用行情 |
| `ggt_daily` | 港股通每日成交统计 | 2014 起 |
| `ggt_monthly` | 港股通每月成交统计 | 月度统计 |
| `ggt_top10` | 港股通十大成交股 | 每日 TOP10 |
| `hsgt_top10` | 沪深股通十大成交股 | 每日 TOP10 |

### 1.3 财务数据

| 接口 | 标题 | 说明 |
|------|------|------|
| `income` | 利润表 | 营收/净利润等 |
| `balancesheet` | 资产负债表 | 资产/负债/所有者权益 |
| `cashflow` | 现金流量表 | 经营/投资/筹资活动现金流 |
| `fina_indicator` | 财务指标数据 | ROE/毛利率/净利率等 |
| `fina_mainbz` | 主营业务构成 | 分地区/产品 |
| `fina_audit` | 财务审计意见 | 审计结果 |
| `forecast` | 业绩预告 | 预告数据 |
| `express` | 业绩快报 | 快报数据 |
| `dividend` | 分红送股数据 | 分红/送转 |
| `disclosure_date` | 财报披露日期表 | 预约披露日 |

### 1.4 资金流向数据

| 接口 | 标题 | 说明 |
|------|------|------|
| `moneyflow` | 个股资金流向 | 大单/小单成交（2010 起） |
| `moneyflow_hsgt` | 沪深港通资金流向 | 北向/南向每日净流入 |
| `moneyflow_ths` | 个股资金流向(THS) | 同花顺口径 |
| `moneyflow_dc` | 个股资金流向(DC) | 东方财富口径 |
| `moneyflow_ind_ths` | 行业资金流向(THS) | 同花顺行业 |
| `moneyflow_ind_dc` | 板块资金流向(DC) | 东方财富板块 |
| `moneyflow_mkt_dc` | 大盘资金流向(DC) | 东方财富大盘 |
| `moneyflow_cnt_ths` | 板块资金流向(THS) | 同花顺概念板块 |

### 1.5 参考数据

| 接口 | 标题 | 说明 |
|------|------|------|
| `top10_holders` | 前十大股东 | 持有数量和比例 |
| `top10_floatholders` | 前十大流通股东 | 流通股东 |
| `stk_holdernumber` | 股东人数 | 不定期公布 |
| `stk_holdertrade` | 股东增减持 | 重要股东增减变化 |
| `pledge_stat` | 股权质押统计 | 质押统计 |
| `pledge_detail` | 股权质押明细 | 质押明细 |
| `repurchase` | 股票回购 | 回购数据 |
| `share_float` | 限售股解禁 | 解禁数据 |
| `block_trade` | 大宗交易 | 大宗交易明细 |
| `stk_account` | 股票开户数据(停) | 周度开户统计 |
| `stk_account_old` | 股票开户数据(旧) | 2008-2015 |
| `margin` | 融资融券交易汇总 | 每日汇总 |
| `margin_detail` | 融资融券交易明细 | 个股明细 |
| `margin_secs` | 融资融券标的(盘前) | 当日标的列表 |
| `slb_len` | 转融资交易汇总 | 转融资 |
| `slb_sec` | 转融券交易汇总(停) | 转融券 |
| `slb_sec_detail` | 转融券交易明细(停) | 转融券明细 |
| `slb_len_mm` | 做市借券交易汇总(停) | 做市借券 |

### 1.6 打板 / 情绪 / 主题专题

| 接口 | 标题 | 说明 |
|------|------|------|
| `limit_list_d` | 涨跌停和炸板数据 | 2020 起 |
| `limit_step` | 涨停股票连板天梯 | 连板进阶 |
| `limit_cpt_list` | 涨停最强板块统计 | 强势板块 |
| `limit_list_ths` | 同花顺涨跌停榜单 | 2023.11 起 |
| `top_list` | 龙虎榜每日统计单 | 交易明细 |
| `top_inst` | 龙虎榜机构交易单 | 机构成交 |
| `hm_list` | 市场游资名录 | 游资分类 |
| `hm_detail` | 游资交易每日明细 | 2022.08 起 |
| `kpl_list` | 榜单数据(开盘啦) | 涨停/跌停/炸板 |
| `kpl_concept_cons` | 题材成分(开盘啦) | 概念成分股 |
| `stk_auction` | 开盘竞价成交(当日) | 集合竞价 |
| `stk_auction_o` | 股票开盘集合竞价 | 盘后更新 |
| `stk_auction_c` | 股票收盘集合竞价 | 盘后更新 |
| `dc_hot` | 东方财富 App 热榜 | 多市场热榜 |
| `ths_hot` | 同花顺 App 热榜 | 多市场热榜 |
| `ths_index` | 同花顺行业概念板块 | 板块列表 |
| `ths_daily` | 同花顺概念和行业行情 | 板块行情 |
| `ths_member` | 同花顺行业概念成分 | 成分股 |
| `dc_index` | 东方财富概念板块 | 板块列表 |
| `dc_member` | 东方财富概念成分 | 成分股 |
| `dc_daily` | 东财概念和行业指数行情 | 板块行情 |
| `tdx_index` | 通达信板块信息 | 概念/行业/风格/地域 |
| `tdx_member` | 通达信板块成分 | 成分股 |
| `tdx_daily` | 通达信板块行情 | 含估值 |

### 1.7 特色数据

| 接口 | 标题 | 说明 |
|------|------|------|
| `hk_hold` | 沪深股通持股明细 | 港交所数据 |
| `broker_recommend` | 券商月度金股 | 每月 1-3 日更新 |
| `stk_surv` | 机构调研数据 | 调研记录 |
| `report_rc` | 券商盈利预测数据 | 卖方研报预测 |
| `cyq_perf` | 每日筹码及胜率 | 2018 起 |
| `cyq_chips` | 每日筹码分布 | 各价位占比 |
| `ccass_hold` | 中央结算系统持股统计 | 全历史 |
| `ccass_hold_detail` | 中央结算系统持股明细 | 机构席位 |
| `stk_factor_pro` | 股票技术面因子(专业版) | Tushare 自产 |
| `stk_nineturn` | 神奇九转指标 | TD 序列 |
| `stk_ah_comparison` | AH 股比价 | 历史比价 |

---

## 二、指数专题

| 接口 | 标题 | 说明 |
|------|------|------|
| `index_basic` | 指数基本信息 | 指数列表 |
| `index_daily` | 指数日线行情 | 日线（含南华期货指数） |
| `index_weekly` | 指数周线行情 | 周线 |
| `index_monthly` | 指数月线行情 | 月线 |
| `index_weight` | 指数成分和权重 | 月度权重 |
| `index_dailybasic` | 大盘指数每日指标 | 上证/深证/50/500/创业板 |
| `index_classify` | 申万行业分类 | 2014/2021 两版 |
| `index_member_all` | 申万行业成分(分级) | 三级分类 |
| `sw_daily` | 申万行业指数日行情 | 默认 2021 版 |
| `ci_index_member` | 中信行业成分 | 三级分类 |
| `ci_daily` | 中信行业指数日行情 | 日线 |
| `index_global` | 国际主要指数 | 全球指数日线 |
| `daily_info` | 沪深市场每日交易统计 | 各板块统计 |
| `sz_daily_info` | 深圳市场每日交易情况 | 深圳概况 |
| `idx_mins` | 指数历史分钟 | 1-60min |
| `rt_idx_min` | 指数实时分钟 | 实时 |
| `rt_idx_k` | 指数实时日线 | 实时 |
| `rt_sw_k` | 申万实时行情 | 实时截面 |
| `idx_factor_pro` | 指数技术面因子(专业版) | Tushare 自产 |

---

## 三、ETF 专题

| 接口 | 标题 | 说明 |
|------|------|------|
| `etf_basic` | ETF 基本信息 | 含 QDII |
| `fund_daily` | ETF 日线行情 | 10 年+ |
| `fund_adj` | ETF 复权因子 | 复权计算用 |
| `etf_index` | ETF 基准指数 | 基准列表 |
| `etf_share_size` | ETF 份额规模 | 每日份额/规模/净值 |
| `stk_mins` | ETF 历史分钟 | 1-60min |
| `rt_min` | ETF 实时分钟 | 实时 |
| `rt_etf_k` | ETF 实时日线 | 实时 |

---

## 四、公募基金

| 接口 | 标题 | 说明 |
|------|------|------|
| `fund_basic` | 基金列表 | 场内+场外 |
| `fund_company` | 基金管理人 | 管理人列表 |
| `fund_manager` | 基金经理 | 含简历 |
| `fund_nav` | 基金净值 | 净值数据 |
| `fund_div` | 基金分红 | 分红记录 |
| `fund_portfolio` | 基金持仓 | 季度更新 |
| `fund_share` | 基金规模 | 含 ETF |
| `fund_factor_pro` | 基金技术面因子(专业版) | 场内基金 |
| `fund_sales_ratio` | 各渠道销售保有规模占比 | 年度 |
| `fund_sales_vol` | 销售机构保有规模 | 季度 |

---

## 五、债券专题

| 接口 | 标题 | 说明 |
|------|------|------|
| `cb_basic` | 可转债基础信息 | 基本信息 |
| `cb_issue` | 可转债发行 | 发行数据 |
| `cb_daily` | 可转债行情 | 日线 |
| `cb_call` | 可转债赎回信息 | 到期/强制赎回 |
| `cb_share` | 可转债转股结果 | 转股 |
| `cb_price_chg` | 可转债转股价变动 | 转股价调整 |
| `cb_rate` | 可转债票面利率 | 票面利率 |
| `cb_factor_pro` | 可转债技术面因子(专业版) | Tushare 自产 |
| `repo_daily` | 债券回购日行情 | 回购行情 |
| `bond_blk` | 大宗交易 | 债券大宗 |
| `bond_blk_detail` | 大宗交易明细 | 债券大宗明细 |
| `yc_cb` | 国债收益率曲线 | 中债即期/到期 |
| `eco_cal` | 全球财经事件 | 财经日历 |
| `bc_otcqt` | 柜台流通式债券报价 | 柜台报价 |
| `bc_bestotcqt` | 柜台流通式债券最优报价 | 最优报价 |

---

## 六、期货数据

| 接口 | 标题 | 说明 |
|------|------|------|
| `fut_basic` | 合约信息 | 合约列表 |
| `trade_cal` | 交易日历(期货) | 各期货交易所 |
| `fut_daily` | 日线行情 | 期货日线 |
| `fut_mapping` | 主力与连续合约 | 合约映射 |
| `fut_holding` | 每日持仓排名 | 成交持仓 |
| `fut_wsr` | 仓单日报 | 仓库/厂库 |
| `fut_settle` | 每日结算参数 | 交易交割费率 |
| `fut_weekly_detail` | 主要品种交易周报 | 2010 起 |
| `fut_weekly_monthly` | 期货周月线行情(每日更新) | 日度更新 |
| `ft_mins` | 历史分钟行情 | 1-60min |
| `rt_fut_min` | 实时分钟行情 | 实时 |
| `ft_limit` | 合约涨跌停价格 | 2005 起 |

---

## 七、期权数据

| 接口 | 标题 | 说明 |
|------|------|------|
| `opt_basic` | 期权合约信息 | 合约列表 |
| `opt_daily` | 期权日线行情 | 日线 |
| `opt_mins` | 期权分钟行情 | 1-60min |

---

## 八、港股数据

| 接口 | 标题 | 说明 |
|------|------|------|
| `hk_basic` | 港股基础信息 | 港股列表 |
| `hk_daily` | 港股日线行情 | 每日 18 点更新 |
| `hk_daily_adj` | 港股复权行情 | 含市值/成交 |
| `hk_adjfactor` | 港股复权因子 | 每日更新 |
| `hk_mins` | 港股分钟行情 | 1-60min |
| `hk_tradecal` | 港股交易日历 | 交易日历 |
| `hk_income` | 港股利润表 | 财务数据 |
| `hk_balancesheet` | 港股资产负债表 | 财务数据 |
| `hk_cashflow` | 港股现金流量表 | 财务数据 |
| `hk_fina_indicator` | 港股财务指标 | 财务指标 |
| `rt_hk_k` | 港股实时日线 | 实时 |

---

## 九、美股数据

| 接口 | 标题 | 说明 |
|------|------|------|
| `us_basic` | 美股基础信息 | 美股列表 |
| `us_tradecal` | 美股交易日历 | 交易日历 |
| `us_daily` | 美股日线行情 | 含市场/估值指标 |
| `us_daily_adj` | 美股复权行情 | 含股本/市值 |
| `us_adjfactor` | 美股复权因子 | 每日更新 |
| `us_income` | 美股利润表 | 主要美股+中概股 |
| `us_balancesheet` | 美股资产负债表 | 主要美股+中概股 |
| `us_cashflow` | 美股现金流量表 | 主要美股+中概股 |
| `us_fina_indicator` | 美股财务指标 | 主要美股+中概股 |

---

## 十、外汇数据

| 接口 | 标题 | 说明 |
|------|------|------|
| `fx_obasic` | 外汇基础信息(海外) | FXCM |
| `fx_daily` | 外汇日线行情 | 日线 |

---

## 十一、现货数据

| 接口 | 标题 | 说明 |
|------|------|------|
| `sge_basic` | 上海黄金基础信息 | 合约信息 |
| `sge_daily` | 上海黄金现货日行情 | 日线 |

---

## 十二、宏观经济

### 12.1 国内宏观

| 接口 | 标题 | 说明 |
|------|------|------|
| `cn_gdp` | 国内生产总值(GDP) | 季度 |
| `cn_cpi` | 居民消费价格指数(CPI) | 全国/城市/农村 |
| `cn_ppi` | 工业生产者出厂价格指数(PPI) | 出厂价 |
| `cn_pmi` | 采购经理指数(PMI) | 景气度 |
| `cn_m` | 货币供应量(月) | M0/M1/M2 |
| `sf_month` | 社融增量(月度) | 社会融资 |

### 12.2 利率数据

| 接口 | 标题 | 说明 |
|------|------|------|
| `shibor` | Shibor 利率 | 上海银行间拆借 |
| `shibor_lpr` | LPR 贷款基础利率 | 贷款定价 |
| `shibor_quote` | Shibor 报价数据 | 报价明细 |
| `libor` | Libor 利率 | 国际拆借 |
| `hibor` | Hibor 利率 | 香港拆借 |
| `wz_index` | 温州民间借贷利率 | 温州指数 |
| `gz_index` | 广州民间借贷利率 | 广州指数 |

### 12.3 美国宏观

| 接口 | 标题 | 说明 |
|------|------|------|
| `us_tycr` | 国债收益率曲线利率 | 美债收益率 |
| `us_trycr` | 国债实际收益率曲线利率 | 实际收益率 |
| `us_tbr` | 短期国债利率 | 短端利率 |
| `us_tltr` | 国债长期利率 | 长端利率 |
| `us_trltr` | 国债长期利率平均值 | 长端均值 |

---

## 十三、大模型语料 / 新闻公告

| 接口 | 标题 | 说明 |
|------|------|------|
| `news` | 新闻快讯(短讯) | 6 年+主流新闻 |
| `major_news` | 新闻通讯(长篇) | 8 年+长篇 |
| `cctv_news` | 新闻联播文字稿 | 2017 起 |
| `anns_d` | 上市公司公告 | 含 PDF URL |
| `research_report` | 券商研究报告 | 2017 起 |
| `npr` | 国家政策库 | 法规/条例/通知 |
| `irm_qa_sh` | 上证 e 互动问答 | 互动平台 |
| `irm_qa_sz` | 深证易互动问答 | 互动平台 |

---

## 十四、行业经济 (TMT / 票房)

| 接口 | 标题 | 说明 |
|------|------|------|
| `tmt_twincome` | 台湾电子产业月营收 | TMT 产业 |
| `tmt_twincomedetail` | 台湾电子产业月营收明细 | 产品级 |
| `bo_daily` | 电影日度票房 | 日度 |
| `bo_weekly` | 电影周度票房 | 周度 |
| `bo_monthly` | 电影月度票房 | 月度 |
| `bo_cinema` | 影院日度票房 | 各影院 |
| `film_record` | 全国电影剧本备案 | 公示 |
| `teleplay_record` | 全国电视剧备案公示 | 2009 起 |

---

## 统计

| 分类 | 接口数 |
|------|-------|
| 股票数据（基础+行情+财务+资金流+参考+打板+特色） | ~100 |
| 指数专题 | ~19 |
| ETF 专题 | ~8 |
| 公募基金 | ~10 |
| 债券专题 | ~15 |
| 期货数据 | ~12 |
| 期权数据 | 3 |
| 港股数据 | ~11 |
| 美股数据 | ~9 |
| 外汇+现货 | 4 |
| 宏观经济 | ~18 |
| 新闻公告/语料 | 8 |
| 行业经济 | 8 |
| **合计** | **~225** |

> **Vortex 数据域分阶段覆盖策略**：
> - **P0（MVP）**：stock_basic, trade_cal, daily, daily_basic, income, balancesheet, cashflow, fina_indicator, namechange, adj_factor（~10 个接口，支撑 FCF+LowVol+Momentum 实例策略）
> - **P1**：index_basic, index_daily, index_weight, sw_daily, index_classify, index_member_all, moneyflow_hsgt, dividend, forecast, express（+10 个，支撑指数对标与更多研究场景）
> - **P2**：全部 A 股行情+财务+资金流+参考数据（~60 个，覆盖 A 股全量）
> - **P3**：港股、美股、期货、期权、基金、债券、宏观、新闻（剩余全部，全量覆盖）
