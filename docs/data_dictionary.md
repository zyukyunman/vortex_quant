# QuantPilot 数据字典

> 数据存储格式: Apache Parquet (列式存储，高压缩比)
> 数据源: Tushare Pro API (<https://tushare.pro>)
> 账户积分: 2000 (进阶版)

---

## 目录结构

```
data/
├── meta/                          # 元数据 (基础信息+日历)
│   ├── stock_basic.parquet        # 全A股票列表
│   └── trade_cal.parquet          # 交易日历
├── market/                        # 行情数据
│   ├── daily/                     # 个股日线 (按年分片)
│   │   ├── 2005.parquet
│   │   ├── ...
│   │   └── 2026.parquet
│   ├── adj_factor/                # 复权因子 (待下载)
│   └── index_daily/               # 指数日线 (按指数分片)
│       ├── 000300_SH.parquet      # 沪深300
│       ├── 000905_SH.parquet      # 中证500
│       ├── 000922_CSI.parquet     # 中证红利
│       └── ...
├── fundamental/                   # 基本面数据
│   ├── valuation/                 # 每日估值 (按年分片)
│   │   ├── 2005.parquet
│   │   ├── ...
│   │   └── 2026.parquet
│   ├── fina_indicator.parquet     # 财务指标 (全市场合并)
│   ├── income.parquet             # 利润表
│   ├── cashflow.parquet           # 现金流量表
│   ├── balancesheet.parquet       # 资产负债表
│   └── dividend.parquet           # 分红送股
├── signal/                        # 策略信号
│   └── 2026.parquet               # 最新选股信号
└── reports/                       # 回测报告
    ├── *.html                     # HTML 可视化报告
    └── *.json                     # 结构化回测数据
```

---

## 1. meta/stock_basic.parquet — 股票列表

| API | `pro.stock_basic()` |
|-----|-----|
| Tushare 文档 | <https://tushare.pro/document/2?doc_id=25> |
| 数据量 | ~5,493 行 × 11 列 |
| 更新频率 | 每次回测前更新 |

| 列名 | 类型 | 说明 |
|------|------|------|
| `ts_code` | str | 股票代码 (如 `000001.SZ`) |
| `symbol` | str | 纯数字代码 (如 `000001`) |
| `name` | str | 股票名称 |
| `area` | str | 所在地区 (如 `深圳`) |
| `industry` | str | Tushare 自有行业分类 (⚠️ 非申万分类) |
| `market` | str | 市场类型: 主板/创业板/科创板/北交所/CDR |
| `list_date` | str | 上市日期 (YYYYMMDD) |
| `is_hs` | str | 沪深港通标的: H=沪股通, S=深股通, N=否 |
| `curr_type` | str | 交易货币: CNY/HKD/USD |
| `act_name` | str | 实际控制人名称 |
| `act_ent_type` | str | 实际控制人类型 |

---

## 2. meta/trade_cal.parquet — 交易日历

| API | `pro.trade_cal(exchange='SSE')` |
|-----|-----|
| Tushare 文档 | <https://tushare.pro/document/2?doc_id=26> |
| 数据量 | ~8,035 行 × 2 列 |
| 覆盖范围 | 至 2027 年末 |

| 列名 | 类型 | 说明 |
|------|------|------|
| `cal_date` | str | 日历日期 (YYYYMMDD) |
| `is_open` | int | 是否交易日: 1=是, 0=否 |

---

## 3. market/daily/{year}.parquet — 个股日线行情

| API | `pro.daily(trade_date=...)` |
|-----|-----|
| Tushare 文档 | <https://tushare.pro/document/2?doc_id=27> |
| 数据量 | 每年约 130 万行 × 11 列 |
| 分片方式 | 按年份存储 (2005 ~ 2026) |
| 下载方式 | 按 `trade_date` 逐日遍历全市场 |

| 列名 | 类型 | 说明 |
|------|------|------|
| `ts_code` | str | 股票代码 |
| `trade_date` | str | 交易日期 (YYYYMMDD) |
| `open` | float | 开盘价 (元) |
| `high` | float | 最高价 (元) |
| `low` | float | 最低价 (元) |
| `close` | float | 收盘价 (元，未复权) |
| `pre_close` | float | 前收盘价 (元) |
| `change` | float | 涨跌额 (元) |
| `pct_chg` | float | 涨跌幅 (**百分比**, 如 1.5 = 涨 1.5%，使用时需 ÷ 100) |
| `vol` | float | 成交量 (手) |
| `amount` | float | 成交额 (千元) |

> ⚠️ `pct_chg` 是百分比形式，回测引擎在读取时自动除以 100。

---

## 4. market/adj_factor/ — 复权因子

| API | `pro.adj_factor(ts_code=...)` |
|-----|-----|
| Tushare 文档 | <https://tushare.pro/document/2?doc_id=28> |
| 状态 | **待下载** (目录为空) |

> 后复权价 = close × adj_factor

---

## 5. market/index_daily/{code}.parquet — 指数日线行情

| API | `pro.index_daily(ts_code=..., start_date=..., end_date=...)` |
|-----|-----|
| Tushare 文档 | <https://tushare.pro/document/2?doc_id=95> |
| 分片方式 | 按指数代码存储 |
| 覆盖范围 | 2014 ~ 2026 (约 2,973 交易日) |

已下载的指数:

| 文件名 | 指数代码 | 指数名称 | 用途 |
|--------|---------|---------|------|
| `000300_SH.parquet` | 000300.SH | 沪深300 | 大盘蓝筹基准 |
| `000905_SH.parquet` | 000905.SH | 中证500 | 中盘成长基准 |
| `000852_SH.parquet` | 000852.SH | 中证1000 | 小盘基准 |
| `000922_CSI.parquet` | 000922.CSI | 中证红利 | **红利策略主基准** |
| `000016_SH.parquet` | 000016.SH | 上证50 | 超级大盘基准 |
| `000985_CSI.parquet` | 000985.CSI | 中证全指 | 全市场基准 |
| `932000_CSI.parquet` | 932000.CSI | 中证2000 | 微盘基准 |

| 列名 | 类型 | 说明 |
|------|------|------|
| `ts_code` | str | 指数代码 |
| `trade_date` | str | 交易日期 (YYYYMMDD) |
| `open` | float | 开盘点位 |
| `high` | float | 最高点位 |
| `low` | float | 最低点位 |
| `close` | float | 收盘点位 |
| `vol` | float | 成交量 |
| `amount` | float | 成交额 |
| `pct_chg` | float | 涨跌幅 (**百分比**, 如 0.56 = 涨 0.56%) |

---

## 6. fundamental/valuation/{year}.parquet — 每日估值指标

| API | `pro.daily_basic(trade_date=...)` |
|-----|-----|
| Tushare 文档 | <https://tushare.pro/document/2?doc_id=32> |
| 数据量 | 每年约 130 万行 × 10 列 |
| 分片方式 | 按年份存储 (2005 ~ 2026) |
| 下载方式 | 按 `trade_date` 逐日遍历全市场 |

| 列名 | 类型 | 说明 |
|------|------|------|
| `ts_code` | str | 股票代码 |
| `trade_date` | str | 交易日期 (YYYYMMDD) |
| `pe_ttm` | float | 市盈率 TTM (滚动12个月净利润) |
| `pb` | float | 市净率 (最近报告期净资产) |
| `ps_ttm` | float | 市销率 TTM (滚动12个月营收) |
| `dv_ratio` | float | 股息率 — 近12个月现金分红 / 当日总市值 |
| `dv_ttm` | float | 股息率 TTM — 滚动12个月盈利预估口径 |
| `total_mv` | float | 总市值 (**万元**) |
| `circ_mv` | float | 流通市值 (**万元**) |
| `turnover_rate_f` | float | 换手率 (自由流通股本口径, %) |

> ⚠️ `dv_ratio` vs `dv_ttm`: 高股息策略使用 `dv_ttm`，它基于滚动12个月分红数据。
> ⚠️ `total_mv` / `circ_mv` 单位是**万元**，非元。

---

## 7. fundamental/fina_indicator.parquet — 财务指标

| API | `pro.fina_indicator(ts_code=...)` |
|-----|-----|
| Tushare 文档 | <https://tushare.pro/document/2?doc_id=79> |
| 数据量 | ~22,683 行 × 16 列 |
| 下载方式 | 逐股遍历 (~5,000只)，每次最多返回 100 条 |
| 覆盖范围 | 2021 ~ 2025 报告期 (2014~2020 增量下载中) |

| 列名 | 类型 | 说明 |
|------|------|------|
| `ts_code` | str | 股票代码 |
| `ann_date` | str | 公告日期 (YYYYMMDD) |
| `end_date` | str | 报告期 (如 `20241231` 表示年报) |
| `roe` | float | 净资产收益率 ROE (%) |
| `roe_dt` | float | 扣非净资产收益率 (%) |
| `roe_waa` | float | 加权平均 ROE (%) |
| `grossprofit_margin` | float | 毛利率 (%) |
| `profit_dedt` | float | 扣非净利润 (元) |
| `netprofit_yoy` | float | 归母净利润同比增速 (%) |
| `or_yoy` | float | 营收同比增速 (%) |
| `q_profit_yoy` | float | 单季度净利润同比 (%) |
| `equity_yoy` | float | 净资产同比增速 (%) |
| `debt_to_assets` | float | 资产负债率 (%) |
| `op_yoy` | float | 营业利润同比 (%) |
| `ocfps` | float | 每股经营现金流 (元/股) |
| `cfps` | float | 每股现金流量净额 (元/股) |

> 报告期 `end_date` 规则: `XXXX0331`=Q1, `XXXX0630`=半年报, `XXXX0930`=三季报, `XXXX1231`=年报

---

## 8. fundamental/income.parquet — 利润表

| API | `pro.income(ts_code=...)` |
|-----|-----|
| Tushare 文档 | <https://tushare.pro/document/2?doc_id=33> |
| 数据量 | ~22,272 行 × 10 列 |
| 下载方式 | 逐股遍历 |

| 列名 | 类型 | 说明 |
|------|------|------|
| `ts_code` | str | 股票代码 |
| `ann_date` | str | 公告日期 (YYYYMMDD) |
| `end_date` | str | 报告期 (YYYYMMDD) |
| `report_type` | str | 报表类型: `1`=合并报表, `4`=调整合并 |
| `revenue` | float | 营业总收入 (元) |
| `operate_profit` | float | 营业利润 (元) |
| `n_income` | float | 净利润 (元) |
| `n_income_attr_p` | float | 归属母公司净利润 (元) |
| `total_profit` | float | 利润总额 (元) |
| `ebit` | float | 息税前利润 (元, 部分股票为空) |

> ⚠️ 去重: 同一 `end_date` 可能有多条 (原始+调整)，使用 `report_type='1'` 取合并报表。

---

## 9. fundamental/cashflow.parquet — 现金流量表

| API | `pro.cashflow(ts_code=...)` |
|-----|-----|
| Tushare 文档 | <https://tushare.pro/document/2?doc_id=44> |
| 数据量 | ~22,235 行 × 9 列 |
| 下载方式 | 逐股遍历 |

| 列名 | 类型 | 说明 |
|------|------|------|
| `ts_code` | str | 股票代码 |
| `ann_date` | str | 公告日期 (YYYYMMDD) |
| `end_date` | str | 报告期 (YYYYMMDD) |
| `report_type` | str | 报表类型: `1`=合并报表 |
| `n_cashflow_act` | float | 经营活动现金流净额 (元) |
| `c_pay_acq_const_fiolta` | float | 购建固定资产、无形资产支出 (元) — 即 CapEx |
| `n_cashflow_inv_act` | float | 投资活动现金流净额 (元) |
| `n_cash_flows_fnc_act` | float | 筹资活动现金流净额 (元) |
| `free_cashflow` | float | 企业自由现金流 FCF (元, 部分为空) |

> 自由现金流计算: FCF ≈ `n_cashflow_act` - `c_pay_acq_const_fiolta`

---

## 10. fundamental/balancesheet.parquet — 资产负债表

| API | `pro.balancesheet(ts_code=...)` |
|-----|-----|
| Tushare 文档 | <https://tushare.pro/document/2?doc_id=36> |
| 数据量 | ~22,355 行 × 11 列 |
| 下载方式 | 逐股遍历 |

| 列名 | 类型 | 说明 |
|------|------|------|
| `ts_code` | str | 股票代码 |
| `ann_date` | str | 公告日期 (YYYYMMDD) |
| `end_date` | str | 报告期 (YYYYMMDD) |
| `report_type` | str | 报表类型: `1`=合并报表 |
| `total_assets` | float | 总资产 (元) |
| `total_liab` | float | 总负债 (元) |
| `money_cap` | float | 货币资金 (元) |
| `notes_receiv` | float | 应收票据 (元) |
| `accounts_receiv` | float | 应收账款 (元) |
| `total_cur_assets` | float | 流动资产合计 (元) |
| `total_cur_liab` | float | 流动负债合计 (元) |

> 净资产 = `total_assets` - `total_liab`

---

## 11. fundamental/dividend.parquet — 分红送股

| API | `pro.dividend(ts_code=...)` |
|-----|-----|
| Tushare 文档 | <https://tushare.pro/document/2?doc_id=103> |
| 数据量 | ~153,558 行 × 13 列 |
| 下载方式 | 逐股遍历 |

| 列名 | 类型 | 说明 |
|------|------|------|
| `ts_code` | str | 股票代码 |
| `end_date` | str | 分红报告期 (如 `20231231` 表示 2023 年报) |
| `ann_date` | str | 分红公告日期 |
| `div_proc` | str | 分红进度: `预案`/`股东大会`/`实施`/`取消` |
| `cash_div` | float | 每股现金分红 — 税前 (元) |
| `cash_div_tax` | float | 每股现金分红 — 税后 (元) |
| `stk_div` | float | 每股送转股 |
| `stk_bo_rate` | float | 每股送股比例 |
| `stk_co_rate` | float | 每股转增比例 |
| `record_date` | str | 股权登记日 (YYYYMMDD) |
| `ex_date` | str | 除权除息日 (YYYYMMDD) |
| `pay_date` | str | 派息日 (YYYYMMDD) |
| `base_share` | float | 基准总股本 (万股) |

> ⚠️ 高股息策略仅使用 `div_proc='实施'` 的记录 (已实际执行的分红)。

---

## 12. signal/{year}.parquet — 策略选股信号

| 来源 | QuantPilot 策略引擎输出 |
|-----|-----|
| 数据量 | ~30 行 × 9 列 (每次调仓) |

| 列名 | 类型 | 说明 |
|------|------|------|
| `date` | str | 信号日期 (YYYYMMDD) |
| `strategy` | str | 策略名称 (如 `dividend_quality_fcf`) |
| `ts_code` | str | 推荐股票代码 |
| `name` | str | 股票名称 |
| `action` | str | 操作: `buy` |
| `weight` | float | 组合权重 (等权时 ≈ 1/30) |
| `score` | float | 综合评分 (因子加权) |
| `reason` | str | 选股理由 (各因子值摘要) |
| `confidence` | float | 信号置信度 (默认 1.0) |

---

## 数据覆盖时间范围

| 数据表 | 时间范围 | 说明 |
|--------|---------|------|
| stock_basic | 最新快照 | 全部上市/退市/暂停 |
| trade_cal | ~ 2027 年末 | 含未来预估 |
| daily | 2005 ~ 2026 | 按年分片 |
| index_daily | 2014 ~ 2026 | 7 个指数 |
| valuation | 2005 ~ 2026 | 按年分片 |
| fina_indicator | 2021 ~ 2025 | 2014~2020 增量下载中 |
| income | 2021 ~ 2025 | 同上 |
| cashflow | 2021 ~ 2025 | 同上 |
| balancesheet | 2021 ~ 2025 | 同上 |
| dividend | 全历史 | 含所有已实施分红 |

---

## 数据下载命令

```bash
# 全量增量下载 (自动跳过已有)
python scripts/run_full_download.py

# 仅下载指定类型
python scripts/run_full_download.py --valuation-only --start-year 2019
python scripts/run_full_download.py --daily-only --start-year 2023
python scripts/run_full_download.py --fina-only --start-year 2014
python scripts/run_full_download.py --index-only

# 增量财务数据 (断点续传)
python scripts/download_fina_incremental.py
```

---

## API 频率限制 (2000 积分)

| 接口 | 频率限制 | 每次最大行数 | 推荐 sleep |
|------|---------|-------------|-----------|
| `daily` | 500次/分 | 6,000 | 0.1s |
| `daily_basic` | — | 6,000 | 0.1s |
| `index_daily` | — | 8,000 | 0.1s |
| `income` / `cashflow` / `balancesheet` | 50次/分 | 按股票查 | 0.3s |
| `fina_indicator` | 50次/分 | 100条/次 | 0.3s |
| `dividend` | 50次/分 | 按股票查 | 0.3s |
| `stock_basic` | 50次/分 | 6,000 | — |
| `trade_cal` | 50次/分 | — | — |
