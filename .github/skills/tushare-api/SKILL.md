---
name: tushare-api
description: 'Tushare Pro API 完整参考技能。Use when: 调用 Tushare 数据接口、设计数据下载逻辑、处理权限限制、数据频率控制、字段选择。适用于: Tushare API calls, data download design, permission handling, rate limiting, field selection for quant data pipelines.'
argument-hint: '描述您需要的数据类型，如: 日线行情、财务指标、分红数据、指数成分等'
---

# Tushare Pro API 完整参考

## 一、接口总览

Tushare Pro 提供 200+ 接口，覆盖 A 股、港股、期货、基金、宏观、行业等。
本项目账户积分: **2000分** (基础版)，部分高级接口受限。

### 接口分类

| 分类 | 接口数 | 最低积分 | 本项目使用 |
|------|--------|---------|-----------|
| 股票-基础数据 | ~12 | 2000 | ✅ stock_basic, trade_cal, namechange |
| 股票-行情数据 | ~18 | 2000~10000 | ✅ daily, adj_factor, suspend_d, stk_limit |
| 股票-财务数据 | ~10 | 2000 (VIP 5000) | ✅ income, balancesheet, cashflow, fina_indicator, dividend, forecast |
| 股票-参考数据 | ~10 | 2000 | ✅ daily_basic |
| 股票-特色数据 | ~11 | 2000~5000 | ⚠️ moneyflow, margin |
| 指数专题 | ~16 | 2000 | ✅ index_daily, index_weight, index_classify, index_member |
| 宏观经济 | ~15 | 2000 | ⚠️ cn_gdp, cn_cpi, cn_ppi, cn_m, shibor |
| 两融/资金流 | ~20 | 2000~5000 | ⚠️ moneyflow_hsgt, margin_detail |
| ETF专题 | ~8 | 2000 | 未使用 |
| 公募基金 | 7 | 2000 | 未使用 |
| 期货/期权 | ~12 | 2000 | 未使用 |
| 港股/美股 | ~18 | 2000~5000 | 未使用 |

## 二、核心接口详解

### 2.1 股票基础数据

#### stock_basic — 股票列表
```python
pro.stock_basic(exchange='', list_status='L',
    fields='ts_code,symbol,name,area,industry,market,list_date,is_hs,curr_type')
```
- **用途**: 获取全部上市股票基本信息
- **频率限制**: 50次/分, 6000条/次
- **关键字段**:
  - `ts_code`: 股票代码 (如 000001.SZ)
  - `name`: 股票名称
  - `industry`: 所属行业 (Tushare 自有分类，非申万！)
  - `list_date`: 上市日期 (YYYYMMDD)
  - `list_status`: L=上市, D=退市, P=暂停上市
- **⚠️ 注意**: `industry` 字段是 Tushare 自有分类，非申万行业分类。
  正式场景应使用 `index_classify` + `index_member` 获取申万行业。

#### trade_cal — 交易日历
```python
pro.trade_cal(exchange='SSE', start_date='20100101', end_date='20271231',
    fields='cal_date,is_open')
```
- **用途**: 获取交易日历
- **频率限制**: 50次/分
- **关键字段**: `cal_date` (YYYYMMDD), `is_open` (1=交易日)

#### namechange — 股票曾用名
```python
pro.namechange(ts_code='000001.SZ', fields='ts_code,name,start_date,end_date,change_reason')
```
- **用途**: 查询股票历史名称变更 (可用于识别历史 ST 标记)

### 2.2 行情数据

#### daily — 日线行情
```python
pro.daily(trade_date='20250101')  # 按日期查
pro.daily(ts_code='000001.SZ', start_date='20250101', end_date='20250630')  # 按股票查
```
- **频率限制**: **500次/分**, 6000条/次
- **关键字段**: `ts_code, trade_date, open, high, low, close, pre_close, change, pct_chg, vol, amount`
- **⚠️ 注意**: `pct_chg` 是百分比 (如 1.5 表示涨 1.5%)，使用时需除以 100
- **最佳实践**: 按 `trade_date` 遍历所有股票，而非按 `ts_code` 遍历所有日期

#### adj_factor — 复权因子
```python
pro.adj_factor(ts_code='000001.SZ', trade_date='')
```
- **用途**: 前/后复权计算 — 后复权价 = close × adj_factor
- **频率限制**: 500次/分

#### daily_basic — 每日指标 (⭐ 高频使用)
```python
pro.daily_basic(trade_date='20250101',
    fields='ts_code,trade_date,pe_ttm,pb,ps_ttm,dv_ratio,dv_ttm,total_mv,circ_mv,turnover_rate_f')
```
- **频率限制**: 6000条/次
- **关键字段**:
  - `pe_ttm`: 市盈率 TTM (滚动12个月)
  - `pb`: 市净率
  - `ps_ttm`: 市销率 TTM
  - `dv_ratio`: 股息率 (近12个月现金分红/当日总市值)
  - `dv_ttm`: 股息率 TTM
  - `total_mv`: 总市值 (万元)
  - `circ_mv`: 流通市值 (万元)
  - `turnover_rate_f`: 换手率 (自由流通股)
  - `volume_ratio`: 量比
- **⚠️ 重要**: `dv_ratio` ≠ `dv_ttm`
  - `dv_ratio`: 基于近12个月实际现金分红
  - `dv_ttm`: 基于滚动12个月盈利预估
  - **高股息策略应使用 `dv_ttm`**

#### suspend_d — 停复牌信息
```python
pro.suspend_d(trade_date='20250101')
```
- **用途**: 回测时标记停牌股 (不可交易)

#### stk_limit — 涨跌停价格
```python
pro.stk_limit(trade_date='20250101')
```
- **用途**: 回测时标记涨跌停 (涨停不可买入，跌停不可卖出)
- **关键字段**: `ts_code, trade_date, up_limit, down_limit`
- **积分要求**: 2000

### 2.3 财务数据

> **⚠️ 低权限 (2000分) 通用限制**:
> - 财务接口只能按 **单只股票** 查询 (`ts_code=...`)
> - 不能使用 `period` 参数批量查询
> - `fina_indicator` 每次仅返回 **100 条** (约25年季报)
> - 下载全市场需逐股遍历 (~5000只)，合理设置 sleep

#### income — 利润表
```python
pro.income(ts_code='000001.SZ',
    fields='ts_code,ann_date,f_ann_date,end_date,report_type,revenue,operate_profit,n_income,n_income_attr_p,total_profit,ebit')
```
- **关键字段**:
  - `ann_date`: 公告日期 → **Point-in-Time 使用此字段**
  - `f_ann_date`: 实际公告日期 (更准确)
  - `end_date`: 报告期 (如 20241231)
  - `report_type`: 1=合并报表, 4=调整合并报表
  - `n_income_attr_p`: 归母净利润
- **⚠️ 去重**: 同一 end_date 可能多条记录 → 取 `report_type=1`，按 `f_ann_date` 最新

#### balancesheet — 资产负债表
```python
pro.balancesheet(ts_code='000001.SZ',
    fields='ts_code,ann_date,f_ann_date,end_date,report_type,total_assets,total_liab,total_hldr_eqy_inc_min,total_hldr_eqy_exc_min,money_cap,accounts_receiv,total_cur_assets,total_cur_liab')
```
- **关键字段**: `total_assets`, `total_liab`, `total_hldr_eqy_exc_min` (归母净资产)

#### cashflow — 现金流量表
```python
pro.cashflow(ts_code='000001.SZ',
    fields='ts_code,ann_date,f_ann_date,end_date,report_type,n_cashflow_act,c_pay_acq_const_fiolta,n_cashflow_inv_act,n_cash_flows_fnc_act,free_cashflow')
```
- **关键字段**:
  - `n_cashflow_act`: 经营活动现金流净额
  - `free_cashflow`: 自由现金流 (企业口径)
  - `c_pay_acq_const_fiolta`: 购建固定资产

#### fina_indicator — 财务指标 (⭐ 高频使用)
```python
pro.fina_indicator(ts_code='000001.SZ',
    fields='ts_code,ann_date,end_date,roe,roe_dt,roe_waa,grossprofit_margin,profit_dedt,netprofit_yoy,or_yoy,q_profit_yoy,equity_yoy,debt_to_assets,op_yoy,ocfps,cfps')
```
- **⚠️ 每次最多 100 条**
- **关键字段**:
  - `roe`: ROE
  - `roe_dt`: 扣非 ROE
  - `profit_dedt`: 扣非净利润 (用于计算扣非增速)
  - `netprofit_yoy`: 净利润同比增速
  - `debt_to_assets`: 资产负债率
  - `ocfps`: 每股经营现金流
  - `fcff`: 企业自由现金流 (高积分才有)
  - `fcfe`: 股权自由现金流

#### dividend — 分红送股 (⭐ 高股息策略核心)
```python
pro.dividend(ts_code='000001.SZ',
    fields='ts_code,end_date,ann_date,div_proc,cash_div,cash_div_tax,stk_div,stk_bo_rate,stk_co_rate,record_date,ex_date,pay_date,base_share')
```
- **关键字段**:
  - `div_proc`: 分红进度 → **只有 '实施' 才是已执行分红**
  - `cash_div`: 每股现金分红 (税前)
  - `cash_div_tax`: 每股现金分红 (税后)
  - `ex_date`: 除权除息日
  - `end_date`: 报告期 (如 20231231 年报)
- **⚠️ 重要**:
  - 参数至少一个非空 (ts_code 或 ann_date)
  - `div_proc='实施'` 过滤才是真正执行了的分红

#### forecast — 业绩预告
```python
pro.forecast(ann_date='20250101')
```
- **用途**: 提前获取业绩方向 → 事件驱动策略

#### express — 业绩快报
```python
pro.express(ann_date='20250101')
```

### 2.4 指数数据

#### index_daily — 指数日线行情 (⭐ 基准对比)
```python
pro.index_daily(ts_code='000300.SH', start_date='20200101', end_date='20250101',
    fields='ts_code,trade_date,open,high,low,close,vol,amount,pct_chg')
```
- **频率限制**: 8000条/次
- **常用指数代码**:
  - `000300.SH` — 沪深300 (大盘蓝筹)
  - `000905.SH` — 中证500 (中盘成长)
  - `000852.SH` — 中证1000 (小盘)
  - `932000.CSI` — 中证2000 (微盘)
  - `000922.CSI` — 中证红利
  - `000016.SH` — 上证50 (超级大盘)
  - `000985.CSI` — 中证全指

#### index_weight — 指数成分和权重
```python
pro.index_weight(index_code='000300.SH', start_date='20250101', end_date='20250131')
```
- **用途**: 获取指数成分股及权重 → 指数增强/跟踪策略
- **⚠️ 月度数据**: 用当月首/末日期查询

#### index_classify — 申万行业分类 (⭐ 行业分析核心)
```python
pro.index_classify(level='L1', src='SW')  # L1=一级, L2=二级, L3=三级
```
- **用途**: 获取申万行业分类体系
- **关键字段**: `index_code`, `industry_name`, `level`, `src`
- **⚠️ 体系版本**: SW2014 vs SW2021 → 指定 `src='SW'` 获取最新

#### index_member — 申万行业成分股
```python
pro.index_member(index_code='801780.SI')  # 银行
```
- **用途**: 获取某个申万行业的全部成分股
- **关键字段**: `index_code`, `con_code` (成分股代码), `in_date`, `out_date`
- **⚠️ 注意**: 需检查 `out_date` 为空或大于当前日期 → 当前有效成分

### 2.5 资金流向与融资融券

#### moneyflow — 个股资金流向
```python
pro.moneyflow(trade_date='20250101')
```
- **用途**: 主力/散户资金净流入 → 资金流因子
- **关键字段**: `buy_lg_amount` (大单买入), `sell_lg_amount`, `net_mf_amount` (净流入)

#### moneyflow_hsgt — 沪深港通资金流向
```python
pro.moneyflow_hsgt(trade_date='20250101')
```
- **用途**: 北向资金净流入 → 另类因子
- **关键字段**: `north_money`, `south_money`

#### margin_detail — 融资融券明细
```python
pro.margin_detail(trade_date='20250101')
```
- **关键字段**: `rzye` (融资余额), `rzmre` (融资买入), `rqyl` (融券余量)

### 2.6 宏观经济数据

```python
pro.cn_gdp()              # GDP
pro.cn_cpi()              # CPI
pro.cn_ppi()              # PPI
pro.cn_m()                # 货币供应量 (M0/M1/M2)
pro.shibor(date='20250101')  # 银行间拆借利率
```
- **用途**: 宏观择时、无风险利率基准

## 三、权限与限频策略

### 积分等级

| 积分 | 级别 | 可用接口 |
|------|------|---------|
| 120 | 基础 | stock_basic, trade_cal, daily (当日) |
| 2000 | 进阶 | 全部基础 + 财务逐股 + 指数 + 日线全历史 |
| 5000 | VIP | 财务按期批量 + 行情高频 + 特色数据 |
| 10000+ | 超VIP | 分钟级数据 + 高频数据 |

### 限频控制最佳实践

```python
# datastore._api_call 已实现限频 + 重试
# 默认: pause=0.3s, retry=3

# 推荐 sleep 值:
# daily / adj_factor: 0.1s (500次/分)
# income / fina_indicator: 0.3s (单股查询, 50次/分)
# index_classify: 0.3s (单次查询)
```

### 低权限 (2000分) 绕行方案

1. **财务数据逐股遍历**: ~5000只 × 0.3s = ~25分钟
2. **用 daily_basic 替代部分 fina_indicator**: PE/PB/MV 直接从 daily_basic 获取
3. **分红数据**: dividend 逐股遍历，只取 `div_proc='实施'`
4. **行业分类**: 先 index_classify 获取全部行业，再 index_member 逐行业获取成分

## 四、数据质量注意事项

### Point-in-Time (PIT) 原则
- **财务数据**: 用 `ann_date` (公告日期) 而非 `end_date` (报告期) 做时间对齐
- **理由**: 年报截止12月31日，但4月30日前才公告 → 回测中不能在1月1日使用12月数据
- **实现**: `f_ann_date` > `ann_date` > 手动推断

### report_type 去重
```python
# 同一期可能有多条记录 (合并报表、调整报表等)
# 推荐: 取 report_type=1 (合并报表)，按 f_ann_date 取最新
df = df[df['report_type'] == '1']
df = df.sort_values('f_ann_date').drop_duplicates(
    subset=['ts_code', 'end_date'], keep='last'
)
```

### 退市股处理
- `stock_basic(list_status='D')` 获取退市股
- **必须包含退市股** → 避免幸存者偏差
- 回测中退市股按退市前最后交易价计算

### 涨跌停处理
- `stk_limit` 获取涨跌停价格
- **涨停禁买**: close >= up_limit → 当日无法买入
- **跌停禁卖**: close <= down_limit → 当日无法卖出
- 回测引擎需在下单前检查

## 五、本项目数据层映射

| Tushare 接口 | DataStore 方法 | 存储路径 | 状态 |
|-------------|---------------|---------|------|
| stock_basic | download_stock_basic / get_stock_basic | meta/stock_basic.parquet | ✅ |
| trade_cal | download_trade_cal | meta/trade_cal.parquet | ✅ |
| daily | download_daily / get_daily | market/daily/{year}.parquet | ✅ |
| daily_basic | download_daily_basic / get_valuation | fundamental/valuation/{year}.parquet | ✅ |
| income | download_income / get_income | fundamental/income.parquet | ✅ |
| balancesheet | download_balancesheet / get_balancesheet | fundamental/balancesheet.parquet | ✅ |
| cashflow | download_cashflow / get_cashflow | fundamental/cashflow.parquet | ✅ |
| fina_indicator | download_fina_indicator / get_fina_indicator | fundamental/fina_indicator.parquet | ✅ |
| dividend | download_dividend / get_dividend | fundamental/dividend.parquet | ✅ |
| index_daily | download_index_daily / get_index_daily | market/index_daily/{code}.parquet | ✅ |
| index_classify | download_index_classify / get_index_classify | meta/index_classify.parquet | ✅ |
| index_member | download_index_member / get_index_member | meta/index_member.parquet | ✅ |
| adj_factor | — | — | ❌ 待实现 |
| stk_limit | — | — | ❌ 待实现 |
| suspend_d | — | — | ❌ 待实现 |
| moneyflow | — | — | ❌ 待实现 |
| moneyflow_hsgt | — | — | ❌ 待实现 |
| margin_detail | — | — | ❌ 待实现 |
| forecast | — | — | ❌ 待实现 |

## 六、常用查询模式

### 获取某日全市场数据
```python
df_daily = pro.daily(trade_date='20250101')       # 全市场日线
df_basic = pro.daily_basic(trade_date='20250101')  # 全市场估值
```

### 获取某股票历史数据
```python
df = pro.daily(ts_code='000001.SZ', start_date='20240101', end_date='20250101')
```

### 获取申万行业映射
```python
# Step 1: 获取行业列表
classify = pro.index_classify(level='L1', src='SW')
# Step 2: 逐行业获取成分
for _, row in classify.iterrows():
    members = pro.index_member(index_code=row['index_code'])
```

### 获取分红数据 (只取已实施)
```python
df = pro.dividend(ts_code='000001.SZ')
df = df[df['div_proc'] == '实施']
```

### 获取指数基准
```python
df = pro.index_daily(ts_code='000300.SH', start_date='20240101', end_date='20250101')
ret = df.set_index('trade_date')['pct_chg'] / 100.0  # 转为小数收益率
```
