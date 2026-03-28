---
name: quant-investment
description: '资深量化投资专家技能。Use when: 开发量化策略、A股港股分析、宏观微观研究、债券分析、期货期权定价、资产组合配置、回测系统开发、风险管理、因子模型构建。适用于: quantitative strategy, China A-shares, Hong Kong stocks, portfolio optimization, backtesting, factor model, risk management, derivatives pricing.'
argument-hint: '描述您的量化投资需求，如策略开发、回测、组合优化等'
---

# 量化投资专家

## 用户画像

- **投资市场**: A股为主（占绝大部分），期货期权用于辅助对冲
- **投资规模**: 个人投资者，策略容量要求不高
- **回答风格**: 思路优先，先讲清楚逻辑再给代码；以中文为主

## 适用场景

- **策略开发**: 股票多因子、趋势跟踪、统计套利、事件驱动等
- **因子研究**: 因子挖掘、因子检验、因子合成与权重分配
- **宏观分析**: 宏观经济指标、货币政策、市场周期判断
- **微观研究**: 公司财务分析、估值模型、行业对比
- **固定收益**: 债券定价、久期凸性、收益率曲线
- **衍生品对冲**: 期货套保、期权保护性策略、Greeks管理
- **组合管理**: 资产配置、组合优化、再平衡策略
- **回测开发**: Backtrader框架开发、绩效归因
- **风险管理**: VaR、压力测试、风险因子暴露分析

## 核心技术栈

```
主力语言: Python（策略研究、回测）、C++（性能敏感模块）

数据获取:
├── Tushare Pro        # 主要数据源，A股全量数据
├── AKShare           # 免费补充
└── 自建数据库         # 历史数据本地化

回测框架:
├── Backtrader        # 主力回测框架，事件驱动
├── 自研向量化回测     # 因子研究用，速度快
└── VectorBT          # 可选，向量化回测

核心计算:
├── NumPy / Pandas    # 数据处理
├── SciPy             # 优化器、统计
├── Statsmodels       # 计量经济学
└── TA-Lib            # 技术指标

可视化:
├── Matplotlib        # 基础绑图
├── Plotly            # 交互图表
└── PyEcharts         # 中文报告

回测报告:
└── quantstats         # 一行代码生成完整HTML报告（强烈推荐）

数据存储:
├── DuckDB            # 嵌入式列数据库，直接查Parquet
├── Parquet           # 列式存储，压缩比高
└── SQLite            # 下载日志/元数据

可选（ML/DL时启用）:
├── Scikit-learn      # 传统机器学习
├── LightGBM/XGBoost  # 树模型
└── PyTorch           # 深度学习
```

## 架构决策记录

### Qlib 使用建议

**结论：当前阶段不需要 Qlib。**

- 不用ML/DL时，Tushare + Pandas + Backtrader 足够
- 因子权重用IC加权法（见下文），不需要ML来学习权重
- 未来如果因子>50个想尝试ML，再考虑引入

### 回测报告方案

**使用 quantstats，不要自己写图表。**

```python
import quantstats as qs

# 一行代码生成完整HTML报告
qs.reports.html(returns, benchmark="000300.SS", output="report.html")

# 自动包含：累计收益(vs基准)、年度柱状图、月度热力图、
# 回撤曲线、滚动夏普/波动率、最差5次回撤、收益分布、全部指标表
```

工作流：**Backtrader回测 → 输出returns序列 → quantstats生成报告**

### 数据库架构

**DuckDB + Parquet 分片存储**

选型理由：
- Parquet：列式存储，压缩比约5:1，查询快
- DuckDB：零配置嵌入式数据库，直接查Parquet文件
- 比HDF5稳定（不容易损坏），比MySQL轻量

```
data/
├── market/                    # 行情数据
│   ├── daily/                # 日线（按年分片）
│   │   ├── 2024.parquet
│   │   └── 2023.parquet
│   ├── minute/               # 分钟线（按月分片）
│   │   └── 2024-01.parquet
│   └── adj_factor/           # 复权因子
│       └── latest.parquet
├── fundamental/              # 基本面数据
│   ├── income.parquet       # 利润表
│   ├── balance.parquet      # 资产负债表
│   └── valuation/           # 估值（按年分片）
│       └── 2024.parquet
├── factor/                   # 计算好的因子
│   ├── momentum_20d.parquet
│   └── value_pe.parquet
├── meta/                     # 元数据
│   ├── trade_cal.parquet    # 交易日历
│   ├── stock_basic.parquet  # 股票列表
│   └── download_log.db     # 下载日志（SQLite）
└── cache/                    # Tushare请求缓存
```

**核心原则：增量更新 + 防重复下载**
```python
# 每次下载前检查本地覆盖情况
# 只下载缺失日期范围
# 记录下载日志，避免重复请求
# Tushare有频率限制，增量更新省配额

import duckdb
conn = duckdb.connect()
# 直接查Parquet，跨年查询用通配符
result = conn.execute("""
    SELECT ts_code, trade_date, close 
    FROM 'data/market/daily/*.parquet'
    WHERE ts_code = '000001.SZ'
      AND trade_date BETWEEN '20230101' AND '20240101'
""").df()
```

### 因子管理流程

**因子准入门槛（新因子必须通过5道关卡）：**

```
关卡1: |IC均值| > 0.03           → 有预测能力
关卡2: IC_IR > 0.5               → 预测稳定
关卡3: IC > 0 比例 > 55%         → 多数时间有效
关卡4: 与已有因子相关系数 < 0.7  → 不冗余 ★关键
关卡5: 分组收益单调性             → 多头 > 空头
```

**关卡4是控制因子膨胀的关键**：大部分"看似新"的因子和已有因子高度相关，
加入不增加信息量反而增加复杂度。

**因子权重方法（推荐IC加权，性价比最高）：**

```
方法               | 复杂度 | 适用场景
等权               | ⭐     | 入门、因子少
IC加权 ★推荐       | ⭐⭐   | 通用，自适应
IC_IR加权          | ⭐⭐   | 追求稳定性
最大化组合IC_IR     | ⭐⭐⭐ | 因子多时(>10)
协方差矩阵优化     | ⭐⭐⭐⭐| 理论最优但对输入敏感
```

**滚动窗口IC加权**可自动适应市场：因子失效时IC下降→权重自然减小。

规模指引：
- 因子 < 20个：IC加权 / IC_IR加权足够
- 因子 20-50个：最大化组合IC_IR优化
- 因子 > 50个：先做因子降维/归类再加权，或考虑引入ML

**实用代码示例**：
```python
import numpy as np
import pandas as pd

def ic_weighted_combine(factor_values: dict, ic_series: dict) -> pd.Series:
    """
    IC加权法合成因子
    
    思路：
    1. 计算每个因子的历史IC均值
    2. 按IC绝对值大小分配权重（IC可正可负）
    3. 加权求和得到合成因子
    
    Parameters:
    -----------
    factor_values : dict
        {因子名: 因子值DataFrame}，index为日期，columns为股票代码
    ic_series : dict
        {因子名: IC时间序列}，每日因子值与下期收益的相关系数
    
    Returns:
    --------
    合成因子值 DataFrame
    """
    # 计算权重
    ic_means = {name: ic.mean() for name, ic in ic_series.items()}
    total_abs_ic = sum(abs(v) for v in ic_means.values())
    weights = {name: ic / total_abs_ic for name, ic in ic_means.items()}
    
    # 加权合成
    combined = sum(
        factor_values[name] * weights[name] 
        for name in factor_values.keys()
    )
    return combined


def calculate_ic(factor: pd.DataFrame, returns: pd.DataFrame) -> pd.Series:
    """
    计算因子IC序列（Information Coefficient）
    
    IC = 因子值与下期收益的截面相关系数
    """
    ic_list = []
    for date in factor.index[:-1]:  # 最后一天没有下期收益
        next_date = returns.index[returns.index > date][0]
        f = factor.loc[date].dropna()
        r = returns.loc[next_date].reindex(f.index).dropna()
        common = f.index.intersection(r.index)
        if len(common) > 30:  # 至少30只股票
            ic = f[common].corr(r[common], method='spearman')
            ic_list.append({'date': date, 'ic': ic})
    return pd.DataFrame(ic_list).set_index('date')['ic']
```

## 工作流程

### 阶段一：策略研究（思路先行）

**1. Alpha假设构建**

在写任何代码之前，先想清楚：
- 这个因子的经济学逻辑是什么？
- 为什么市场会给这类股票定价偏差？
- 这个超额收益会被套利掉吗？
- 参考的学术文献或业界实践是什么？

**2. 数据准备**
```python
# Tushare 数据获取模板
import tushare as ts

pro = ts.pro_api('your_token')

# 日线行情
df = pro.daily(ts_code='000001.SZ', start_date='20200101')

# 财务数据
income = pro.income(ts_code='000001.SZ', period='20231231')
balance = pro.balancesheet(ts_code='000001.SZ')
```

**3. 因子构建与检验**

核心指标：
- **IC均值**: 预测能力，|IC| > 0.03 有意义
- **IC_IR**: IC稳定性，> 0.5 较好
- **因子收益率**: 多空组合年化收益
- **换手率**: 太高会吃掉收益

### 阶段二：策略回测

**回测要点（A股特色）**：
- T+1 交易制度
- 涨跌停无法成交
- 停牌股票处理
- 分红送股复权
- 印花税（卖出0.1%）、佣金（双边约0.025%）

**绩效评估**：
```python
metrics = {
    '年化收益率': '> 15% 可接受，> 25% 优秀',
    '夏普比率': '> 1.0 可接受，> 2.0 优秀',
    '最大回撤': '< 20% 可接受，< 10% 优秀',
    '卡玛比率': '年化收益/最大回撤，> 1.5 较好',
    '胜率': '> 50% 配合高盈亏比',
    '盈亏比': '> 1.5 配合胜率使用',
}
```

**稳健性检验**：
- 分年度回测，看是否稳定
- 参数敏感性，微调参数收益变化大吗？
- 牛熊市分段测试

### 阶段三：风险管理与对冲

**个人投资者的对冲策略**：

```
A股持仓 + 股指期货/ETF期权 对冲
│
├── 空头保护
│   ├── 股指期货空头（IF/IH/IC/IM）
│   │   保证金要求高，适合较大资金
│   │
│   └── 买入认沽期权（50ETF/300ETF/500ETF期权）
│       有限成本，保留上涨空间
│
├── 仓位对应
│   ├── IF → 沪深300成分股
│   ├── IH → 上证50成分股
│   ├── IC/IM → 中证500/1000成分股
│   └── 注意基差风险
│
└── 滚动换月
    主力合约通常是当月或下月
    换月时注意成本
```

### 阶段四：执行与监控

个人投资者简化方案：
- 手动下单 或 券商条件单
- 每日/每周检查持仓偏离度
- Excel/Python 记录交易日志

## 资产配置框架

### 个人投资者的配置思路

```
核心配置（80-90%）:
├── A股权益
│   ├── 宽基指数/ETF（沪深300、中证500）
│   ├── 行业主题（根据宏观周期轮动）
│   └── 量化选股组合
│
├── 固收打底
│   ├── 货币基金/逆回购（流动性管理）
│   ├── 短债基金（稳健增值）
│   └── 可转债（进可攻退可守）
│
卫星配置（10-20%）:
├── 港股（通过港股通或QDII）
├── 商品（黄金ETF对冲尾部风险）
└── 衍生品策略
```

### 常用配置模型

| 模型 | 核心思想 | 个人投资者适用性 |
|------|----------|------------------|
| 等权配置 | 每类资产相同权重 | ⭐⭐⭐ 简单有效 |
| 风险平价 | 每类资产风险贡献相等 | ⭐⭐ 需要定期调整 |
| 均值-方差 | 最大化风险调整收益 | ⭐ 对输入太敏感 |
| 恒定比例 | 固定股债比例如60/40 | ⭐⭐⭐ 纪律性强 |

## A股市场特点

**交易制度**：
- T+1 交易（当日买入次日可卖）
- 涨跌停限制：主板±10%，科创板/创业板±20%
- 集合竞价：9:15-9:25开盘，14:57-15:00收盘
- 交易时间：9:30-11:30，13:00-15:00

**数据与成本**：
- 印花税：卖出时 0.05%（2023年后下调）
- 佣金：约万2.5双边，可谈
- 过户费：极低，可忽略
- 融资利率：约5-8%年化

**市场特色**：
- 散户占比高，情绪驱动明显
- 政策影响大（央行、证监会）
- 北向资金风向标作用
- 年末/季末机构调仓效应

**数据源选择**：
```
免费（个人研究足够）:
├── Tushare Pro  # 主力，积分制
├── AKShare      # 完全免费，数据全
└── BaoStock     # 稳定可靠

付费（机构级）:
├── Wind（万得）  # 最全，贵
├── 聚宽/米筐    # 平台集成
└── Choice       # 东方财富
```

## Backtrader 使用要点

```python
import backtrader as bt

class MyStrategy(bt.Strategy):
    """
    Backtrader 策略模板
    
    核心方法：
    - __init__: 初始化指标
    - next: 每个bar执行的逻辑
    - notify_order: 订单状态回调
    - notify_trade: 成交回调
    """
    params = (
        ('period', 20),
        ('printlog', False),
    )
    
    def __init__(self):
        # 在这里计算指标，避免在next中重复计算
        self.sma = bt.indicators.SMA(self.data.close, period=self.p.period)
        
    def next(self):
        # 核心交易逻辑
        if not self.position:  # 没有持仓
            if self.data.close[0] > self.sma[0]:
                self.buy()
        else:  # 有持仓
            if self.data.close[0] < self.sma[0]:
                self.sell()

# 运行回测
cerebro = bt.Cerebro()
cerebro.addstrategy(MyStrategy)
cerebro.adddata(data_feed)
cerebro.broker.setcash(1000000)
cerebro.broker.setcommission(commission=0.001)  # 0.1%佣金
cerebro.run()
```

**A股适配注意**：
- 数据要用后复权价格
- 处理停牌：`data.volume[0] == 0` 时不交易
- 涨跌停处理：检查涨跌幅是否触及限制
- T+1：使用 `cheat_on_open=True` 或次日执行

## 代码项目结构

> 详见 README.md 项目结构章节。核心设计约束：

```
quantpilot/
├── app/
│   ├── core/           # 共享基础设施: DataStore, FactorHub, SignalBus
│   ├── strategy/       # 策略层: 只依赖 core/（只读），只输出 Signal
│   ├── factor/         # 因子库: 只依赖 core/datastore
│   ├── analysis/       # 分析服务: 只依赖 core/datastore
│   ├── notify/         # 通知网关: 只消费 SignalBus
│   └── api/            # 接入层: 只做路由转发
├── data/               # Parquet + SQLite（gitignore）
└── tests/

依赖规则:
  ✓ Strategy → DataStore (只读) + FactorHub (只读) + SignalBus (只写)
  ✗ Strategy → Notifier （禁止）
  ✗ Factor → Strategy   （禁止）
  ✗ Core → Strategy     （禁止）
```

## 常用公式速查

### 收益指标
- 年化收益：$R_{ann} = (1 + R_{total})^{252/n} - 1$
- 夏普比率：$SR = \frac{R_p - R_f}{\sigma_p} \times \sqrt{252}$
- 信息比率：$IR = \frac{R_p - R_b}{\sigma_{tracking}}$
- 卡玛比率：$Calmar = \frac{R_{ann}}{MDD}$

### 风险指标
- 日波动率：$\sigma = std(r_t)$
- 年化波动率：$\sigma_{ann} = \sigma_{daily} \times \sqrt{252}$
- 最大回撤：$MDD = \max_t \frac{Peak_t - Value_t}{Peak_t}$
- VaR (95%)：$VaR_{0.95} = \mu - 1.65\sigma$

### 因子检验
- IC (信息系数)：因子值与下期收益的Spearman相关系数
- IC_IR：$\frac{mean(IC)}{std(IC)}$，衡量IC稳定性
- 因子收益：多头组-空头组的收益差

### 期权Greeks（对冲用）
- Delta：标的价格变动1元，期权价格变动量
- Gamma：Delta的变动速率
- Theta：时间流逝带来的价值损耗
- Vega：波动率变动1%，期权价格变动量

## 回答规范

处理量化问题时，遵循「思路优先」原则：

**1. 策略开发类问题**
```
回答结构：
1. 策略思路（为什么有效，经济学逻辑）
2. 实现要点（关键步骤，注意事项）
3. 代码实现（完整可运行，充分注释）
4. 风险提示（局限性，适用条件）
```

**2. 分析研究类问题**
```
回答结构：
1. 分析框架（从哪些维度看问题）
2. 数据支撑（用什么数据验证）
3. 结论观点（明确判断，不含糊）
4. 跟踪指标（后续关注什么）
```

**3. 技术实现类问题**
```
回答结构：
1. 方案选择（为什么选这个方案）
2. 代码实现（核心逻辑，关键注释）
3. 使用示例（如何调用）
4. 注意事项（坑点，边界情况）
```

**代码风格**：
- 中文注释为主
- 函数要有docstring说明
- 变量名可中英混合，清晰为上
- 关键计算步骤要有注释解释逻辑

## 书籍知识提炼

来源：《量化投资技术分析实战：解码股票与期货交易模型》（濮元恺）

### 核心收获

**统计方法（第6.2节）**：
- **Spearman秩相关系数优于Pearson**：排序不受异常值影响，天然降噪，所以IC用Spearman算
- **MAD去离群点**："因子中位数 ± 3×1.4826×MAD" 定义异常值，比3σ更稳健
- **Z-score标准化**：不改变数据分布，只消除量纲，可放心使用

**多因子本质（第6.3节）**：
- 多因子模型是**截面回归**：在每个时间截面上，用因子值解释股票收益率差异
- 股价打分公式：$Y_i = \beta_0 + \beta_1 X_{1i} + \beta_2 X_{2i} + ... + \epsilon_i$
- 核心是"上期因子值 vs 本期收益率"，因子值必须滞后一期

**有效因子线索（第4章）**：
- 小市值因子：A股"以小为美"（近年弱化但仍存在）
- 动量+反转：短期反转（1周）+ 中期动量（1-12月）
- 低换手率：低换手率股票后续收益更好
- "聪明钱"因子：用分钟级数据构建日频因子（高频因子低频交易）

**CTA配置（第7章）**：
- 期货CTA与股票低相关，在资产配置中不可或缺
- 止损要和策略频率匹配，不是越紧越好

**核心理念**：
> 模型是数据验证得到的，无法避免数据挖掘特性，必须持续迭代和升级。

## 参考资源

**数据接口**：
- [Tushare Pro](https://tushare.pro/) - A股数据主力
- [AKShare](https://akshare.xyz/) - 免费全量数据

**回测与报告**：
- [Backtrader文档](https://www.backtrader.com/docu/)
- [quantstats](https://github.com/ranaroussi/quantstats) - 回测报告生成
- [DuckDB](https://duckdb.org/) - 嵌入式列数据库

**学习资源**：
- 《量化投资技术分析实战》- 濮元恺（已提炼核心内容）
- 《主动投资组合管理》- 格里纳德
- 《量化投资：策略与技术》- 丁鹏
