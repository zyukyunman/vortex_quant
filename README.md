# QuantPilot — 个人量化投资平台

> 面向A股个人投资者的模块化量化平台。核心理念：**数据共享、策略并行、模块解耦**。

## 目录

- [设计哲学](#设计哲学)
- [领域模型](#领域模型)
- [系统架构](#系统架构)
  - [八层分层总览](#八层分层总览)
  - [DataStore — 共享数据中心](#datastore--共享数据中心)
  - [FactorHub — 因子计算与管理](#factorhub--因子计算与管理)
  - [StrategyRunner — 策略并行执行器](#strategyrunner--策略并行执行器)
  - [SignalBus — 信号层](#signalbus--信号层-l4)
  - [PortfolioEngine — 组合层](#portfolioengine--组合层-l6)
  - [RiskManager — 风控层](#riskmanager--风控层-l8)
  - [Executor — 执行层](#executor--执行层-l7)
  - [Analyzer — 分析服务](#analyzer--分析服务)
  - [Scheduler — 调度编排](#scheduler--调度编排)
  - [Notifier — 通知网关](#notifier--通知网关)
- [红利策略深度设计](#红利策略深度设计)
- [数据存储设计](#数据存储设计)
- [API设计](#api设计)
- [项目结构](#项目结构)
- [技术选型](#技术选型)
- [开发规划](#开发规划)

---

## 设计哲学

### 三条核心原则

| 原则 | 含义 |
|------|------|
| **数据是共享基础设施** | 行情、基本面、因子值是全局共享的，任何策略都能读取，不允许每个策略独立管理数据 |
| **策略是独立执行单元** | 每个策略独立配置、独立调度、独立产出信号，互不干扰，可同时运行N个策略 |
| **信号是唯一输出协议** | 所有策略的输出统一为 Signal 对象，下游（通知、记录、实盘）只消费 Signal，不关心策略内部逻辑 |

### 为什么需要解耦

```
反面教材（紧耦合）:
  策略A调用 tushare → 拿数据 → 算因子 → 发信号 → 推送微信
  策略B调用 tushare → 拿数据 → 算因子 → 发信号 → 推送微信
  问题: 数据重复下载、因子重复计算、推送逻辑重复、无法并行

正确设计（解耦）:
  DataStore → 统一采集一次 → 所有策略共享读取
  FactorHub → 统一算一次  → 所有策略共享引用
  策略A/B/C → 只做决策逻辑 → 产出 Signal
  SignalBus → 收集所有 Signal → 分发给 Notifier/Logger/Executor
```

### 架构师视角的场景挖掘

在用户明确需求之外，一个量化平台还应当覆盖以下技术场景:

| 场景 | 说明 | 优先级 |
|------|------|--------|
| **因子失效检测** | 滚动监控已入池因子IC，衰减时自动降权或告警 | 高 |
| **策略健康度** | 跟踪每个策略的实时胜率/盈亏比偏离历史基准 | 高 |
| **数据质量校验** | 检测缺失、异常值、Tushare数据错误 | 高 |
| **回撤预警** | 组合回撤超阈值自动通知 | 高 |
| **策略相关性监控** | 多策略同时跑时监控信号重叠度，避免集中风险 | 中 |
| **参数敏感性追踪** | 记录策略参数变更历史与收益变化 | 中 |
| **因子拥挤度** | 热门因子（如低PE）被过度使用时预警 | 低 |
| **模拟盘→实盘过渡** | 先模拟记录信号，攒够样本后再接实盘 | 后期 |

---

## 领域模型

### 核心领域对象

```
┌─────────────────────────────────────────────────────────────────┐
│                        领域对象关系图                             │
│                                                                 │
│  Stock ─── has ──→ DailyBar[]     (行情)                       │
│    │                                                            │
│    ├── has ──→ Fundamental        (基本面: 财务报表+估值)        │
│    │                                                            │
│    ├── has ──→ FactorValue[]      (因子值: 每日每股每因子)       │
│    │                                                            │
│    └── in ───→ Universe[]         (股票池: 沪深300/中证红利/全A)  │
│                                                                 │
│  Factor ─── produces ──→ FactorValue (截面矩阵)                 │
│    │                                                            │
│    └── tested_by ──→ FactorTest    (IC/分组/相关性检验结果)       │
│                                                                 │
│  Strategy ─── reads ──→ DataStore + FactorHub (只读依赖)         │
│    │                                                            │
│    └── produces ──→ Signal[]       (交易信号)                    │
│                                                                 │
│  Signal ─── consumed_by ──→ Notifier / Logger / Executor        │
│                                                                 │
│  Portfolio ─── tracks ──→ Position[] + NAV (组合状态)             │
└─────────────────────────────────────────────────────────────────┘
```

### Signal — 统一输出协议

所有策略的唯一输出类型。下游系统只需要理解 Signal，不关心是哪个策略产出的。

```python
@dataclass
class Signal:
    date: str               # 信号日期 "20260328"
    strategy: str           # 策略名 "dividend"
    ts_code: str            # 股票代码 "000651.SZ"
    action: str             # buy / sell / adjust
    weight: float           # 目标仓位权重 0.05
    score: float            # 综合评分 0~1
    reason: str             # 可读理由
    confidence: float       # 置信度 0~1
    metadata: dict          # 策略自定义附加信息
```

### Universe — 股票池

策略不直接操作全A股，而是在预定义的 Universe 上运行。

```
Universe 类型:
├── all_a           # 全A（剔除ST、停牌、次新股）
├── hs300           # 沪深300成分股
├── zz500           # 中证500成分股
├── zz1000          # 中证1000
├── zz_dividend     # 中证红利成分股
├── custom          # 自定义（白名单/黑名单）
└── union(A, B)     # 组合多个Universe
```

---

## 系统架构

### 八层分层总览

```
层级编号  层名          核心职责                         关键模块
────────  ──────────  ────────────────────────────────  ─────────────────
  L1      数据层       采集、清洗、存储、交易日历        DataStore
  L2      因子层       注册、计算、检验、合成因子        FactorHub
  L3      模型层 ⏸     ML/DL 预测打分（当前不实现）      ModelHub (预留)
  L4      信号层       信号汇聚、冲突仲裁、质量评估      SignalBus
  L5      策略层       选股/择时规则，产出原始信号        StrategyRunner
  L6      组合层       多策略合仓、权重优化、再平衡      PortfolioEngine
  L7      执行层       订单生成、模拟/实盘撮合           Executor
  L8      风控层       事前/事中/事后三道防线             RiskManager
```

⏸ 模型层: 架构上预留位置，当前所有策略走规则引擎(L5)直出信号，不经 ML 预测。
未来如需 LightGBM/LSTM 等模型，在 L3 插入 ModelHub 即可，上下游接口不变。

### 数据流全景

```
┌─────────────────── 接入层 (Gateway) ────────────────────────────┐
│  FastAPI (HTTP)        Server酱 (推送)        CLI (本地调试)     │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────── 编排层 (Orchestration) ───────────────────┐
│  Scheduler (调度)     API Router (路由)      Auth (API Key)     │
└────────────────────────────┬────────────────────────────────────┘
                             │ 触发
          ┌──────────────────┼──────────────────┐
          ▼                  ▼                  ▼
   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
   │ StrategyA   │   │ StrategyB   │   │ StrategyC   │  ← L5 策略层
   │ (红利)      │   │ (指数复制)  │   │ (ETF轮动)   │    并行执行
   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
          │  read            │  read           │  read
          ▼                  ▼                 ▼
   ┌──────────────────────────────────────────────────┐
   │         DataStore (L1) + FactorHub (L2)          │  ← 共享只读
   └──────────────────────────────────────────────────┘
          │                  │                 │
          │  publish         │  publish        │  publish
          ▼                  ▼                 ▼
   ╔══════════════════ SignalBus (L4) ════════════════╗
   ║  汇聚 → 去重 → 冲突标记 → 质量评估 → 分发       ║
   ╚═══════════════════════╤══════════════════════════╝
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
   ┌─────────────┐ ┌──────────────┐ ┌─────────────┐
   │ RiskManager │ │ Portfolio    │ │ Notifier    │
   │ (L8 风控)   │ │ Engine (L6)  │ │ + Logger    │
   │ 事前风控闸门 │ │ 组合优化     │ │ 通知+记录   │
   └──────┬──────┘ └──────┬───────┘ └─────────────┘
          │ 通过           │ 目标持仓
          ▼                ▼
   ┌──────────────────────────────────────┐
   │         Executor (L7 执行层)          │  ← 初期=模拟记录
   │  回测撮合 / 模拟盘 / 实盘桥(预留)     │
   └──────────────────┬───────────────────┘
                      │ 成交回报
                      ▼
   ┌──────────────────────────────────────┐
   │     RiskManager.post_trade (L8)      │  ← 事后归因
   │  PnL归因 / 回撤监控 / 因子暴露检查    │
   └──────────────────────────────────────┘
```

### 模块依赖规则

```
严格的依赖方向（向下依赖，不允许反向）:

  接入层 → 编排层 → 策略层(L5) → 数据层(L1) + 因子层(L2)
                       │
                       ▼ publish
                    信号层(L4) → 风控层(L8) → 组合层(L6) → 执行层(L7)
                       │
                       └──→ 通知 + 日志

禁止:
  ✗ DataStore 调用 Strategy
  ✗ Strategy 调用 Notifier / RiskManager / PortfolioEngine
  ✗ FactorHub 调用 Scheduler
  ✗ 策略之间互相调用
  ✗ Executor 调用 Strategy（反向依赖）

允许:
  ✓ Strategy 读取 DataStore + FactorHub（只读）
  ✓ Strategy 写入 SignalBus（只写）
  ✓ SignalBus 分发给 RiskManager / PortfolioEngine / Notifier / Logger
  ✓ RiskManager 拦截或放行信号 → PortfolioEngine
  ✓ PortfolioEngine 输出目标持仓 → Executor
  ✓ Executor 成交回报 → RiskManager.post_trade()
  ✓ Scheduler 调用各层的 update/run/flush 方法
```

### DataStore — 共享数据中心

**职责**: 数据采集、清洗、存储、查询。唯一的数据写入者。所有模块通过它读取数据。

```
DataStore 接口:

class DataStore:
    # === 数据更新（只有 Scheduler 调用） ===
    def update_daily(date: str = None) -> UpdateResult
        # 增量更新日线数据
    def update_fundamental(period: str = None) -> UpdateResult
        # 更新财报数据
    def update_valuation(date: str = None) -> UpdateResult
        # 更新每日估值
    
    # === 数据查询（所有模块调用） ===
    def query(sql: str) -> pd.DataFrame
        # DuckDB SQL 查询，直查 Parquet
    def get_daily(ts_codes, start, end) -> pd.DataFrame
        # 便捷接口: 获取日线
    def get_fundamental(ts_codes, fields) -> pd.DataFrame
        # 便捷接口: 获取基本面
    def get_universe(name: str, date: str) -> list[str]
        # 获取股票池成分股列表
    def get_trade_dates(start, end) -> list[str]
        # 获取交易日历
    
    # === 数据质量 ===
    def validate(date: str) -> list[DataIssue]
        # 检查数据完整性和异常值

并发安全:
  - 写操作: 顺序执行（Scheduler单线程调度）
  - 读操作: 并发安全（DuckDB支持多读者）
  - 策略并行运行时各自通过 DataStore.query() 读取，互不阻塞
```

### FactorHub — 因子计算与管理

**职责**: 因子注册、计算、检验、合成。输出因子值矩阵供策略消费。

```
FactorHub 接口:

class FactorHub:
    # === 因子注册 ===
    def register(factor: BaseFactor)
        # 注册因子到因子库
    def list_factors() -> list[FactorInfo]
        # 列出所有已注册因子

    # === 因子计算 ===
    def compute(factor_name: str, date: str) -> pd.DataFrame
        # 计算单因子截面值
    def compute_all(date: str)
        # 批量计算所有因子并缓存到 Parquet
    
    # === 因子检验 ===
    def test(factor_name: str, period: str) -> FactorTestResult
        # 运行5道关卡检验
    def monitor_decay() -> list[DecayAlert]
        # 监控因子IC衰减

    # === 因子合成 ===
    def get_composite(date: str, method: str = "ic_weight") -> pd.DataFrame
        # 获取合成因子值
    def get_weights() -> dict[str, float]
        # 当前各因子IC加权权重

因子注册表:
  ┌────────────────────────────────────────────────────────────┐
  │ name            │ category  │ direction │ status │ IC_mean │
  ├─────────────────┼───────────┼───────────┼────────┼─────────┤
  │ ep              │ value     │ +1        │ active │  0.045  │
  │ bp              │ value     │ +1        │ active │  0.038  │
  │ momentum_20d    │ momentum  │ +1        │ active │  0.042  │
  │ roe             │ quality   │ +1        │ active │  0.051  │
  │ revenue_growth  │ growth    │ +1        │ watch  │  0.028  │
  │ turnover_rate   │ technical │ -1        │ active │  0.033  │
  └────────────────────────────────────────────────────────────┘
  status: active(入池) / watch(观察) / retired(淘汰)

因子准入5道关卡:
  ① |IC均值| > 0.03            → 有预测能力
  ② IC_IR > 0.5               → 预测稳定
  ③ IC > 0 比例 > 55%         → 多数时间有效
  ④ 与已有因子相关系数 < 0.7   → 不冗余 ★关键
  ⑤ 分组收益单调性             → 多头 > 空头
```

### StrategyRunner — 策略并行执行器

**职责**: 管理策略注册、并发执行、生命周期。这是解耦设计的核心。

```
设计要点:
  1. 每个策略是一个独立的 BaseStrategy 子类
  2. 策略只依赖 DataStore + FactorHub（只读）
  3. 策略只输出 Signal（通过 SignalBus）
  4. 策略之间完全隔离，互相不感知

class BaseStrategy(ABC):
    name: str
    description: str
    universe: str               # 使用哪个股票池
    schedule: str               # cron 表达式 "0 15 30 * * *"
    
    def __init__(self, datastore: DataStore, factorhub: FactorHub, signalbus: SignalBus):
        self.ds = datastore     # 只读
        self.fh = factorhub     # 只读
        self.bus = signalbus    # 只写
    
    @abstractmethod
    def generate(self, date: str) -> list[Signal]:
        # 子类实现决策逻辑
        pass
    
    def run(self, date: str):
        signals = self.generate(date)
        for sig in signals:
            self.bus.publish(sig)    # 推送到信号总线

策略注册表（运行时可动态增减）:

  StrategyRunner
  ├── dividend          红利策略        schedule="0 30 15 * * *"
  ├── multi_factor      多因子选股      schedule="0 30 15 * * *"
  ├── etf_rotation      ETF轮动        schedule="0 0 16 * * *"
  ├── index_valuation   指数估值定投    schedule="0 0 8 1 * *"
  ├── mean_reversion    超跌反弹        schedule="0 35 15 * * *"
  └── (随时注册新策略...)

并行执行:
  Scheduler触发 → StrategyRunner.run_all(date)
  → 为每个策略创建独立线程/协程
  → 各策略并行读取 DataStore + FactorHub
  → 各策略独立产出 Signal → SignalBus
  → 即使某个策略报错，不影响其他策略

  ┌──────────┐  ┌──────────┐  ┌──────────┐
  │ 红利策略  │  │ 多因子   │  │ ETF轮动  │   ← 并行执行
  └────┬─────┘  └────┬─────┘  └────┬─────┘
       │             │             │
       ▼             ▼             ▼
  ════════════ SignalBus ════════════       ← 汇聚信号
       │
       ├──→ Notifier (推送微信)
       ├──→ Logger   (记录到 Parquet)
       └──→ Executor (未来: 实盘执行)
```

### SignalBus — 信号层 (L4)

**职责**: 收集所有策略产出的 Signal，去重、冲突标记、质量评估，然后分发给下游。
不只是管道，是信号治理中枢。

```
class SignalBus:
    def publish(signal: Signal)
        # 策略调用: 发布一个信号
    
    def subscribe(consumer: SignalConsumer)
        # 注册下游消费者
    
    def flush(date: str)
        # 日终: 汇总当日所有信号 → 去重 → 冲突检测 → 分发

信号治理流水线（flush 内部）:
  ① 去重: 同策略同标的同方向只保留最新
  ② 冲突标记:
     - hard_conflict: buy vs sell → 冻结，推送人工确认
     - soft_conflict: buy vs adjust → 按置信度合并
     - consensus: 多策略同方向 → 共识加分
  ③ 质量评估: 信号命中率滚动统计，低质量策略信号降权
  ④ 分发: 按优先级分发给下游消费者

内置消费者:
  1. SignalLogger        → 信号持久化到 data/signal/ Parquet
  2. NotifierConsumer    → 格式化后推送 Server酱
  3. CorrelationMonitor  → 检查多策略信号重叠度
  4. RiskManager         → 事前风控闸门（L8，独立模块）
  5. PortfolioEngine     → 组合优化（L6，独立模块）

信号 → 组合 → 执行的衔接:
  1) 策略层: Strategy.generate() 输出候选标的与信号方向
  2) 信号层: SignalBus.flush() 去重+冲突标记+分发
  3) 风控层: RiskManager.pre_trade_check() 拦截违规信号
  4) 组合层: PortfolioEngine.optimize() 生成目标持仓
  5) 执行层: Executor.execute() 下单或模拟记录
  6) 风控层: RiskManager.post_trade() 事后归因，回写净值
```

### PortfolioEngine — 组合层 (L6)

**职责**: 接收经风控放行的信号，合并多策略持仓，进行权重优化与再平衡，
输出最终目标持仓给执行层。

```
class PortfolioEngine:
    # === 组合构建 ===
    def optimize(signals: list[Signal], current_positions: dict) -> TargetPortfolio
        # 多策略信号 → 合并 → 权重优化 → 目标持仓
    
    def rebalance(target: TargetPortfolio, current: dict) -> list[Order]
        # 目标持仓 vs 当前持仓 → 调仓订单列表

    # === 组合约束 ===
    def apply_constraints(weights: dict) -> dict
        # 应用硬约束：单票/行业/策略上限

    # === 组合状态 ===
    def get_nav(strategy: str = None) -> pd.DataFrame
        # 获取净值曲线（按策略或整体）
    def get_positions() -> dict
        # 当前持仓快照

@dataclass
class TargetPortfolio:
    date: str
    positions: dict[str, float]     # ts_code → target_weight
    orders: list[Order]             # 需要执行的调仓订单
    metadata: dict                  # 优化方法、风险预算等

权重优化方法（可配置）:
  ┌───────────────┬───────────────────────────────┬──────────┐
  │ 方法          │ 适用场景                       │ 优先级   │
  ├───────────────┼───────────────────────────────┼──────────┤
  │ 等权 (1/N)    │ 初期默认，简单有效             │ P0 实现  │
  │ 信号强度加权   │ 得分越高权重越大               │ P1 实现  │
  │ 风险平价      │ 各标的风险贡献相等             │ P2 实现  │
  │ 均值-方差优化  │ Markowitz，需要协方差矩阵      │ P3 预留  │
  └───────────────┴───────────────────────────────┴──────────┘

硬约束:
  - 单票上限: ≤ 10%（红利策略可放宽到 ≤ 5%更严格）
  - 行业上限: ≤ 30%
  - 策略上限: 单策略占总仓位 ≤ 50%
  - 最小交易单位: 100股（A股整手约束）

多策略合仓逻辑:
  各策略独立产出信号 → PortfolioEngine 统一合仓

  场景1: 红利策略买A，多因子策略也买A
    → 合并权重，取信号层 consensus_score 加权

  场景2: 红利策略持有B，多因子策略卖出B
    → 已在信号层标记 hard_conflict，推送人工确认
    → 默认: 不执行，维持现有仓位

  场景3: 新目标持仓 vs 当前持仓差异 < 阈值
    → 不调仓（降低换手率）

换手率控制:
  - 最小调仓阈值: 权重变化 < 1% 的不调
  - 缓冲带: 目标持仓排名在 Top N+5 内的老样本保留
  - 单次换仓上限: ≤ 总持仓的 20%
```

### RiskManager — 风控层 (L8)

**职责**: 独立于策略和组合的三道防线。事前把关 → 事中监控 → 事后归因。

```
class RiskManager:
    # === 事前风控 (Pre-Trade) ===
    def pre_trade_check(signals: list[Signal]) -> list[Signal]
        # 过滤违规信号，返回放行的信号
        # 被拦截信号写入 rejected_signals 日志

    # === 事中风控 (Intra-Day) ===
    def monitor(portfolio: dict) -> list[RiskAlert]
        # 实时/日终监控组合状态，触发告警

    # === 事后风控 (Post-Trade) ===
    def post_trade(executions: list[Execution]) -> AuditReport
        # 成交归因、回撤分析、因子暴露检查

事前风控规则（pre_trade_check 内部）:
  ┌───────────────────────────────────────────────────────┐
  │ 规则                    │ 动作          │ 优先级      │
  ├─────────────────────────┼───────────────┼─────────────┤
  │ 目标股 ST/*ST           │ 拦截          │ P0          │
  │ 目标股停牌/涨跌停       │ 拦截          │ P0          │
  │ 单票仓位 > 10%          │ 削减到上限    │ P0          │
  │ 行业集中度 > 30%        │ 削减到上限    │ P0          │
  │ 买入标的在黑名单        │ 拦截          │ P0          │
  │ 持仓数量 < 10 只        │ 告警          │ P1          │
  │ 组合 Beta > 1.2         │ 告警          │ P2          │
  └───────────────────────────────────────────────────────┘

事中监控规则:
  - 单日亏损 > 2%  → P0 告警推送
  - 累计回撤 > 10% → P1 告警 + 建议减仓到 50%
  - 累计回撤 > 15% → P0 告警 + 自动停止新开仓
  - 年化波动率 > 25% → P1 告警
  - 策略IC连续3个月 < 0 → P1 告警（因子/策略衰退）

事后归因:
  - Brinson 归因: 选股效应 + 配置效应 + 交互效应
  - 因子暴露分解: 风格偏移检测（市值/价值/动量/波动）
  - 实盘 vs 回测偏差分析
  - 月度/季度自动归因报告
```

### Executor — 执行层 (L7)

**职责**: 接收 PortfolioEngine 产出的目标持仓，生成订单并撮合。
初期为模拟记录器，后期可对接实盘。

```
class Executor:
    def execute(orders: list[Order]) -> list[Execution]
        # 执行订单，返回成交记录

    def get_fill_report(date: str) -> FillReport
        # 当日成交汇总

执行模式:
  ┌────────────┬──────────────────────────────┬──────────┐
  │ 模式       │ 说明                          │ 阶段     │
  ├────────────┼──────────────────────────────┼──────────┤
  │ backtest   │ 事件驱动回测撮合              │ Phase 2  │
  │ paper      │ 模拟盘: 按收盘价记录成交      │ Phase 3  │
  │ live       │ 实盘: 对接 easytrader/vnpy   │ Phase 4+ │
  └────────────┴──────────────────────────────┴──────────┘

回测与实盘一致性:
  - 统一订单模型: Order → Execution，三种模式共用
  - 统一成本模型: 佣金万三 + 印花税千五(卖) + 滑点
  - 统一交易约束: T+1、涨跌停不可交易、整手100股
  - 统一交易日历: DataStore.get_trade_dates()
```

### Analyzer — 分析服务

**职责**: 单股分析、指数估值、行业轮动等即时查询能力。不产出 Signal，只回答问题。

```
Analyzer 接口:

class Analyzer:
    def analyze_stock(ts_code: str) -> StockReport
        # 综合分析: 基本面 + 技术面 + 估值
    
    def valuation_percentile(index_code: str) -> ValuationReport
        # 指数估值百分位（PE/PB 近5年/10年）
    
    def oversold_scan(threshold: float = -0.05) -> list[OversoldStock]
        # 超跌扫描: RSI + 跌幅 + 布林带
    
    def industry_rotation() -> IndustryReport
        # 行业轮动: 各行业动量 + 估值 + 资金流

StockReport 结构:
  ┌─ 基本面 ─────────────────────────────────────────┐
  │  PE/PB/PS 当前值 & 历史百分位                      │
  │  ROE/ROA/毛利率 趋势 (3年)                         │
  │  营收/净利增长率                                    │
  │  现金流质量、分红记录                               │
  │  vs 行业中位数对比                                  │
  └──────────────────────────────────────────────────┘
  ┌─ 技术面 ─────────────────────────────────────────┐
  │  均线位置(5/10/20/60/120日)、多空排列               │
  │  RSI(14)、布林带位置、MACD                         │
  │  近N日涨跌幅、换手率、量比                          │
  └──────────────────────────────────────────────────┘
  ┌─ 综合评分 ───────────────────────────────────────┐
  │  基本面 × 40% + 技术面 × 30% + 估值 × 30%        │
  │  → 总分 + 建议(买入/持有/卖出) + 理由              │
  └──────────────────────────────────────────────────┘
```

### Scheduler — 调度编排

**职责**: 时间驱动的任务编排。只做"什么时候调用谁"，不做业务逻辑。

```
调度表 (八层完整链路):

频率          │ 时间   │ 任务                             │ 所属层  │ 依赖
─────────────┼───────┼──────────────────────────────────┼────────┼──────────
每交易日     │ 06:30 │ DataStore.update_daily()          │ L1     │ 无
每交易日     │ 07:00 │ FactorHub.compute_all()           │ L2     │ L1完成
每交易日     │ 15:30 │ StrategyRunner.run_all()          │ L5     │ 收盘+L2
每交易日     │ 15:40 │ SignalBus.flush()                 │ L4     │ L5完成
每交易日     │ 15:42 │ RiskManager.pre_trade_check()     │ L8     │ L4完成
每交易日     │ 15:44 │ PortfolioEngine.optimize()        │ L6     │ L8放行
每交易日     │ 15:46 │ Executor.execute()                │ L7     │ L6完成
每交易日     │ 15:50 │ RiskManager.post_trade()          │ L8     │ L7完成
每交易日     │ 16:00 │ FactorHub.monitor_decay()         │ L2     │ 无
每交易日     │ 16:00 │ RiskManager.monitor()             │ L8     │ 无
每周一       │ 08:00 │ Analyzer.industry_rotation()      │ -      │ 无
每月1日      │ 08:00 │ Analyzer.valuation_report()       │ -      │ 无
每季报后     │ 手动  │ DataStore.update_fundamental()     │ L1     │ 无

任务链 DAG (完整八层):
  L1 数据更新 → L2 因子计算 → L5 策略执行 → L4 信号汇总
    → L8 事前风控 → L6 组合优化 → L7 执行 → L8 事后归因
  (前一步完成才能触发下一步，不是固定时间间隔)

实现: APScheduler
  - CronTrigger 定时触发
  - 交易日历感知（跳过非交易日）
  - 失败重试(最多3次) + 失败通知
  - 支持 API 手动触发
```

### Notifier — 通知网关

**职责**: 消息格式化与推送。只消费 Signal，不生产。

```
推送通道: Server酱 (https://sct.ftqq.com/)
  免费额度: 每天5条（足够）

消息分级:
  P0 紧急: 回撤预警、数据异常          → 立即推送
  P1 重要: 调仓信号、超跌预警           → 实时推送
  P2 日报: 收盘信号汇总                 → 合并为一条推送
  P3 周/月: 估值报告、策略绩效          → 定时推送

消息模板:
  ──────────────────────────
  📊 收盘信号汇总 | 2026-03-28
  ──────────────────────────
  
  【红利策略】
  🟢 买入: 中国神华(601088) 5% 仓位
    股息率 5.8% | PE 8.2 | 连续分红10年
  
  【多因子策略】
  🟢 买入: 格力电器(000651) 5% 仓位
    综合评分 0.85 | 估值低位+ROE>20%
  🔴 卖出: 中国平安(601318) 清仓
    动量转负 | 因子得分下降
  
  【策略信号重叠】
  ⚠️ 格力电器 被 红利+多因子 同时选中
  ──────────────────────────

防骚扰:
  同一信号当日不重复推送
  P2/P3 合并为单条
  静默时段: 23:00-07:00
```

---

## 红利策略深度设计

> 参考雪球 @高股息之家、@超级鲁鼎公 等成熟高股息投资者的方法论,
> 提炼为可量化的规则体系。

### 策略哲学

高股息策略的本质不是"追求高股息率"，而是:

1. **分红是盈利质量的试金石** — 能持续高分红的公司，现金流和盈利是真实的
2. **股息率隐含安全边际** — 高股息率往往意味着低估值，向下空间有限
3. **强制纪律** — 按股息率排名机械选股，避免情绪干扰

### 选股规则（量化实现）

```
DividendStrategy 选股流程:

Step 1: 初筛（剔除不合格）
  ✗ ST / *ST 股票
  ✗ 上市不满 3 年
  ✗ 最近 1 年净利润为负
  ✗ 最近 1 年经营性现金流为负
  ✗ 资产负债率 > 70%（金融股除外）
  ✗ 最近 1 年营收同比下降 > 20%

Step 2: 分红质量筛选
  ✓ 过去 3 年连续现金分红                    ★核心条件
  ✓ 过去 3 年平均股息率 > 3%
  ✓ 过去 3 年平均分红比例 30%~75%            （过低吝啬，过高不可持续）
  ✓ 最近一期股息率 > 当年10年国债收益率

Step 3: 估值过滤
  ✓ PE_TTM < 20                              （排除成长股伪高息）
  ✓ PB < 3                                   （排除泡沫资产）
  ✓ PE_TTM 处于自身近5年 < 50% 百分位         （不追高）

Step 4: 排名与选股
  综合得分 = 当期股息率 × 40%
           + 连续分红年数(归一化) × 20%
           + 分红增长率(3年复合) × 20%
           + 估值得分(PE百分位越低越好) × 20%
  按综合得分降序，取 Top 20~30 只

Step 5: 权重分配
  方案A: 等权（简单有效，推荐初期使用）
  方案B: 股息率加权（股息率越高权重越大）
  方案C: 市值加权（偏向大盘股，更稳健）
  单只上限: ≤ 5%（分散风险）
```

### 调仓规则

```
调仓频率: 半年 或 年度（红利策略天然低换手）
调仓时机:
  - 年报发布后（4月中~5月）  ★主调仓窗口
  - 半年报后（9月~10月）     次调仓窗口
  
触发调仓:
  定期: 到达调仓窗口时重新排名选股
  被动: 
    - 个股分红政策重大变化（取消分红）→ 剔除
    - 个股基本面恶化（季报亏损）      → 剔除
    - 个股估值过高（PE升至历史80%+分位）→ 减仓

不调仓的情况:
  - 短期股价波动（红利策略不做波段）
  - 行业轮动（红利策略是cross-sector的）
```

### 行业分布约束

```
防集中风险:
  单行业上限: ≤ 30%（银行股容易超配，需限制）
  
A股高股息常见行业:
  银行       │ 股息率高，但PB普遍 < 1，需注意坏账风险
  煤炭       │ 周期股，高分红期可能是周期顶部
  公用事业    │ 稳定但成长性差，长期持有优势
  交通运输    │ 高速公路、港口类现金流好
  石化       │ 中国石化等，受油价周期影响

周期股处理:
  周期股（煤炭、钢铁、石化）的高股息率可能是盈利周期顶部信号
  增加条件: 近3年ROE标准差 < 5%（排除盈利波动过大的周期股）
  或者: 用近3年平均利润替代当期利润计算PE
```

### 与多因子策略的对比

```
维度          │ 红利策略            │ 多因子策略
换手率        │ 低（年化 < 50%）    │ 较高（月度调仓）
选股逻辑      │ 规则透明、可解释     │ 因子合成、较黑箱
适合市场      │ 震荡市/熊市          │ 各市场阶段
收益来源      │ 股息 + 估值修复      │ Alpha因子超额
风险特征      │ 回撤小但弹性不足     │ 回撤中等但弹性好
重叠度        │ 与多因子的value因子部分重叠

建议: 两者同时运行，互为补充
  - 牛市: 多因子策略贡献主要收益
  - 熊市: 红利策略提供防御
  - SignalBus 的 CorrelationMonitor 负责监控信号重叠度
```

---

## 数据存储设计

### 存储架构

```
data/
├── market/                        # 行情数据（DataStore 写入，所有模块只读）
│   ├── daily/                     # 日线按年分片
│   │   ├── 2020.parquet
│   │   └── ...
│   ├── adj_factor/
│   │   └── latest.parquet
│   └── index_daily/
│       └── 2026.parquet
│
├── fundamental/                   # 基本面
│   ├── income.parquet
│   ├── balance.parquet
│   ├── cashflow.parquet
│   ├── dividend.parquet           # ★ 分红数据（红利策略核心）
│   ├── financial_indicator.parquet
│   └── valuation/
│       └── 2026.parquet
│
├── factor/                        # FactorHub 写入
│   ├── momentum_20d.parquet
│   ├── value_ep.parquet
│   ├── composite.parquet
│   └── _registry.json             # 因子注册表元数据
│
├── signal/                        # SignalBus 写入
│   └── 2026.parquet
│
├── portfolio/                     # 各策略模拟净值
│   ├── dividend_nav.parquet
│   ├── multi_factor_nav.parquet
│   └── combined_nav.parquet
│
├── risk/                          # L8 风控层数据
│   ├── alerts/                    # 风控告警历史
│   │   └── 2026.parquet
│   ├── rejected_signals/          # 被拦截信号日志
│   │   └── 2026.parquet
│   └── attribution/               # 归因报告
│       └── 2026_Q1.parquet
│
├── execution/                     # L7 执行层数据
│   └── fills/                     # 成交记录
│       └── 2026.parquet
│
├── meta/
│   ├── trade_cal.parquet
│   ├── stock_basic.parquet
│   ├── index_weight/
│   │   ├── 000300.parquet
│   │   └── 000922.parquet         # 中证红利
│   └── download_log.db
│
└── cache/
    └── tushare_cache.db
```

### 数据更新策略

```
数据类型      │ 更新频率     │ 方式       │ 说明
──────────────┼─────────────┼───────────┼───────────────────
交易日历      │ 年初1次     │ 全量覆盖   │ 数据量小
日线行情      │ 每交易日    │ 增量追加   │ 补齐到最新交易日
复权因子      │ 每交易日    │ 全量覆盖   │ 数据量小
估值数据      │ 每交易日    │ 增量追加   │ 按年分片
财务报表      │ 季报后      │ 增量追加   │ 手动或检测触发
分红数据      │ 年报后      │ 增量追加   │ 红利策略核心
指数成分股    │ 每月        │ 全量覆盖   │ 跟踪调整
```

---

## API设计

### 路由总览

```
FastAPI 路由:

/api
├── /health                          GET    健康检查
│
├── /stock/{ts_code}                 GET    单股综合分析
├── /stock/{ts_code}/fundamental     GET    基本面详情
├── /stock/{ts_code}/technical       GET    技术面详情
├── /stock/{ts_code}/valuation       GET    估值分析
│
├── /strategy
│   ├── /list                        GET    可用策略列表（含运行状态）
│   ├── /run                         POST   手动执行指定策略
│   ├── /{name}/params               GET    策略参数说明
│   └── /{name}/health               GET    策略健康度（胜率/盈亏比偏离）
│
├── /factor
│   ├── /list                        GET    因子库列表（含IC/状态）
│   ├── /test                        POST   单因子检验（5道关卡）
│   ├── /composite                   GET    当前合成因子权重
│   └── /decay                       GET    因子衰减监控
│
├── /backtest
│   ├── /submit                      POST   提交回测任务
│   ├── /status/{task_id}            GET    查询状态
│   └── /report/{task_id}            GET    获取HTML报告
│
├── /signal
│   ├── /latest                      GET    最新信号（可按策略过滤）
│   ├── /history                     GET    历史信号查询
│   └── /conflicts                   GET    多策略冲突信号
│
├── /portfolio
│   ├── /nav                         GET    各策略模拟净值
│   ├── /positions                   GET    当前目标持仓
│   ├── /orders                      GET    待执行/已执行订单
│   └── /correlation                 GET    策略信号重叠度
│
├── /risk
│   ├── /status                      GET    组合风控状态（回撤/波动/暴露）
│   ├── /alerts                      GET    历史风控告警
│   ├── /rejected                    GET    被拦截信号列表
│   └── /attribution                 GET    归因报告（月度/季度）
│
├── /index/valuation                 GET    指数估值百分位
│
├── /scheduler
│   ├── /status                      GET    任务状态
│   └── /trigger/{task}              POST   手动触发
│
└── /data/update                     POST   手动数据更新
```

### API认证

```
请求头: X-API-Key: your-secret-key
配置在 .env 中，不硬编码
局域网内访问 (初期优先方案)
后期可选 Cloudflare Tunnel / Tailscale
```

### 请求/响应示例

```json
GET /api/stock/000651.SZ

{
  "ts_code": "000651.SZ",
  "name": "格力电器",
  "price": 38.52,
  "fundamental": {
    "pe_ttm": 8.5,
    "pe_percentile": 0.15,
    "pb": 1.8,
    "roe": 22.3,
    "dividend_yield": 5.2,
    "dividend_years": 12
  },
  "technical": {
    "ma_position": "below_ma20",
    "rsi_14": 35.2,
    "bollinger_position": 0.22
  },
  "score": {
    "total": 72,
    "suggestion": "hold",
    "reason": "估值低位(PE百分位15%), 基本面优秀(ROE>20%), 短期趋势偏弱"
  }
}
```

---

## 项目结构

```
quantpilot/
│
├── README.md
├── pyproject.toml
├── .env.example                      # API Key, Tushare Token 等
│
├── config/
│   ├── settings.py                   # 配置管理（从 .env 读取）
│   └── schedule.yaml                 # 调度表定义
│
├── app/
│   ├── main.py                       # FastAPI 入口 + 生命周期
│   │
│   ├── api/                          # 接入层: 路由（不含业务逻辑）
│   │   ├── stock.py
│   │   ├── strategy.py
│   │   ├── factor.py
│   │   ├── backtest.py
│   │   ├── signal.py
│   │   ├── portfolio.py
│   │   ├── risk.py                   # 风控状态/告警查询
│   │   ├── scheduler.py
│   │   └── auth.py                   # API Key 中间件
│   │
│   ├── core/                         # 共享基础设施 (L1 + L2 + L4)
│   │   ├── datastore.py              # L1 DataStore — 数据读写
│   │   ├── factorhub.py              # L2 FactorHub — 因子管理
│   │   ├── signalbus.py              # L4 SignalBus — 信号层
│   │   └── scheduler.py              # 调度编排
│   │
│   ├── strategy/                     # L5 策略层（每个策略一个文件）
│   │   ├── base.py                   # BaseStrategy 抽象基类
│   │   ├── runner.py                 # StrategyRunner 并行执行器
│   │   ├── dividend.py               # 红利策略
│   │   ├── index_replica.py          # 指数复制策略（自由现金流/红利质量）
│   │   ├── etf_rotation.py           # ETF轮动
│   │   ├── index_valuation.py        # 指数估值定投
│   │   └── mean_reversion.py         # 超跌反弹
│   │
│   ├── factor/                       # L2 因子库
│   │   ├── base.py                   # BaseFactor 抽象基类
│   │   ├── value.py                  # EP, BP, DP, 股息率
│   │   ├── momentum.py               # 收益率动量, 成交量动量
│   │   ├── quality.py                # ROE, 毛利率, 现金流
│   │   ├── cashflow.py               # FCF/EV, OCF覆盖, 自由现金流率
│   │   ├── growth.py                 # 营收增长, 净利增长
│   │   ├── technical.py              # 换手率, 波动率, RSI
│   │   └── composite.py              # IC加权合成
│   │
│   ├── portfolio/                    # L6 组合层
│   │   ├── engine.py                 # PortfolioEngine — 权重优化
│   │   ├── optimizer.py              # 等权/信号加权/风险平价
│   │   ├── constraints.py            # 硬约束（单票/行业/策略上限）
│   │   └── rebalancer.py             # 再平衡 & 换手率控制
│   │
│   ├── risk/                         # L8 风控层
│   │   ├── manager.py                # RiskManager — 三道防线统一入口
│   │   ├── pre_trade.py              # 事前风控规则
│   │   ├── monitor.py                # 事中监控（回撤/波动/暴露）
│   │   └── attribution.py            # 事后归因（Brinson/因子暴露）
│   │
│   ├── executor/                     # L7 执行层
│   │   ├── base.py                   # BaseExecutor 抽象基类
│   │   ├── backtest.py               # 回测撮合（Backtrader封装）
│   │   ├── paper.py                  # 模拟盘（按收盘价记录）
│   │   └── live.py                   # 实盘桥（预留）
│   │
│   ├── analysis/                     # 分析服务（非核心链路）
│   │   ├── analyzer.py               # Analyzer 主类
│   │   ├── fundamental.py            # 基本面分析
│   │   ├── technical.py              # 技术面分析
│   │   └── valuation.py              # 估值分析
│   │
│   ├── notify/                       # 通知网关
│   │   ├── notifier.py               # 消息分发
│   │   ├── serverchan.py             # Server酱实现
│   │   └── templates.py              # 消息模板
│   │
│   └── utils/
│       ├── date_utils.py             # 交易日历
│       ├── data_utils.py             # 数据处理
│       └── math_utils.py             # 统计工具
│
├── data/                             # 数据目录（gitignore）
│
├── reports/                          # 回测报告输出
│
├── scripts/
│   ├── init_data.sh                  # 初始化历史数据下载
│   └── install_service.sh            # 注册系统服务
│
└── tests/
    ├── test_datastore.py
    ├── test_factors.py
    ├── test_strategies.py
    ├── test_signalbus.py
    ├── test_portfolio.py
    ├── test_risk.py
    └── test_api.py
```

### 关键设计约束

```
目录与八层的映射:

  app/core/       → L1 数据层 + L2 因子层 + L4 信号层（共享基础设施）
  app/factor/     → L2 因子库实现（只依赖 core/datastore）
  app/strategy/   → L5 策略层（只依赖 core/，不依赖其他业务模块）
  app/portfolio/  → L6 组合层（消费信号，输出目标持仓）
  app/executor/   → L7 执行层（消费目标持仓，输出成交记录）
  app/risk/       → L8 风控层（贯穿事前/事中/事后）
  app/analysis/   → 分析服务（辅助模块，非核心链路）
  app/notify/     → 通知网关（消费信号，向外推送）
  app/api/        → 接入层（路由只做参数校验和转发）

依赖方向（只允许向下/向右）:
  strategy/ → core/
  portfolio/ → core/ + risk/
  executor/ → core/ + portfolio/
  risk/ → core/
  notify/ → core/

禁止:
  ✗ strategy/ 导入 notify/ / portfolio/ / risk/ / executor/
  ✗ factor/ 导入 strategy/
  ✗ core/ 导入任何业务模块
  ✗ executor/ 导入 strategy/
```

---

## 技术选型

| 组件 | 选型 | 理由 |
|------|------|------|
| 语言 | Python 3.11+ | 量化生态最好 |
| Web | FastAPI | 异步、类型安全、自带文档 |
| 调度 | APScheduler | 轻量Cron、嵌入FastAPI |
| 存储 | DuckDB + Parquet | 列式查询快、零配置 |
| 元数据 | SQLite | 下载日志、任务记录 |
| 回测 | Backtrader | 事件驱动、A股适配成熟 |
| 报告 | quantstats | 一行HTML报告 |
| 数据源 | Tushare Pro | A股数据最全免费源 |
| 推送 | Server酱 | 免费、微信直达 |
| 依赖管理 | uv | 快速、现代 |

### 核心依赖

```
fastapi, uvicorn[standard], apscheduler
tushare, duckdb, pyarrow
numpy, pandas, scipy, statsmodels, ta-lib
backtrader, quantstats
requests
```

---

## 开发规划

### 优先级矩阵

```
        高价值
          │
    P1    │    P0
  (重要    │  (立即做)
   不紧急) │
──────────┼──────────── 高紧急
    P3    │    P2
  (以后做) │  (快速解决)
          │
        低价值
```

### Phase 0: 骨架搭建（Week 1）

**目标**: 项目跑起来，能下载数据能查询

```
P0 ✅ 项目初始化: pyproject.toml + 目录结构 + .env
P0 ✅ DataStore 基础版: Tushare日线下载 → Parquet存储
P0 ✅ DataStore.query(): DuckDB SQL 查询接口
P0 ✅ FastAPI 骨架: /api/health + /api/stock/{ts_code} 基础查询
P0 ✅ 交易日历: 本地缓存 + is_trade_day() 工具

交付物: 能通过 API 查询任意股票的历史行情
验证: curl localhost:8000/api/stock/000651.SZ 返回数据
```

### Phase 1: 最小可用系统（Week 2-3）

**目标**: 自动化运行一个策略、推送微信

```
P0 ✅ DataStore 完整版: 基本面 + 估值 + 分红 + 增量更新
P0 ✅ Analyzer: 单股分析（基本面+技术面+评分）
P0 ✅ Notifier: Server酱推送
P0 ✅ Scheduler: APScheduler + 交易日历感知
P0 ✅ 第一个策略: 指数估值百分位 → 月度推送

P1 ✅ 数据质量校验: 缺失检测 + 异常值告警
P1 ✅ API Key 认证

交付物: 每月自动推送指数估值、每日可查个股分析
验证: 等到月初，微信收到估值推送
```

### Phase 2: 因子体系 + 红利策略（Week 4-6）

**目标**: 因子框架可用、第一个策略（红利+指数复制）上线

```
P0 ✅ FactorHub: 因子注册 + 计算 + 缓存
P0 ✅ 基础因子: EP/BP/momentum/ROE/换手率（先做5个）
P0 ✅ 现金流因子: fcf_yield/ocf_to_op/roe_stability（指数复制需要）
P0 ✅ 因子检验: 5道关卡自动化
P0 ✅ SignalBus: 信号收集 + 去重 + 冲突标记 + 分发
P0 ✅ StrategyRunner: 策略注册 + 并行执行

P0 ✅ 红利策略: 高股息之家4进3出完整流程
P0 ✅ 指数复制策略: 980092自由现金流 + 932315红利质量
P0 ✅ 红利策略回测: 验证近5年收益

P1 ✅ IC加权合成因子
P1 ✅ quantstats 回测报告

交付物: 红利+指数复制策略自动运行，收盘后推送信号
验证: 每交易日 15:45 收到调仓推送
```

### Phase 3: 组合+风控+回测（Week 7-9）

**目标**: 多策略合仓、独立风控、完整回测

```
P0 ✅ PortfolioEngine: 等权合仓 + 硬约束（单票/行业上限）
P0 ✅ RiskManager.pre_trade: 事前风控闸门
P0 ✅ RiskManager.monitor: 事中回撤/波动监控
P0 ✅ Executor (backtest): Backtrader封装（T+1/涨跌停/手续费）

P1 ✅ 回测API: 提交任务 → 异步执行 → 报告下载
P1 ✅ PortfolioEngine: 信号强度加权
P1 ✅ ETF轮动策略
P1 ✅ 策略健康度监控
P1 ✅ 因子衰减监控
P1 ✅ 策略信号相关性监控

交付物: 多策略合仓后统一风控+执行，可提交回测
```

### Phase 4: 增强与可选（Week 10+）

**目标**: 按需扩展，优先级由使用体验驱动

```
P1 Executor (paper): 模拟盘记录（按收盘价记录信号表现）
P1 RiskManager.post_trade: 事后归因（Brinson/因子暴露）
P1 PortfolioEngine: 风险平价/换手率控制

P2 行业轮动分析
P2 更多因子开发（扩展到20+）
P2 超跌反弹策略
P2 API文档优化 (Swagger)

P3 远程访问: Cloudflare Tunnel / Tailscale
P3 Agent引擎: LLM + Function Calling
P3 Executor (live): 实盘对接 easytrader/vnpy
P3 同花顺持仓同步
P3 Web Dashboard
```
