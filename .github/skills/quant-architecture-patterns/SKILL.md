---
name: quant-architecture-patterns
description: '量化系统架构模式技能。Use when: 设计多策略并行系统、研究/交易解耦、信号总线治理、回测与实盘一致性建设。适用于: quant system architecture, strategy decoupling, signal orchestration, research-to-production workflow.'
argument-hint: '描述你的架构问题，如并发策略、模块解耦、回测实盘一致性、风控与执行分层'
---

# 量化系统架构模式

## 核心目标

- 让“研究迭代快”与“生产稳定”同时成立
- 支持多策略并发、信号透明、执行可追溯
- 为未来实盘执行预留清晰边界

## 推荐八层架构（模型层当前预留）

```
L1 数据层       DataStore       采集/清洗/存储/交易日历
L2 因子层       FactorHub       注册/计算/检验/合成因子
L3 模型层 ⏸     ModelHub        ML/DL预测（当前不实现，预留位置）
L4 信号层       SignalBus       汇聚/去重/冲突标记/质量评估/分发
L5 策略层       StrategyRunner  选股/择时规则引擎，产出原始信号
L6 组合层       PortfolioEngine 多策略合仓/权重优化/再平衡/换手率控制
L7 执行层       Executor        订单生成/回测撮合/模拟盘/实盘桥
L8 风控层       RiskManager     事前闸门/事中监控/事后归因
```

数据流: L1→L2→(L3跳过)→L5→L4→L8事前→L6→L7→L8事后

---

## 你当前设计可继续强化的点

### 1) SignalBus 消费者应多实例化

建议最少拆成 6 类消费者：
- SignalLogger（信号审计）
- PortfolioTracker（组合状态）
- RiskGateConsumer（预交易风控）
- NotifierConsumer（告警与推送）
- ExecutionBridge（实盘/模拟执行桥）
- FeatureStoreWriter（训练样本回流）

### 2) 冲突信号治理

对同标的冲突信号建议加入策略：
- `hard_conflict`: buy vs sell，冻结并人工复核
- `soft_conflict`: buy vs adjust，按风险预算降杠杆合并
- `consensus_score`: 跨策略投票分数

### 3) 研究-生产一致性

- 同一套撮合与成本模型（回测/模拟盘/实盘）
- 同一份交易日历与停牌规则
- 信号版本号 + 参数快照 + 数据快照可复现

---

## 与对冲基金常见实践对齐（可借鉴）

- 研究、组合、交易、风控四职能分离
- 策略容器化，单策略故障不拖垮系统
- “预交易风控 + 盘中风控 + 事后归因”闭环
- 用组合层约束而不是单票直觉（风险预算优先）

---

## 落地检查清单

- [ ] 策略是否只读数据、只写信号
- [ ] 信号是否有统一协议（action/weight/confidence/reason）
- [ ] 冲突是否可解释且可追溯
- [ ] 执行前是否有统一风控闸门
- [ ] 回测与实盘是否共享同一订单模型
- [ ] 每日任务失败是否具备降级与重试
- [ ] 筛选逻辑是否封装为 StockFilter，可跨策略复用
- [ ] 因子权重是否由 WeightOptimizer 动态决定（非硬编码）

---

## 选股逻辑封装模式

### 筛选器管道模式 (FilterPipeline)

**原则**: 每个筛选条件是独立的 `StockFilter`，按顺序组装成 `FilterPipeline`。
策略只负责"选哪些筛选器、按什么顺序排列"，不负责实现筛选逻辑。

```
app/strategy/filters.py
├── StockFilter(ABC)           # 筛选器基类: apply(pool, factor_data, ctx) → pool
├── FilterPipeline             # 管道: 顺序执行 + trace 记录
├── FilterContext              # 共享上下文: date, df_basic, settings
│
├── NonSTFilter                # 剔除 ST / 退市
├── MinListedDaysFilter        # 上市天数门槛
├── IndustryExcludeFilter      # 行业排除
├── FactorThresholdFilter      # 通用因子阈值: factor >= / <= / > / < threshold
├── FactorRangeFilter          # 因子范围: lo <= factor <= hi
└── QuantileCutoffFilter       # 截面分位截断: 保留前 N%
```

**新策略复用方式**:
```python
# 纯价值策略: 复用部分筛选器，权重不同
value_pipeline = FilterPipeline([
    NonSTFilter(),
    MinListedDaysFilter(min_days=365),
    FactorThresholdFilter("ep", op="gt", threshold=0),
])
```

### 因子权重优化模式 (WeightOptimizer)

**原则**: 策略指定参与打分的因子列表，权重由 optimizer 计算。

```
app/core/weight_optimizer.py
├── WeightOptimizer(ABC)       # 基类: optimize(factors, date) → {name: weight}
├── FixedWeightOptimizer       # 固定权重（调试用 / 无历史时退化）
├── EqualWeightOptimizer       # 等权
├── ICWeightOptimizer          # IC加权（推荐，自适应）
└── ICIRWeightOptimizer        # IC_IR加权（追求稳定性）
```

**IC加权公式**: `w_i = |IC_mean_i| / Σ|IC_mean_j|`
**退化机制**: 历史不足 min_periods 时自动 fallback 到 FixedWeightOptimizer
**IC诊断**: `get_ic_report()` 输出 ic_mean / ic_std / ic_ir / ic_positive_pct
