---
name: cogalpha-factor-mining
description: "CogAlpha 方法附录与参考 skill。默认不要作为量化研究入口；Use only when: 用户明确要求只讨论 CogAlpha/Cognitive Alpha Mining 方法论、agent catalog、diversified guidance、quality gate、fitness evaluation、mutation/crossover 或 lineage 细节。常规因子研究应使用 factor-mining-research。"
argument-hint: "描述目标市场、可用字段、希望探索的 agent 方向、预测周期、调仓约束、已有因子池和本轮是否允许生成/评测候选代码"
tags: [vortex, vortex/skill, vortex/research-domain, cogalpha, factor-mining]
obsidian_links:
  - "[[CogAlpha学习资料]]"
  - "[[CogAlpha Agent体系设计]]"
  - "[[CogAlpha Quality Gate与Fitness规范]]"
  - "[[CogAlpha因子研究落地路线]]"
  - "[[因子研究与评测全流程说明]]"
  - "[[研究协作与产物治理]]"
---

# CogAlpha 因子挖掘与进化研究

本 skill 是 CogAlpha 方法附录，不是 Vortex 量化研究的默认入口。常规因子研究、策略优化、preset 对照和新接口调用都应从 `factor-mining-research` 开始；本文件只保留 CogAlpha 论文方法、agent catalog、quality gate、fitness、lineage 和 mutation/crossover 的细节，供总流程引用。

核心原则：**CogAlpha 是候选生成与进化方法，不是因子本身，不是策略定版工具，也不是实盘入口。** 先生成可检查的研究候选，再回到 `factor-mining-research` 进入因子评测、策略角色判定、`version-review` preset 对照和归档；elite alpha 仍只是研究候选，不是策略。

---

## 一、什么时候使用

默认不要直接使用本 skill。只有出现以下明确场景时才使用：

1. 用户明确要求只讨论 CogAlpha、Cognitive Alpha Mining、LLM-driven alpha mining 的方法论。
2. 需要查阅多个 agent 视角、diversified guidance、quality gate 或 mutation/crossover 规则细节。
3. 需要把 `factor-mining-research` 中的某个研究方向展开成 CogAlpha agent / recipe 设计。
4. 需要审查 LLM 生成因子是否有未来函数、数值不稳定或经济逻辑薄弱。
5. 需要把成功因子和失败因子写回下一轮 prompt/context。

不要在这些场景使用：

1. 用户只要求普通单因子 IC 评测，此时优先用 `factor-evaluation`。
2. 用户要求继续量化研究、策略优化、因子融合、容量研究或 preset 对照，此时从 `factor-mining-research` 开始。
3. 用户只要求从书籍/指数方案提炼候选因子，此时从 `factor-mining-research` 开始；该 skill 会把 CogAlpha 作为内部方法使用。
4. 用户要求策略上线或 QMT 执行，此时优先用执行与评审类 skill。

---

## 二、不可突破的边界

1. 不直接运行任意 LLM 生成的 Python 代码。
2. 不把 CogAlpha 论文结果当成 Vortex 本地结论。
3. 不把 `elite alpha` 直接接入策略或实盘。
4. 不跳过 PIT、复权、ST、停牌、涨跌停、成本、容量和可交易性检查。
5. 不为小账户牺牲至少 5000 万容量评估原则。
6. 不用未来收益、未来价格、公告前不可见数据或负向 shift 构造因子。
7. 不把单一 horizon、单一年份或单次随机结果包装成“最终策略”。

如果发现未来函数、字段可见时间不明、输出覆盖率过低、数值不稳定或交易约束缺失，应标记为 `invalid_experiment` 或 `rejected`，而不是继续优化参数。

---

## 三、标准工作流

```text
Step 1: 明确研究目标、市场、universe、字段和 horizon
Step 2: 选择 CogAlpha agent 角色
Step 3: 用 diversified guidance 生成多个候选表达
Step 4: 转成安全候选：优先公式 DSL / FormulaSpec 风格，不直接写任意 Python
Step 5: 执行 quality gate：字段、算子、PIT、数值、覆盖率、经济逻辑
Step 6: 调用 factor-evaluation 做多周期 fitness
Step 7: 调用 factor-research-archive 记录好因子、坏因子和失败原因
Step 8: 汇总 qualified / elite / rejected
Step 9: 基于失败原因设计下一轮 mutation / crossover
```

当前工程入口：

```python
from vortex.research.cogalpha import run_cogalpha_generation
from vortex.research.cogalpha import run_cogalpha_research_cycle
```

推荐优先使用 `run_cogalpha_research_cycle()`：它在 `run_cogalpha_generation()` 之上增加研究方向、recipe 选择、artifact 输出、parent pool 和下一轮 mutation/crossover 队列。

当前实现状态：

| 项 | 状态 |
|---|---|
| 21 个 CogAlpha agent | 已登记 |
| 21 个 executable recipe | 已实现 |
| `semantic_status=faithful_proxy` | `AgentMarketCycle`、`AgentCrashPredictor`、`AgentFractal`、`AgentHerding` |
| `semantic_status=mutation_proxy` | `AgentCreative` |
| 完整论文 LLM agent 复现 | 未完成；不要这样宣称 |

每轮输出必须包含：

1. 本轮目标和约束。
2. 使用的 agent 角色。
3. 候选因子列表与字段映射。
4. quality gate 通过/失败原因。
5. fitness 指标和准入判断。
6. lineage：父代、变异、交叉、prompt/guidance 来源。
7. 下一轮 evolution 建议。

---

## 四、七层 Agent Catalog

先把 21 个 agent 作为研究角色卡使用，不立即拆成 21 个独立 skill。

| 层级 | Agent | 研究重点 | Vortex 初始落点 |
|---|---|---|---|
| Market Structure & Cycle | AgentMarketCycle | 长期趋势、周期转换、市场阶段 | 市场状态、择时过滤 |
| Market Structure & Cycle | AgentVolatilityRegime | 波动状态切换 | 市场风险门控 |
| Extreme Risk & Fragility | AgentTailRisk | 下行敏感性、尾部暴露 | 防守因子、风险过滤 |
| Extreme Risk & Fragility | AgentCrashPredictor | 崩盘前兆、流动性枯竭 | crash guard、空仓条件 |
| Price-Volume Dynamics | AgentLiquidity | 价格冲击、换手、交易摩擦 | 流动性 alpha、容量约束 |
| Price-Volume Dynamics | AgentOrderImbalance | 单边参与压力 | 资金流、价量不平衡 |
| Price-Volume Dynamics | AgentPriceVolumeCoherence | 价量同步与背离 | 量价背离、拥挤反转 |
| Price-Volume Dynamics | AgentVolumeStructure | 成交活动形状和节奏 | 峰岭谷、成交集中度 |
| Price-Volatility Behavior | AgentDailyTrend | 多日趋势延续 | 动量 |
| Price-Volatility Behavior | AgentReversal | 过度反应和均值回复 | 短反、中反 |
| Price-Volatility Behavior | AgentRangeVol | 区间压缩与扩张 | range、ATR、低波 |
| Price-Volatility Behavior | AgentLagResponse | 收益、成交量、波动滞后反馈 | lagged response |
| Price-Volatility Behavior | AgentVolAsymmetry | 上下行波动不对称 | downside risk |
| Multi-Scale Complexity | AgentDrawdown | 回撤深度、持续时间、恢复形态 | drawdown recovery |
| Multi-Scale Complexity | AgentFractal | 多尺度粗糙度、长记忆 | 多 horizon 稳定性 |
| Stability & Regime-Gating | AgentRegimeGating | 状态激活/关闭信号 | 因子门控 |
| Stability & Regime-Gating | AgentStability | 时间一致性和平滑性 | 稳定性过滤 |
| Geometric & Fusion | AgentComposite | 多因子协同和正交性 | 组合 alpha |
| Geometric & Fusion | AgentCreative | 非线性变换、软门控 | 创造性变体 |
| Geometric & Fusion | AgentBarShape | K 线实体、影线和对称性 | OHLC shape |
| Geometric & Fusion | AgentHerding | 群体拥挤和方向一致性 | 拥挤、羊群效应 |

---

## 五、Diversified Guidance 模板

同一假设至少从五种 guidance 中选择 2-3 种：

| Guidance | 用途 | 输出要求 |
|---|---|---|
| Light | 保持原意，轻微清晰化 | 不改变核心字段和经济含义 |
| Moderate | 加入轻微研究丰富度 | 允许调整窗口、归一化方式 |
| Creative | 引入更深金融解释 | 必须说明为什么不是乱变换 |
| Divergent | 扩展到相关但不同视角 | 必须保留同一研究主题 |
| Concrete | 落到可计算表达 | 给出字段、算子、窗口、方向 |

Concrete 输出应优先使用 Vortex 已有安全算子：`cs_rank`、`cs_zscore`、`delay`、`delta`、`ts_rank`、`ts_mean`、`ts_std`、`correlation`、`decay_linear`、`scale`、`neutralize_by_group`。

---

## 六、Quality Gate

候选必须先过 quality gate，再进入 fitness。

最低检查：

1. 字段白名单：只使用本轮声明的可见字段。
2. 算子白名单：优先安全公式算子，不使用任意 Python。
3. 时间安全：禁止未来收益、未来价格、负向 shift、错误 rolling 对齐。
4. 数值稳定：检查 NaN、inf、除零、log 非正、overflow/underflow。
5. 覆盖率：低覆盖率要降级或拒绝。
6. distinct ratio：有效值太少的截面不能进入评测。
7. 经济逻辑：必须解释为什么这个表达可能有 alpha，而不只是数学拼接。
8. 相关性：与已入选因子高度重复时，除非有新增解释，否则拒绝。

---

## 七、Fitness 与状态

Fitness 不等于策略收益。标准指标：

1. IC / RankIC。
2. ICIR / RankICIR。
3. positive_rate。
4. coverage。
5. long-short return。
6. group monotonicity。
7. factor correlation。
8. 可选 mutual information。

状态建议：

| 状态 | 含义 | 下一步 |
|---|---|---|
| `generated` | 已生成但未检查 | quality gate |
| `invalid` | 未来函数/数值/字段错误 | 丢弃或重写 |
| `rejected` | 通过质量检查但 fitness 不足 | 归档失败原因 |
| `qualified` | 达到基础准入 | 进入下一代 parent pool |
| `elite` | 同代高分且过绝对阈值 | 进入候选池，但仍需 hardening |
| `research_lead` | 有研究价值 | 因子档案 |
| `candidate` | 值得拥有 | walk-forward、成本、容量、可交易性 |

---

## 八、Mutation / Crossover 规则

允许的 mutation：

1. 窗口变化：5/10/20/60/120，但要说明 horizon 假设。
2. 标准化变化：raw → rank → zscore → neutralized。
3. 门控变化：加入低波、流动性、市场状态或行业门控。
4. 有界变换：clip、signed_power、scale，避免极端值主导。
5. 方向翻转：只有当评测显示方向稳定相反时允许。

允许的 crossover：

1. 同主题组合：如低波 + 反转。
2. 风险门控组合：如动量 + crash guard。
3. 量价组合：如成交拥挤 + 价格反转。
4. 事件/基本面 + 交易可行性过滤。

每个 child 必须记录父代、变异/交叉类型、保留的经济假设和新增风险。

---

## 九、与其他 skills 的协作

| skill | 协作方式 |
|---|---|
| `factor-mining-research` | 默认上游和总编排：读取资料/档案、选择方向，然后调用 CogAlpha generation |
| `factor-evaluation` | 执行多周期 IC、多空和准入判断 |
| `factor-research-archive` | 记录好因子、坏因子、失败原因和 artifact |
| `goal-achievement-review` | 判断 experiment / candidate / promoted |
| `strategy-development-experience` | 防过拟合、walk-forward、容量和可交易性 |
| `strategy-review-officer` | 防止把因子误包装成实盘策略 |
| `tushare` | 明确字段来源、权限、PIT 和可见时间 |

---

## 十、输出模板

```text
阶段：generated / invalid / rejected / qualified / elite / research_lead / candidate
研究目标：
选择的 CogAlpha agents：
可用字段与可见时间：
候选因子：
Quality Gate：
Fitness：
Lineage：
归档位置：
是否进入下一轮：
下一轮 mutation/crossover：
禁止进入策略的原因或补充 hardening 项：
```
