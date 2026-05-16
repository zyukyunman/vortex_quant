---
name: cogalpha-factor-council
description: "CogAlpha 因子委员会流程。适用场景： 需要围绕研究方向组织 21 个 CogAlpha 视角、生成候选、变异/交叉、争论并给出下一轮评测队列和 handoff。"
argument-hint: "提供研究方向、可用字段、预测周期、父代候选、失败原因、候选预算和目标策略上下文"
tags: [vortex, vortex/skill, vortex/research-domain, cogalpha, vortex/ai-quant-os]
obsidian_links:
  - "[[CogAlpha Agent体系设计]]"
  - "[[Agent协作规格]]"
  - "[[因子研究与评测全流程说明]]"
---

# CogAlpha 因子委员会

目标是把研究方向变成一代可审查候选，而不是自由生成公式。21 个 CogAlpha agent 是研究视角池，不是公司岗位；公司岗位负责提出问题、审查证据、决定路由和归档。

第一轮允许 21 个视角全部上台，但每个视角只输出一张 `AgentOpinionCard`：支持、反对、请求补证据、弃权或建议接力。深挖阶段才从 21 个视角中收敛到 3-8 个重点视角，避免无差别暴力搜索和过拟合。

## 流程

1. 研究总监定义方向和停止条件。
2. 归档审计员提供已失败路径、已有效父代和禁止重复方向。
3. CogAlpha PI 组织 21 个视角先做一轮轻量评议。
4. CogAlpha PI 汇总 21 张 opinion card，选择 3-8 个视角进入候选生成、变异或交叉，并说明为什么收敛。
5. 生成候选、mutation 或 crossover，并记录父代。
6. 因子质量审查员初筛。
7. 输出评测队列、拒绝原因和证据评审 handoff。

## 21 视角 Opinion Card

```text
agent:
relevance: high / medium / low / abstain
action: support / challenge / request_evidence / propose_mutation / abstain
view:
candidate_ideas:
risks:
handoff:
```

全 21 视角评议不是为了让 21 个 Agent 都生成公式，而是为了保证研究对象被不同市场行为、风险、成交、稳定性和组合角度审查过。

## 21 个视角的调度方式

| 场景 | 优先视角 |
|---|---|
| 市场状态、择时、风险开关 | AgentMarketCycle, AgentVolatilityRegime, AgentRegimeGating |
| 防回撤、尾部风险、坏持仓修理 | AgentTailRisk, AgentCrashPredictor, AgentDrawdown, AgentVolAsymmetry |
| 成交拥挤、流动性、容量 | AgentLiquidity, AgentVolumeStructure, AgentHerding |
| 量价背离、资金压力 | AgentOrderImbalance, AgentPriceVolumeCoherence, AgentLagResponse |
| 动量、反转、区间波动 | AgentDailyTrend, AgentReversal, AgentRangeVol, AgentBarShape |
| 多周期稳定性和组合腿 | AgentFractal, AgentStability, AgentComposite |
| 变形、软门控、非线性探索 | AgentCreative, AgentComposite, AgentRegimeGating |

## 下一步 handoff

| 委员会输出 | 下一位 Agent |
|---|---|
| 候选字段或表达式有硬伤 | 因子质量审查员 |
| 候选已可评测 | factor-evaluation |
| 评测完成但结论不清 | 因子研究证据评审员 |
| 出现 qualified/elite parent | 因子研究证据评审员，再由其决定是否给策略晋升专员或风险官 |
| 全部 rejected | 归档审计员 + CogAlpha PI，决定 close、regenerate 或缩小视角 |

## 输出

```text
研究方向：
选用视角：
未选视角及原因：
候选队列：
拒绝队列：
需要补证据：
证据评审 handoff：
下一轮演化：
```

## 规则

1. 21 个视角服务研究，不是 21 个并列决策者；第一轮全员评议，第二轮重点深挖。
2. 候选必须有字段、预测周期、版本血统和风险说明。
3. 不直接进入策略。
4. 每个视角必须服务同一张假设卡；不能为了凑数量转移研究问题。
5. 进入下一轮前必须保留 rejected_pool 和 next_generation_queue。
