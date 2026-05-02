---
tags: [vortex, vortex/research-domain, vortex/factor-methodology]
aliases: [CogAlpha因子研究落地路线, CogAlpha落地路线, Vortex CogAlpha Roadmap]
created: 2026-05-02
updated: 2026-05-02
status: roadmap
---

# CogAlpha 因子研究落地路线

关联：[[CogAlpha学习资料]]、[[CogAlpha Agent体系设计]]、[[CogAlpha Quality Gate与Fitness规范]]、[[CogAlpha与Vortex落地讨论]]、[[研究协作与产物治理]]、[[因子研究与评测全流程说明]]

## 一句话结论

CogAlpha 值得成为 Vortex 的一条正式研究路线，但落地顺序必须是：**skill 和 agent catalog 先行，quality gate 先于 generator，workspace 小实验先于核心产品化，elite alpha 先进入因子档案而不是策略。**

## 总体分层

```text
Skill 层
  -> 文档与规范层
  -> workspace 小 CogAlpha runner
  -> vortex/research/cogalpha 核心模块
  -> Research signal / Strategy candidate hardening
```

这条路线允许“大改”，但把大改拆成可审计、可回滚、可验证的阶段。

## Phase 0：Skill 与 Agent 体系

目标：让 Vortex 的 AI 研究员学会 CogAlpha 工作法。

改动：

- 新增 `.github/skills/cogalpha-factor-mining/SKILL.md`。
- 新增 [[CogAlpha Agent体系设计]]。
- 新增 [[CogAlpha Quality Gate与Fitness规范]]。
- 更新 [[CogAlpha与Vortex落地讨论]] 和 MOC。

不做：

- 不写核心 Python。
- 不接 LLM provider。
- 不运行任意生成代码。
- 不把 CogAlpha 候选接入策略。

验收：

1. 能用 skill 指导一次 CogAlpha 式因子研究。
2. 21 个 agent 的职责、字段和风险清楚。
3. quality gate 和 fitness 的概念契约清楚。

## Phase 1：Schema 与 Quality Gate 内核

目标：把候选 alpha 从聊天文本变成可检查对象。

建议模块：

```text
vortex/research/cogalpha/
  __init__.py
  schema.py
  quality.py
```

核心对象：

- `AlphaCandidate`
- `AgentSpec`
- `GuidanceSpec`
- `QualityCheckResult`
- `LineageRecord`

Quality Gate 先做规则检查：

1. schema 必填字段。
2. 字段白名单。
3. 算子白名单。
4. 时间安全。
5. NaN / inf / distinct / coverage。
6. 经济逻辑元数据。

不做：

- 不执行任意 LLM Python。
- 不允许网络、文件、环境变量访问。
- 不做复杂自动修复。

验收：

- 单元测试覆盖通过和失败候选。
- 失败原因结构化。
- 无 silent fallback。

## Phase 2：Fitness 扩展

目标：从“能评测一个因子”升级为“能给一代候选排序和分级”。

建议模块：

```text
vortex/research/cogalpha/fitness.py
```

指标：

- IC。
- RankIC。
- ICIR。
- RankICIR。
- positive_rate。
- coverage。
- distinct ratio。
- long-short。
- group monotonicity。
- factor correlation。
- 可选 MI。

与现有代码关系：

- 复用 `vortex/research/evaluation.py`。
- 不破坏 `FactorEvaluationResult`。
- 新增 CogAlpha 专用 `FitnessResult` 可以包装基础结果。

验收：

1. 一批候选可输出 `qualified`、`elite`、`rejected`。
2. rejected 原因可回写下一轮 prompt。
3. 报告 JSON 可作为事实来源。

## Phase 3：Workspace 小 CogAlpha Runner

目标：验证闭环，不污染核心仓库。

建议 workspace 结构：

```text
workspace/cogalpha/
  raw_candidates.jsonl
  quality_review.jsonl
  fitness_report.json
  elite_pool.jsonl
  rejected_pool.jsonl
  generation_summary.md
```

输入：

- 人工或 agent 生成的候选 JSONL。
- 已准备好的宽表数据。
- 评测 horizon 和 universe。

输出：

- 通过/失败候选。
- fitness 排名。
- 下一轮 mutation/crossover 建议。
- 可归档的失败原因。

不做：

- 不进入默认 CLI。
- 不作为策略入口。
- 不保存大 CSV/HTML 到核心仓库。

验收：

1. 能跑 10-30 个候选。
2. 能产出 elite/rejected。
3. 能把失败原因变成下一轮研究输入。

## Phase 4：Agentic Generation 与 Evolution

目标：让 CogAlpha 从评测批处理升级为半自动进化系统。

建议模块：

```text
vortex/research/cogalpha/
  agent_catalog.py
  guidance.py
  evolution.py
  lineage.py
```

能力：

- 选择 agent。
- 生成 light / moderate / creative / divergent / concrete guidance。
- 生成候选公式。
- 对 qualified/elite 做 mutation。
- 对低相关候选做 crossover。
- 记录 generation lineage。

仍然坚持：

- 优先公式 DSL 或 `FormulaSpec` 风格。
- 不默认执行任意 Python。
- 每个 child 必须有父代和变异说明。

验收：

1. 每个 child 可追溯。
2. 同代阈值可配置。
3. 成功和失败都进入反馈上下文。

## Phase 5：Research 域产品化

目标：把稳定的 CogAlpha 能力接入 Research workflow。

可能入口：

- `vortex research cogalpha generate`
- `vortex research cogalpha evaluate`
- `vortex research cogalpha inspect-generation`
- `vortex research cogalpha promote-factor`

进入该阶段的前置条件：

1. workspace runner 已证明流程有用。
2. quality gate 测试充分。
3. fitness schema 稳定。
4. artifact JSON 是事实来源。
5. 用户确认要把 CogAlpha 从研究工具升级为核心能力。

## 与策略层的边界

```text
CogAlpha elite alpha
  -> 因子档案
  -> 多周期评测
  -> 正交性和组合贡献
  -> walk-forward
  -> 成本、容量、可交易性
  -> signal snapshot
  -> Strategy consumer
```

禁止：

- 从 CogAlpha elite 直接到策略。
- 从 LLM 代码直接到 signal snapshot。
- 用论文报告指标替代本地评测。
- 用小账户结果反向调优因子。

## 为什么这条路线适合 Vortex

Vortex 已经具备：

- 公式算子和 Alpha101 风格 registry。
- RankIC 和多空评测。
- JSON/HTML artifact writer。
- 因子档案和实验总表。
- Research Spike / Candidate Hardening / Product Promotion 治理。

所以 CogAlpha 不需要推翻 Vortex，而是把 Vortex 的研究流程升级成更强的闭环：

```text
外部资料和研究假设
  -> agent 化扩展
  -> 代码化候选
  -> 质量门禁
  -> 多周期 fitness
  -> 档案和失败反馈
  -> 下一轮进化
```

## 当前推荐动作

当前只执行 Phase 0。Phase 1 之后再决定是否进入核心代码。

原因：

1. 先统一研究语言，避免后面写出随意 prompt。
2. 先建立 quality gate 契约，避免 LLM 生成风险。
3. 先让 skill 帮助研究员工作，而不是立刻创建黑盒自动系统。
4. 大改可以做，但应该从 schema 和门禁开始，而不是从 generator 开始。
