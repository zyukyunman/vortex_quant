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

当前实现：

```text
vortex/research/cogalpha/
  __init__.py
  schema.py
  agent_catalog.py
  quality.py
```

核心对象：

- `AlphaCandidate`
- `AgentSpec`
- `QualityCheckResult`
- `LineageRecord`
- `QualityIssue`

Quality Gate 已做规则检查：

1. schema 必填字段。
2. 字段白名单。
3. 算子白名单。
4. 时间安全。
5. NaN / inf / distinct / coverage。
6. agent 是否属于 21 个 CogAlpha agent。

仍不做：

- 不执行任意 LLM Python。
- 不允许网络、文件、环境变量访问。
- 不做复杂自动修复。
- 不自动解析公式生成因子值；候选表达仍是可审计元数据。

验收：

- `tests/test_research_cogalpha_schema.py` 与 `tests/test_research_cogalpha_quality.py` 覆盖通过和失败候选。
- 失败原因以 `QualityIssue.code` 结构化输出。
- 默认 fail-closed：危险 token、未来函数、未声明字段、低覆盖率、inf、低 distinct ratio 均拒绝。

## Phase 2：Fitness 扩展

目标：从“能评测一个因子”升级为“能给一代候选排序和分级”。

当前实现模块：

```text
vortex/research/cogalpha/fitness.py
vortex/research/cogalpha/reports.py
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
- MI 暂未实现，保留到后续 Phase 2 增强。

与现有代码关系：

- 复用 `vortex/research/evaluation.py`。
- 不破坏 `FactorEvaluationResult`。
- 新增 CogAlpha 专用 `FitnessStats` 与 `CogAlphaEvaluationResult` 包装基础结果。

验收：

1. 一批候选可输出 `qualified`、`elite`、`rejected`。
2. rejected 原因可回写下一轮 prompt。
3. 报告 JSON 使用 `vortex.cogalpha_generation_report.v1`，可作为事实来源。
4. 现有 `vortex.research.evaluation` 行为不被破坏。

当前边界：

- fitness 输入仍是已计算好的 date × symbol 因子宽表。
- `rank_cogalpha_candidates` 只负责一代候选排序、elite 标记和同代相关性过滤。
- 还没有 prompt 生成、mutation/crossover 或 LLM provider。

## Phase 2.5：Research 集成与可执行 Agent Recipe

目标：解决 CogAlpha 旁路化问题，让 agent 真正接回现有 Research 主流程。

当前实现模块：

```text
vortex/research/cogalpha/recipes.py
vortex/research/cogalpha/adapters.py
vortex/research/cogalpha/workflow.py
```

核心能力：

1. `CogAlphaAgentRecipe`：把 agent 从角色卡升级为可执行研究单元。
2. `formula_spec_from_recipe()`：recipe 生成安全 `FormulaSpec`。
3. `candidate_from_recipe()`：recipe 生成 `AlphaCandidate`。
4. `candidate_from_formula_spec()`：现有 `FormulaSpec` 反向包装成 CogAlpha candidate。
5. `cogalpha_candidates_from_registered_specs()`：把现有 `registered_specs()` 纳入 CogAlpha 质量/适应度治理。
6. `run_cogalpha_generation()`：串起 recipe、`compute_formula`、quality gate、fitness ranking 和 generation JSON。

Phase 2.6 后当前可执行 baseline proxy recipe 已覆盖全部 21 个 agent catalog entry：

| Agent | recipe |
|---|---|
| `AgentMarketCycle` | `market_cycle_relative_trend_60d` |
| `AgentVolatilityRegime` | `volatility_regime_compression_20d` |
| `AgentTailRisk` | `tail_risk_downside_vol_20d` |
| `AgentCrashPredictor` | `crash_fragility_high_range_low_liquidity_20d` |
| `AgentLiquidity` | `liquidity_range_impact` |
| `AgentOrderImbalance` | `order_imbalance_close_strength_5d` |
| `AgentPriceVolumeCoherence` | `price_volume_coherence_20d` |
| `AgentVolumeStructure` | `volume_structure_surge_decay_20d` |
| `AgentDailyTrend` | `daily_trend_20d` |
| `AgentReversal` | `short_reversal_5d` |
| `AgentRangeVol` | `range_vol_20d` |
| `AgentLagResponse` | `lag_response_volume_leads_price_20d` |
| `AgentVolAsymmetry` | `vol_asymmetry_downside_upside_20d` |
| `AgentDrawdown` | `drawdown_recovery_position_60d` |
| `AgentFractal` | `fractal_multiscale_consistency_20_60d` |
| `AgentRegimeGating` | `regime_gated_trend_lowvol_60d` |
| `AgentStability` | `stability_signal_smoothness_20d` |
| `AgentComposite` | `composite_trend_reversal_liquidity` |
| `AgentCreative` | `creative_soft_rank_range_liquidity` |
| `AgentBarShape` | `bar_shape_close_location_5d` |
| `AgentHerding` | `herding_amount_crowding_reversal_20d` |

重要边界：

- `planned_recipes()` 已为空；后续新增 agent 时必须配套 builder、quality/fitness 测试和文档。
- recipe builder 仍是 Vortex 内置安全代码，不执行任意 LLM Python。
- CogAlpha 仍不替代 `alpha101_registry.py`，而是给现有公式体系增加认知归因、质量门禁和 generation-level 报告。

## Phase 2.6：全量可执行 Agent 与研究演示闭环

目标：回答“CogAlpha 在 Vortex 里最小能做什么”，用本地可复现代码跑通 21 个 baseline proxy recipe 的生成、计算、门禁、适应度排序和 artifact 沉淀。这里的 21 个 recipe 是论文 agent 的可执行代理，不是论文 multi-agent LLM 推理系统的完整复现。

入口：

```python
from vortex.research.cogalpha import build_demo_daily_inputs, run_cogalpha_demo, run_cogalpha_generation

run_cogalpha_demo("workspace/cogalpha/latest")
```

产物：

```text
workspace/cogalpha/latest/generation_report.json
workspace/cogalpha/latest/generation_summary.json
```

`generation_summary.json` 包含：

1. `recipe_count`：本轮实际执行 recipe 数。
2. `decision_counts`：`elite` / `qualified` / `rejected` / `invalid` 数量。
3. `semantic_status_counts`：`proxy` / `faithful_proxy` / `mutation_proxy` 的数量。
4. `top_candidates`：按 fitness score 排序的候选。
5. `agent_results`：每个 agent 的 alpha_id、decision、score、fitness、rejection reasons、semantic_status、semantic_notes 和 parent_templates。

这个 demo 使用 deterministic synthetic OHLCV panel，只证明工程闭环和 schema 稳定性，不作为真实 A 股 alpha 结论。迁移到真实数据时，只需把 `build_demo_daily_inputs()` 换成真实 `DailyFactorInputs`，继续调用 `run_cogalpha_generation()`。后续如果要“更像论文”，需要把 `semantic_status=proxy` 的 recipe 逐步升级为更忠实的 agent implementation，并接入 LLM guidance / mutation / crossover。

## Phase 2.7：Agent 语义硬化

目标：根据 review 结论，把最弱的 5 个 proxy 从“能跑”推进到“更贴近论文语义”：

| Agent | 语义硬化结果 |
|---|---|
| `AgentMarketCycle` | 从简单 60 日相对趋势升级为市场趋势、上涨宽度和市场波动状态共同构造的 PIT-safe regime gate |
| `AgentCrashPredictor` | 从普通 range/liquidity 风险分升级为波动短长比、波动扩张、下行 range、市场同步和流动性枯竭组合的 fragility risk filter |
| `AgentFractal` | 从 20/60 日收益一致性升级为 path efficiency、variance-ratio proxy 和多 horizon gap 的粗糙度代理 |
| `AgentHerding` | 从放量上涨反转升级为截面方向共识、个股与群体方向对齐和成交额拥挤的 herding pressure |
| `AgentCreative` | 从普通非线性因子改成 deterministic mutation proxy，记录 `parent_templates`：`daily_trend_20d`、`short_reversal_5d`、`liquidity_range_impact` |

边界：

- 这些仍是 OHLCV 内代理，不是完整论文 LLM agent。
- `faithful_proxy` 只表示“比 baseline proxy 更贴近论文语义”，不表示已完成论文复现。
- 真实研究结论必须用真实 A 股 `DailyFactorInputs` 重新评估，不能引用 synthetic demo 的收益指标。

## Phase 3：Workspace 小 CogAlpha Runner

目标：让 CogAlpha 从 demo 进入默认因子研究编排层；`factor-mining-research` 负责读取资料/档案并选择方向，CogAlpha 负责 agent/recipe、quality、fitness、lineage 和下一轮 evolution。

Phase 3 的第一条默认方向：

```text
cogalpha_101_price_volume_defensive_evolution
```

它服务于 [[101因子全库研究路线]] 和 [[动量与101量价因子]]：围绕低波、反转、成交拥挤、静默流动性、路径质量和风险门控，自动生成 parent pool 和 mutation/crossover 队列。

建议 workspace 结构：

```text
workspace/cogalpha/<research_direction>/latest/
  generation_report.json
  generation_summary.json
  research_cycle.json
```

输入：

- `CogAlphaResearchDirection`。
- 已准备好的 `DailyFactorInputs` 宽表数据。
- 评测 horizon 和 universe。

输出：

- 通过/失败候选。
- fitness 排名。
- parent pool。
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
