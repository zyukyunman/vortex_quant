---
tags: [vortex, vortex/research-domain, vortex/factor-methodology]
aliases: [CogAlpha与Vortex落地讨论, CogAlpha与Vortex因子研究映射, CogAlpha Vortex 映射]
created: 2026-05-02
updated: 2026-05-02
status: design_note
---

# CogAlpha 与 Vortex 落地讨论

关联：[[CogAlpha学习资料]]、[[CogAlpha完整中文译文]]、[[CogAlpha课堂讲义]]、[[CogAlpha因子研究落地路线]]、[[CogAlpha Agent体系设计]]、[[CogAlpha Quality Gate与Fitness规范]]、[[外部Alpha资料线索]]、[[因子研究与评测全流程说明]]、[[研究协作与产物治理]]

## 一句话结论

CogAlpha 不要求 Vortex 推翻重做，但它值得被升级为 Vortex 因子研究的一条正式方法论和工程路线。推荐路线不是“照搬论文的 21 agent + H100 多代进化”，而是：**先生成 `cogalpha-factor-mining` skill 和 agent catalog，再逐步落地 schema、quality gate、fitness、lineage、workspace runner，最后才进入核心 Research 产品化。**

## 当前判断：可以渐进式大改

上一阶段我们把 CogAlpha 先定位为论文学习资料，这是对的，因为当时还没有共同语言。现在用户已经确认论文思想与 Vortex 很接近，而且希望真正落地，因此判断升级为：

| 选项 | 新判断 | 原因 |
|---|---|---|
| 只作为论文学习 | 不够 | CogAlpha 的价值在闭环，不落到 skill/agent 会浪费 |
| 直接完整复刻论文 | 不建议 | 多 agent、大模型、多代 evolution、Qlib/H100 环境过重 |
| 生成 CogAlpha skill | 立即做 | 风险低，能让 AI 研究员学会统一工作法 |
| 建 agent catalog 和质量规范 | 立即做 | 后续代码前必须先统一角色、字段、门禁和 fitness |
| workspace 小 CogAlpha runner | 下一阶段 | 可验证闭环，不污染核心仓库 |
| 核心 `vortex/research/cogalpha/` 模块 | 成熟后做 | 需要 schema、测试、artifact 和安全边界 |

也就是说：**可以大改，但大改必须从契约和门禁开始，而不是从 generator 开始。**

## CogAlpha 真正补上的能力

Vortex 现在已经有不少因子研究底座，但 CogAlpha 补的是“研究组织方式”：

```text
外部资料 / 研究假设
  -> agent 视角拆解
  -> diversified guidance 生成多个表达
  -> 代码化 alpha
  -> quality gate
  -> fitness evaluation
  -> archive 成功与失败
  -> mutation / crossover 下一轮
```

这比“测一个因子，写一个报告，结束”更强，因为它让失败因子也变成下一轮研究材料，让好因子继续演化，而不是直接上策略。

## Vortex 已有底座

| CogAlpha 需要的能力 | Vortex 当前对应 | 现状 |
|---|---|---|
| 代码化 alpha 表达 | `vortex/research/factor_ops.py` | 已有 Alpha101 风格基础算子 |
| 公式候选 registry | `vortex/research/alpha101_registry.py` | 已有大量 `FormulaSpec` 候选 |
| 多周期评测 | `vortex/research/evaluation.py` | 已有 forward return、RankIC、多空、候选准入 |
| 报告 artifact | `vortex/research/reports.py` | 已有 `research_report.v1`、HTML tear sheet、`signal_snapshot.v1` |
| 因子档案 | `docs/因子研究/` | 已有因子实验总表、因子档案规范、评价框架 |
| 研究治理 | [[研究协作与产物治理]] | 已有 Research Spike / Candidate Hardening / Product Promotion |
| 论文学习资料 | [[CogAlpha学习资料]] | 已有完整中文译文、课堂讲义和资料包 |

这说明 CogAlpha 不是替代 Vortex，而是把 Research 轨道上方的“AI 研究员层”补齐。

## 当前缺口

| 缺口 | 说明 | 推荐处理 |
|---|---|---|
| CogAlpha skill | AI 研究员还没有统一工作法 | 已规划新增 `cogalpha-factor-mining` |
| agent catalog | 七层 21 agents 还未映射到 Vortex 因子族 | 详见 [[CogAlpha Agent体系设计]] |
| quality gate | 缺系统化字段、算子、PIT、数值和经济逻辑检查 | 详见 [[CogAlpha Quality Gate与Fitness规范]] |
| fitness 扩展 | 当前以 RankIC、多空为主，缺 IC/RankICIR/MI/单调性 | 后续 Phase 2 |
| lineage | 缺父代、变异、交叉、prompt hash、失败原因 | 后续 schema |
| evolution loop | 无 parent pool、elite pool、mutation/crossover 调度 | 先 workspace，后核心模块 |

## 分层落地映射

| CogAlpha 模块 | Vortex 语言 | 落点 |
|---|---|---|
| Seven-Level Agent Hierarchy | 因子研究员角色卡 | `.github/skills/cogalpha-factor-mining` + [[CogAlpha Agent体系设计]] |
| Diversified Guidance | 同一假设的多种可计算表达 | skill 模板，未来 `guidance.py` |
| Generated Alpha Code | 受限公式 / `FormulaSpec` / 安全候选 | 先 workspace，后 `schema.py` |
| Multi-Agent Quality Checker | fail-closed 质量门禁 | [[CogAlpha Quality Gate与Fitness规范]]，未来 `quality.py` |
| Fitness Evaluation | 多 horizon IC/RankIC/ICIR/MI/多空 | 复用并扩展 `evaluation.py` |
| Qualified Alpha | 通过基础准入的 parent | 因子档案和下一代 parent pool |
| Elite Alpha | 同代高分研究候选 | candidate hardening 输入，不直接进策略 |
| Adaptive Generation | 失败原因反馈下一轮 prompt | 因子档案 + lineage |
| Thinking Evolution | mutation / crossover | workspace runner，未来 `evolution.py` |

## GitHub 相近实现调研结论

当前仍未找到 CogAlpha 官方完整实现。可借鉴但不能照搬的项目：

| 仓库 | 相关性 | 可借鉴点 | Vortex 判断 |
|---|---|---|---|
| [JacobDu/agentic-alpha](https://github.com/JacobDu/agentic-alpha) | 高 | Research → Retrieve → Generate → Evaluate → Validate → Distill；因子库记录、稳定性和相关性 | 最值得参考 Validate 与因子创意管理 |
| [sw0843/Auto-Alpha-Finding](https://github.com/sw0843/Auto-Alpha-Finding) | 中高 | GPlearn 遗传规划、残差正交化、相关性过滤、因子池归档 | 可参考 evolution 和因子池治理 |
| [Parsnip77/Multi-factor-Model-for-Stock-Selection](https://github.com/Parsnip77/Multi-factor-Model-for-Stock-Selection) | 中 | A 股 Tushare pipeline、清洗、中性化、IC、分层、LightGBM/SHAP | 可参考 A 股预处理，但数据口径回到 Vortex |
| [laox1ao/Alpha101-WorldQuant](https://github.com/laox1ao/Alpha101-WorldQuant) | 中 | Alpha101 语法 | 只做公式结构参考 |

这些项目共同说明：自动挖因子一定要有 Validate、相关性过滤、因子池和失败归档，否则会产生大量重复或伪有效公式。

## 推荐实施顺序

详见 [[CogAlpha因子研究落地路线]]。简化版如下：

1. **Phase 0**：skill + agent catalog + quality/fitness 文档。
2. **Phase 1**：`schema.py` 和 `quality.py`，只检查受限候选。
3. **Phase 2**：fitness 扩展，支持一代候选排序和分级。
4. **Phase 3**：workspace 小 CogAlpha runner。
5. **Phase 4**：agentic generation / mutation / crossover。
6. **Phase 5**：Research 域产品化。

## 不应短期做的事

1. 直接运行任意 LLM 生成 Python。
2. 直接接实盘或 shadow。
3. 直接把 elite alpha 加入默认策略。
4. 一次性复刻 21 个 agent 的全自动系统。
5. 大规模并发调用外部模型。
6. 把论文结果当作 Vortex 策略证据。
7. 没有 quality gate 就开始追求 generation 数量。

## 当前结论

CogAlpha 对 Vortex 的正确定位是：

```text
AI 因子研究操作系统
  不是单个因子
  不是单个策略
  不是黑盒自动交易
```

第一步应落地 `cogalpha-factor-mining` skill 和三类规范文档。这样既能让 AI 研究员真正学会论文框架，也能为后续大改保留清晰边界：**先可审计，再自动化；先 workspace，再核心；先因子候选，再策略。**
