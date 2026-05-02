---
tags: [vortex, vortex/moc, vortex/research-domain, vortex/factor-methodology]
aliases: [CogAlpha学习资料, CogAlpha 论文资料包]
created: 2026-05-02
updated: 2026-05-02
---

# CogAlpha 学习资料

> 本资料包用于一起学习并落地 CogAlpha 论文：先完整理解论文，再把“代码化 alpha + 多 agent 质量检查 + fitness + 进化反馈”转成 Vortex 可审计的因子研究工作流。

关联：[[论文学习资料]]、[[CogAlpha完整中文译文]]、[[CogAlpha课堂讲义]]、[[CogAlpha因子研究落地路线]]、[[CogAlpha Agent体系设计]]、[[CogAlpha Quality Gate与Fitness规范]]、[[CogAlpha与Vortex落地讨论]]、[[外部Alpha资料线索]]

---

## 原始资料

| 资料 | 位置 |
|---|---|
| arXiv 摘要 | <https://arxiv.org/abs/2511.18850> |
| arXiv HTML | <https://arxiv.org/html/2511.18850v3> |
| 本地 PDF | `docs/论文/CogAlpha/Cognitive_Alpha_Mining_LLM-Driven_Code-Based_Evolution.pdf` |

---

## 学习顺序

1. 先读 [[CogAlpha课堂讲义]]：用老师讲解方式理解论文为什么重要。
2. 再读 [[CogAlpha完整中文译文]]：按论文原始结构完整过一遍。
3. 再读 [[CogAlpha与Vortex落地讨论]]：理解它为什么适合 Vortex 渐进式落地。
4. 最后读 [[CogAlpha因子研究落地路线]]、[[CogAlpha Agent体系设计]] 和 [[CogAlpha Quality Gate与Fitness规范]]：进入 skill/agent/quality/fitness 的工程化讨论。

---

## 落地资料

| 文档 | 说明 |
|---|---|
| [[CogAlpha因子研究落地路线]] | Phase 0-5 的渐进式大改路线：skill、文档、workspace runner、核心模块、Research 产品化 |
| [[CogAlpha Agent体系设计]] | 七层 21 agents 的 Vortex 角色卡、字段、horizon、风险和输出模板 |
| [[CogAlpha Quality Gate与Fitness规范]] | LLM/agent 候选因子的字段白名单、时间安全、数值稳定、fitness、qualified/elite/rejected 规则 |
| [[CogAlpha与Vortex落地讨论]] | 为什么从“论文学习”升级为“CogAlpha 因子研究操作系统” |

---

## 当前结论

CogAlpha 最值得学习的是“代码化 alpha + 多 agent 质量检查 + fitness evaluation + adaptive generation + thinking evolution”的研究闭环。它不是一个可以直接搬到实盘的策略，也不是一个已经经过 Vortex 验证的 A 股因子；但它应该升级为 Vortex 的 `cogalpha-factor-mining` 研究 skill，并逐步沉淀为可审计的 agent catalog、quality gate、fitness 和 lineage 体系。
