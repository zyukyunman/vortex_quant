---
name: factor-research-archive
description: "因子研究 Obsidian 档案沉淀技能。Use when: 因子实验完成后需要记录好因子、坏因子、失败原因、关键指标、workspace 产物路径、下一轮研究队列，并维护 docs/因子研究 Obsidian 知识图谱。"
argument-hint: "提供因子名称、研究假设、数据字段、评测区间、IC/多空/回测指标、失败原因、artifact 路径和下一步建议"
tags: [vortex, vortex/skill, vortex/research-domain, vortex/factor-archive, obsidian]
obsidian_links:
  - "[[因子研究档案规范]]"
  - "[[因子实验总表]]"
  - "[[因子研究与评测全流程说明]]"
  - "[[研究协作与产物治理]]"
---

# 因子研究档案沉淀

本 skill 用于在每轮因子实验后判断是否需要把结果沉淀到仓库 Obsidian vault。核心原则：

**仓库沉淀可复用经验，不沉淀大产物；好因子、坏因子、无效路径都要可检索。**

---

## 一、什么时候必须建档

满足任一条件，就应在 `docs/因子研究/实验档案/` 建档：

1. 因子通过准入，可能进入策略回测或组合。
2. 因子失败但形成可复用经验，例如方向相反、回撤过大、容量不足。
3. 发现未来函数、PIT、复权、成交口径、数据单位等关键问题。
4. 出现“IC 有效但 long-only 回测无效”的重要现象。
5. 因子会被后续研究反复引用。
6. 用户或研究员明确要求保留该研究记忆。

---

## 二、什么时候不建档

以下情况默认不建档，只保留 workspace artifact 或会话记录：

1. 命令失败且没有形成研究结论。
2. 数据权限缺失导致实验没有可解释结果。
3. 重复参数微调，没有新增经验。
4. 临时排障、格式修复、路径修复。
5. 只生成了大 CSV/HTML，但没有结论、边界和下一步。

---

## 三、状态机

| 状态 | 含义 | 下一步 |
|---|---|---|
| `raw_idea` | 只有假设，还没系统评测 | 明确字段、PIT 和评测周期 |
| `evaluated` | 已完成基础 IC/多空评测 | 判断是否进策略回测 |
| `research_lead` | 有可用线索，但还不是策略 | 继续组合、风控或对冲验证 |
| `candidate` | 值得拥有的因子/策略候选 | 补 walk-forward、容量、可交易性 |
| `rejected` | 明确无效或方向不适合 | 记录失败原因，避免重复 |
| `invalid_experiment` | 数据口径或未来函数不可信 | 修复后重跑，不能引用结果 |
| `promoted` | 已晋升为正式信号/策略组件 | 接入 Research signal snapshot 或 Strategy consumer |

状态不是评级炫耀，而是后续动作路由。

---

## 四、仓库与 workspace 边界

进入仓库：

1. Markdown 经验档案。
2. 因子 MOC、因子族页、实验总表。
3. 可复用 skill 和方法论。
4. 少量代表性指标和结论。

留在 workspace：

1. 大 CSV。
2. 大 JSON。
3. HTML tear sheet。
4. 参数网格。
5. 每日持仓、逐笔成交、诊断明细。

档案里只记录 workspace 路径，不复制大产物。

---

## 五、Frontmatter 模板

每篇因子档案必须遵守 Obsidian frontmatter：

```yaml
---
tags: [vortex, vortex/research-domain, vortex/factor-archive]
aliases: [中文因子名, english_factor_name]
created: YYYY-MM-DD
updated: YYYY-MM-DD
status: research_lead
factor_family: moneyflow
data_sources: [moneyflow, bars]
artifact_root: /Users/zyukyunman/Documents/vortex_workspace/research/factor-reports/example
---
```

`aliases` 首项必须与 wikilink 使用的中文名一致。

---

## 六、正文模板

每篇档案至少包含以下章节：

1. `一句话结论`
2. `研究假设`
3. `字段映射`
4. `评测口径`
5. `关键指标`
6. `阶段判断`
7. `失败或保留原因`
8. `可复用经验`
9. `关联链接`
10. `产物路径`

坏因子也必须写清楚为什么坏：方向错、交易不可用、回撤过大、覆盖率不足、未来函数、容量不足、相关性过高等。

---

## 七、MOC 更新规则

新增或修改因子档案后，必须同步维护：

1. `docs/因子研究/README.md`
2. `docs/因子研究/因子实验总表.md`
3. 对应 `docs/因子研究/因子族/*.md`
4. 必要时更新 `docs/README.md`

每个档案至少要有：

1. 一条来自 MOC 或因子族页的入链。
2. 一条指向评测方法或策略页的出链。
3. 一条指向数据源、artifact 或 skill 的引用。

---

## 八、与其他 skills 的协作

| skill | 协作方式 |
|---|---|
| `factor-mining-research` | 产生候选因子和假设 |
| `factor-evaluation` | 产生 IC、多空、覆盖率和准入判断 |
| `goal-achievement-review` | 给出状态分级和下一步 |
| `obsidian` | 保证 frontmatter、wikilink、MOC 和断链检查 |
| `tushare` | 记录数据来源、字段口径、权限和 PIT 风险 |
| `strategy-review-officer` | 审查是否把因子误包装成可实盘策略 |

---

## 九、最低质量要求

档案不能只有“效果不好”。至少要能回答：

1. 哪个方向被验证了？
2. 使用了哪些字段？
3. 哪个区间、哪个 horizon？
4. 结果为什么好或坏？
5. 后续应该避免什么？
6. 是否值得继续？

如果回答不了这些问题，不应进入仓库。
