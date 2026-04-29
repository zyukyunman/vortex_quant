---
name: strategy-development-experience
description: 'Vortex 策略开发经验与防过拟合审查技能。Use when: 开发新策略、把研究 runner 产品化、整理 artifact、审查策略是否值得晋升。'
argument-hint: '提供策略假设、数据口径、回测结果、交易约束、artifact 状态和待晋升问题'
tags: [vortex, vortex/skill, vortex/research-domain, vortex/strategy-domain, strategy-experience, anti-overfit]
obsidian_links:
  - "[[研究协作与产物治理]]"
  - "[[业绩预告漂移策略研究总结]]"
  - "[[业绩预告漂移策略测试报告]]"
  - "[[因子研究与评测全流程说明]]"
---

# 策略开发经验与防过拟合审查

本 skill 是策略研究的工程检查清单。核心原则：**研究可以快，晋升必须严**。早期可以用 runner 快速探索；一旦结果进入 candidate，就要补因子评测、可交易性、容量、报告和测试。

---

## 一、两速流程

| 阶段 | 目标 | 允许产物 | 必须补齐 |
|---|---|---|---|
| Research Spike | 快速判断有没有 alpha | 临时脚本、runner、小网格、临时 HTML | 无未来函数、计成本、记录参数 |
| Candidate Hardening | 判断是否值得拥有 | 标准 JSON/HTML、持仓、测试报告 | IC/多空、walk-forward、容量、可交易性 |
| Product Promotion | 进入 Strategy / 模拟盘 | signal snapshot、正式策略配置、测试 | Research → Signal → Strategy 链路 |

不要让每个想法一开始就走完整流程；也不要让 candidate 长期停留在临时脚本状态。

---

## 二、可信度底线

以下任一问题存在，结果直接视为 `invalid_experiment`：

1. 未复权价格用于长期收益。
2. 使用公告前不可见数据。
3. 财务数据没有 PIT 或披露延迟假设。
4. 没有计成本。
5. 事后剔除触发日亏损。
6. 用同日收盘后才知道的数据在同日成交。
7. 用复权 open 和未复权涨跌停价比较。

---

## 三、candidate 检查清单

一个策略从 experiment 升为 candidate 前，至少要回答：

| 问题 | 要求 |
|---|---|
| 假设 | 为什么应该赚钱，数据何时可见 |
| 因子 | 是否做过 IC、多空、覆盖率、分组单调性 |
| 回测 | 是否复权、PIT、计成本、无未来函数 |
| 稳健性 | 参数邻域、walk-forward、弱年份/弱月份 |
| 可交易性 | ST、涨跌停、停牌、退市、冲击成本 |
| 容量 | 单笔参与率 P95/P99、单票集中度 |
| 持仓 | 每日持仓、贡献、行业/市值/估值暴露 |
| artifact | JSON 是事实来源，HTML 只是展示 |

---

## 四、artifact 归位

| 产物 | 默认位置 |
|---|---|
| 临时脚本、临时 SQL、参数网格全量结果 | workspace / research repo |
| 大 CSV、PNG、历史中间 HTML | workspace artifact |
| 策略卡、正式测试报告、少量代表性 HTML/JSON | `docs/策略研究/` |
| 可复用分析函数、回测内核、报告 writer | `vortex/research` 或 `vortex/strategy` |
| 正式策略候选 | `vortex/strategy` + signal snapshot + CLI/API 入口 |

每个保留的 HTML 必须有同名或可追溯 JSON；不要只保留不可解析的页面。

---

## 五、满仓 alpha 分级

| 等级 | 条件 |
|---|---|
| S | 年化 ≥30%，最大回撤 ≥ -15%，Sharpe ≥2，Calmar ≥3，可信度通过 |
| A | 年化 ≥20%，最大回撤 ≥ -15%，Sharpe ≥1.5，Calmar ≥2.5，可信度通过 |
| B | 有正 alpha 但效率不足 |
| Reject | 收益、风险效率或样本外不达标 |
| Invalid | 基础可信度不通过 |

满仓 alpha 的回撤可以交给组合层控制，但不能绕过可交易性和样本外检查。

---

## 六、输出模板

```text
阶段：experiment / candidate / promoted / production-ready / invalid
策略定位：
核心假设：
数据口径：
因子评测：
回测结果：
样本外/成本/容量/可交易性：
artifact 位置：
是否值得晋升：
下一步：
```
