---
name: goal-achievement-review
description: '目标审查、策略分级与下一步决策技能。Use when: 每轮因子/策略/模型实验后，判断结果是 experiment、candidate、promoted 还是 production-ready，并给出下一步动作。'
argument-hint: '提供本轮指标、数据口径、策略定位、是否复权/PIT/计成本/样本外、容量和可交易性检查'
tags: [vortex, vortex/skill, vortex/research-domain, vortex/strategy-domain, goal-review]
obsidian_links:
  - "[[研究协作与产物治理]]"
  - "[[研究协作与产物治理]]"
  - "[[因子研究与评测全流程说明]]"
---

# 目标审查、策略分级与下一步决策

本 skill 是研究结果的守门员。它不要求每个早期想法都走完整产品流程；它要判断当前结果处在什么阶段，以及下一步该快速探索、候选加固，还是产品晋升。

---

## 一、先判断策略定位

| 定位 | 目标 | 审查重点 |
|---|---|---|
| 低回撤单策略 | 单策略自己控制回撤 | 最大回撤、现金管理、弱市表现 |
| 满仓 alpha 子策略 | 提供高质量收益，组合层控风险 | 年化、Sharpe、Calmar、样本外、容量、可交易性 |
| 因子候选 | 证明预测力 | IC、多空、覆盖率、稳定性 |
| 交易执行策略 | 证明能成交 | 滑点、冲击成本、涨跌停、停牌 |

不要用低回撤单策略标准否决所有 alpha，也不要用 alpha 标准掩盖未来函数和不可交易收益。

---

## 二、分级结论

| 状态 | 含义 | 下一步 |
|---|---|---|
| `invalid_experiment` | 数据口径不可信 | 修复复权、PIT、成本或未来函数 |
| `experiment` | 早期探索，有线索但证据不足 | 继续 Research Spike，不进核心产品 |
| `candidate` | 值得继续拥有 | 补因子评测、walk-forward、容量、可交易性和持仓复盘 |
| `promoted` | 可进入正式策略候选 | 改成 Research signal snapshot + Strategy consumer |
| `production-ready` | 可模拟盘/实盘观察 | 进入 Trade 风控、执行和审计 |

---

## 三、最低可信度检查

以下任一不满足，直接 `invalid_experiment`：

1. 价格口径不清楚或未复权却做长期收益。
2. 使用公告前不可见数据。
3. 没有计入交易成本或成本假设缺失。
4. 触发日亏损被事后剔除。
5. 没有记录数据范围、参数、代码版本或 run_id。

---

## 四、满仓 alpha 候选分级

| 等级 | 条件 |
|---|---|
| S | 年化 ≥30%，最大回撤 ≥ -15%，Sharpe ≥2，Calmar ≥3 |
| A | 年化 ≥20%，最大回撤 ≥ -15%，Sharpe ≥1.5，Calmar ≥2.5 |
| B | 有正 alpha，但效率不足 |
| Reject | 收益、风险效率或可信度不达标 |

S/A 也只代表 candidate 或 promoted 的候选质量，不代表可以直接实盘。

---

## 五、未晋升时的下一步

| 问题 | 下一步 |
|---|---|
| 因子没评测 | 先跑多周期 IC、多空、覆盖率 |
| 回撤过大 | 区分单策略控回撤还是组合层控风险 |
| 样本外不足 | 做参数冻结 walk-forward |
| 容量不明 | 做 P95/P99 参与率和冲击成本压力 |
| 可交易性不明 | 补涨跌停、停牌、ST、退市过滤 |
| artifact 混乱 | 先归档到 workspace，再决定是否进核心仓库 |

---

## 六、输出模板

```text
阶段判断：experiment / candidate / promoted / production-ready / invalid_experiment
策略定位：低回撤单策略 / 满仓 alpha / 因子候选 / 执行策略
关键指标：年化、最大回撤、Sharpe、Calmar、换手、容量
可信度：复权/PIT/成本/未来函数/样本外
主要缺口：...
下一步动作：
1. ...
2. ...
3. ...
```
