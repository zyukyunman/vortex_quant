---
name: factor-research-operating-cycle
description: "因子研究操作循环。适用场景： 需要从研究议题开始，组织资料检索、CogAlpha 候选、质量门禁、多周期评测、证据评审、策略角色判定、归档和下一轮演化。"
argument-hint: "提供研究目标、当前策略瓶颈、已有因子档案、可用字段、评测周期、时间/候选预算、停止条件和是否允许代码候选"
tags: [vortex, vortex/skill, vortex/research-domain, vortex/ai-quant-os]
obsidian_links:
  - "[[因子研究操作流程]]"
  - "[[因子研究方法论]]"
  - "[[因子研究运行契约]]"
  - "[[CogAlpha因子研究落地路线]]"
  - "[[因子研究与评测全流程说明]]"
---

# 因子研究操作循环

目标是把“想研究一个方向”变成可追踪、可继续、可中断恢复的研究生产线，而不是临时聊天或临时脚本。

这个 workflow 不按“30 分钟版 / 1 小时版 / 2 小时版”拆分。研究循环可以在同一协议下持续多轮；是否继续取决于证据、队列和停止条件。时间越长，只是允许它多做几轮 `candidate -> review -> handoff -> archive -> next_queue`，不是放松门禁。

注意：这里的“接力”和“预算”是研究操作协议，不是要求在代码里写死调度链。第二版默认由 Codex 客户端、skill、Markdown 接力包和研究档案继续工作，不假设 API key。

## 流程

1. 研究总监定义议题、目标、预算和停止条件。
2. 归档审计员检索已做过的好因子、坏因子和失败路径。
3. 数据运营负责人检查字段、快照和可见时间。
4. CogAlpha PI 组织研究视角和候选生成。
5. 因子质量审查员执行质量门禁。
6. 通过门禁的候选进入多周期评测。
7. 研究证据评审员审查结论可信度、过拟合风险、重复度和是否值得继续。
8. 策略晋升专员判断候选角色：排序、过滤、门控、风控或解释。
9. 风险官审查样本外、容量、回撤、成本、集中度和自动化动作风险。
10. 需要执行验证时交给执行运营；否则不进入交易链路。
11. 归档审计员记录结论、失败原因和下一轮演化队列。

## 长跑接力

每个长时间研究会话都应维护以下接力字段：

```text
session_budget:
  available_time:
  candidate_budget:
  max_failed_rounds_without_new_evidence:
  must_stop_for_human_decision:
```

默认循环：

```text
hypothesis_card
  -> archive_lookup
  -> cogalpha_perspective_selection
  -> candidate_generation
  -> quality_gate
  -> factor_evaluation
  -> evidence_review
  -> handoff_note
  -> archive_writeback
  -> next_generation_queue
```

只要还有时间、没有触发停止条件、`next_generation_queue` 非空，就允许继续下一轮。停止不是失败；停止必须输出本轮学到了什么、哪些路径已经关闭、下一轮如果继续应该从哪里开始。

## Codex Thread Automation

用于 Codex automation 时，本 skill 是执行真源；`docs/AI量化公司OS/10-因子研究运行契约-v1.md` 只定义状态卡和 schema。

Automation prompt：

```text
$factor-research-operating-cycle

你是 Vortex 因子研究长跑接力线程。每次被唤醒时：

1. 不要重新发明研究题目，先读取当前线程、研究状态卡、最后一条接力包和相关研究档案。
2. 判断 current_stage，只推进一个明确阶段：候选生成、质量门禁、评测、证据评审、归档或下一轮队列。
3. 如果当前阶段需要本地评测，可以运行已有离线评测命令；不要新增业务代码。
4. 如果缺少研究议题、数据证据、用户决策或安全边界，停止并说明需要什么。
5. 不要修改交易配置、凭证、QMT bridge、实盘参数，不要下单。
6. 输出本轮完成了什么、证据在哪里、下一位 Agent、下一次醒来应该做什么。
```

被唤醒后的执行顺序：

1. 读取状态卡中的 `current_stage`、`last_handoff`、`next_wakeup_goal`。
2. 只选择一个阶段推进，不能一次跨过质量门禁、评测和证据评审。
3. 如果要写代码，先说明触发条件；默认只更新研究文档、归档和接力包。
4. 如果需要用户判断方向、风险或交易动作，停止并汇报，不继续自动推进。

### 21-agent 自动化样例 prompt

这个 prompt 用于证明 Codex 客户端可以通过 heartbeat、skill 和 Markdown 状态卡让 21 个 CogAlpha 视角自动接力；它不是 API key / Agents SDK / 写死调度代码方案。

```text
$factor-research-operating-cycle
$cogalpha-factor-council

你是 Vortex 21-agent 因子研究自动接力样例。目标是让 21 个 CogAlpha 研究视角围绕同一张假设卡完成 opinion card、证据推进、关闭或 handoff。

每次被唤醒时：

1. 读取当前线程、研究状态卡、最后接力包和相关研究档案。
2. 判断 current_stage：agent_progress_review、candidate_generation、quality_gate、factor_evaluation、evidence_review、risk_review、execution_review、archive_writeback 或 done。
3. 只推进一个阶段；不要一次完成候选生成、评测、证据评审和归档。
4. 如果 current_stage 是 agent_progress_review，则检查 21 个视角是否都有状态：advanced、closed、blocked、waiting_evidence 或 queued。
5. 对缺状态的视角补一张 AgentOpinionCard；对 queued 视角只推进一个最重要证据动作。
6. 如果需要本地评测，只运行已有 CLI/runner；不要新增业务代码。
7. 不要修改交易配置、凭证、QMT bridge、实盘参数，不要下单。
8. 如果需要写代码、进入 paper shadow ledger、模拟盘或实盘，停止并请求用户决定。
9. 写回本轮结果：完成阶段、证据路径、agent_progress 计数、下一位 Agent、下次醒来目标。

停止条件：

- 21 个视角全部 advanced 或 closed；
- 需要用户选择新研究议题；
- 需要写代码或交易相关审批；
- 连续多轮没有新增证据。
```

## 路由规则

| 状态 | 下一位 Agent | 动作 |
|---|---|---|
| `invalid` | 因子质量审查员 | 阻断；修字段、PIT、表达式或直接关闭路径 |
| `rejected_no_signal` | 归档审计员 + CogAlpha PI | 记录失败原因；如有新信息则缩小视角或重生候选 |
| `rejected_informative` | 研究证据评审员 | 判断是否值得变形、反向、门控或 crossover |
| `qualified` | 研究证据评审员 | 复核多周期、分年、重复度、搜索预算 |
| `elite` | 风险官 + 策略晋升专员 | 只允许进入 shadow / overlay / robustness，不直接进入策略 |
| `overlay_improved` | 风险官 | 做鲁棒性、成本、容量和 drawdown delta |
| `robustness_passed` | 执行运营 | 做成交覆盖、整手、目标价和参与率复核 |
| `execution_passed` | 策略晋升委员会 | 请求用户审批 shadow / paper，不自动实盘 |

## 输出

```text
研究议题：
已有证据：
预算与停止条件：
候选队列：
质量门禁：
评测计划：
证据评审：
策略角色：
接力包：
归档动作：
下一轮演化：
```

## 规则

1. 不先画产品页面再倒推研究流程。
2. 不让候选跳过质量门禁。
3. 不把失败实验丢掉，失败原因必须进入下一轮上下文。
4. 不把运行时间当作研究质量；长跑的核心是每一轮都能被审计和恢复。
5. 没有证据评审结论时，不允许把 `qualified/elite` 直接交给策略晋升。
