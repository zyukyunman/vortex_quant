---
name: vortex-ops
description: 运维域技能。用于调度编排、任务状态机、故障恢复、审计日志。
---

# Vortex Ops Skill

## 目标
- 确保平台可持续运行、可恢复、可审计

## 必做清单
1. 每个任务必须有 run_id 与状态机
2. 失败任务必须支持重试与补跑
3. 全链路日志必须统一字段
4. 关键任务必须支持超时与熔断
5. 每日结束必须输出运行摘要

## 输出产物
- run_state.json
- retry_log.json
- daily_ops_summary.md

## 接口约定
- start_pipeline(profile, date)
- run_task(task_name, run_id)
- retry_task(task_name, run_id)
- recover_run(run_id)
