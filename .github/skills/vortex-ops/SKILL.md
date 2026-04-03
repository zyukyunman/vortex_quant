---
name: vortex-ops
description: 运维域技能。用于调度编排、任务状态机、故障恢复与审计，支持可配置快照时间与严格阻断流程。
---

# Vortex Ops Skill

## 目标

1. 保障流水线可持续运行、可恢复、可审计。
2. 保证质量门禁在数据->研究->执行链路中的阻断语义正确。

## 必做清单

1. 每个任务必须具备 run_id 与状态机。
2. 任务依赖必须显式 DAG 化，禁止隐式顺序。
3. 默认 22:00 快照发布，且必须配置化。
4. 失败任务支持重试与补跑，但不得绕过质量门禁。
5. 全链路日志字段统一（run_id/module/status/cost_ms）。

## 输出产物

1. run_state.json
2. retry_log.json
3. daily_ops_summary.md

## 接口约定

- start_pipeline(profile, date)
- run_task(task_name, run_id)
- retry_task(task_name, run_id)
- recover_run(run_id)
- publish_snapshot_if_passed(run_id)
