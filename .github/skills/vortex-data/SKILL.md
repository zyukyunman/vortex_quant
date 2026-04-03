---
name: vortex-data
description: 数据域技能。用于构建可替换数据源/可替换存储的数据底座，支持多市场、多频率、严格PIT与阻断式质量门禁。
---

# Vortex Data Skill

## 目标

1. 数据链路可插拔：Provider 可替换、Storage 可替换。
2. 研究与执行共享同一权威快照。
3. 严格 PIT 与质量阻断优先于吞吐。

## 已确认约束（全局）

1. 市场不限：只要有数据即可接入。
2. 频率双轨：日频 + 分钟频。
3. 默认快照时间：22:00，可配置。
4. 财务口径：严格 PIT。
5. Tushare 路线默认全A。
6. 失败策略：Fail-Closed（阻断）。

## 必做清单

1. 增量同步幂等（run_id + 分区）。
2. Provider 抽象与注册机制必须实现。
3. Storage 抽象与注册机制必须实现。
4. 每日质量门禁必须先于快照发布。
5. PIT 对齐失败必须阻断流程。

## 输出产物

1. sync_manifest.json
2. data_health_report.json
3. pit_alignment_report.json
4. qlib_build_manifest.json
5. snapshot_manifest.json

## 接口约定

- register_provider(name, provider)
- register_storage(name, backend)
- fetch_raw(source, start, end)
- validate_dataset(dataset, rules)
- align_pit(dataset, disclosure_calendar)
- build_qlib_view(start, end, fields)
- publish_snapshot(date)
