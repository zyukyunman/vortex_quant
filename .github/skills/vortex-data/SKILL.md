---
name: vortex-data
description: 数据域技能。用于 Tushare 数据同步、质量校验、PIT 对齐、Lakehouse 与 Qlib Bin 双存储转换。
---

# Vortex Data Skill

## 目标
- 保障研究与交易使用同一份可信数据
- 形成 Parquet + DuckDB 权威层与 Qlib 研究层的双存储体系

## 必做清单
1. 增量同步必须幂等（run_id + 日期分区）
2. 每日健康检查必须执行（缺失、跳变、交易日对齐、复权一致）
3. 财务字段必须按公告日期对齐，禁止未来函数
4. 研究数据必须可转换为 Qlib Bin 格式
5. 任何数据任务失败必须输出可追踪错误与重试建议

## 输出产物
- data_health_report.json
- sync_manifest.json
- qlib_build_manifest.json

## 接口约定
- fetch_raw(source, start, end)
- validate_dataset(dataset, rules)
- build_qlib_view(start, end, fields)
- publish_snapshot(date)
