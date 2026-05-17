---
tags: [vortex, vortex/moc, vortex/skill]
aliases: [技能目录, Vortex技能目录]
created: 2026-05-16
updated: 2026-05-16
---

# Vortex 技能目录

Vortex skills 的唯一正文保留在本仓库 `.codex/skills/`。仓库内 `.github/skills` 是一个目录级软链接，指向 `../.codex/skills`；用户级 `~/.codex/skills/<skill-name>` 也应指向这里对应的项目 skill。不要再维护两份正文。

## 文档和 Skill 的边界

1. 文档写架构、契约、状态机、UI 字段和治理边界。
2. Skill 写触发条件、执行步骤、读取顺序、输出格式、停止条件和 automation prompt。
3. 具体流程如果同时出现在文档和 skill，以 skill 为执行真源，文档只保留链接和对象说明。

## 研究类

| 技能 | 用途 |
|---|---|
| `factor-mining-research` | 因子挖掘与策略研究默认入口 |
| `cogalpha-factor-mining` | CogAlpha 方法附录 |
| `factor-evaluation` | 新因子多周期评测 |
| `factor-research-archive` | 因子档案归档 |
| `factor-evidence-reviewer` | 评测后证据可信度和下一步接力 |
| `goal-achievement-review` | 目标达成和下一步审查 |

## AI 量化公司 OS

| 类型 | 例子 | 用途 |
|---|---|---|
| 岗位 | `research-director`、`cogalpha-pi`、`risk-officer` | 定义岗位职责、输入、输出和权限边界 |
| 流程 | `factor-research-operating-cycle`、`cogalpha-factor-council` | 定义可重复执行的研究、交易、通知、复盘流程 |

## 维护规则

1. 新增或修改 Vortex skill 时，只改 `.codex/skills/<skill-name>`。
2. `.github/skills` 只能是指向 `../.codex/skills` 的目录级软链接；不要在 `.github/skills` 下逐个维护 skill 目录。
3. 长期研究结论不写进 skill，写入 docs 或研究档案；skill 只保留方法、边界和标准输出。
4. 如果某个工具只服务一次性任务，不新增 skill。
