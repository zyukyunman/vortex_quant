---
tags: [vortex, vortex/ai-quant-os, vortex/prd]
aliases: [工作台PRD, Vortex工作台PRD, AI量化公司工作台PRD]
created: 2026-05-16
updated: 2026-05-16
---

# 工作台 PRD v1

## 目标

第一版工作台要让你能像经营一家小型量化研发公司一样使用 Vortex：

1. 看清楚当前公司在做什么：哪些任务在跑、哪些研究有结果、哪些策略任务待处理。
2. 能启动已有策略和因子研究闭环，但默认不执行真实交易。
3. 能管理 Lark、QMT、数据源、模型供应商等本机集成配置。
4. 所有动作都写入结构化事实源：`job`、`run_manifest`、策略任务、通知记录或后续的 `decision`。

第一版不是聊天页，也不是单页表单。它是本地 Mac Mini 上的公司控制台。

## 用户与权限

唯一默认用户是你本人，不做登录、多账号、团队权限和审计人管理。

权限边界通过动作级门禁表达：

| 动作 | v1 默认权限 |
|---|---|
| 查看运行、任务、产物 | 允许 |
| 启动演示因子研究 | 允许 |
| 生成策略任务和目标组合 | 允许 |
| 启动自动编排但禁用交易 | 允许 |
| 允许下单、修改实盘配置 | 必须显式确认 |
| 删除产物、重置工作区 | v1 不提供 |

## 产品形态

v1 使用当前本地 HTTP 控制台承载，不先引入 React。页面以应用壳、左侧导航、右侧工作区组织。后续迁移到 React 时，页面对象和动作保持不变。

导航顺序：

1. `运营驾驶舱`
2. `运行中心`
3. `因子研究实验室`
4. `策略启动向导`
5. `交易台`
6. `设置 / 集成管理`
7. `运行输出`

## 页面需求

## 因子研究与策略晋升边界

因子研究和“把因子放进策略”是两条相邻但不同的生产线。

| 生产线 | 目标 | 产物 | 负责人 | 不能做什么 |
|---|---|---|---|---|
| 因子研究 | 提出、生成、评测和筛选候选因子 | `factor_candidate`、fitness、quality gate、研究 run | Research Director / CogAlpha PI / Factor Quality Reviewer | 不能直接改变策略仓位 |
| 因子审查 | 检查经济含义、未来函数、代码实现、相关性、容量和稳定性 | quality review、code review、risk memo | Factor Quality Reviewer / Risk Officer | 不能跳过审查进入回测 |
| 策略晋升 | 把通过审查的因子转成信号、策略候选和回测任务 | signal snapshot、strategy candidate、backtest report | Strategy Promotion Officer / Strategy PM | 不能直接进入模拟盘或实盘 |
| 交易运行 | 执行已经晋升的策略任务、生成目标组合、交接 QMT | target portfolio、pending QMT task、execution report | Execution Ops / Risk Officer | 不能由研究 run 直接触发下单 |

因此工作台里的任务不能只显示“研究完成”。它必须显示下一步是：

1. `因子质量审查`
2. `代码审查`
3. `策略晋升评估`
4. `信号快照生成`
5. `策略候选回测`
6. `影子 / 模拟 / 实盘门禁`

每一步都应该挂到明确 agent 角色，并写入结构化任务状态。

### 运营驾驶舱

回答：现在系统是否正常，哪里需要我看一眼。

必须展示：

| 模块 | 内容 |
|---|---|
| 状态卡片 | 活动任务数、研究运行数、策略任务数、Lark 配置状态、QMT 配置状态 |
| 当前任务 | 正在排队或运行的 job，包含名称、状态、阶段、更新时间 |
| 最近研究 | 最近 CogAlpha run，包含 run id、质量门禁、候选数量、产物入口 |
| 待处理策略任务 | pending QMT 任务数量和最近任务 |

主要动作：

| 动作 | 结果 |
|---|---|
| 刷新状态 | 重新读取 `/api/status`、`/api/jobs`、`/api/runs` |
| 查看任务详情 | 打开 job 的结构化 JSON |
| 查看运行摘要 | 打开 run_manifest 摘要 |

### 运行中心

回答：每个流程具体走到哪里了，失败点是什么。

必须展示：

| 模块 | 内容 |
|---|---|
| job 列表 | job id、类型、名称、状态、阶段、耗时、错误 |
| research run 列表 | run id、输入摘要、门禁状态、候选数量、产物路径 |
| strategy task 列表 | 交易日、策略版本、preset、状态、目标组合路径 |

状态定义：

| 状态 | 含义 |
|---|---|
| `queued` | 已提交，等待执行 |
| `running` | 后台执行中 |
| `success` | 已完成并产生结构化结果 |
| `failed` | 执行失败，必须显示错误类型和消息 |
| `blocked` | 后续引入，用于等待人工审批或外部数据 |

### 因子研究实验室

回答：这次研究为什么跑，哪些候选通过或失败，下一步候选队列是什么。

v1 范围：

| 能力 | 说明 |
|---|---|
| 启动 CogAlpha demo 闭环 | 使用确定性演示数据，调用公司级 `run_manifest` 输出 |
| 参数设置 | 交易日数量、股票数量、最少期数、分组数、Top N、完成后 Lark 通知 |
| 结果摘要 | 运行状态、质量门禁、晋升候选数量、产物路径 |
| 下一代候选 | 从 run_manifest 读取候选队列，后续做详情页 |

v1 不做：

| 不做 | 原因 |
|---|---|
| 直接接真实 A 股数据挖因子 | 需要先完成数据快照和 PIT 门禁 |
| 让因子直接进实盘 | 必须走信号快照、策略候选、回测、影子、模拟、实盘 |
| 多智能体实时聊天 UI | 先落结构化 run 和 artifact |

### 策略启动向导

回答：现有策略怎么被正确、安全地跑起来。

第一条支持策略：业绩预告漂移策略。

向导步骤：

| 步骤 | 需要你确认的内容 | 系统动作 |
|---|---|---|
| 1. 选择策略 | 策略版本、preset、资金规模 | 读取默认参数 |
| 2. 检查前置条件 | 回看起始日、基准日期、数据是否允许缺精确约束 | 生成输入摘要 |
| 3. 配置执行通道 | QMT Bridge URL、token、账户 ID | 只保存本地配置，不下单 |
| 4. 生成任务 | 生成目标组合和 pending QMT 任务 | 写入 `state/trade/pending_qmt` |
| 5. 自动编排 | 可启动一次自动编排，但交易默认禁用 | 输出 job 状态和结果 |

交易门禁：

| 条件 | v1 行为 |
|---|---|
| 默认状态 | 禁用交易 |
| 用户未填写确认语 | 拒绝允许交易 |
| Lark 未配置 | 允许生成任务，但提示无法移动通知 |
| QMT 未配置 | 禁止生成需要 QMT 的任务 |

### 交易台

回答：QMT 和策略交接包现在是否可用。

v1 展示：

| 模块 | 内容 |
|---|---|
| QMT 配置状态 | Bridge URL、账户 ID、token 是否已配置 |
| pending QMT 任务 | 待执行/已完成/失败任务 |
| 最近目标组合 | target portfolio 路径、持仓数量 |

v1 不直接提供实盘下单按钮。交易执行需要后续单独做审批卡片和二次确认。

### 设置 / 集成管理

回答：本机这套系统连接了哪些外部服务。

设置页不是登录页，也不做用户体系。它只管理本地运行需要的集成变量。

| 集成 | 字段 | v1 动作 |
|---|---|---|
| Lark 国际版 | `LARK_APP_ID`、`LARK_APP_SECRET`、`LARK_DEFAULT_RECEIVE_ID`、`LARK_DEFAULT_RECEIVE_ID_TYPE`、`LARK_API_BASE` | 保存、发送测试 |
| QMT Bridge | `QMT_BRIDGE_URL`、`QMT_BRIDGE_TOKEN`、`QMT_ACCOUNT_ID` | 保存、用于策略向导预填 |
| Tushare | `TUSHARE_TOKEN` | v1 只展示占位，后续接数据状态 |
| 模型供应商 | OpenAI、DeepSeek compatible key | v1 只展示占位，后续接 agent backend |

所有 secret 在状态接口中必须脱敏。

### 运行输出

回答：刚刚点的动作返回了什么结构化结果。

展示最近 API 返回、job 详情、错误消息、run 摘要。它是开发期控制台的辅助区域，不是长期主页面。

## API 需求

| API | 用途 |
|---|---|
| `GET /api/status` | 工作区、配置状态、概览、活动任务、最近 run |
| `GET /api/jobs` | 最近 job 列表 |
| `GET /api/jobs/{id}` | job 详情和结果 |
| `GET /api/runs` | research run 和 strategy task 列表 |
| `POST /api/config/lark` | 保存 Lark 变量 |
| `POST /api/config/qmt` | 保存 QMT Bridge 变量 |
| `POST /api/lark/test` | 发送 Lark 测试 |
| `POST /api/research/cogalpha-cycle` | 启动 CogAlpha demo 研究闭环 |
| `POST /api/strategy/earnings-forecast/prepare` | 生成业绩预告策略任务 |
| `POST /api/strategy/earnings-forecast/auto-once` | 执行一次自动编排，默认禁用交易 |

## 验收标准

| 验收项 | 标准 |
|---|---|
| 页面结构 | 不再是单页表单；有清晰导航和独立设置页 |
| 状态可见 | 启动研究或策略后，运行中心能看到 job 状态 |
| 研究闭环 | demo run 结束后能看到 run_manifest 和候选数量 |
| 策略启动 | 能从向导生成 pending QMT 任务，默认不下单 |
| 配置管理 | Lark 和 QMT 能在设置页保存，状态接口脱敏 |
| 安全边界 | 交易默认关闭，允许交易必须显式确认 |
| 中文表达 | 页面主文案和文档默认中文，字段名和命令保留英文 |

## 插件策略

当前实现不依赖 Codex 插件。

| 插件 | 何时需要 |
|---|---|
| Figma | 当需要把原型交给专业 UI 设计工具继续打磨时安装 |
| OpenAI Developers | 当接 agent backend、OpenAI API 或模型配置时安装 |
| GitHub | 当需要发布 PR、查 issue、跑远端 CI 时安装 |
| Notion / Google Drive | 当 PRD 需要同步到外部文档系统时安装 |

插件安装需要你在 Codex 界面确认；不能由我静默安装。
