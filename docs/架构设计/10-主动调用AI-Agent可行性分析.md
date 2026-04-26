---
tags: [vortex, vortex/architecture]
aliases: [AI Agent 可行性分析, AI-Agent可行性分析, AI Agent Feasibility]
created: 2026-04-04
updated: 2026-04-26
---

# 主动调用 AI Agent 的可行性与需求分析

> 文档编号：10  
> 版本：v0.1（草案）  
> 归属：架构设计  
> 状态：需求分析阶段  
> 关联 Skill：[[feishu skill]]、[[context7-mcp skill]]

---

## 1. 背景与动机

Vortex 的核心工作流（数据同步、因子评测、回测、信号发布）都是**长时间运行、无人值守**的。
用户经常面临这样的困境：

- 管线跑了几个小时，跑完了也不知道；跑挂了更不知道。
- 即使有飞书通知，收到通知后还是要自己回来操作（看日志、修代码、跑分析）。
- 很多操作是**可以被 AI Agent 自动完成的**，比如"数据更新完后跑一轮因子筛选"、
  "回测跑完后做一份摘要分析"、"出错时自动诊断原因"。

**核心诉求**：让 Vortex 在特定事件发生时，能主动唤起一个 AI Agent 来执行后续任务，
而不是只发一条消息等人来处理。

---

## 2. 场景枚举

### 2.1 数据域事件 → Agent 后续动作

| 触发事件 | Agent 可做的事 | 价值 |
|----------|---------------|------|
| 数据全量同步完成 | 基于 Skill 跑一轮因子筛选 / 数据质量概要分析 | 第二天一早就能看到分析报告 |
| 数据增量更新完成 | 检查今日数据异常 / 与昨日对比 / 生成每日简报 | 替代人工巡检 |
| 数据同步失败（代码 bug） | 分析 traceback，尝试修复代码 | 减少人工介入 |
| 数据质量门禁报警 | 自动定位是哪些股票/字段异常，给出处理建议 | 加速排障 |
| 新数据集首次上线 | 验证字段完整性，对比预期 schema | 减少上线后才发现的问题 |

### 2.2 研究域事件 → Agent 后续动作

| 触发事件 | Agent 可做的事 | 价值 |
|----------|---------------|------|
| 因子评测报告生成完毕 | 自动解读 IC、分组收益，写总结发飞书 | 替代人工肉眼看报告 |
| 新因子接入 | 自动跑一轮多周期评测，输出"是否值得纳入"的结论 | 加速研究循环 |
| 批量因子扫描完成 | 按评分排序，输出 Top-N 候选因子列表 | 从海量因子中快速筛出优质因子 |

### 2.3 策略域事件 → Agent 后续动作

| 触发事件 | Agent 可做的事 | 价值 |
|----------|---------------|------|
| 回测报告生成完毕 | 解读关键指标，与基准对比，写策略评估摘要 | 替代人工逐页翻报告 |
| 策略调参完成 | 对比不同参数组的表现，推荐最优参数集 | 加速调参循环 |
| 信号发布后 | 校验信号与历史信号的一致性、异常检测 | 防止带病上线 |

### 2.4 通用场景

| 触发事件 | Agent 可做的事 | 价值 |
|----------|---------------|------|
| 任何管线出错 | 诊断错误原因，尝试修复或生成诊断报告 | 减少人工 debug 时间 |
| 定时触发（cron） | 每日/每周运行固定分析 skill | 定期自动生成研究报告 |

---

## 3. 技术可行性

### 3.1 Copilot CLI 的非交互调用能力

Copilot CLI 原生支持非交互模式：

```bash
copilot -p "<prompt>" \
  --yolo                  # 全权限（读写文件 + 执行命令）
  --json                  # 结构化输出
  --effort high           # 高推理强度
  --add-dir <directory>   # 限制文件访问范围
```

**验证结果**：本机 `/opt/homebrew/bin/copilot` 可用，`-p` 模式可被 `subprocess.run()` 直接调用。

### 3.2 Skill 的可编程调用

Copilot CLI 支持 Skill 系统（`.github/copilot/skills/`），
每个 Skill 是一个 `SKILL.md` 文件，定义了触发条件和执行逻辑。

Agent 被唤起后，可以通过 prompt 引导它使用特定 Skill：

```bash
copilot -p "数据更新已完成，请使用 factor-evaluation skill 对最新数据做一轮因子筛选" --yolo
```

### 3.3 其他 Agent 后端的可行性

| 后端 | 调用方式 | 可行性 |
|------|----------|--------|
| Copilot CLI | `subprocess.run(["copilot", "-p", ...])` | ✅ 已验证 |
| Claude API | HTTP 请求 + tool_use | ✅ 技术可行，需 API key |
| OpenAI API | HTTP 请求 + function calling | ✅ 技术可行，需 API key |
| 本地 LLM | Ollama / vLLM 本地推理 | ⚠️ 能力受限，适合简单任务 |

### 3.4 现有架构的支撑度

| 已有能力 | 位置 | 复用度 |
|----------|------|--------|
| 事件体系 | `vortex/shared/events.py` | 可直接作为 Agent 触发源 |
| 通知路由 | `vortex/notification/router.py` | 可扩展一个 `agent` 渠道 |
| 通知渠道协议 | `vortex/notification/channel/base.py` | Agent 可作为一种渠道实现 |
| 通知服务 | `vortex/notification/service.py` | 自动发现并注册渠道 |
| 恢复/重试 | `vortex/data/recovery.py` | Agent 修复后可衔接重试 |
| Profile 配置 | `vortex/config/profile/models.py` | 可在 profile 中配置 agent 开关 |
| CLI init | `vortex/cli.py` | 可在 init 时交互式设置 agent 配置 |

**结论：现有架构已经为"Agent 作为通知渠道"预留了完整的扩展点，
核心工作量在于实现 Agent 渠道本身和错误上下文收集器。**

---

## 4. 架构方案

### 4.1 Agent 不是一个独立系统，而是一种通知渠道

设计哲学：**Agent 调用 = 一种特殊的"通知投递"**。

现有通知链路：`事件 → Router → Channel(feishu) → 发消息`

扩展后：`事件 → Router → Channel(feishu) + Channel(agent) → 发消息 + 唤起 Agent`

好处：
- 复用现有的事件体系、路由规则、配置模型
- Agent 开关/触发规则/投递目标都走 profile 配置
- 不需要新建一套并行的事件分发系统

### 4.2 Agent 后端协议

```python
class AgentBackend(Protocol):
    """Agent 后端协议。"""

    @property
    def name(self) -> str: ...

    def execute(
        self,
        prompt: str,
        *,
        timeout: float | None = None,
        allowed_dirs: list[str] | None = None,
    ) -> AgentResult: ...

    def is_available(self) -> bool: ...
```

### 4.3 Agent 通知渠道

```python
class AgentChannel:
    """Agent 通知渠道：收到通知消息后，组装 prompt 并调用 Agent 后端。"""

    def send(self, message: NotificationMessage, ...) -> dict:
        prompt = self._build_prompt(message)
        result = self._backend.execute(prompt, ...)
        return {"status": result.status, "output": result.output}
```

### 4.4 配置模型

```yaml
# profiles/default.yaml
agent:
  enabled: false
  backend: "copilot"
  max_attempts: 2
  notify_on_complete: true   # Agent 完成后是否再发飞书
  allowed_scopes:
    - "vortex/"
```

`vortex init` 时交互式询问是否启用。

---

## 5. 分阶段实施建议

### Phase 1：基础设施（本次）
- 实现 `AgentBackend` 协议 + `CopilotBackend`
- 实现 `AgentChannel`（作为 NotificationChannel 的一种）
- Profile 扩展 `agent` 配置段
- `vortex init` 增加 agent 配置交互
- 可行性分析文档（本文档）

### Phase 2：代码修复场景
- 实现 `AgentFixableError` + 上下文收集器
- 集成到 CLI retry 循环
- 修复后验证

### Phase 3：分析报告场景
- 数据更新完成后自动触发 Skill 分析
- 因子/回测报告生成后自动解读
- 定时触发

### Phase 4：多后端支持
- Claude API / OpenAI 等后端实现
- 后端选择策略（根据任务类型选不同后端）

---

## 6. 风险与约束

| 风险 | 缓解 |
|------|------|
| Copilot CLI 版本更新可能改参数 | 封装在 CopilotBackend 中，修改集中 |
| Agent 执行时间不可控 | 设 timeout，超时通知人类 |
| Agent 可能产生错误输出 | 重要操作（如改代码）需要验证环节 |
| Copilot 配额限制 | 记录调用次数，接近限额时降级为只通知 |
| 默认关闭可能导致用户不知道有这功能 | init 交互 + 文档说明 |
