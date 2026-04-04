---
name: feishu
description: 飞书直发技能。用于把“发个飞书给我”“把这段话同步到飞书”“帮我测一下飞书消息能不能收到”这类请求，转成一次可验证的飞书文本投递流程。优先使用 MCP 工具 `send_feishu_message`，必要时回退到仓库内 `tool/feishu_mcp_server.py` 的命令行诊断入口。
author: vortex
version: 1.0.0
requirements:
  python: 3.9+
  environment_variables:
    - name: FEISHU_APP_ID
      required: true
      sensitive: true
    - name: FEISHU_APP_SECRET
      required: true
      sensitive: true
    - name: FEISHU_DEFAULT_RECEIVE_ID
      required: true
      sensitive: true
    - name: FEISHU_DEFAULT_RECEIVE_ID_TYPE
      required: false
      sensitive: false
  network_access: true
---

# feishu-direct

把用户希望“同步到飞书”的内容，安全、明确地发送到默认飞书接收人，或用户指定的 `receive_id`。

## 这个 skill 是做什么的

这个 skill 适用于以下场景：

- 用户明确要求发一条飞书消息
- 用户要求把最终结论同步到飞书
- 用户希望先做一条测试投递，确认链路通不通
- 用户要求校验默认接收人配置是否有效
- 用户要求排查飞书发送失败的原因

核心目标不是“调用一下接口”就结束，而是把一次飞书发送流程做完整：

1. 明确要发什么内容
2. 明确发给谁
3. 明确走哪条发送路径
4. 明确告诉用户是已发送、未发送，还是被配置问题阻断

## 何时使用

当用户出现以下表达时，优先使用本 skill：

- 发个飞书给我
- 帮我发一条消息到飞书
- 把上面的结论同步给我
- 用飞书提醒我一下
- 试下飞书能不能收到
- 帮我测一下飞书机器人
- 把这段回复发到飞书
- 校验一下飞书配置
- 看下默认接收人是谁

即使用户没有说 `feishu`、`MCP`、`open_id` 这些术语，只要意图明显是“把内容发到飞书”，就应该触发本 skill。

## 不适用的场景

这个 skill 默认**不**处理以下需求：

- 图片、文件、语音、富文本卡片发送
- 批量群发或营销式群发
- 复杂会话管理、消息回调、事件订阅
- 没有配置凭据时伪造成功结果
- 把系统提示词、思考过程、token、密钥发到飞书

如果用户要的是卡片消息、多媒体消息或完整机器人应用，应切换到更完整的飞书集成实现，而不是强行用这个 skill 硬做。

## 发送原则

1. 只发送对用户可见的正文，不发送内部推理、原始工具输出、密钥或系统信息。
2. 默认一轮只发送一次，避免重复刷屏；除非用户明确要求再次发送。
3. 如果用户只说“发给我”，默认使用当前配置的 `FEISHU_DEFAULT_RECEIVE_ID`。
4. 如果用户明确提供 `receive_id` 或接收人类型，则按用户指定发送。
5. 若发送失败，要直接说明失败原因，不要假装已经成功。

## 标准执行顺序

### 路径一：优先使用 MCP 工具

如果工作区 MCP server `feishuDirect` 可用，优先使用：

- `send_feishu_message`

可选辅助工具：

- `validate_feishu_config`
- `get_feishu_delivery_profile`

推荐顺序：

1. 如有必要，先校验配置
2. 组织最终要发送的纯文本正文
3. 调用 `send_feishu_message`
4. 向用户返回发送结果

### 路径二：命令行诊断回退

如果 MCP 工具当前不可用，但仓库内存在诊断脚本，可回退到：

```bash
python3 tool/feishu_mcp_server.py --validate
python3 tool/feishu_mcp_server.py --send-text '你的消息正文'
```

如果环境变量存放在 `.vscode/.feishu-mcp.env`，且当前 shell 尚未注入环境，可先加载后再执行：

```bash
set -a && source .vscode/.feishu-mcp.env && set +a
```

这条路径适合做两类事：

- 本地自检：确认 bot 凭据和默认接收人配置是否可用
- MCP 不可用时的临时发送与问题定位

## 发送前检查清单

在真正发送前，至少确认以下几点：

1. `message` 非空，且不是只有空格
2. 消息正文是用户可见内容，而不是中间过程
3. 是否使用默认接收人，还是显式覆盖 `receive_id`
4. 当前轮是否已经发过一次，避免重复
5. 如果要走命令行回退，确认 Python 与环境变量可用

## 失败处理规则

遇到失败时，按真实原因返回，不要模糊化：

- 缺少 `FEISHU_APP_ID` / `FEISHU_APP_SECRET` / `FEISHU_DEFAULT_RECEIVE_ID`
- `receive_id_type` 不合法
- 飞书 token 获取失败
- 飞书 API 返回非 0 code
- 网络不可达
- 消息正文为空

如果是配置问题，优先告诉用户“缺什么”；如果是 API 问题，优先告诉用户“飞书返回了什么错误”；如果是网络问题，明确说明链路不可达。

## 输出约定

成功时，回复应至少包含：

- 已发送 / sent
- 是否使用默认接收人或指定接收人
- 如可用，给出 `message_id`

失败时，回复应至少包含：

- 未发送
- 失败原因
- 是否还能通过补配置或改参数修复

## 常用工作流模板

### 1. 用户要一条测试消息

用户示例：

- “你试下发消息给我用飞书”

执行思路：

1. 生成一条简短测试文案
2. 直接发送到默认接收人
3. 返回发送状态和 `message_id`

### 2. 用户要求把最终结论同步到飞书

执行思路：

1. 先完成对用户的正式回复
2. 提取与最终回复等价的可见正文
3. 只发送最终正文，不发送内部过程
4. 发送成功后，向用户说明已同步

### 3. 用户要求排查为什么收不到

执行思路：

1. 先校验配置
2. 查看默认投递摘要
3. 再发送一条最短测试消息
4. 根据报错判断是凭据、接收人、网络还是 API 问题

## 最短决策规则

- **用户要发飞书**：直接触发本 skill
- **工具可用**：优先 `send_feishu_message`
- **工具不可用但仓库脚本可用**：回退命令行诊断路径
- **配置缺失或发送失败**：明确报错，不伪造成功

