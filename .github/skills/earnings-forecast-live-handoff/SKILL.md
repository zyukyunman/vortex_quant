---
name: earnings-forecast-live-handoff
description: "业绩预告漂移策略实盘/影子跟踪交接技能。Use when: 另一个 agent 要实现 MiniQMT/QMT 执行、shadow-plan 日更、目标持仓下单、开盘调仓、100万小资金实盘前置、策略条件确认。"
argument-hint: "提供交易日期、资金规模、是否 shadow/paper/live、QMT bridge 地址、目标持仓文件或 workspace 路径"
tags: [vortex, vortex/skill, vortex/strategy-domain, vortex/trade-domain, live-handoff, qmt, shadow-trading]
obsidian_links:
  - "[[业绩预告漂移策略研究总结]]"
  - "[[Mac虚拟机QMT实盘桥接方案]]"
  - "[[交易域设计说明书]]"
---

# 业绩预告漂移策略实盘交接

本 skill 是给“策略落地 / QMT 执行 agent”使用的交接说明。核心原则：

**真正要落地的是业绩预告漂移策略的小资金影子/纸面实盘版本，不是开盘容量验证策略。**

---

## 一、不要搞错策略入口

| 入口 / 模块 | 是否实盘策略 | 正确用途 |
|---|---:|---|
| `vortex.strategy.earnings_forecast_drift` | 是 | 策略 alpha 内核 |
| `vortex strategy earnings-forecast shadow-plan` | 是，实盘前置 | 每日生成目标持仓和调仓变化，不下单 |
| `vortex strategy earnings-forecast precise-review` | 否 | 研究复核/报告，不是每日下单器 |
| `vortex strategy earnings-forecast opening-liquidity-review` | 否 | 开盘容量压力测试 |
| `vortex strategy earnings-forecast auction-execution-review` | 否 | 集合竞价成交量代理的 all-or-nothing 执行压力测试 |

执行 agent 默认只消费 `shadow-plan` 的目标持仓产物，不能把 `opening-liquidity-review` 或 `auction-execution-review` 当策略。

---

## 二、当前可落地版本

当前只允许进入 **100 万级 shadow / paper trading**，不直接进入大资金自动实盘。

| 项 | 当前口径 |
|---|---|
| 策略 | 业绩预告公告后漂移 |
| 策略内核 | `earnings_forecast_drift` |
| 资金规模 | 100 万级先行 |
| 持仓上限 | Top30 |
| 仓位模式 | `capped_with_cash` |
| 单票上限 | 5% |
| 事件延迟 | 公告后第 1 个可交易日，`delay_days=1` |
| 持有窗口 | 40 个交易日，`hold_days=40` |
| 成本压力 | 20 bps 基础成本；执行层另计佣金、印花税、滑点 |
| 交易节奏 | 开盘前生成计划，开盘附近执行 |

1000 万以上只能作为容量探索，不是当前实盘版本。若测试 1000 万以上，优先 Top80；5000 万和 1 亿当前证据不支持。

---

## 三、信号条件

信号来源是 Tushare `forecast` 业绩预告：

1. 使用 `ann_date` 作为事件公告日。
2. 公告日不是交易日时，映射到后续可交易日。
3. 默认 `delay_days=1`，避免把公告当天不可见信息用于交易。
4. 使用 `p_change_min` / `p_change_max` 计算平均增长率。
5. 增长率裁剪到 `[-200%, 500%]` 后除以 100。
6. 叠加业绩预告类型分：

| 类型 | 分数 |
|---|---:|
| 预增 | +1.0 |
| 扭亏 | +0.8 |
| 略增 | +0.5 |
| 续盈 | +0.3 |
| 减亏 | +0.2 |
| 略减 | -0.5 |
| 预减 | -0.8 |
| 续亏 | -0.7 |
| 首亏 | -1.0 |

每日按事件分数做横截面排名，选择排名靠前标的。

---

## 四、过滤与风控条件

执行 agent 必须按 fail-closed 原则处理：任何关键数据缺失、状态不确定或 QMT 回报异常时，不自动下单。

策略层过滤：

1. 剔除 ST / 风险警示股票。
2. 剔除财务 ST 风险候选。
3. 停牌不买不卖。
4. 开盘涨停不买。
5. 开盘跌停不卖。
6. 20 日平均成交额过滤，当前阈值 `min_avg_amount=30000`。
7. 市场门控：沪深300 / 中证500 / 中证1000 多指数确认，默认 `momentum_window=5`、`support_window=20`、至少 2 个指数 risk-on。

实盘前必须注意：

1. 流动性过滤和市场门控必须只使用 T-1 及更早可见数据。
2. 若当前代码仍使用当日全天成交额或当日指数收盘状态，需要先修正或在执行层强制滞后一日。
3. 不允许为了提高回测收益使用当天收盘后才可见的数据决定当天开盘交易。

---

## 五、标准 workflow（必须按此顺序）

`vortex strategy earnings-forecast live-handoff` 是这条链路的**工作流入口**，不是单个脚本。
执行 agent 必须先做“链路可用性确认”，再做“交接包生成”，最后再进入模拟/实盘动作。

### Step 1：先做链路探测（只读）

```bash
vortex trade status \
  --root /Users/zyukyunman/Documents/vortex_workspace \
  --qmt-bridge-url http://<windows-ip>:8000 \
  --qmt-bridge-token <token> \
  --qmt-account-id <account_id> \
  --format json
```

可选：盘中补一轮行情连通性检查。

```bash
vortex trade quote \
  --root /Users/zyukyunman/Documents/vortex_workspace \
  --symbols 000001.SZ,600000.SH \
  --qmt-bridge-url http://<windows-ip>:8000 \
  --qmt-bridge-token <token> \
  --format json
```

### Step 2：生成交接包（唯一主命令）

```bash
vortex strategy earnings-forecast live-handoff \
  --root /Users/zyukyunman/Documents/vortex_workspace \
  --start 20250101 \
  --as-of YYYYMMDD \
  --qmt-bridge-url http://<windows-ip>:8000 \
  --qmt-bridge-token <token> \
  --qmt-account-id <account_id> \
  --output-dir /Users/zyukyunman/Documents/vortex_workspace/strategy/handoff \
  --artifact-dir /Users/zyukyunman/Documents/vortex_workspace/strategy/artifacts \
  --label 业绩预告漂移策略实盘交接 \
  --format json
```

交接包会同时包含：

1. 影子目标持仓（目标权重与调仓变化）
2. bridge 健康状态与连接状态
3. 账户资产、持仓、委托、成交快照
4. `qmt_ready` 与 `blocking_reasons` 门禁结论

### Step 3：门禁判定（fail-closed）

仅当以下条件全部满足，才允许推进下一阶段：

1. `qmt_ready == true`
2. `blocking_reasons` 为空
3. 目标持仓文件存在且可解析
4. 交易账户与目标策略账户一致

任一条件不满足：只允许停在 shadow / 只读联调，不得自动下单。

---

## 六、目标持仓生成（兼容旧流程）

若只需要生成目标持仓（不做 bridge 账户快照），可单独执行 shadow plan：

```bash
vortex strategy earnings-forecast shadow-plan \
  --root /Users/zyukyunman/Documents/vortex_workspace \
  --start 20250101 \
  --as-of YYYYMMDD \
  --output-dir /Users/zyukyunman/Documents/vortex_workspace/strategy/shadow \
  --artifact-dir /Users/zyukyunman/Documents/vortex_workspace/strategy/artifacts \
  --label 业绩预告漂移策略shadow跟踪
```

产物包含：

1. JSON 摘要。
2. HTML 人工审阅页。
3. 目标持仓 CSV，字段包括 `date/symbol/weight/prev_weight/trade_delta/action`。

执行 agent 只能基于目标持仓 CSV 生成订单草案，不应重新实现 alpha 逻辑。

---

## 七、QMT / MiniQMT 执行边界

当前用户环境：

1. Mac 主系统负责 Vortex 数据、策略、报告、风控。
2. Windows 虚拟机负责国金 QMT / MiniQMT / xtquant。
3. Mac 与 Windows 已可 ping 通。
4. 用户已确认可与 MiniQMT 接口打通。

执行 agent 的架构边界：

```text
Mac Vortex
  生成 shadow target / order intents / risk check
  ↓ HTTP/RPyC/WebSocket bridge
Windows VM
  MiniQMT / xtquant
  提交委托、查询资产、持仓、委托、成交
  ↑ 回报同步
Mac Vortex
  记录执行流水、对账、报告、异常停机
```

Mac 端不要直接 import `xtquant` 下单。

---

## 八、下单规则

第一阶段只允许 shadow / paper，不自动真实下单。进入模拟盘后，订单规则如下：

1. 开盘前读取目标持仓。
2. 查询 QMT 当前资产、可用现金、持仓、可用股份。
3. 计算目标市值与当前市值差额。
4. 先卖后买。
5. 100 股整手。
6. 低于最小订单金额的交易跳过。
7. 单票目标仓位不超过 5%。
8. 单日总买入金额、单日总卖出金额、单票买入金额必须有硬阈值。
9. 涨停、停牌、无行情、价格异常时不买。
10. 跌停、停牌、无可用股份时不卖。
11. 委托失败、回报超时、账户查询异常时停止后续自动动作。

建议第一版只生成订单草案和人工确认页面；模拟盘稳定后再提交真实委托。

---

## 九、执行回报必须沉淀

每笔目标订单必须有完整生命周期：

1. `OrderIntent`：策略想做什么。
2. `RiskCheckedOrder`：风控通过后的订单。
3. `OrderSubmitted`：已提交 QMT 的委托。
4. `OrderStatus`：已报、部成、已成、已撤、废单。
5. `TradeFill`：成交价、成交量、成交金额、成交时间。
6. `RejectReason`：资金不足、价格非法、涨跌停、停牌、权限不足、通道异常等。
7. `ReconcileResult`：盘后账户与 Vortex 账本是否一致。

不能只记录“接口调用成功”；必须记录最终是否成交。

---

## 十、容量判断

现阶段容量结论：

| 资金 | 结论 |
|---:|---|
| 30 万 / 50 万 / 100 万 | 可进入 shadow / paper 验证 |
| 300 万 / 500 万 | 需要分钟级补买和滑点模型再判断 |
| 1000 万 | 上限探索区，若测则用 Top80 |
| 5000 万 / 1 亿 | 当前证据不支持 |

注意：`stk_auction_o.volume` 是开盘集合竞价历史总成交量，不是卖一可买量，也不是新增买单可独占流动性。它只能做压力测试代理，不能作为最终实盘容量承诺。

---

## 十一、上线升级路径

1. **Shadow 日更**：只生成目标持仓和订单草案，不提交委托。
2. **只读联调**：QMT 查询资产、持仓、行情、委托、成交。
3. **模拟盘人工确认**：人工确认后提交 100 股级别小单。
4. **模拟盘自动小单**：硬风控 + 异常停机。
5. **真实账户人工确认**：只生成真实账户订单草案，由人确认。
6. **小资金自动化**：连续稳定、盘后对账无差异、滑点可解释后再考虑。

任一阶段出现数据缺失、委托回报不一致、盘后对账失败、异常滑点或未成交率超阈值，必须降级到上一阶段。
