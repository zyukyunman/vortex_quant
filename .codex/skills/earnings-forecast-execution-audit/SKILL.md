---
name: earnings-forecast-execution-audit
description: "业绩预告漂移策略每日自动执行巡检技能。Use when: 检查 earnings-forecast auto-run 当日是否正常拉数、生成目标、提交 QMT、成交回报、未成交、ST 清理、任务状态与账户持仓是否一致。"
argument-hint: "提供交易日期、workspace 根目录、QMT bridge URL、账户 ID；默认检查当天业绩预告漂移策略自动执行情况"
tags: [vortex, vortex/skill, vortex/strategy-domain, vortex/trade-domain, live-trading, qmt, audit]
obsidian_links:
  - "[[业绩预告漂移策略研究总结]]"
  - "[[Mac虚拟机QMT实盘桥接方案]]"
  - "[[交易域设计说明书]]"
---

# 业绩预告漂移策略每日执行巡检

本 skill 用于检查 `vortex strategy earnings-forecast auto-run` 的当日执行情况。核心原则：**不要只看 auto-run tick 成功，也不要只看本地 execution_report；必须一路核对到 QMT 当前委托、成交和账户持仓。**

---

## 一、巡检结论分级

| 结论 | 含义 |
|---|---|
| `正常完成` | 数据、目标、风控、委托、成交和持仓都与预期一致，无未解释异常。 |
| `部分完成` | 已生成目标并提交委托，但存在未成交、部分成交或待撤单，需要继续跟踪。 |
| `需人工处理` | 有未成交 ST 清理、废单、权限/资金/涨跌停问题，或本地状态与 QMT 回报不一致。 |
| `自动链路异常` | data server、auto-run、目标生成或执行任务失败，需要先修复服务。 |
| `禁止继续下单` | 关键数据缺失、ST/停牌/涨跌停状态不确定、QMT bridge 不可用或账户不一致。 |

默认先按 `需人工处理` 审查，只有证据完整时才降级为 `正常完成`。

---

## 二、标准巡检顺序

### 1. 检查 auto-run 与健康监控

先看服务是否活着、最近 tick 是否成功，以及是否只是因为“今日已有计划”而跳过重复执行。

```bash
python3 - <<'PY'
import json
from pathlib import Path

root = Path("../vortex_workspace")
for rel in [
    "state/strategy/earnings_forecast_auto/status.json",
    "state/live-service-health-latest.json",
]:
    path = root / rel
    print("\n##", path)
    print(json.dumps(json.loads(path.read_text()), ensure_ascii=False, indent=2)[:12000])
PY
```

重点字段：

1. `service_status` 必须是 `running`。
2. `pid_alive` 必须是 `true`。
3. `last_tick_status` 必须是 `success`。
4. `last_error` 必须为空。
5. `last_tick.skipped` 若为 `trade-day plan already exists for today`，说明已有当日任务，不代表停机。

### 2. 检查 data 更新与快照

`data` 服务负责晚间自动更新，策略服务只在盘前 freshness 不足时定向补关键数据。

```bash
uv run vortex data status \
  --root ../vortex_workspace \
  --format json
```

检查：

1. `latest_run.status == success`。
2. 交易日的 `latest_snapshot.as_of` 应覆盖最近一个已完成交易日。
3. 非交易日更新可能 `quality_status=skipped`、`snapshot_id=null`，这是正常低频更新，不等于失败。
4. 策略执行前关键数据至少覆盖 `strategy_as_of`：`bars`、`valuation`、`stk_limit`、`stock_st`。

### 3. 找到当日 target、task、exec

```bash
TODAY=YYYYMMDD
ROOT=../vortex_workspace

find "$ROOT/trade/targets/$TODAY" -maxdepth 1 -type f -print | sort
find "$ROOT/state/trade" -maxdepth 5 -type f \
  \( -name "*$TODAY*" -o -path "*/$TODAY/*" \) -print | sort
find "$ROOT/trade/executions" -maxdepth 2 -type f \
  \( -name "execution_report.json" -o -name "reconcile_report.json" \) -print | sort
```

正常至少应有：

1. `trade/targets/YYYYMMDD/tp_*.json`
2. `state/trade/pending_qmt/YYYYMMDD-tp_*.json` 或迁移后的任务记录
3. `trade/executions/exec_*/execution_report.json`

注意：当前实现可能出现任务内容 `status=done`，但文件仍留在 `pending_qmt/` 的情况。此时不能只按目录名判断 pending，要读取 JSON 内部 `status`、`exec_id` 和 `execution_report_path`。

### 4. 解析目标与诊断

目标文件检查：

1. `trade_date` 是否为当天。
2. `strategy_version` 是否为 `baseline_top110_large`。
3. `positions` 数量、目标股数、目标现金是否合理。
4. 是否出现 ST 股票；出现则直接列为异常。

任务文件检查：

1. `status`、`created_at`、`updated_at`。
2. `as_of` 与 `requested_as_of`：执行日可能是今天，但策略信号日通常是最近一个已完成交易日。
3. `target_diagnostics.data_freshness.status == ok`。
4. `desired_top_n`、`eligible_signal_count`、`final_position_count`、`shortfall_reason`。
5. `skipped_counts`，重点看 `st`、`market_rule`、`market_permission`、`unaffordable`。

### 4.1 打印每日策略观察指标

每日巡检必须把“为什么持仓/为什么空仓”单独说清楚，不要只说下单成功或失败。

重点读取 `target_diagnostics`：

1. `market_gate.risk_on`：是否允许持仓；为 `false` 时应解释为空仓或降仓原因。
2. `market_gate.benchmark`：市场门控使用的基准指数。
3. `market_gate.benchmark_momentum`：基准动量。
4. `market_gate.benchmark_above_support`：是否仍在支撑线之上。
5. `market_gate.risk_on_confirmations` 与 `required_confirmations`：确认指数数量是否达标。
6. `selection_funnel`：候选漏斗，每层过滤后还剩多少股票。

候选漏斗字段按下面顺序打印：

| 字段 | 含义 |
|---|---|
| `raw_signal_count` | 当日业绩预告事件信号数量。 |
| `positive_signal_count` | 正向信号数量；当前等同于进入多头候选的原始正向事件。 |
| `after_liquidity_count` | 流动性过滤后数量。 |
| `after_st_filter_count` | ST/风险警示过滤后数量。 |
| `after_market_cap_top50_count` | A 股总市值前 50% 过滤后数量。 |
| `after_open_block_count` | 涨停/停牌等买入阻断后数量。 |
| `after_quality_block_count` | 持仓质量审查 blocked 后数量。 |
| `after_permission_count` | 账户市场权限过滤后数量。 |
| `executable_candidate_count` | 资金、整手、科创板 200 股等执行规则后可买数量。 |
| `selected_position_count` | 最终目标持仓数量。 |

`shortfall_reason` 解释：

| 原因 | 含义 |
|---|---|
| `market_gate_off` | 市场门控关闭，指数下行或确认不足，应空仓/不新增。 |
| `market_cap_filter_shortfall` | 市值前 50% 之后已不足 TopN。 |
| `open_block_shortfall` | 涨停、停牌等开盘交易约束后不足 TopN。 |
| `quality_filter_shortfall` | 持仓质量 blocked 后不足 TopN。 |
| `market_permission_shortfall` | 账户市场权限过滤后不足 TopN。 |
| `execution_rule_shortfall` | 资金、整手、最低订单金额、科创板最低申报等执行规则后不足 TopN。 |
| `no_positive_signal_candidates` | 当日没有正向业绩预告候选。 |

### 5. 读取本地执行报告

```bash
uv run vortex trade inspect \
  --root ../vortex_workspace \
  --exec-id <exec_id> \
  --format json
```

本地报告用于看当时下单快照：

1. `risk_passed` 是否为 `true`。
2. `order_count`、`fill_count`、`unfilled_summary`。
3. `risk_result.blocking_reasons` 是否为空。
4. `orders` 的状态是否有 open、rejected、cancelled。

限制：本地 `execution_report.json` 可能是提交订单后立即生成的快照，**不一定包含后续成交回报**。盘中巡检必须继续查 QMT 当前回报。

### 6. 直接查 QMT bridge 当前委托、成交、持仓

只读查询示例：

```bash
python3 - <<'PY'
import json
import urllib.request

base = "http://<windows-ip>:8000"
token = "<token>"
account_id = "<account_id>"
headers = {"Authorization": f"Bearer {token}", "X-API-Key": token}

for name, endpoint in {
    "cash": f"/api/trading/asset?account_id={account_id}",
    "positions": f"/api/trading/positions?account_id={account_id}",
    "orders": f"/api/trading/orders?account_id={account_id}",
    "fills": f"/api/trading/trades?account_id={account_id}",
}.items():
    req = urllib.request.Request(base + endpoint, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=8) as response:
        print("\n##", name)
        print(json.dumps(json.loads(response.read().decode()), ensure_ascii=False, indent=2)[:12000])
PY
```

必须以 QMT 当前回报为准判断：

1. `orders.order_type == 24` 是卖出，`23` 是买入；不要用 `direction=48` 推断买卖。
2. `order_status == 56` 通常表示已成；`traded_volume` 和 `traded_price` 才是成交证据。
3. `fills` 中必须有成交编号、成交价、成交量、成交金额。
4. 持仓的 `can_use_volume` 可能因未成交卖单冻结为 0。
5. 若本地 report 与 QMT 当前回报不一致，结论以 QMT 为准，同时标记“本地执行报告未刷新”。

### 7. 未成交与异常处理

未成交不能简单视为失败，需要分类：

| 类型 | 判断 | 处理 |
|---|---|---|
| ST 清理未成交 | 卖出 ST、`traded_volume=0`、持仓仍在 | 继续跟踪或人工撤改单；不允许买入补仓。 |
| 小额减仓未成交 | 例如 100 股卖单未成 | 盘中可等待；收盘前仍未成则记录现金/持仓偏差。 |
| 跌停卖不出 | 卖单当日跌停 | 记录为交易物理约束，不要强行判策略失败。 |
| 废单/拒单 | 有拒单状态或错误信息 | 需人工处理，必要时停机。 |
| 本地报告陈旧 | QMT 有成交但 report 没有 | 标记对账问题，后续应刷新执行报告或补 reconciliation。 |

---

## 三、今日审计输出模板

每次巡检按这个格式回复：

1. **一句话结论**：正常完成 / 部分完成 / 需人工处理 / 自动链路异常。
2. **服务状态**：auto-run PID、last_tick、health、data server、latest data run。
3. **目标生成**：target 文件、信号 as_of、目标持仓数、TopN 缺口原因、ST/权限/市值过滤诊断。
4. **执行情况**：exec_id、订单数、风控结果、提交订单列表。
5. **QMT 当前回报**：委托状态、成交列表、未成交列表、当前持仓与现金。
6. **策略净值**：若已启用 `vortex trade nav`，打印子账本权益、账户总资产、外部资金偏移、最新净值、基准净值、近一周/一月/六月超额。
7. **差异与风险**：本地 report vs QMT 当前回报差异、未成交原因、是否需要人工撤单/改价/继续等待。
8. **下一步动作**：只给必要动作，不做无依据优化。

### 8. 净值台账检查

策略净值统计属于框架能力，不属于业绩预告策略 alpha 本身。默认口径是“账户子账本”：一个 QMT 账户只运行一个自动策略，但账户里可能有超过策略名义本金的闲置现金。首条快照会锁定 `external_cash_offset = account_total_asset - initial_equity`，后续用 `strategy_equity = account_total_asset - external_cash_offset` 计算策略净值。

```bash
uv run vortex trade nav status \
  --root ../vortex_workspace \
  --strategy-name earnings_forecast_auto \
  --strategy-version baseline_top110_large \
  --qmt-account-id <account_id> \
  --format json
```

`earnings-forecast auto-run` 在成功执行 QMT 调仓后会自动记录当日快照。若当日没有 pending 执行任务、QMT 资产接口临时不可用，或需要补记历史日期，再手工运行：

```bash
uv run vortex trade nav snapshot \
  --root ../vortex_workspace \
  --strategy-name earnings_forecast_auto \
  --strategy-version baseline_top110_large \
  --initial-equity 1000000 \
  --benchmark 000852.SH \
  --qmt-bridge-url http://<windows-ip>:8000 \
  --qmt-bridge-token <token> \
  --qmt-account-id <account_id> \
  --format json
```

巡检时重点看：

1. `binding.nav_mode == account_subledger`。
2. `binding.external_cash_offset`：锁定的非策略资金偏移。
3. `summary.latest_total_asset`：策略子账本权益，不是 QMT 全账户总资产。
4. `summary.latest_account_total_asset`：QMT 全账户总资产。
5. `summary.latest_net_value`：`latest_total_asset / initial_equity`。

连续性约定：

1. 同一账户、同一策略、同一初始资金，服务短暂停止后重启继续同一净值曲线。
2. 如果其余资金没有被动用，继续沿用首条快照锁定的外部资金偏移。
3. 如果停机期间发生手工调仓或外部资金被动用，第一版仍按子账本净值延续，但必须标记并说明外部持仓漂移或现金流风险。
4. 只有显式 `--reset`、更换账户、或用户决定开启新策略 run，才新开净值序列。

---

## 四、硬性停机条件

出现以下任一情况，不能继续自动下单：

1. `stock_st`、`stk_limit`、`suspend_d`、`bars`、`valuation` 对策略 `as_of` 不新鲜。
2. QMT bridge 不可用或账户 ID 不一致。
3. 目标组合包含 ST 主动买入。
4. 风控 `risk_passed=false`。
5. 委托/成交状态无法解释，或本地记录与 QMT 回报持续不一致。
6. 存在未处理的拒单、废单、权限不足或资金不足。
7. 当前有未成交卖单冻结可用股份，仍尝试重复卖出同一股份。

---

## 五、常用判断语

- “auto-run tick 成功只说明编排器没崩，不等于订单成交。”
- “本地 execution_report 是证据之一，但 QMT 当前 orders/fills 才是成交事实。”
- “ST 清理卖单允许存在，但 ST 主动买入必须阻断。”
- “任务文件在 pending_qmt 目录里不一定代表 pending，必须读取 JSON 内的 status。”
- “未成交不是策略收益问题，先归因到价格、涨跌停、冻结、权限或柜台状态。”
