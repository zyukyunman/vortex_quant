---
tags: [vortex, vortex/architecture, vortex/trade-domain]
aliases: [Mac虚拟机QMT实盘桥接方案, QMT桥接方案, MiniQMT实盘接入方案]
created: 2026-05-02
updated: 2026-05-02
---

# Mac 虚拟机 QMT 实盘桥接方案

## 1. 背景与目标

当前策略研究已经进入 shadow trading 和小资金模拟盘前的阶段。用户主力设备是 Mac，但 MiniQMT / QMT / xtquant 的实际运行环境通常依赖 Windows 客户端。因此实盘化不能设计成“Mac 直接 import xtquant 下单”，而应设计成：

```text
Mac 主机：Vortex 研究、策略、风控、报告
Windows 虚拟机：国金 QMT / MiniQMT、xtquant、桥接服务
Mac ↔ Windows：HTTP / WebSocket / RPyC 远程调用
```

目标不是立刻自动实盘，而是先打通国金模拟盘账户，在模拟盘中验证行情、资产、持仓、委托、成交、拒单、滑点和隔夜段归因。

## 2. 调研结论

### 2.1 MiniQMT / xtquant 限制

调研 `miniqmt.com` 和 xtquant 文档后，关键约束如下：

1. 使用 xtquant 通常需要 QMT / MiniQMT 客户端保持登录。
2. 实盘交易需要券商开通权限、协议和可能的白名单。
3. MiniQMT 不适合作为回测引擎；Vortex 继续负责研究和回测。
4. Mac 不作为 xtquant 的直接运行环境，Windows 环境才是主路径。

### 2.2 GitHub 项目对比

| 项目 | 当前热度 | 定位 | 使用建议 |
|---|---:|---|---|
| `shidenggui/easytrader` | 约 9670 stars / 2947 forks | 老牌交易适配，支持 MiniQMT、同花顺、雪球和远程服务 | 作为成熟交易接口参考 |
| `quant-king299/EasyXT` | 约 355 stars / 133 forks | xtquant 二次封装、QMT 自动登录、101 因子、回测工具 | 作为 QMT 简化封装和因子资料参考 |
| `jasonhu/xqshare` | 约 41 stars / 9 forks | Windows 运行 xtquant，Mac/Linux 透明远程调用 | 作为 RPyC 透明调用 POC 候选 |
| `atompilot/qmt-bridge` | 约 39 stars / 15 forks | FastAPI HTTP/WebSocket 桥接 xtquant | 作为 HTTP 桥接 POC 首选 |
| `happybeta/xtquantservice` | 约 38 stars / 7 forks | miniQMT 服务 API | 作为 HTTP/API 后备候选 |

选型结论：

1. 如果要求现成 HTTP 接口，优先 `qmt-bridge`，后备 `xtquantservice`、`qmt2http`、`quant-qmt-proxy`。
2. 如果要求 Mac 像本地一样调用 xtquant，优先 `xqshare`。
3. `easytrader` 成熟度最高，但范围更广，适合作为接口语义和交易流程参考。
4. `EasyXT` 可参考自动登录、简化 API 和 101 因子资料，但不直接并入 Vortex。

## 3. 推荐落地架构

```text
Mac 主机
  vortex strategy earnings-forecast shadow-plan
  目标持仓、订单草案、风控、报告
  ↓
BridgeAdapter
  ↓ HTTP / RPyC
Windows 虚拟机
  国金 QMT / MiniQMT，保持登录
  xtquant
  qmt-bridge / xqshare / easytrader remote
  ↓
国金模拟盘 / 真实账户
```

Vortex 不自研 QMT HTTP 服务，也不把第三方桥接项目复制进仓库。Vortex 只保留薄适配器和统一的风控、对账、归因模型。

## 4. 环境搭建手册

本节面向“Mac 本机研究 + Windows 虚拟机接 QMT/MiniQMT”的第一版 POC。它的目标不是一次性完成自动实盘，而是先让使用者可以稳定完成：

```text
Mac Vortex → Windows VM bridge → QMT/MiniQMT → 国金模拟盘
```

所有下单验证必须先使用国金模拟盘。真实账户只允许在模拟盘连续稳定后、并加入人工确认和风控拦截后再进入。

### 4.1 总体拓扑

| 位置 | 组件 | 职责 |
|---|---|---|
| Mac 宿主机 | Vortex、研究脚本、回测报告、目标持仓生成 | 负责研究、风控、订单草案和复盘 |
| Windows 虚拟机 | 国金 QMT / MiniQMT、xtquant、桥接服务 | 负责登录券商客户端、查询账户、提交模拟盘委托 |
| 局域网连接 | HTTP / WebSocket / RPyC | 负责 Mac 到 Windows 的远程调用 |

为什么这样拆分：xtquant 与 QMT/MiniQMT 的运行依赖 Windows 客户端登录状态；Mac 更适合做研究、报告和自动化控制，不应承担券商客户端兼容性风险。

### 4.2 Mac 本机准备

Mac 侧只需要运行 Vortex 和访问桥接服务，不需要安装 QMT 或 xtquant。

```bash
cd /Users/zyukyunman/Documents/vortex_quant
uv sync
uv run python -m pytest tests/ -q
```

Mac 侧必须记录以下信息，后续配置桥接适配器会用到：

| 配置项 | 示例 | 说明 |
|---|---|---|
| `QMT_BRIDGE_BASE_URL` | `http://192.168.1.88:8000` | Windows VM 桥接服务地址 |
| `QMT_BRIDGE_TOKEN` | 自定义强随机字符串 | 交易接口鉴权令牌 |
| `QMT_ACCOUNT_TYPE` | `SIM` | 第一阶段固定为模拟盘 |
| `VORTEX_WORKSPACE` | `/Users/.../vortex_workspace` | Vortex 研究产物目录 |

### 4.3 Windows 虚拟机准备

优先使用 Parallels Desktop，其次 VMware Fusion 或 UTM。Apple Silicon Mac 运行 Windows ARM 时，要特别验证国金 QMT/MiniQMT 的登录控件、加密模块和 Python/xtquant 兼容性。

建议配置：

| 项目 | 建议 |
|---|---|
| CPU | 4 核以上 |
| 内存 | 8GB 以上 |
| 磁盘 | 80GB 以上 |
| 网络 | 优先桥接模式，失败时使用 NAT + 端口转发 |
| 电源 | 交易时段禁止休眠 |

虚拟机系统检查：

```powershell
python --version
ipconfig
netsh advfirewall show currentprofile
```

如果 Windows ARM 无法稳定运行国金 QMT/MiniQMT，优先改用独立 Windows 小主机，而不是继续在 Mac 上硬凑兼容层。

### 4.4 国金 QMT / MiniQMT 安装

Windows VM 内按以下顺序执行：

1. 安装国金 QMT / MiniQMT。
2. 申请并登录国金模拟盘账户。
3. 首次登录完成验证码、协议确认、插件初始化。
4. 选择极简模式 / 独立交易模式，减少 UI 资源占用。
5. 保持客户端在线，不要最小化到被系统自动冻结的状态。
6. 找到 `userdata_mini` 路径，并记录绝对路径。
7. 在 QMT 客户端内验证行情、资产、持仓、当日委托和当日成交页面可用。

`userdata_mini` 是 xtquant 连接 QMT 的关键路径。不同券商和安装目录可能不同，常见位置类似：

```text
C:\国金证券QMT\userdata_mini
D:\国金证券QMT\userdata_mini
```

不要把账号密码、交易密码、Token 写入仓库。后续配置一律放在本机环境变量或未纳入版本控制的本地配置文件中。

### 4.5 xtquant 验证

在 Windows VM 内使用与 QMT 兼容的 Python 环境验证 xtquant。具体 Python 版本以国金 QMT/MiniQMT 随附说明为准，不要强行升级到最新版本。

最小验证脚本：

```python
from xtquant import xtdata

print("xtquant import ok")
print(xtdata.get_trading_dates("SH", "20240101", "20240131"))
```

如果 `import xtquant` 失败，优先检查：

1. 当前 Python 是否与 QMT 自带 xtquant 匹配。
2. `PYTHONPATH` 是否包含 QMT 的 xtquant 目录。
3. QMT/MiniQMT 是否已经登录。
4. Windows 是否缺少运行库。

只有 xtquant 本地查询可用后，才进入桥接服务安装；否则 Mac 侧任何 HTTP/RPyC 调用都会失败。

### 4.6 qmt-bridge 首选安装路径

`qmt-bridge` 是第一版 POC 首选，因为它提供 HTTP/WebSocket 与 Swagger，更符合 Vortex 后续 `BrokerAdapter` 的薄适配方向。

Windows VM 内执行：

```powershell
cd C:\work
git clone https://github.com/atompilot/qmt-bridge.git
cd qmt-bridge
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

启动前确认 QMT/MiniQMT 已登录，然后启动服务。具体启动命令以项目 README 为准；如果项目提供 `qmt-server` 或 FastAPI 入口，优先使用项目推荐入口：

```powershell
python -m qmt_bridge
```

本机验证：

```powershell
curl http://127.0.0.1:8000/docs
curl http://127.0.0.1:8000/api/meta/health
```

Mac 验证：

```bash
curl http://<Windows虚拟机IP>:8000/docs
curl http://<Windows虚拟机IP>:8000/api/meta/health
```

如果 Windows 本机可访问但 Mac 不可访问，优先排查网络和防火墙，不要修改 Vortex 代码。

### 4.7 xqshare 备选路径

如果 `qmt-bridge` 的接口覆盖不足或服务不稳定，再尝试 `xqshare`。它的定位是让 Mac/Linux 透明远程调用 Windows 上的 xtquant，适合验证“Mac 侧像本地一样调用 xtquant”的方案。

Windows VM 内：

```powershell
cd C:\work
git clone https://github.com/jasonhu/xqshare.git
cd xqshare
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
```

Mac 侧验证重点不是 Swagger，而是能否从远端拿到行情、资产和持仓。若后续选择 `xqshare`，Vortex 侧适配器应命名为 `XqshareAdapter`，与 `QmtBridgeAdapter` 并列，不要把两套第三方接口混在一个类里。

### 4.8 网络桥接与防火墙

优先使用桥接网络，让 Windows VM 获得局域网 IP：

```powershell
ipconfig
```

Mac 上确认连通：

```bash
ping <Windows虚拟机IP>
curl http://<Windows虚拟机IP>:8000/api/meta/health
```

Windows 防火墙只放行桥接服务端口，例如 8000：

```powershell
New-NetFirewallRule `
  -DisplayName "QMT Bridge 8000" `
  -Direction Inbound `
  -Action Allow `
  -Protocol TCP `
  -LocalPort 8000
```

安全边界：

1. 桥接服务只允许局域网或 Mac 宿主机访问。
2. 不做公网端口映射。
3. 交易端点必须有 Token 或 API Key。
4. 第一阶段禁用真实账户下单。
5. 如果桥接服务不支持鉴权，不允许暴露写交易接口，只能做只读 POC。

### 4.9 健康检查清单

进入模拟盘 POC 前，逐项检查：

| 检查项 | 命令或动作 | 通过标准 |
|---|---|---|
| QMT 登录 | 查看 Windows QMT 客户端 | 模拟盘账号在线 |
| xtquant | Windows Python `import xtquant` | 无异常 |
| 本机桥接 | Windows `curl 127.0.0.1:8000` | 返回健康状态 |
| 跨机桥接 | Mac `curl <VM-IP>:8000` | 返回健康状态 |
| 行情 | 查询 `000001.SZ` 或同类股票 | 有最新行情 |
| 资产 | 查询资金 | 返回可用资金、总资产 |
| 持仓 | 查询持仓 | 返回列表，可为空 |
| 委托 | 查询当日委托 | 返回列表，可为空 |
| 成交 | 查询当日成交 | 返回列表，可为空 |
| 模拟下单 | 100 股限价单 | 可提交、可撤单或收到明确拒单 |

这张表全部通过后，才允许把 Vortex 的订单草案接入模拟盘；任何一项失败，都应 fail-closed（失败即停止），不继续下单。

### 4.10 常见失败与处理

| 现象 | 常见原因 | 处理 |
|---|---|---|
| QMT 无法安装或登录 | Windows ARM / 券商控件不兼容 | 改用独立 Windows 小主机 |
| `import xtquant` 失败 | Python 版本或路径不匹配 | 使用 QMT 推荐 Python，补 `PYTHONPATH` |
| Mac 访问不到桥接服务 | NAT、防火墙或服务只监听 127.0.0.1 | 改桥接网络，放行端口，服务监听 `0.0.0.0` |
| 能查行情但不能交易 | 模拟盘权限、账号类型或交易协议未开通 | 在 QMT 客户端确认权限 |
| 下单无回报 | WebSocket/回调未接通或 QMT 掉线 | 先查当日委托/成交，再查桥接日志 |
| 盘中突然断线 | 虚拟机休眠、QMT 被踢、网络切换 | 禁止休眠，固定网络，增加心跳检查 |

### 4.11 POC 交付物

环境 POC 完成后，应至少保存以下本地产物，作为后续 Vortex 适配器开发输入：

```text
bridge_health.json
account_snapshot.json
positions_snapshot.json
orders_snapshot.json
fills_snapshot.json
paper_order_lifecycle.md
```

这些文件不要包含账号密码、Token、身份证号、手机号等敏感信息。它们的用途是让后续适配器开发明确字段名、状态枚举、错误码和交易生命周期，而不是泄露账户信息。

如果 `qmt-bridge` 不稳定，再依次尝试：

1. `xtquantservice`。
2. `xqshare`。
3. `easytrader` remote server。

## 5. 网络与安全

1. 虚拟机网络优先桥接模式，让 Windows VM 有局域网 IP。
2. 桥接模式不可用时，用 NAT + 端口转发。
3. Windows 防火墙只放行必要端口。
4. 不暴露公网，只允许 Mac 宿主机或局域网访问。
5. 交易端点必须启用 API Key / Token。
6. 第一阶段只读，不允许真实下单。
7. 第二阶段只在国金模拟盘下单。
8. 第三阶段真实账户必须人工确认。

## 6. POC 验收标准

| 编号 | 检查项 | 通过标准 |
|---:|---|---|
| 1 | 健康检查 | Mac 能访问 Windows VM 桥接服务 |
| 2 | QMT 登录 | QMT/MiniQMT 保持登录 |
| 3 | 行情 | 能查询至少一只股票实时行情 |
| 4 | 资产 | 能查询账户资产 |
| 5 | 持仓 | 能查询账户持仓 |
| 6 | 委托 | 能查询当日委托 |
| 7 | 成交 | 能查询当日成交 |
| 8 | 模拟下单 | 国金模拟盘可提交 100 股限价委托 |
| 9 | 回报 | 能收到成交、部成、撤单或拒单原因 |
| 10 | 稳定性 | 连续一个完整交易日不掉线 |

未通过全部 POC 前，不进入真实账户。

## 7. 开盘价假设与执行验证

当前业绩预告策略的小资金整手结果，是按“公告后下一交易日开盘价成交”测算的。这个口径适合研究，但实盘必须验证：

1. 开盘涨停是否买不到。
2. 集合竞价量是否足够。
3. 高开 3%、5%、8% 后是否仍有收益。
4. 实际成交价相对开盘价的滑点。
5. 未成交、部分成交和排队失败。

在没有分钟数据前，先做日频折损测试：

```text
开盘涨停：不成交
高开超过阈值：跳过 / 降权 / 加滑点
成交额不足：部分成交
```

模拟盘上线后，再用真实委托和成交回报验证开盘价假设是否过于乐观。

### 7.1 分钟级执行验证口径

分钟级验证只保留两条正式路线：

1. 使用券商模拟盘/实盘回报沉淀真实成交与盘口约束。
2. 使用正式分钟数据源做离线复盘和开盘执行折损测算。

无论采用哪条路线，都必须在报告中明确覆盖率、缺失原因与样本范围，避免把“接口不可用”误解为“执行安全”。

## 8. 与 Vortex 的边界

Vortex 负责：

1. 生成目标持仓。
2. 生成订单草案。
3. 做风控检查。
4. 调用桥接服务查询账户和行情。
5. 记录委托、成交、拒单、未成交。
6. 拆分 open→close、close→next open、成本、滑点、未成交和现金拖累。
7. 生成 shadow / 模拟盘 / 实盘复盘报告。

第三方桥接服务负责：

1. 连接 QMT/MiniQMT。
2. 调用 xtquant。
3. 暴露 HTTP / RPyC / WebSocket。
4. 返回底层委托、成交和账户数据。

## 9. Vortex 侧适配器设计

Vortex 不直接绑定某一个桥接项目，而是通过统一的 `BrokerAdapter` 语义隔离第三方接口差异。第一版只要求设计和 shadow / 模拟盘可用，不要求覆盖融资融券、期权或复杂条件单。

### 9.1 BrokerAdapter 最小接口

| 方法 | 作用 | 第一阶段要求 |
|---|---|---|
| `health()` | 检查桥接服务、QMT 登录和账号状态 | 必须 |
| `get_cash()` | 查询可用资金、冻结资金、总资产 | 必须 |
| `get_positions()` | 查询持仓、可用股数、冻结股数、市值 | 必须 |
| `get_orders()` | 查询当日委托 | 必须 |
| `get_fills()` | 查询当日成交 | 必须 |
| `get_quotes(symbols)` | 查询实时行情或快照 | 必须 |
| `submit_order(intent)` | 提交订单 | 仅模拟盘阶段启用 |
| `cancel_order(order_id)` | 撤单 | 仅模拟盘阶段启用 |

订单统一用 `OrderIntent` 表达：

| 字段 | 说明 |
|---|---|
| `symbol` | A 股代码，例如 `000001.SZ` |
| `side` | `buy` / `sell` |
| `shares` | 股数，必须是 100 股整数倍 |
| `price_type` | `limit` / `market` / 桥接项目支持的价格类型 |
| `limit_price` | 限价价格；市价单可为空 |
| `reason` | 调仓、清仓、风控卖出等原因 |
| `strategy_version` | 策略版本 |
| `run_id` | 当日运行 ID |

### 9.2 PaperBrokerAdapter

`PaperBrokerAdapter` 在 Mac 本地运行，不连接 QMT。它用于在有真实桥接服务前，先验证订单生成、整手约束、未成交和收益归因。

当前代码入口：

```text
vortex/trade/broker.py
vortex/trade/models.py
vortex/trade/serialization.py
vortex/trade/target_portfolio.py
vortex/trade/order_plan.py
vortex/trade/risk.py
vortex/trade/execution.py
vortex/trade/reconcile.py
tests/test_trade_broker.py
```

已支持：

1. `BrokerHealth`、`CashSnapshot`、`Position`、`Quote`、`OrderIntent`、`OrderRecord`、`FillRecord` 基础数据结构。
2. 本地现金、持仓、订单、成交记录。
3. 100 股整手校验。
4. 停牌拒单。
5. 涨停不买、跌停不卖。
6. 限价买卖价格校验。
7. 现金不足或成交量参与率约束下的部分成交。
8. 交易禁用开关，便于只读阶段 fail-closed。
9. `TargetPortfolio`、`OrderPlan`、`RiskCheckResult`、`ExecutionReport`、`ReconcileReport` 标准工件。
10. `target_portfolio → order_plan → pre_trade_risk → order_intent → paper broker → execution_report` 本地闭环。
11. 日终 reconcile，对比现金、持仓、订单和成交差异。

第一版规则：

1. 输入目标持仓和本地行情。
2. 先卖后买。
3. 买卖数量向下取 100 股。
4. 开盘涨停不买。
5. 开盘跌停不卖。
6. 停牌不交易。
7. 高开超过阈值时可配置为跳过、降权或额外滑点。
8. 成交额不足时按参与率上限模拟部分成交。
9. 每日保存现金、持仓、订单、成交、未成交和收益归因。

它回答的问题是：

```text
在比研究回测更保守的开盘执行假设下，策略还能剩多少收益？
```

### 9.3 QmtBridgeAdapter / XqshareAdapter

真实桥接适配器只做很薄的一层映射：

当前代码入口：

```text
vortex/trade/qmt_bridge.py
tests/test_trade_qmt_bridge.py
```

当前实现是只读优先：

1. `health()`
2. `get_cash()`
3. `get_positions()`
4. `get_orders()`
5. `get_fills()`
6. `get_quotes()`
7. `submit_order()` 默认 `allow_trading=False`，直接拒绝。
8. `cancel_order()` 在交易禁用时抛 `PermissionError`。

真实端点路径通过 `QmtBridgeConfig.endpoints` 配置。由于不同 bridge 项目的 URL 和字段可能不同，第一版测试使用 fake transport；等 Windows VM / QMT / bridge 可用后，再按 POC 清单校准字段映射。

| Vortex 语义 | qmt-bridge HTTP | xqshare / xtquant 语义 |
|---|---|---|
| `health()` | `/api/meta/health` 或项目等价接口 | 远程连接状态 |
| `get_quotes()` | 行情快照接口 | `xtdata.get_full_tick` |
| `get_cash()` | 资产接口 | `query_stock_asset` |
| `get_positions()` | 持仓接口 | `query_stock_positions` |
| `get_orders()` | 当日委托接口 | `query_stock_orders` |
| `get_fills()` | 当日成交接口 | `query_stock_trades` |
| `submit_order()` | 交易下单接口 | `order_stock` / `order_stock_async` |
| `cancel_order()` | 撤单接口 | `cancel_order_stock` |

第一版 `QmtBridgeAdapter` 必须默认只读。只有配置显式开启模拟盘交易时，才允许调用 `submit_order()`。

## 10. Shadow / 模拟盘每日报告模板

每个交易日必须输出一份机器可读 JSON 和一份人可读 Markdown/HTML。报告不是展示收益曲线，而是验证真实执行。

### 10.1 报告结构

1. **运行摘要**
   - 日期、策略版本、run_id、数据更新时间、桥接服务状态。
2. **目标组合**
   - 目标股票、目标权重、目标市值、目标股数。
3. **当前账户**
   - 现金、总资产、当前持仓、可用股数、冻结股数。
4. **订单草案**
   - 买卖方向、股数、参考价、限价、原因。
5. **风控结果**
   - 涨停、跌停、停牌、ST、资金不足、持仓不足、单票金额、日交易额。
6. **实际委托**
   - 委托编号、状态、委托价、委托量、已成交量、撤单状态。
7. **实际成交**
   - 成交价、成交量、成交金额、成交时间。
8. **未成交与拒单**
   - 未成交数量、拒单原因、是否需要次日处理。
9. **收益归因**
   - open→close、close→next open、成本、滑点、未成交、现金拖累。
10. **结论**
   - 今日是否通过执行验证，是否允许进入下一阶段。

### 10.2 fail-closed 条件

出现以下情况，当日不允许自动下单：

1. QMT/MiniQMT 未登录。
2. 桥接服务健康检查失败。
3. 账户资产或持仓查询失败。
4. 目标组合生成失败。
5. 涨跌停、停牌或 ST 数据缺失。
6. 当前账户持仓和 Vortex 账本不一致。
7. 单日计划交易金额超过阈值。
8. 任何订单参数无法映射到桥接项目支持的价格类型。

## 11. 当前主线的重新聚焦

在 recent 联调中，一个容易跑偏的点是把 `qmt-bridge` 的“全部接口”当成了实盘 readiness 的验收目标。这里需要把边界重新钉死：

1. `/api/download/*` 属于**服务端数据预下载 / 本地缓存预热接口**，用于拉历史行情、财务、板块成分、指数权重、ETF 信息、节假日历等研究或缓存数据。
2. 这些接口可能影响 bridge 进程稳定性，但**不是 Vortex 实盘执行链路的核心依赖**。
3. 当前 Vortex 的 shadow / status / quote / live-handoff 主线只依赖：
   - `/api/meta/health`
   - `/api/meta/connection_status`
   - `/api/market/full_tick`
   - `/api/trading/asset`
   - `/api/trading/positions`
   - `/api/trading/orders`
   - `/api/trading/trades`
   - 交易日真实验证时再加 `/api/trading/order` 与 `/api/trading/cancel`
4. 因此，当前推进重点不是“补齐 bridge server 的所有功能”，而是围绕 [[交易域设计说明书]] 和 [[业绩预告漂移策略研究总结]]，确认：
   - 对模拟账户，是否已经具备“盘前 dry-run + 当天最小模拟盘验证”的条件
   - 还差哪些交易日验证，才能稳定升级到 `可模拟盘`

## 12. 交易日验证 Runbook（模拟账户默认走 dry-run + 模拟盘）

本节是当前最重要的执行清单。只要 bridge 核心读接口稳定，交易日就按下面顺序做，不再被 `/api/download/*` 分散注意力。

对于**模拟账户**，这里不要求先单独经历一个“只看不下”的独立 shadow 阶段，而是把 shadow 语义折叠成**盘前 dry-run**：

1. 开盘前先生成目标持仓、订单草案和账户快照；
2. 随后直接进入**最小规模模拟盘委托验证**；
3. 收盘后立即做对账和执行归因。

也就是说，当前推荐路径不是：

```text
连续多日 shadow → 再模拟盘
```

而是：

```text
盘前 dry-run → 当天模拟盘最小下单 → 收盘后对账
```

这样既保留了 shadow 的“下单前预演”价值，又不会把模拟账户推进节奏拖慢。

### 12.1 开盘前（08:45-09:20）

| 步骤 | 动作 | 通过标准 | 失败处理 |
|---|---|---|---|
| 1 | 确认 Windows VM、QMT/MiniQMT、bridge 进程都在线 | `health=ok`，QMT 已登录 | fail-closed，当日不下单 |
| 2 | 运行 `vortex strategy earnings-forecast live-handoff` | 生成目标持仓、交易变化、账户快照 | 作为盘前 dry-run；通过后可进入最小模拟盘验证 |
| 3 | 运行 `vortex trade status` | 现金、持仓、委托、成交可读 | fail-closed |
| 4 | 运行 `vortex trade quote` 拉目标股票行情 | 目标股票报价可读，缺失数量可解释 | 缺失则降级为人工检查 |
| 5 | 对照订单草案检查风控 | 涨跌停、停牌、ST、整手、现金约束全部明确 | 任一不明则 fail-closed |

### 12.2 开盘阶段（09:20-09:35）

重点不是“能不能发单”，而是验证开盘成交假设是否成立。

| 检查项 | 要回答的问题 | 记录字段 |
|---|---|---|
| 开盘涨停 | 买入标的是否开盘即封板、理论上买不到 | `is_limit_up`、是否跳过 |
| 高开幅度 | `open / prev_close - 1` 是否超过 3%、5%、8% 阈值 | `gap_pct` |
| 集合竞价量 | 目标股数相对开盘可成交量是否过大 | `auction_volume_ratio` |
| 部分成交 | 若流动性不足，是否部分成交 | `filled_shares`、`remaining_shares` |
| 实际滑点 | 实际成交价相对计划开盘价偏离多少 | `slippage_bps` |

### 12.3 委托生命周期验证（09:30-15:00）

模拟盘阶段必须把以下对象都落成结构化记录，不能只看“下单成功”：

1. `OrderIntent`：策略订单草案
2. `OrderSubmitted`：桥接实际报单结果
3. 委托状态：已报、部成、已成、已撤、废单
4. 成交回报：成交价、成交量、成交金额、成交时间
5. 拒单原因：非交易时间、价格非法、资金不足、权限不足、涨跌停、停牌
6. 撤单结果：是否成功，剩余股数多少

推荐最小验证动作：

1. 先挑 **1-2 个低风险、低金额标的** 做模拟盘委托。
2. 至少验证一次“可成交订单”与一次“需要撤单或部分成交的订单”。
3. 如果 bridge / QMT / 网络任一环节状态不确定，立即停止新增订单。

### 12.4 收盘后对账（15:00 以后）

收盘后要回答的不是“今天赚没赚”，而是“策略计划与执行结果差了多少、为什么差”。

| 对账项 | 说明 |
|---|---|
| 账户现金 | QMT 可用现金 vs Vortex 记录 |
| 持仓 | QMT 持仓股数 vs Vortex 账本 |
| 委托 | 当日委托总数、状态分布 |
| 成交 | 成交价、成交量、成交金额、手续费 |
| 未成交 | 哪些目标仓位未达成，原因是什么 |
| 收益归因 | `open→close`、`close→next open`、成本、滑点、未成交、现金拖累 |

### 12.5 升级门槛

按 [qmt-execution-readiness skill](../../.github/skills/qmt-execution-readiness/SKILL.md) 的口径，当前建议分级如下：

1. **当前状态：已具备盘前 dry-run 能力**
   - 已能生成目标持仓与账户快照
   - 已能读取资金、持仓、委托、成交和行情
2. **对模拟账户的升级到可模拟盘条件**
   - 交易日完成最小委托生命周期验证
   - 盘后对账无结构性差异
   - 开盘成交折损、滑点和未成交可解释
3. **升级到可人工确认实盘 / 可小资金自动化**
   - 需要多个交易日稳定样本，而不是单日通过就上

## 13. 下一步

1. 按本 Runbook 在交易日先完成 **盘前 dry-run + 模拟盘最小委托生命周期验证**，不要再把 `/api/download/*` 纳入主线。
2. 把交易日报告落到 `execution_report` / `reconcile_report` 体系，沉淀到 [[用户手册]] 和交易域工件里。
3. 对业绩预告策略补齐开盘折损、滑点和未成交归因后，再判断是否从 `可 shadow` 升到 `可模拟盘`。
