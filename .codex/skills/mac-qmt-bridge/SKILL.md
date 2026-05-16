---
name: mac-qmt-bridge
description: "Mac 上接入 QMT/MiniQMT/xtquant 的虚拟机与桥接技能。Use when: Mac 设备、Windows 虚拟机、国金 QMT 模拟盘、MiniQMT、xtquant、qmt-bridge、xqshare、easytrader、EasyXT、HTTP/RPyC 桥接、跨平台交易接口打通。"
argument-hint: "说明 Mac 型号、虚拟机软件、券商/QMT 版本、是否有模拟盘账号、希望 HTTP 还是透明 xtquant 调用"
tags: [vortex, vortex/skill, vortex/trade-domain, qmt, miniqmt, mac, bridge]
obsidian_links:
  - "[[Mac虚拟机QMT实盘桥接方案]]"
  - "[[交易域设计说明书]]"
  - "[[研究协作与产物治理]]"
---

# Mac QMT 桥接技能

用于把 Mac 主力研究机接到 Windows 内的 QMT/MiniQMT。核心原则：**Mac 不直接跑 xtquant；Windows 虚拟机或交易机跑 QMT，Vortex 只做策略、风控、报告和桥接调用方。**

---

## 一、默认架构

```text
Mac 主机
  Vortex 数据 / 策略 / 风控 / 报告
  ↓ HTTP / RPyC / WebSocket
Windows 虚拟机
  国金 QMT / MiniQMT，保持登录
  xtquant
  qmt-bridge / xqshare / easytrader remote / xtquantservice
```

不要设计成：

```text
Mac 上直接 import xtquant 并下单
```

MiniQMT/xtquant 通常依赖 Windows 端 QMT 客户端登录和券商权限。

---

## 二、项目选型速记

| 项目 | 适合什么 | 注意事项 |
|---|---|---|
| `qmt-bridge` | 需要 HTTP/WebSocket、Swagger、Mac 通过 REST 调用 | 更贴合“现成 HTTP 接口”，先做 POC |
| `xqshare` | 需要 Mac/Linux 像本地一样透明调用 xtquant | RPyC，不是 HTTP；适合作为 qmt-bridge 失败备选 |
| `easytrader` | 需要成熟交易适配、MiniQMT 买卖/撤单/持仓参考 | star 高、范围广；可作为接口语义参考 |
| `EasyXT` | 需要更友好的 xtquant 封装、自动登录、101 因子参考 | 范围大，不直接并入 Vortex；因子需重新 PIT 评测 |
| `xtquantservice` / `qmt2http` / `quant-qmt-proxy` | 需要更多 HTTP 备选 | 先看文档、许可证、维护状态和交易回报能力 |

推荐 POC 顺序：

1. HTTP 优先：`qmt-bridge`。
2. 若 HTTP 不稳定：`xtquantservice` 或其他 qmt2http 项目。
3. 若更想透明调用 xtquant：`xqshare`。
4. `easytrader` 和 `EasyXT` 主要作接口设计、排障和资料参考。

---

## 三、Mac 虚拟机安装检查清单

1. 安装虚拟机：优先 Parallels Desktop，其次 VMware Fusion / UTM。
2. 安装 Windows。
3. 安装国金 QMT / MiniQMT。
4. 用模拟盘或测试账户登录，优先勾选“极简模式 / 独立交易模式”。
5. 找到 `userdata_mini` 路径。
6. 安装券商版本兼容的 Python 与 `xtquant`。
7. 验证：
   - `import xtquant`
   - 查询一只股票行情；
   - 查询资产；
   - 查询持仓；
   - 查询委托和成交。
8. 启动桥接服务。
9. Mac 访问虚拟机 IP 的健康检查接口。
10. 连续跑满一个交易日，确认不掉线、不休眠、不丢回报。

Apple Silicon Mac 必须额外确认 Windows ARM、券商登录控件、加密模块和 xtquant 的兼容性。失败时不要硬调，改用独立 Windows 小主机。

---

## 四、网络与安全规则

1. 虚拟机网络优先桥接模式；不稳定时用 NAT + 端口转发。
2. Windows 防火墙只放行必要端口。
3. 只允许局域网或 VPN，不暴露公网。
4. 交易端点必须启用 Token / API Key。
5. 第一阶段只读：健康检查、行情、资产、持仓、委托、成交。
6. 第二阶段只连国金模拟盘。
7. 第三阶段真实账户必须人工确认后再下单。

---

## 五、POC 通过标准

必须全部通过：

1. Mac 能访问 Windows 虚拟机健康检查。
2. 能查询 QMT/MiniQMT 连接状态。
3. 能查询实时行情。
4. 能查询资产。
5. 能查询持仓。
6. 能查询当日委托。
7. 能查询当日成交。
8. 国金模拟盘能提交 100 股限价委托。
9. 能收到委托回报、成交回报或拒单原因。
10. 连续一个完整交易日不断线。

未通过前，不写真实账户自动下单逻辑。
