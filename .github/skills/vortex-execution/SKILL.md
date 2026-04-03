---
name: vortex-execution
description: 执行域技能。用于回测执行、xqshare-miniQMT 实盘下单、成交回报与对账。
---

# Vortex Execution Skill

## 目标
- 统一模拟与实盘执行语义，确保订单可控、回报可追踪

## 必做清单
1. 模拟盘与实盘必须共用 OrderPlan 协议
2. 实盘下单前必须做二次风控校验
3. 下单必须支持幂等键避免重复提交
4. 成交回报必须结构化落库
5. 当日执行结束必须自动对账并给出差异明细

## 输出产物
- order_plan.json
- execution_report.json
- reconcile_report.json

## 接口约定
- submit_orders(order_plan)
- query_positions(account)
- query_asset(account)
- reconcile(date, account)
