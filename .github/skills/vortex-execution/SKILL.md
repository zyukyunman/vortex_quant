---
name: vortex-execution
description: 执行域技能。用于回测执行、模拟盘、miniQMT 实盘与对账，强调统一订单协议与网关可替换。
---

# Vortex Execution Skill

## 目标

1. 统一模拟盘与实盘执行语义。
2. Gateway 可替换，miniQMT 只是默认实现之一。

## 必做清单

1. 所有执行入口必须消费同一 OrderPlan 协议。
2. 下单前必须执行二次风控校验。
3. 下单必须幂等（plan_hash + account + trade_date）。
4. 成交回报必须结构化落库。
5. 日终必须自动对账并输出差异。

## 输出产物

1. order_plan.json
2. execution_report.json
3. reconcile_report.json

## 接口约定

- submit_orders(order_plan)
- cancel_order(order_id)
- query_positions(account)
- query_asset(account)
- query_fills(account, date)
- reconcile(date, account)
