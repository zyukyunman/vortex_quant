---
name: vortex-risk
description: 风控域技能。用于事前/事中/事后三层风控，默认 Fail-Closed，支持跨市场规则包。
---

# Vortex Risk Skill

## 目标

1. 建立可审计、可配置、可阻断的风控闭环。
2. 不同市场通过规则包适配，核心策略不变。

## 必做清单

1. 事前风控：集中度、行业偏离、黑名单、换手上限。
2. 事中风控：异常成交、订单超时、滑点偏离。
3. 事后风控：回撤、风格暴露、对账偏差。
4. 风控规则必须外置化（yaml/profile）。
5. 关键规则失败默认阻断（Fail-Closed）。

## 输出产物

1. pre_trade_check.json
2. risk_events.log
3. daily_risk_snapshot.json

## 接口约定

- check_pre_trade(order_plan, limits)
- monitor_intra_trade(fills, live_state)
- evaluate_post_trade(nav, exposures)
- load_rulepack(market, account_type)
