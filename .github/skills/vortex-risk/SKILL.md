---
name: vortex-risk
description: 风控域技能。用于事前、事中、事后三层风控与告警闭环。
---

# Vortex Risk Skill

## 目标
- 在策略收益目标之外，构建稳定的风险边界与可审计风控链路

## 必做清单
1. 事前风控：集中度、行业偏离、黑名单、换手上限
2. 事中风控：异常成交、订单超时、滑点偏离
3. 事后风控：回撤、风格暴露、对账偏差
4. 规则配置必须外置化（yaml）
5. 触发事件必须记录并可回放

## 输出产物
- pre_trade_check.json
- risk_events.log
- daily_risk_snapshot.json

## 接口约定
- check_pre_trade(order_plan, limits)
- monitor_intra_trade(fills)
- evaluate_post_trade(nav, exposures)
