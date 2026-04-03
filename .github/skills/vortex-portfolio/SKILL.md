---
name: vortex-portfolio
description: 组合域技能。用于多策略信号融合、约束优化与目标组合编译，支持跨市场交易规则适配。
---

# Vortex Portfolio Skill

## 目标

1. 将研究信号稳定转化为可执行目标组合。
2. 组合引擎与市场细节解耦，通过规则适配器处理差异。

## 必做清单

1. 输入信号必须标准化为 SignalSnapshot。
2. 支持单策略与多策略融合（等权、ICIR、风险预算等）。
3. 通过 MarketRuleAdapter 处理市场差异（最小交易单位、价格精度、交易时段）。
4. 编译阶段必须输出换手、预估成本、暴露变化。
5. 结果必须可对账、可重建。

## 输出产物

1. target_portfolio.json
2. rebalance_diff.csv
3. order_plan_preview.csv

## 接口约定

- merge_signals(signal_list, method)
- optimize_weights(alpha, risk, constraints)
- compile_orders(target, current, account, market_rules)
