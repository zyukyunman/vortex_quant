---
name: vortex-portfolio
description: 组合域技能。用于信号融合、约束优化、目标权重生成、调仓编译。
---

# Vortex Portfolio Skill

## 目标
- 将研究信号稳健转化为可执行目标组合

## 必做清单
1. 输入信号必须标准化到统一协议
2. 支持单策略与多策略融合（等权、ICIR、风险预算）
3. 编译目标组合时必须考虑交易约束（整手、最小成交额、单票上限）
4. 输出必须包括预估换手与成本
5. 生成结果必须可对账（目标仓位可重建）

## 输出产物
- target_portfolio.json
- rebalance_diff.csv
- order_plan_preview.csv

## 接口约定
- merge_signals(signal_list, method)
- optimize_weights(alpha, risk, constraints)
- compile_orders(target, current, account)
