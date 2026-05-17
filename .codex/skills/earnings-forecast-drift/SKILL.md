---
name: earnings-forecast-drift
description: '业绩预告公告后漂移策略研究技能。Use when: 研究 forecast 业绩预告、盈利 surprise、公告后漂移、满仓 alpha 候选、A 股事件策略测试报告。'
argument-hint: '提供 forecast/express 数据、回测区间、执行口径、是否需要因子评测、walk-forward、容量或可交易性复核'
tags: [vortex, vortex/skill, vortex/research-domain, vortex/strategy-domain, earnings, event-alpha]
obsidian_links:
  - "[[业绩预告漂移策略研究总结]]"
  - "[[业绩预告漂移策略测试报告]]"
  - "[[研究协作与产物治理]]"
  - "[[因子研究与评测全流程说明]]"
---

# 业绩预告公告后漂移策略

这是当前 Vortex 的 **candidate 级满仓 alpha 候选**，不是 production-ready 策略。它可以保留快速研究 runner；晋升前还需要 shadow trading、真实冲击成本和 signal snapshot 化。

---

## 一、核心假设

A 股业绩预告发布后，市场可能不会在公告后的第一个交易日完全消化信息。预增、扭亏、利润同比大幅增长等事件，可能带来 20-60 个交易日的公告后漂移。

当前信号很简单：

```text
forecast.ann_date
  → 公告后下一个交易日才可见
  → p_change_min / p_change_max + 公告类型打分
  → 截面排名
  → 市场 risk-on 时买入 Top-N
```

最新因子评测显示，它不是纯粹“强因子弱模型”，更准确是：**中等强度事件因子 + 简单规则模型 + 市场门控**。

---

## 二、当前最新结论

| 版本 | 定位 | 关键结论 |
|---|---|---|
| v3 默认版 | 收益优先 candidate | 全历史精确可交易复核后，年化约 67.63%，最大回撤约 -10.69%，Sharpe 约 3.69，Calmar 约 6.32 |
| 单票 3% 安全版 | 更稳健的并行观察版 | 全历史精确可交易复核后，年化约 62.59%，最大回撤约 -8.56%，Calmar 约 7.31 |

默认参数锚点：

| 参数 | 值 |
|---|---:|
| `delay_days` | 1 |
| `hold_days` | 40 |
| `top_n` | 80 |
| `target_exposure` | 1.0 |
| `transaction_cost_bps` | 20 |
| `min_avg_amount` | 30000 |
| 市场门控 | `momentum_window=5, support_window=20, confirmations=2` |

上线节奏：

1. 当前先 shadow trading / paper trading。
2. 5 月适合 shadow 或小资金观察。
3. 6 月底到 7 月更贴近半年度业绩预告窗口。
4. 默认版和单票 3% 安全版并行观察。

---

## 三、必须补齐的缺口

| 缺口 | 为什么重要 | 下一步 |
|---|---|---|
| forecast surprise 因子评测 | 已补，结论是单因子中等偏正，60 日最好 | 后续看分组单调性和是否能发布 signal |
| 全历史 `stk_limit` / `suspend_d` | 已补齐并完成精确复核 | 后续用 shadow trading 验证当期数据延迟和真实成交 |
| 1 亿元容量尾部 | 默认版 P99 参与率约 8.43%，安全版约 8.20% | 继续评估更高流动性阈值、冲击成本和小资金上线规模 |
| 开盘卖一容量复核 | 已补 conservative ask1 容量分析框架，可对比计划买单与 `ask1_volume` | 下一步接入历史开盘快照，先跑 100 万 / Top30-50 小资金矩阵 |
| 持仓归档与报告 | 用户要看每天买了什么 | 保留持仓 CSV、JSON 摘要和安全性 HTML |
| artifact 治理 | 大 CSV/PNG 不应长期堆在核心仓库 | 迁移到 workspace，核心仓库保留摘要和代表性报告 |

---

## 四、无效路径，不要重复

| 方法 | 结论 |
|---|---|
| 龙虎榜同日收盘买 | 有未来函数风险 |
| 涨停强度同日收盘买 | 需要分钟/实时事件流，当前不能当安全策略 |
| 热榜共振 | 未显著改善 forecast 主策略 |
| 资金流正向过滤 | 明显降低事件覆盖和收益效率 |
| 复杂财务质量组合 | 没有改善回撤，容易过拟合 |
| 月份硬开关 | 近期可降回撤但训练/测试稳定性不足 |
| 低仓位硬压 5% 回撤 | 会牺牲 alpha，应交给组合层控风险 |

---

## 五、执行规则

1. 早期研究可使用 `vortex.strategy.earnings_forecast_drift` 快速 runner。
2. 只要结果用于“策略候选”展示，必须同时保存 JSON、HTML、持仓和参数。
3. 晋升 promoted 前必须经过因子评测、walk-forward、成本、容量和精确可交易性。
4. 晋升 promoted 时，再改成 Research signal snapshot + Strategy consumer。
5. 不打印、不提交 Tushare token；补数据时通过 zsh 环境或 workspace `.env` 注入。
6. 复核不要再复制临时脚本，使用正式入口：
   - `vortex strategy earnings-forecast precise-review`
   - `vortex strategy earnings-forecast shadow-plan`
   - `vortex strategy earnings-forecast opening-liquidity-review`

开盘卖一容量复核要求外部快照至少提供：

```text
date,symbol,open_price,ask1_price,ask1_volume
```

这条复核的定位是保守下界，不是完整撮合回放：

1. 连卖一都不够，说明“开盘价能买到”的假设不稳。
2. 卖一已经足够覆盖至少一手或大部分目标股数，小资金版本才更接近可执行。

---

## 六、因子评测速记

真实数据区间：2017-01-01 至 2026-04-24，流动性门槛 `min_avg_amount=30000`。

| Horizon | IC mean | ICIR | 正 IC 占比 |
|---:|---:|---:|---:|
| 1d | 0.0290 | 0.179 | 55.93% |
| 5d | 0.0234 | 0.147 | 57.29% |
| 20d | 0.0135 | 0.075 | 53.40% |
| 40d | 0.0277 | 0.164 | 55.82% |
| 60d | 0.0530 | 0.296 | 64.79% |

20 日多空均值约 0.284%。结论：单因子有正向预测力，但不算极强；完整策略收益还依赖持有窗口、市场门控和事件组合构建。
