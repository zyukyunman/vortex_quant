---
tags: [vortex, vortex/strategy-domain, vortex/research-domain, vortex/ai-quant-os]
aliases: [回测与CPCV评估协议, 策略替代评估协议, CPCV回测协议]
created: 2026-05-16
updated: 2026-05-17
---

# 回测与 CPCV 评估协议

## 一句话定义

回测不是“跑赢 baseline 就替代 baseline”。Vortex 把回测拆成训练级、测试级、执行级和上线级：训练级允许研究员和 Agent 搜索；测试级只允许验证冻结候选；执行级确认可交易；上线级必须人工审批。

## 替代 baseline 的两种含义

| 替代对象 | 是否可只凭回测 | 要求 |
|---|---|---|
| 研究 reference baseline | 可以在测试级通过后替代 | 候选已冻结，CPCV / 样本外 / 成本 / 容量均通过 |
| 正式 preset / live 默认 | 不可以只凭回测替代 | 还需要 paper shadow、模拟盘或用户审批 |

因此，`tail_risk_soft_q10_p25` 这类候选可以进入“替代研究 reference baseline 的候选评估”，但不能因为全样本回测好看就直接替换正式 preset。2026-05-17 PIT 修正复跑后，该候选未通过 CPCV；研究 reference 仍保持 `baseline_top110_large`。用户随后确认 100 万实盘默认采用资金量适配的 `stable_100w`，这属于 live preset 选择，不等同于替代研究 reference baseline。

## 四层评估

| 层级 | 名称 | 目的 | 允许动作 | 禁止动作 |
|---|---|---|---|---|
| L0 | Research spike | 快速发现线索 | 小网格、全样本初筛、临时 runner | 宣称晋升 |
| L1 | Train / dev | 选择候选和参数 | mutation grid、训练期 walk-forward、参数冻结 | 偷看测试期后再改参数 |
| L2 | Test / CPCV | 验证冻结候选是否稳健 | Purged + embargo CPCV、锁定 holdout、baseline delta 分布 | 调参、换候选、事后删失败 fold |
| L3 | Execution / paper | 验证可交易和实盘偏差 | 成本、容量、整手、分钟目标价、paper shadow | 直接实盘 |

## CPCV 常规口径

CPCV 用于解决普通 k-fold 或单一路径 walk-forward 对金融时间序列不够稳的问题。它把时间序列切成 `N` 个连续组，每次选 `k` 个组做测试，其余做训练；训练集里与测试标签窗口重叠的样本要 purge，测试段之后还要 embargo。

默认参数建议：

| 参数 | 默认 | 说明 |
|---|---:|---|
| `n_groups` | 8 或 10 | 时间组太少分布不稳定，太多会让每个测试段太短 |
| `n_test_groups` | 2 | 先用双测试组获得多条样本外路径 |
| `purge_horizon` | 策略持有期或标签前瞻期 | 业绩预告漂移当前 `hold_days=40`，初始用 40 个交易日 |
| `embargo` | 20 个交易日或样本 1%-3% | 防止测试段附近序列相关污染训练 |
| `max_combinations` | 可选 | 组合过多时限流，但必须记录抽样种子 |

常见规模：

```text
N=8, k=2 -> 28 个 train/test split，约 7 条组合路径
N=10, k=2 -> 45 个 train/test split，约 9 条组合路径
```

## Vortex 策略替代门槛

候选想替代研究 reference baseline，必须先冻结候选版本，然后满足：

1. 全样本只作为说明，不作为替代依据。
2. CPCV 样本外 `calmar_delta` 中位数 > 0。
3. CPCV 样本外 `annual_return_delta` 中位数 > 0。
4. CPCV 样本外胜率 >= 60%，如果候选来自大规模搜索，要求 >= 70%。
5. `max_drawdown_delta` 的 25% 分位不低于 -1 个百分点。
6. 成本压力测试下仍不明显劣化：至少覆盖 20bps、50bps、100bps。
7. 容量/执行测试不明显劣化：买入份额填充率、现金残留、目标价触达率不能弱于 baseline 太多。
8. 搜索预算必须公开：试了多少候选、多少 mutation、为什么选它。
9. 风险官确认没有数据泄漏、重复暴露、过拟合或容量硬伤。

候选想替代正式 preset，还必须额外满足：

1. 至少一段冻结后的 paper shadow。
2. 用户明确批准。
3. 变更写入策略版本血统和回滚方案。

## 实盘安全修复与 alpha 优化分离

实盘安全修复不能被当作策略 alpha 晋升证据。以下变化即使会明显改变 shadow/live target，也不直接改变研究 reference baseline：

| 变化 | 归属 | 是否需要 CPCV |
|---|---|---|
| 修复同日收盘数据用于同日交易 | PIT / 编排安全 | 需要用修正口径重跑候选 CPCV；旧结果作废 |
| 手动 prepare 强制 `execution_trade_date > signal_as_of` | 实盘编排安全 | 不作为 alpha 候选，只需回归测试和目标产物审计 |
| 执行日 ST 缺失 fail-closed | 交易风控 | 不作为 alpha 候选，只需交易风控测试 |
| QMT health fail 写 blocked artifacts | 执行审计 | 不作为 alpha 候选，只需执行审计测试 |
| 已持有赢家的惯性保留 | live sizing / 执行优化 | 应先做实盘偏差和成交层复核；若要沉淀为 base 策略规则，再补回测近似与 paper shadow |

资金量相关的 TopN 选择也不能从单一全样本理论收益外推。`aggressive_100w`、`stable_100w`、`baseline_top110_large` 代表不同本金和执行约束：100 万、1000 万、1 亿应分别看整手、最低下单额、现金残留、市场权限和成交容量。一个 TopN 在理论 event backtest 中更高，不代表在对应资金量的 live/lot 执行中更优。

## 当前样例应用

当前 `tail_risk_soft_q10_p25` 的状态：

| 检查 | 状态 |
|---|---|
| L0 research spike | 通过；mutation grid 最优 |
| L1 train/dev | 部分通过；但 mutation grid 本身属于搜索过程，且必须用 T-1 overlay 复验 |
| L2 test/CPCV | 2026-05-17 PIT 修正后失败：`cpcv_fail_keep_baseline` |
| L3 execution | 初步执行复核历史上通过，但在 L2 失败后不进入默认实盘链路 |

当前结论：

```text
tail_risk_soft_q10_p25 -> explicit shadow/challenge only
rerank_tail_risk_w010 -> paper shadow reference / parent lineage
baseline_top110_large -> 继续作为研究 reference / 大容量回滚
stable_100w -> 100 万 live 默认 preset
```

## 产物要求

CPCV runner 最终应输出：

```json
{
  "schema": "vortex.strategy.cpcv_backtest.v1",
  "candidate": "tail_risk_soft_q10_p25",
  "baseline": "baseline_top110_large",
  "n_groups": 8,
  "n_test_groups": 2,
  "purge_horizon": 40,
  "embargo": 20,
  "folds": [],
  "paths": [],
  "delta_distribution": {
    "annual_return_delta_median": null,
    "calmar_delta_median": null,
    "max_drawdown_delta_p25": null,
    "test_win_rate": null
  },
  "search_budget": {
    "candidate_count": null,
    "mutation_count": null,
    "selected_before_test": true
  },
  "decision": "pass / pass_with_conditions / fail"
}
```

## 参考资料

- Marcos López de Prado, *Advances in Financial Machine Learning*, 2018.
- Mizar Labs, Combinatorial Purged Cross Validation: https://docs.mizar.com/mizar/mizarlabs/model/combinatorial-purged-cross-validation
- ML4T Diagnostic, Combinatorial Purged Cross-Validation: https://ml4trading.io/docs/diagnostic/methods/cpcv/
- ML4T Diagnostic, Cross-Validation: https://ml4trading.io/docs/diagnostic/user-guide/cross-validation/
