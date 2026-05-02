---
tags: [vortex, vortex/research-domain, vortex/factor-methodology]
aliases: [CogAlpha Quality Gate与Fitness规范, CogAlpha质量门禁与适应度规范, CogAlpha Fitness规范]
created: 2026-05-02
updated: 2026-05-02
status: design_note
---

# CogAlpha Quality Gate 与 Fitness 规范

关联：[[CogAlpha学习资料]]、[[CogAlpha Agent体系设计]]、[[CogAlpha因子研究落地路线]]、[[因子研究与评测全流程说明]]、[[研究协作与产物治理]]

## 一句话结论

CogAlpha 在 Vortex 里最先要产品化的不是 generator，而是 **Quality Gate**。没有质量门禁的 LLM 因子挖掘会把未来函数、数值错误、字段不可见、重复公式和过拟合包装成“发现 alpha”。Fitness 只能评价通过门禁的候选，不能替代门禁。

## 总流程

```text
AlphaCandidate
  -> schema validation
  -> field/operator whitelist
  -> temporal safety
  -> execution/numerical check
  -> coverage/distinct check
  -> economic logic review
  -> fitness evaluation
  -> qualified / elite / rejected
  -> archive and feedback
```

所有检查默认 fail-closed：不确定就拒绝或降级，不用“看起来能跑”作为通过理由。

## AlphaCandidate 最小 schema

```yaml
alpha_id: vtx_cogalpha_0001
name: liquidity_impact_reversal_20d
agent: AgentLiquidity
generation: 0
parents: []
guidance_type: concrete
hypothesis: 低成交额下的大价格冲击可能含有短期反转或流动性溢价
expression_type: formula
expression: "cs_rank((high - low) / amount)"
required_fields: [high, low, close, amount]
lookback_windows: [20]
direction: unknown
horizons: [1, 5, 20]
created_by: cogalpha-factor-mining
lineage:
  mutation_type: none
  crossover_type: none
  prompt_hash: optional
  code_hash: optional
```

候选必须能序列化成 JSON/YAML；不能只保存在聊天里。

## Quality Gate 检查项

### 1. Schema 检查

必填：

- `alpha_id`
- `name`
- `agent`
- `hypothesis`
- `expression_type`
- `expression`
- `required_fields`
- `horizons`
- `direction`

失败条件：

- 缺少必填字段。
- `horizons` 为空或包含非正整数。
- `direction` 未声明为 `positive`、`negative` 或 `unknown`。
- `agent` 不在 [[CogAlpha Agent体系设计]] 的 21 个 agent 中。

### 2. 字段白名单

候选只能使用本轮声明的数据字段。日频量价候选默认优先：

- `open`
- `high`
- `low`
- `close`
- `volume`
- `amount`
- 可明确 PIT 的 `turnover_rate`、`total_mv`、`industry`

财务、公告、资金流、涨跌停、停牌字段必须额外声明可见时间和 PIT 口径。

失败条件：

- 使用未声明字段。
- 使用未来收益、未来标签或目标变量。
- 使用公告前不可见的财务/事件字段。

### 3. 算子白名单

优先允许 Vortex 已有安全算子：

- 截面：`cs_rank`、`cs_zscore`、`scale`、`neutralize_by_group`
- 时序：`delay`、`delta`、`ts_rank`、`ts_sum`、`ts_mean`、`ts_std`
- 关系：`correlation`、`covariance`
- 衰减：`decay_linear`
- 稳定变换：`signed_power`、`clip`、安全除法

失败条件：

- 任意 `eval` / `exec`。
- 任意文件、网络、环境变量访问。
- 任意负向 `shift` 或未来窗口。
- 未白名单第三方库调用。

### 4. 时间安全检查

必须证明因子在截面日期 `t` 只使用 `t` 及以前可见数据。

失败条件：

- `shift(-n)`。
- `close.shift(-horizon) / close` 被混入因子。
- rolling 窗口中心对齐或未来对齐。
- 财务数据未按公告日或可见日延迟。
- 同日收盘后数据被用于同日开盘成交。

### 5. 数值稳定检查

检查：

- NaN 比例。
- inf 比例。
- 除零风险。
- log 非正风险。
- overflow / underflow。
- 极端值是否由少数样本支配。

建议阈值：

| 指标 | 默认阈值 | 处理 |
|---|---:|---|
| 全样本有效覆盖率 | >= 70% | 低于阈值降级 |
| 单截面有效样本 | >= 30 | 低于阈值跳过该截面 |
| inf 比例 | 0 | 非零拒绝 |
| distinct ratio | >= 5% | 过低拒绝或降级 |

### 6. 经济逻辑检查

每个候选必须回答：

1. 高值代表什么？
2. 为什么高值可能对应未来收益？
3. 方向是预设还是需要评测决定？
4. 它可能只是哪个已有因子的重复？
5. 它在 A 股有什么交易约束？

如果只能解释为“数学公式复杂”，不能通过。

### 7. 重复性和相关性检查

候选通过基础 fitness 后，还要与已入选候选比较相关性：

- `max_abs_correlation > 0.85`：默认拒绝或降级。
- 如果高度相关但解释更强、成本更低、覆盖率更高，可以保留为替代版本。

## Fitness 指标

Fitness 只对通过 Quality Gate 的候选计算。

### 1. IC 与 RankIC

- IC：Pearson 截面相关，衡量线性预测力。
- RankIC：Spearman 截面相关，衡量排序预测力。

Vortex 当前已有 RankIC；CogAlpha 扩展时应补 Pearson IC，但不能用 IC 替代 RankIC。

### 2. ICIR 与 RankICIR

```text
ICIR = mean(IC_series) / std(IC_series)
RankICIR = mean(RankIC_series) / std(RankIC_series)
```

ICIR 衡量稳定性，不只是均值大小。

### 3. Positive Rate

```text
positive_rate = 正 IC 截面数 / 有效截面数
```

默认希望主要 horizon `positive_rate >= 55%`。

### 4. 多空组合

默认 5 分组：

- long：因子最高 20%。
- short：因子最低 20%。
- 输出 long、short、long-short、sharpe。

多空为负时，即使 IC 看起来好，也必须解释原因。

### 5. 分组单调性

至少检查分组收益是否大体单调：

```text
group_5 >= group_4 >= group_3 >= group_2 >= group_1
```

可以允许轻微噪声，但如果只靠极端组贡献，要降级。

### 6. 覆盖率和有效样本

Fitness 报告必须同时输出：

- 总覆盖率。
- 有效日期数。
- 每个截面平均有效股票数。
- 被跳过截面数。

低覆盖率强因子不能直接晋升，只能作为特殊 universe 因子继续研究。

### 7. Mutual Information

MI 可作为非线性补充指标，但不是第一优先。原因：

- 口径更敏感。
- 对样本量和离散化方式依赖更强。
- 不如 RankIC 直观。

建议在 Phase 2 后再加入。

## Qualified / Elite / Rejected 规则

### 最小准入

默认候选进入 `qualified` 至少满足：

| 条件 | 建议 |
|---|---|
| `abs(RankIC_mean)` | >= 0.02 或方向翻转后满足 |
| `RankICIR` | >= 0.3 |
| `positive_rate` | >= 55% |
| `long_short_mean` | > 0 |
| 覆盖率 | >= 70% |
| max correlation | <= 0.85 |

### Elite

Elite 不是策略，只是优先研究候选。默认规则：

1. 同代 fitness 排名前 20%。
2. 通过绝对阈值。
3. 无重大质量风险。
4. 与已入选 elite 不高度重复。

### Rejected

常见 rejected 原因：

- `future_leakage`
- `field_not_visible`
- `invalid_operator`
- `nan_or_inf`
- `coverage_too_low`
- `distinct_too_low`
- `rank_ic_below_rule`
- `icir_below_rule`
- `long_short_below_rule`
- `correlation_too_high`
- `economic_logic_weak`

Rejected 也要归档核心原因，供下一轮 generation 避免重复错误。

## 输出 JSON 草案

```json
{
  "schema": "vortex.cogalpha_fitness.v1",
  "alpha_id": "vtx_cogalpha_0001",
  "quality": {
    "status": "passed",
    "warnings": ["capacity_risk"],
    "failed_checks": []
  },
  "fitness": {
    "primary_horizon": 20,
    "rank_ic_mean": 0.031,
    "rank_icir": 0.42,
    "positive_rate": 0.58,
    "long_short_mean": 0.004,
    "coverage": 0.91,
    "max_abs_correlation": 0.37
  },
  "decision": "qualified",
  "next_actions": ["try_low_vol_gate", "test_60d_decay"]
}
```

## 与 Vortex 现有评测的关系

`vortex/research/evaluation.py` 仍是基础评测内核。CogAlpha fitness 不应复制一套完全独立逻辑，而应：

1. 复用已有 `evaluate_factor` 和 `evaluate_factor_batch`。
2. 补充 IC、RankICIR、覆盖率、分组单调性和 MI。
3. 保持 `FactorEvaluationResult` 可序列化。
4. 不破坏现有测试和报告 schema。

## 策略前置边界

通过 fitness 只能说明“值得研究”，不能说明“值得交易”。进入策略前还必须补：

- walk-forward。
- 成本。
- 容量。
- 涨跌停。
- 停牌。
- ST。
- 退市。
- 行业/市值/估值暴露。
- 组合贡献。
