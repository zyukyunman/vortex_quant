---
tags: [vortex, vortex/research-domain, vortex/factor-methodology]
aliases: [CogAlpha Agent体系设计, CogAlpha Agent Catalog, CogAlpha七层研究员体系]
created: 2026-05-02
updated: 2026-05-02
status: design_note
---

# CogAlpha Agent 体系设计

关联：[[CogAlpha学习资料]]、[[CogAlpha课堂讲义]]、[[CogAlpha因子研究落地路线]]、[[CogAlpha Quality Gate与Fitness规范]]、[[因子研究与评测全流程说明]]

## 一句话结论

Vortex 已把 CogAlpha 的 21 个 agent 从“角色卡”推进到 **21 个 baseline proxy recipe**：每个 agent 至少有一个可执行、可审计、可评测的本地代理因子。但这不等于已经完整复现论文中的智能 agent；当前实现是工程闭环和研究起点，真正的 LLM generation / mutation / crossover 仍在后续阶段。

## 为什么先做 agent catalog

如果直接让 LLM “生成一些 alpha”，输出很容易坍缩到动量、反转、均线、波动这些常见公式。Agent catalog 的价值是把搜索空间拆成明确研究视角，让每个候选因子都能回答：

1. 它来自哪个研究视角？
2. 它解释什么市场现象？
3. 它能用哪些字段和算子？
4. 它主要评测哪个 horizon？
5. 它最容易犯什么未来函数或经济逻辑错误？

这会让 Vortex 的 AI 研究员不是“随机生成公式”，而是像一个分工明确的因子研究团队。

## Agent 的三层实现状态

为了避免“agent 只有名字但不能工作”，Vortex 把 CogAlpha agent 分成三层：

| 状态 | 含义 | 当前落点 |
|---|---|---|
| `catalog` | 只有研究视角、字段、horizon、风险说明 | `agent_catalog.py` |
| `recipe` | 能生成 `AlphaCandidate` 和安全 `FormulaSpec.builder`；当前多为 baseline proxy | `recipes.py` |
| `prompt/evolution` | 能接 LLM guidance、mutation、crossover | 后续 Phase 4 |

当前已实现的最小可执行层是 `recipe`，不是 prompt 人格。一个 agent 只有满足以下条件，才从角色卡升级为可执行研究单元：

1. 有明确 `template_id`。
2. 有可审计 `hypothesis` 和 `expression`。
3. 有 `required_fields` 和 `default_horizons`。
4. 有安全 `builder(DailyFactorInputs) -> DataFrame`。
5. builder 只调用 Vortex 内置安全算子或 PIT-safe pandas rolling/shift，不使用 `eval` / `exec` / 任意文件网络访问。

## 当前可执行 recipe

Phase 2.6 已补齐全部 21 个低依赖、可用日频 OHLCV 数据验证的 baseline proxy recipe。Phase 2.7 进一步对 review 中最弱的 5 个 proxy 做了语义硬化，让它们更接近论文描述，但仍不等同于完整 LLM agent 复现。

| Agent | recipe | 作用 |
|---|---|---|
| `AgentMarketCycle` | `market_cycle_relative_trend_60d` | 市场趋势、宽度、波动 regime 门控后的个股趋势 |
| `AgentVolatilityRegime` | `volatility_regime_compression_20d` | 短波动相对长波动压缩 |
| `AgentTailRisk` | `tail_risk_downside_vol_20d` | 下行波动风险过滤 |
| `AgentCrashPredictor` | `crash_fragility_high_range_low_liquidity_20d` | 波动扩张、下行 range、市场同步和流动性枯竭的脆弱性过滤 |
| `AgentLiquidity` | `liquidity_range_impact` | 价格区间/成交额冲击，连接流动性与容量风险 |
| `AgentOrderImbalance` | `order_imbalance_close_strength_5d` | 收盘位置、K 线实体和成交参与代理买压 |
| `AgentPriceVolumeCoherence` | `price_volume_coherence_20d` | 价格变化与成交额变化的滚动一致性 |
| `AgentVolumeStructure` | `volume_structure_surge_decay_20d` | 成交量相对基准的稳定性 |
| `AgentDailyTrend` | `daily_trend_20d` | 20 日相对强弱趋势 |
| `AgentReversal` | `short_reversal_5d` | 5 日短反转 |
| `AgentRangeVol` | `range_vol_20d` | 20 日高低价区间波动 |
| `AgentLagResponse` | `lag_response_volume_leads_price_20d` | 成交额领先、价格滞后响应 |
| `AgentVolAsymmetry` | `vol_asymmetry_downside_upside_20d` | 下行/上行波动非对称 |
| `AgentDrawdown` | `drawdown_recovery_position_60d` | 当前价格相对 60 日高点位置 |
| `AgentFractal` | `fractal_multiscale_consistency_20_60d` | path efficiency、方差比例和多尺度 gap 的粗糙度代理 |
| `AgentRegimeGating` | `regime_gated_trend_lowvol_60d` | 低波门控后的 60 日趋势 |
| `AgentStability` | `stability_signal_smoothness_20d` | 20 日收益路径平滑度 |
| `AgentComposite` | `composite_trend_reversal_liquidity` | 趋势、短反转、流动性等权融合 |
| `AgentCreative` | `creative_soft_rank_range_liquidity` | trend/reversal/liquidity 父模板的确定性非线性 mutation proxy |
| `AgentBarShape` | `bar_shape_close_location_5d` | 5 日 K 线收盘位置与实体强度 |
| `AgentHerding` | `herding_amount_crowding_reversal_20d` | 截面方向共识、个股对齐和成交额拥挤的羊群代理 |

`planned_recipes()` 现在返回空 tuple；如果未来新增 agent，必须先补安全 builder 和测试，不能只登记角色名。每个 recipe 都带 `semantic_status`、`semantic_notes` 和可选 `parent_templates`：

| semantic_status | 含义 |
|---|---|
| `proxy` | 可执行代理公式，能研究，但不能宣称完整复现论文 agent |
| `faithful_proxy` | 在当前 OHLCV 字段内对论文语义做过强化，但仍不是完整 LLM agent |
| `mutation_proxy` | 更接近变换、组合、变异器的可执行代理，当前用于 `AgentCreative` |

Phase 2.7 中 `AgentMarketCycle`、`AgentCrashPredictor`、`AgentFractal`、`AgentHerding` 标为 `faithful_proxy`；`AgentCreative` 标为 `mutation_proxy`。

## 与现有研究代码的接缝

CogAlpha recipe 通过 `FormulaSpec` 接回现有 Research 主流程：

```text
CogAlphaAgentRecipe
  -> AlphaCandidate
  -> FormulaSpec
  -> compute_formula(DailyFactorInputs)
  -> run_quality_gate
  -> rank_cogalpha_candidates
  -> write_generation_report_json
```

同时，`adapters.py` 可以把现有 `registered_specs()` 包装为 CogAlpha candidates，让旧有 101 个公式也进入同一套 agent 归因、quality gate 和 fitness 审查。

最小演示入口：

```python
from vortex.research.cogalpha import run_cogalpha_demo

run_cogalpha_demo("workspace/cogalpha/latest")
```

它会真实执行 21 个 baseline proxy recipe，写出 `generation_report.json` 和 `generation_summary.json`。这个 synthetic demo 只证明工程闭环，不代表真实 A 股 alpha 结论，也不代表论文 21 agent 已完整复现。

## 七层结构总览

| 层级 | 研究问题 | Vortex 初始用途 |
|---|---|---|
| Market Structure & Cycle | 市场处于什么阶段？趋势或波动 regime 是否切换？ | 市场状态、择时过滤、因子门控 |
| Extreme Risk & Fragility | 是否存在尾部风险、崩盘前兆或流动性枯竭？ | 防守腿、风险过滤、空仓条件 |
| Price-Volume Dynamics | 价格变化与成交活动是否一致？是否有拥挤或流动性冲击？ | 量价因子、资金流、峰岭谷、容量约束 |
| Price-Volatility Behavior | 动量、反转、区间、波动非对称是否含有信息？ | 量价基础 alpha、低波反转 |
| Multi-Scale Complexity | 信号跨尺度是否稳定？回撤和恢复形态是否有预测力？ | 多 horizon 稳定性、回撤结构 |
| Stability & Regime-Gating | 信号什么时候应该打开或关闭？ | 状态门控、稳定性过滤 |
| Geometric & Fusion | K 线形态、拥挤和多因子组合是否有协同？ | OHLC shape、组合 alpha、非线性变体 |

## Agent 角色卡

### 1. AgentMarketCycle

- **问题**：市场是否处于趋势延续、横盘、反转或周期切换状态？
- **字段**：指数行情、个股 close、行业指数、市场宽度。
- **候选表达**：长期动量、均线斜率、指数相对强弱、行业扩散度。
- **默认 horizon**：20d、60d、120d。
- **主要风险**：用全样本周期划分、事后识别 regime。
- **Vortex 落点**：市场状态过滤，不能直接当个股 alpha。

### 2. AgentVolatilityRegime

- **问题**：波动状态是否从平静切换到剧烈，或从剧烈回归平稳？
- **字段**：close、high、low、指数波动、ATR。
- **候选表达**：波动压缩/扩张、realized volatility 分位、ATR 变化。
- **默认 horizon**：10d、20d、60d。
- **主要风险**：把未来波动作为当前状态。
- **Vortex 落点**：因子门控、仓位风控、低波组合过滤。

### 3. AgentTailRisk

- **问题**：个股是否暴露于下行尾部风险？
- **字段**：close、return、downside return、drawdown。
- **候选表达**：下行波动、极端负收益频率、左尾偏度代理。
- **默认 horizon**：20d、60d、120d。
- **主要风险**：用未来 drawdown 标签构造因子。
- **Vortex 落点**：防守因子、candidate hardening 风控项。

### 4. AgentCrashPredictor

- **问题**：是否有崩盘前的脆弱性信号，例如波动压缩后放量、流动性枯竭？
- **字段**：close、high、low、amount、volume、market state。
- **候选表达**：高位缩量、波动压缩后破位、成交退潮。
- **默认 horizon**：5d、10d、20d。
- **主要风险**：事后只挑崩盘前样本。
- **Vortex 落点**：风险过滤，不单独作为多头因子。

### 5. AgentLiquidity

- **问题**：价格变化是否由较少成交推动，反映流动性冲击或交易摩擦？
- **字段**：close、high、low、amount、volume、turnover。
- **候选表达**：Amihud illiquidity、价格冲击/成交额、低成交放大波动。
- **默认 horizon**：1d、5d、20d。
- **主要风险**：忽略容量，选出无法成交的小票。
- **Vortex 落点**：流动性 alpha 与容量门槛同时使用。

### 6. AgentOrderImbalance

- **问题**：日频 OHLCV 是否暗示单边买卖压力？
- **字段**：open、high、low、close、amount、moneyflow。
- **候选表达**：强收盘位置配合放量、下跌放量、上冲回落。
- **默认 horizon**：1d、5d、10d。
- **主要风险**：用不可获得盘口数据替代日频可见字段。
- **Vortex 落点**：资金流、短线拥挤反转。

### 7. AgentPriceVolumeCoherence

- **问题**：价量是否同步，还是出现背离？
- **字段**：close、volume、amount、return。
- **候选表达**：价格排名与成交排名差、价量滚动相关、背离强度。
- **默认 horizon**：5d、20d、60d。
- **主要风险**：相关性方向不稳定，要允许方向翻转评测。
- **Vortex 落点**：现有 Alpha101 风格量价因子的主要扩展区。

### 8. AgentVolumeStructure

- **问题**：成交活动的形状、集中度和节奏是否有信息？
- **字段**：volume、amount、分钟成交量、日频成交分布代理。
- **候选表达**：成交峰值、成交持续性、放量衰竭、峰岭谷结构。
- **默认 horizon**：1d、5d、20d。
- **主要风险**：分钟数据覆盖、权限和口径不稳定。
- **Vortex 落点**：与 `volume-peak-ridge-valley` skill 协同。

### 9. AgentDailyTrend

- **问题**：多日价格趋势是否延续？
- **字段**：close、open、high、low。
- **候选表达**：skip momentum、risk-adjusted momentum、突破强度。
- **默认 horizon**：20d、60d、120d。
- **主要风险**：最近反转和中期动量混在一起。
- **Vortex 落点**：动量族和市场状态诊断。

### 10. AgentReversal

- **问题**：短期过度反应后是否均值回复？
- **字段**：close、return、amount、volatility。
- **候选表达**：5d/20d 反转、低波反转、拥挤反转。
- **默认 horizon**：1d、5d、20d。
- **主要风险**：高换手、涨跌停无法成交。
- **Vortex 落点**：短线/中线反转候选池。

### 11. AgentRangeVol

- **问题**：日内或多日价格区间压缩/扩张是否有预测力？
- **字段**：high、low、close、open。
- **候选表达**：true range、range compression、range expansion reversal。
- **默认 horizon**：5d、20d、60d。
- **主要风险**：复权 high/low 与涨跌停口径混用。
- **Vortex 落点**：低风险、波动状态和执行过滤。

### 12. AgentLagResponse

- **问题**：收益、成交量、波动之间是否存在滞后反馈？
- **字段**：return、volume、amount、volatility。
- **候选表达**：成交先行价格、波动滞后反转、收益后成交衰竭。
- **默认 horizon**：5d、20d。
- **主要风险**：滞后方向写反导致未来函数。
- **Vortex 落点**：量价反应速度、拥挤消退。

### 13. AgentVolAsymmetry

- **问题**：上涨和下跌波动是否不对称？
- **字段**：return、downside return、upside return、range。
- **候选表达**：downside volatility、upside exhaustion、asymmetric range。
- **默认 horizon**：10d、20d、60d。
- **主要风险**：只在熊市有效，牛市失效。
- **Vortex 落点**：防守低波和下行风险过滤。

### 14. AgentDrawdown

- **问题**：回撤深度、持续时间和恢复速度是否预测未来收益？
- **字段**：close、rolling max、drawdown、recovery。
- **候选表达**：回撤后修复、深回撤反转、长时间弱势惩罚。
- **默认 horizon**：20d、60d、120d。
- **主要风险**：恢复速度容易引用未来路径。
- **Vortex 落点**：反转和风险过滤。

### 15. AgentFractal

- **问题**：价格在不同尺度上的粗糙度或长记忆是否稳定？
- **字段**：close、multi-horizon returns、volatility。
- **候选表达**：多窗口收益一致性、多尺度波动比、粗糙度代理。
- **默认 horizon**：20d、60d、120d。
- **主要风险**：复杂公式缺解释、过拟合。
- **Vortex 落点**：稳定性诊断，先谨慎作为辅助因子。

### 16. AgentRegimeGating

- **问题**：一个因子应该在什么状态下激活？
- **字段**：market state、volatility、liquidity、trend。
- **候选表达**：低波门控、市场趋势门控、成交活跃门控。
- **默认 horizon**：跟随被门控因子。
- **主要风险**：门控条件用未来状态。
- **Vortex 落点**：从单因子走向策略前的关键桥梁。

### 17. AgentStability

- **问题**：信号是否时间一致、平滑、不过度跳变？
- **字段**：factor history、returns、coverage。
- **候选表达**：因子自相关、rolling IC 稳定、信号换手惩罚。
- **默认 horizon**：20d、60d、120d。
- **主要风险**：把样本内稳定性误当样本外稳定。
- **Vortex 落点**：candidate hardening。

### 18. AgentComposite

- **问题**：多个弱信号能否互补形成更强、更稳的信号？
- **字段**：已有因子、风险暴露、相关性矩阵。
- **候选表达**：低相关因子组合、rank blend、残差化组合。
- **默认 horizon**：20d、60d。
- **主要风险**：用全样本最优权重。
- **Vortex 落点**：因子组合，不直接越过单因子评测。

### 19. AgentCreative

- **问题**：是否存在非线性变换、软门控或重参数化能增强解释？
- **字段**：依赖具体父因子。
- **候选表达**：clip、signed power、soft gate、分段 rank。
- **默认 horizon**：跟随父因子。
- **主要风险**：数学花活掩盖经济逻辑。
- **Vortex 落点**：mutation 工具，不单独作为因子来源。

### 20. AgentBarShape

- **问题**：K 线实体、影线、收盘位置是否反映交易行为？
- **字段**：open、high、low、close。
- **候选表达**：上影线衰竭、下影线支撑、收盘位置、实体压缩。
- **默认 horizon**：1d、5d、10d。
- **主要风险**：短线信号交易成本高。
- **Vortex 落点**：OHLC shape 与执行过滤。

### 21. AgentHerding

- **问题**：是否存在追涨拥挤、集体踩踏或方向一致性过强？
- **字段**：return、amount、volume、limit list、moneyflow。
- **候选表达**：放量上涨衰竭、连涨拥挤、板块一致性过高。
- **默认 horizon**：1d、5d、20d。
- **主要风险**：情绪数据可见时间和停牌/涨跌停约束。
- **Vortex 落点**：拥挤反转和风险过滤。

## 从 agent 到候选因子的输出模板

```yaml
agent: AgentLiquidity
hypothesis: 低成交额下的大价格冲击可能预示短期反转或流动性溢价
fields: [high, low, close, amount]
horizon: [1, 5, 20]
candidate_expression: "cs_rank((high - low) / amount)"
direction: "unknown_until_evaluated"
quality_risks:
  - amount 为 0 或极低导致除零
  - 小市值/低流动性容量不足
  - 需要与成交额下限联合使用
archive_policy: "无论通过或失败，都记录方向、覆盖率和容量风险"
```

## 拆分独立 skill 的条件

某个 agent 只有满足以下条件，才考虑拆为独立 skill：

1. 至少在两轮研究中被反复使用。
2. 有稳定字段白名单和默认 horizon。
3. 有明确 quality gate。
4. 有成功或失败档案可供复用。
5. 输出能进入标准 `factor-evaluation` 和 `factor-research-archive`。

在此之前，21 个 agent 都应留在 `cogalpha-factor-mining` skill 内作为角色卡。
