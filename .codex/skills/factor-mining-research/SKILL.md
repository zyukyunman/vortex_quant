---
name: factor-mining-research
description: '因子挖掘与策略研究唯一默认入口。Use when: 需要继续量化研究、挑战当前策略 preset、用 CogAlpha-first 流程生成候选因子、评测因子、做策略 overlay、用 version-review 对照现有 A 股策略版本。'
argument-hint: '描述当前策略瓶颈、已有数据字段、目标收益/回撤约束、希望挖掘的因子方向或资料来源'
tags: [vortex, vortex/skill, vortex/research-domain, vortex/strategy-domain]
obsidian_links:
  - "[[因子研究与评测全流程说明]]"
  - "[[研究协作与产物治理]]"
  - "[[超级鹿鼎公资料集]]"
  - "[[CogAlpha因子研究落地路线]]"
  - "[[CogAlpha Agent体系设计]]"
---

# 因子挖掘与策略研究

> 本 skill 是 Vortex 量化研究的唯一默认总入口。它不是直接抄论坛策略，也不是保证收益，而是把书籍、论坛、指数编制方案、公开策略样本、既有因子档案和当前策略版本转化为 **可验证的因子假设 → CogAlpha 方法生成候选 → quality gate → fitness → factor-evaluation → 策略角色判定 → version-review preset 对照 → 失败复盘与下一轮 evolution**。

默认原则：**除非用户只要求单个已知因子的评测，否则新的量化研究任务都从本 skill 开始。** CogAlpha 是本流程内部的候选生成和进化方法，不是独立策略入口、不是因子本身；`factor-evaluation` 是多周期实证层；`factor-evidence-reviewer` 是研究结论评审和接力层；`version-review` 是策略 preset 对照层；因子档案是经验沉淀层。

历史研究结论不写在本 skill 主体。当前 baseline、challenger、PRV、regime、execution 和评价协议复评结论，统一查阅：`docs/策略研究/策略研究结论档案.md`。

---

## 一、适用场景

当出现以下情况时使用本 skill：

1. 当前策略未达到目标，例如 A 股回测年化不足、最大回撤或 Calmar 仍有优化空间。
2. 现有红利、价值、低波、动量等常见组合无法继续改善。
3. 需要阅读仓库中的 `docs/book/`、`docs/index/`、`docs/html/` 资料，提炼新因子方向。
4. 需要参考聚宽、JoinQuant、GitHub 等公开社区中的策略思想，但不能直接复制代码。
5. 需要形成一批候选因子并进入 Vortex 的 `factor-evaluation` 标准评测流程。
6. 需要让 AI session 自动选择下一轮因子研究方向，并持续产生可归档 artifact。
7. 需要把新因子、overlay、容量 gate 或风险预算与当前策略 preset 做对照。
8. 需要判断“定版策略”是否仍可被新研究挑战，而不是只进入实盘维护。

---

## 二、资料使用原则

### 2.1 允许做什么

- 提炼思想、因子方向、数据需求和验证流程。
- 总结公开指数编制方案中的选样逻辑，例如红利质量、自由现金流。
- 从公开策略样本中识别常见因子族，例如小市值、成长价值、低波、反转、涨停动量。
- 将自然语言观点转化成 Vortex 可计算字段。

### 2.2 不允许做什么

- 不复制书籍正文、付费资料正文或论坛大段原文。
- 不直接搬运外部策略代码作为仓库实现。
- 不用未来函数、幸存者偏差、未披露数据或事后调参制造达标回测。
- 不把单段过拟合结果包装成最终策略。

---

## 三、已纳入的本地资料线索

### 3.0 统一研究主链

后续 Vortex 的量化研究不应在多个 skill 间分叉，而应走一条主链：

```text
factor-mining-research
  → 读取当前策略版本、因子档案、数据可用性
  → 选择研究问题：收益增强 / 回撤降低 / 容量提升 / 容错率提升
  → 使用 CogAlpha 方法生成候选（agent/recipe/quality gate/lineage）
  → factor-evaluation 多周期实证
  → factor-evidence-reviewer 复核可信度、过拟合、重复度和下一步接力
  → 策略角色判定：排序 / 过滤 / 风险预算 / 执行 gate
  → 与现有 preset 做 version-review 对照
  → 归档结论：promote / keep shadow / reject / mutate
```

策略 preset 定版只是当前稳定基线，不是量化研究结束。后续任何新因子、新 overlay、新执行逻辑都必须以当前 preset 为靶子，证明它能改善收益、最大回撤、Calmar、成交覆盖或容错率。

### 3.1 CogAlpha 默认研究引擎

当前 Vortex 已有 `vortex.research.cogalpha`。它是本 skill 的内部候选生成和进化方法：

| 能力 | 当前状态 |
|---|---|
| 21 个 CogAlpha agent catalog | 已登记 |
| 21 个 executable recipe | 已实现，`planned_recipes()` 应为空 |
| 语义成熟度 | `proxy` / `faithful_proxy` / `mutation_proxy` |
| 质量门禁 | 字段、算子、PIT、覆盖率、数值稳定性 |
| fitness | RankIC、ICIR、多空、coverage、决策分层 |
| artifact | `generation_report.json`、`generation_summary.json` |

本 skill 的默认动作不是手写一堆候选列表，而是：

```text
读取因子档案和资料线索
  → 选择一个研究方向 CogAlphaResearchDirection
  → 映射到 CogAlpha agents / recipes
  → run_cogalpha_research_cycle(...)
  → 读取 semantic_status_counts、elite/qualified/rejected、parent_templates
  → 把 qualified/elite 送入 factor-evaluation 和 factor-evidence-reviewer
  → 由证据评审决定进入策略 overlay、风险复核、执行复核或下一轮 mutation/crossover
  → 用 version-review 与当前策略 preset 对照
  → 写入因子档案、实验总表和策略版本候选
```

当前默认自动化方向：

```text
cogalpha_101_price_volume_defensive_evolution
```

研究重点：101/量价日频体系中的低波、反转、成交拥挤、静默流动性、路径质量、羊群拥挤和风险门控。选择它作为默认方向，是因为它与现有 `DailyFactorInputs(open/high/low/close/volume/amount)`、Alpha101 registry 和 CogAlpha 21 个 OHLCV recipe 最契合。

CogAlpha 相关细节可参考 `.github/skills/cogalpha-factor-mining/SKILL.md`，但默认不要把它作为并列入口调用；新研究应先从本 skill 开始。

### 3.2 超级鹿鼎公资料集

仓库索引：`docs/book/超级鹿鼎公资料集.md`

可提炼的因子方向：

| 因子方向 | 可量化代理 |
|---|---|
| 价值安全边际 | PE、PB、股息率、FCF yield 相对历史分位 |
| 现金流质量 | 经营现金流、自由现金流、OCF/负债、OCF/营业利润 |
| 垄断/确定性 | 高 ROE 稳定性、毛利率稳定性、行业集中度代理、低费用率 |
| 盈利底线 | 近 8-12 季度盈利波动、扣非净利润同比、ROE 标准差 |
| 周期位置 | 行业价格、库存、产能、毛利率分位、盈利修复斜率 |
| 趋势确认 | 均线、动量、相对强弱、突破/回撤位置 |
| 流动性安全 | 成交金额、换手率、停牌/涨跌停约束 |
| 杠杆风险 | 资产负债率、有息负债、现金短债比 |

### 3.3 中证红利质量指数

本地文件：`docs/index/932315_中证红利质量_编制方案.pdf`

可转化为因子/筛选规则：

1. 过去三年连续现金分红。
2. 过去三年平均股利支付率在 10% 到 100% 之间。
3. 过去三年平均现金股息率较高。
4. 过去 12 个季度 ROE 标准差较低。
5. 非金融：综合 ROE、DROE、经营现金流/总负债、现金分红/总市值。
6. 金融：综合 ROE、DROE、现金分红/总市值。
7. 权重约束：单样本不超过 10%，前五大不超过 40%，一级行业不超过 30%。

### 3.4 国证自由现金流指数

本地文件：`docs/index/980092_自由现金流_编制方案.pdf`

可转化为因子/筛选规则：

1. 剔除 ST、重大违规、经营异常、重大亏损。
2. 剔除最近半年日均成交金额排名后 20%。
3. 剔除金融和房地产行业。
4. 剔除近 12 个季度 ROE 稳定性排名后 10%。
5. 近一年自由现金流、企业价值、近三年经营现金流均为正。
6. 剔除经营现金流/营业利润排名后 30%。
7. 按自由现金流率 `FCF / EV` 排名。

### 3.5 公开社区/代码样本线索

直接抓取 JoinQuant 社区页面时可能只得到前端加载壳；可以改用公开搜索、GitHub 代码搜索和本地资料交叉验证。已观察到的公开样本类型包括：

| 线索 | 可提炼方向 |
|---|---|
| smallcap / 小市值策略 | 小市值 + 流动性 + ST/停牌过滤，但必须严控容量和回撤 |
| growth_value / 成长价值 | 估值便宜 + 盈利增长 + 质量过滤 |
| multi_factor_OOP / 多因子框架 | 标准化、排序、组合因子、定期调仓 |
| 涨停/龙头策略 | 情绪动量，可能高收益但高回撤、高交易约束 |
| 机器学习多因子 | 需要严格训练/验证分割，不能直接用全样本调参 |

---

## 四、因子挖掘工作流

### Step 1：把资料转成“因子假设”

每条候选必须写成下面格式：

```text
假设：高 FCF/EV 且 ROE 稳定的公司，未来 20-60 日收益更高。
原因：自由现金流代表真实可分配能力，ROE 稳定降低价值陷阱概率。
适用周期：20d / 60d / 120d。
风险：周期行业现金流高点、财报 PIT 延迟、行业暴露过重。
```

### Step 2：映射到 Vortex 数据字段

优先从已落盘数据中找字段：

| 数据集 | 典型字段 |
|---|---|
| `bars` | close、amount、volume |
| `valuation` | pe_ttm、pb、dv_ttm、total_mv、turnover_rate_f |
| `fina_indicator` | roe、roe_dt、gross_margin、dt_netprofit_yoy、ocf_yoy、profit_dedt |
| `cashflow` | n_cashflow_act、free_cashflow、c_cash_equ_end_period |
| `balancesheet` | total_assets、total_liab、money_cap、st_borr、lt_borr |
| `events/dividend` | 分红、送转、除权除息 |
| `stock_st/instruments` | ST、上市时间、行业 |

### Step 3：生成候选因子队列

优先从低实现成本、高解释力的候选开始，但不要只停留在自然语言列表；每条候选都要映射到 CogAlpha agent、recipe 或待实现 recipe。

1. `dividend_quality_score`
   - 股息率 + 分红连续性 + payout 合理性 + ROE 稳定性。
2. `fcf_yield_quality`
   - FCF/EV + OCF/营业利润 + ROE 稳定性。
3. `value_reversal_quality`
   - 低 PB/PE + 短期反转 + 财务质量过滤。
4. `low_vol_dividend_momentum`
   - 高股息 + 低波 + 中期趋势确认。
5. `smallcap_quality_guarded`
   - 小市值 + 盈利/现金流过滤 + 流动性约束。
6. `cycle_recovery_score`
   - 毛利率改善 + 盈利同比修复 + 价格趋势确认。
7. `smart_money_volume`
   - 成交额结构、放量持续性、量价位置；可与 `volume-peak-ridge-valley` skill 联动。

如果没有用户指定方向，默认先从 `cogalpha_101_price_volume_defensive_evolution` 展开，因为它是当前因子档案中的 active queue，且不依赖新增财务/事件数据。

### Step 4：低成本批量评测

对每个候选因子先做 CogAlpha generation + 低成本准入：

1. 调用 `run_cogalpha_research_cycle()` 或 `run_cogalpha_generation()` 生成本轮 `generation_report.json`、`generation_summary.json`、`research_cycle.json`。
2. 先看 `invalid/rejected` 原因，禁止绕过 quality gate。
3. 只把 `qualified/elite` 或具备明确研究价值的 `rejected` 送入后续 `factor-evaluation`。
4. 对 mutation/crossover 子代，必须记录 `parent_templates` 和 lineage。

低成本评测之后必须增加证据评审，不允许只凭 `qualified/elite` 状态继续推进：

1. 检查是否回答原始假设卡。
2. 检查是否只在单一 horizon、单一年份、单个参数窗口有效。
3. 检查与已有因子或父代的重复度。
4. 检查搜索预算和失败路径是否被记录。
5. 输出下一位 Agent 和接力包：CogAlpha PI、归档审计员、风险官、策略晋升专员或执行运营。

进入 factor-evaluation 后，再做：

1. 多周期 RankIC：`1d, 5d, 20d, 60d, 120d`。
2. 多空组合：默认 5 组，先看 5d/20d。
3. 稳定性：ICIR、positive_rate、年度分段。
4. 覆盖率：低于 70% 先降级或补数据。
5. 相关性：与已有效因子相关过高时降权。

准入建议：

| 条件 | 说明 |
|---|---|
| `IC_mean_20d > 0.03` 或方向翻转后满足 | 有基础预测力 |
| `ICIR_20d > 0.3` | 稳定性最低线 |
| `positive_rate_20d > 55%` | 不是少数月份贡献 |
| 多空收益 > 0 | 能转成交易价值 |
| 分组收益大体单调 | 避免只由极端组贡献 |

### Step 5：进入策略角色判定与 overlay

只有通过准入的 signal 才进入策略层。先判断它在策略里扮演什么角色，不要直接替代主策略：

| 角色 | 判断问题 | 接入方式 |
|---|---|---|
| 候选池排序 | 是否能在主策略候选池内改善买入优先级 | candidate pool rerank / factor fusion |
| 坏持仓过滤 | 是否能识别未来拖累组合的持仓 | hard filter / trim / shadow warning |
| 风险预算 | 是否能降低弱市左尾或拥挤风险 | position haircut / regime gate |
| 执行 gate | 是否能提高可成交性、容量或补买质量 | capacity gate / buy limit / participation replay |
| 独立 alpha | 是否与主策略低相关且独立贡献收益 | 单独策略或组合腿，不直接混入 live |

策略搜索顺序：

1. 先固定简单组合：Top-N、等权、月频。
2. 再加风控：单股上限、行业上限、流动性、ST、停牌、涨跌停。
3. 再加择时：市场均线、宽度、波动、指数趋势。
4. 再加组合优化：多因子加权、低相关因子组合。
5. 最后做滚动样本验证。

### Step 6：用策略 preset 做 version-review 对照

策略层有效不代表可以直接上线。必须与当前已定版 preset 对照，证明是否值得进入 shadow、paper/live 或新版本候选。

当前工程入口：

```python
from vortex.strategy.earnings_forecast_runner import run_earnings_forecast_version_review
```

CLI：

```bash
vortex strategy earnings-forecast version-review \
  --root /path/to/workspace \
  --preset aggressive_100w \
  --start 20170101 \
  --end 20261231
```

当前还可使用的策略研究闭环入口：

```bash
# 选股有效性/稳定性审判：事件分桶、排名层级、赢家/输家画像、风格暴露
vortex strategy earnings-forecast selection-stability-review \
  --root /path/to/workspace \
  --start 20170101 \
  --end 20260430

# CogAlpha 按策略角色生成候选：bad_holder / candidate_quality / regime_execution
vortex strategy earnings-forecast cogalpha-role-cycle \
  --root /path/to/workspace \
  --role bad_holder \
  --start 20170101 \
  --end 20260430

# 把通过 CogAlpha 的候选按角色接入 preset，对照收益/回撤/Calmar
vortex strategy earnings-forecast factor-overlay-challenge \
  --root /path/to/workspace \
  --preset baseline_top110_large \
  --start 20170101 \
  --end 20260430

# 对 promoted overlay 做时间、成本、TopN 邻域扰动
vortex strategy earnings-forecast robustness-matrix \
  --root /path/to/workspace \
  --preset baseline_top110_large \
  --challenger rerank_tail_risk_w010 \
  --start 20170101 \
  --end 20260430

# 对 tail-risk 进行日频长窗口 mutation 网格：权重、候选池、soft trim、轻量 regime 二级腿
vortex strategy earnings-forecast daily-mutation-grid \
  --root /path/to/workspace \
  --preset baseline_top110_large \
  --start 20170101 \
  --end 20260430

# 对 promoted overlay 做整手撮合 + 目标价全天分钟容量执行复核
vortex strategy earnings-forecast overlay-execution-review \
  --root /path/to/workspace \
  --preset baseline_top110_large \
  --challenger tail_risk_soft_q10_p25 \
  --start 20170101 \
  --end 20260430

# 对 overlay challenger 测试轻量 market/regime 风险预算
vortex strategy earnings-forecast regime-budget-challenge \
  --root /path/to/workspace \
  --preset baseline_top110_large \
  --challenger tail_risk_soft_q10_p25 \
  --start 20170101 \
  --end 20260430

# 对已有全 A / long-window PRV panel 做目标池复验，判断 alpha、risk shadow、execution warning 或 reject
vortex strategy earnings-forecast prv-target-pool-review \
  --root /path/to/workspace \
  --preset baseline_top110_large \
  --challenger tail_risk_soft_q10_p25 \
  --start 20170101 \
  --end 20260430
```

当前 preset：

| preset | 定位 | 规则 |
|---|---|---|
| `aggressive_100w` | 100 万进攻候选 | Top60 候选池，amount20 liquidity rerank，选 Top30，整手撮合 |
| `stable_100w` | 100 万稳健参考 | baseline Top50，整手撮合 |
| `liquidity_top80` | 5000 万/1 亿研究版 | Top160 候选池，amount20 liquidity rerank，选 Top80 |
| `liquidity_top90_1000w` | 1000 万候选版 | Top180 候选池，amount20 liquidity rerank，选 Top90 |
| `baseline_top110_large` | 中大资金 challenger | baseline Top110，不做 rerank，按 1 亿整手执行口径复核 |

version-review 至少产出：

1. JSON summary。
2. 指标 CSV。
3. 每日权重。
4. 整手成交明细。
5. 买单意图。
6. 执行诊断。

#### 策略评价协议：不只看年化和最大回撤

后续策略晋级必须使用 **Strategy Evaluation Protocol**，而不是只看年化、最大回撤或单一 Sharpe。

主次顺序：

| 层级 | 指标 | 用途 |
|---|---|---|
| 第一核心 | Calmar / MAR | 年化收益相对最大回撤，衡量单位回撤换来的收益 |
| 第二核心 | Sortino | 只惩罚下行波动，避免把上涨波动当成坏事 |
| 辅助参考 | Sharpe | 收益平滑度参考，不作为唯一晋级标准 |
| 左尾风险 | CVaR 5%、worst5d、worst20d | 衡量坏日子、最差一周和最差一个月的压力 |
| 路径质量 | 最大回撤恢复天数、回撤持续天数 | 衡量资金和心理占用 |
| 执行质量 | 容量折损后 Calmar、成交覆盖率、成本敏感性 | 判断理论收益是否可实现 |

术语白话定义：

| 指标 | 中文解释 | 计算口径 |
|---|---|---|
| CVaR 5% | 平均极端亏损 | 把每日收益从差到好排序，取最差 5% 的交易日，计算这些坏日子的平均收益 |
| worst5d | 最差 5 日累计收益 | 在所有滚动 5 个交易日窗口中，找累计收益最低的一段 |
| worst20d | 最差 20 日累计收益 | 在所有滚动 20 个交易日窗口中，找累计收益最低的一段 |
| recovery days | 回撤恢复天数 | 从净值创新高后跌入回撤，到重新创新高之间的交易日数 |

##### Hard gate：任一失败直接 reject

| gate | 具体标准 |
|---|---|
| PIT | 不得使用未来公告、未来财务、未来复权、未来成分或未来成交信息 |
| 数据覆盖 | 核心字段覆盖低于 70% 时必须降级；无解释则 reject |
| 成本 | 必须计入当前 preset 对应成本，不能只看零成本回测 |
| 交易约束 | 必须处理涨跌停、停牌、ST/风险警示、整手约束 |
| 参数透明 | 不允许只展示最优参数；必须展示邻域、分段或样本外 |
| 执行复核 | 大资金策略必须做整手/分钟容量或成交覆盖复核 |

##### `promote_challenger`

新策略、因子 overlay 或 preset mutation 想成为 paper/shadow challenger，至少满足：

| 维度 | 具体标准 |
|---|---|
| 收益 | 年化高于 baseline；或年化不低于 baseline - 1pct 且显著改善风险 |
| Calmar | 高于 baseline 至少 5%，或绝对提升 >= 0.20 |
| Sortino | 不低于 baseline，优先要求提升 >= 5% |
| 最大回撤 | 不比 baseline 恶化超过 1pct，且相对恶化不超过 10% |
| 回撤恢复 | 最大回撤恢复天数不比 baseline 恶化超过 20% |
| CVaR 5% | 平均极端亏损不比 baseline 更差 |
| worst5d / worst20d | 最差 5 日 / 20 日累计亏损不比 baseline 更差超过 10% |
| 稳健性 | 年度、牛熊、成本、TopN、资金规模场景中 Calmar 胜率 >= 60% |
| 执行 | 容量折损后仍高于 baseline；理论收益保留率 >= 85% |

##### `keep_shadow`

不能进入默认策略，但值得保留为 warning / risk shadow / 执行辅助：

| 维度 | 具体标准 |
|---|---|
| 收益 | 年化可低于 baseline，但降幅通常不超过 2pct |
| 风险 | 最大回撤、CVaR、worst5d/worst20d、恢复天数至少一个明显改善 |
| 解释 | 能稳定解释坏持仓、拥挤、成交失败、极端状态或行业/事件风险 |
| 稳健性 | 至少在样本外或多年份中保持方向一致 |
| 接入 | 不改变默认持仓，只作为半权 shadow、warning、审计字段或下一轮 mutation parent |

##### `reject`

满足任一项则拒绝：

| 情况 | 说明 |
|---|---|
| 单窗口有效 | 只在 2025-2026 或某个短窗口有效 |
| 执行后失效 | 理论回测胜出，但整手/分钟容量后输给 baseline |
| 尾部变差 | 年化提升但 CVaR / worst5d / worst20d 明显变差 |
| 回撤恢复变差 | 最大回撤差不多，但恢复时间显著变长 |
| 参数脆弱 | 相邻 TopN、成本、权重参数一变就失效 |
| 与 baseline 重复 | 增量来自已有 liquidity/tail-risk 等因子，不能提供独立价值 |

阶段性结论档案：

- 具体 baseline/challenger、tail-risk、execution、regime、PRV 与评价协议复评结论，不写在本 skill 主体。
- 查询 `docs/策略研究/策略研究结论档案.md`。
- 每次新研究若形成阶段性结论，应追加到该档案，并记录 workspace artifact 路径。

#### 训练 / 验证 / 测试与 CPCV

后续研究必须区分样本角色：

| 阶段 | 用途 | 是否允许调参 |
|---|---|---|
| Discovery / 训练集 | 找因子方向、调阈值、生成候选 | 可以，但必须记录参数搜索范围 |
| Validation / 验证集 | 选择少数候选，做参数邻域和 robustness | 可以有限选择，不能反复回看 |
| Test / 测试集 | 最终样本外验收 | 不允许调参，只能一次性报告 |

默认低成本方法是 Walk-forward：

```text
用过去一段训练 → 下一段测试 → 向前滚动。
```

更严格的方法是 CPCV（Combinatorial Purged Cross-Validation，组合式净化交叉验证）：

```text
把时间序列切成 N 个连续区块。
每次选择其中 k 个区块作为测试集，剩余区块作为训练集。
遍历多种测试区块组合，得到很多条样本外路径。
训练集与测试集之间做 purge / embargo，避免标签重叠和信息泄漏。
```

| 概念 | 解释 |
|---|---|
| N groups | 把 2017-2026 按时间切成 N 段 |
| k test groups | 每次拿 k 段做测试，其余做训练 |
| Purge | 删除训练集中与测试标签窗口重叠的样本，避免收益标签泄漏 |
| Embargo | 测试集前后留出缓冲期，避免持仓期或事件影响穿透 |
| Path distribution | 不只看一个 OOS 结果，而看多条 OOS 路径的分布 |

CPCV 验收建议：

| 输出 | 标准 |
|---|---|
| OOS Calmar 分布 | 中位数高于 baseline，且 25% 分位不明显低于 baseline |
| OOS Sortino 分布 | 中位数不低于 baseline |
| OOS 年化分布 | 中位数高于 baseline，极差不能过大 |
| OOS max drawdown 分布 | 75% 分位回撤不比 baseline 明显差 |
| OOS win rate | 多数 CPCV path 打赢 baseline，建议 >= 60% |

#### Quant harness engineer

`harness engineer` 在 Vortex 里应理解为 **量化评测线束工程师**：不是只发明因子的人，而是负责把“怎么测策略”工程化的人。

| 职责 | Vortex 落地 |
|---|---|
| 设计测试集 | 年度、牛熊、事件密度、行业、流动性、资金规模切分 |
| 防数据泄漏 | PIT、purge、embargo、未来数据检查 |
| 建指标协议 | Calmar、Sortino、CVaR、worst5d、执行折损、OOS win rate |
| 自动化回归 | 新因子不能破坏已有 baseline / paper challenger |
| 实验可复现 | 固定 artifact schema、参数、数据版本、随机种子 |
| 失败归因 | 区分 alpha 失败、执行失败、容量失败、过拟合失败 |
| 晋级门禁 | promote / shadow / reject 不靠感觉，而靠协议 |

产品判断：Vortex 后续不仅要做 factor engineering，也要做 quant harness engineering。评测协议和回测线束的重要性不低于因子本身。

### Step 7：失败复盘

未达标时必须归因：

| 问题 | 下一步 |
|---|---|
| 年化不足 | 挖更强 alpha：动量、反转、周期修复、小市值质量 |
| 回撤过大 | 加市场过滤、行业约束、低波、止损冷却、降低集中度 |
| 换手过高 | 延长调仓周期、加入因子自相关过滤 |
| IC 有效但回测无效 | 检查交易成本、涨跌停、容量、分组单调性 |
| 回测好但样本外差 | 降低参数自由度，做 Walk-Forward |

---

## 五、A 股收益/回撤目标的处理

这是优化方向，不是可保证承诺，也不是对所有研究阶段都强制适用的硬门槛。skill 执行时必须遵守：

1. 不把某个固定回撤阈值当作用户永久硬约束；除非用户在当前任务中明确指定。
2. 可以输出“当前最优策略候选”和剩余风险，不因未触及某个单一阈值否定已有增量。
3. 必须继续生成下一轮因子或风控实验队列。
4. 不允许用未来函数、全样本调参、未计成本或错误数据单位制造达标。
5. 策略 preset 定版只代表“当前稳定基线/可复核候选”，不代表量化研究结束。
6. 新研究必须挑战当前 preset，而不是绕开基线重新讲故事。

当前研究主线：

| 研究线 | 目标 | 与 preset 的关系 |
|---|---|---|
| 版本 hardening | 年度分段、牛熊、成本、停牌/涨跌停、数据缺失压力测试 | 验证 preset 是否可 paper/live |
| 因子融合 | 候选池排序、坏持仓过滤、风险预算、执行 gate | 必须用 version-review 打赢或解释 current preset |
| 分钟容量 | 目标价全天容量、partial/AON、补买、容量退化曲线 | 提高可成交性和容错率 |
| PRV / 微观结构 | 拥挤风险、成交结构、容量质量、极端 guard | 先 shadow，不直接进默认 alpha |
| CogAlpha 下一轮 | 事件 alpha + 流动性安全、regime-gated 变体、坏持仓识别 | 生成候选，再回到本流程评测和对照 |

---

## 六、与其他 Vortex skills 的协作

| skill | 协作方式 |
|---|---|
| `cogalpha-factor-mining` | CogAlpha 方法附录；提供 agent/recipe、quality gate、semantic status、lineage 和 mutation/crossover 细节。默认不要作为并列入口直接调用 |
| `factor-evaluation` | 对新因子做多周期 IC、多空组合和准入判断 |
| `factor-research-archive` | 把 CogAlpha artifact、好坏因子、失败原因和下一轮队列沉淀到 Obsidian 档案 |
| `dividend-yield-strategy` | 处理红利/股息率、4 进 3 出、分红持续性 |
| `volume-peak-ridge-valley` | 挖掘分钟成交量微观结构因子 |
| `tushare` | 补齐缺失数据、检查字段权限和导出研究表 |

---

## 七、输出模板

每次使用本 skill 后，至少输出：

1. 本轮资料来源和可用性。
2. 本轮 CogAlpha research direction、agents、recipes。
3. `generation_report.json`、`generation_summary.json`、`research_cycle.json` 路径。
4. `semantic_status_counts`、elite/qualified/rejected/invalid 分布。
5. 每个因子的字段映射、lineage、parent_templates。
6. 评测周期和准入阈值。
7. 策略角色判定：排序、过滤、风险预算、执行 gate、独立 alpha 或 reject。
8. 与当前 preset 的 version-review 对照结果：收益、回撤、Calmar、成交覆盖、执行诊断。
9. 是否进入因子档案、factor-evaluation、策略 overlay、新 preset 候选或下一轮 mutation/crossover。
10. 未达标时的下一轮实验队列。
11. 若做了 overlay：输出 `promote_challenger / keep_shadow / reject / mutate`，并说明是否已通过 robustness matrix。
