---
tags: [vortex, vortex/research-domain, vortex/factor-archive]
aliases: [外部Alpha资料线索, external_alpha_sources]
created: 2026-05-01
updated: 2026-05-02
status: research_lead
factor_family: research_source
data_sources: [external_research, github, quantpedia, jkp]
artifact_root: /Users/zyukyunman/Documents/vortex_workspace/research/factor-reports/
---

# 外部 Alpha 资料线索

关联：[[因子研究档案]]、[[动量与101量价因子]]、[[红利与自由现金流因子线索]]、[[CogAlpha学习资料]]、[[CogAlpha因子研究落地路线]]、[[CogAlpha与Vortex落地讨论]]

## 一句话结论

外部资料最值得转化的不是单个公式，而是研究框架：JKP 的跨国 153 因子主题库适合补齐基础因子地图，WorldQuant Alpha101 适合提供公式生成语法，Quantpedia 适合提供低频策略主题，CogAlpha 适合提供“LLM 生成—质量检查—适应度评估—进化反馈”的自动化研究闭环，但所有线索都必须在 Vortex 的 PIT 数据和 A 股交易约束下重新实现。

## 已确认来源

| 来源 | 价值 | 本轮判断 |
|---|---|---|
| JKP Factors | 153 characteristics/factors、13 个主题、跨 93 个国家，网站数据更新到 2025-12 | 最适合做“因子地图”和遗漏检查 |
| `bkelly-lab/jkp-data` | 提供因子数据项目结构、portfolio construction、characteristics 辅助函数 | 可参考主题组织和贡献规范，不直接复制代码 |
| WorldQuant Alpha101 | `rank / ts_rank / decay / correlation / delta` 等公式语法 | 可转成 Vortex 自有公式队列 |
| `laox1ao/Alpha101-WorldQuant` | Python 实现 Alpha101，GitHub 搜索约 61 stars | 只作为公式名称/结构线索 |
| `Parsnip77/Multi-factor-Model-for-Stock-Selection` | A 股多因子 pipeline：Alpha101、清洗、中性化、IC、分层回测、滚动 OLS、LightGBM/SHAP | 对 Vortex 的后续工程化很有参考价值 |
| Quantpedia strategies | 大量策略主题，覆盖日频/周频/月频权益策略 | 可做策略灵感索引，但页面摘要不能直接当因子结论 |
| CogAlpha / Cognitive Alpha Mining | LLM 驱动的代码化 alpha 生成、质量检查、fitness evaluation、mutation/crossover 进化框架 | 当前未找到官方 GitHub 实现；已升级为 Vortex 方法论与 skill 路线，详见 [[CogAlpha学习资料]] 和 [[CogAlpha因子研究落地路线]] |
| `JacobDu/agentic-alpha` | 基于 Qlib 的 agent 辅助因子挖掘环境，包含 Research → Retrieve → Generate → Evaluate → Validate → Distill 工作流 | 与 CogAlpha 落地思路最接近，适合参考 Validate、因子库和 agent workflow |
| `sw0843/Auto-Alpha-Finding` | GPlearn 遗传规划挖因子，包含残差正交化、相关性过滤、周频多空和因子池归档 | 可参考自动挖掘和因子池治理，但市场、交易和执行环境不同 |

## 可转化研究队列

| 队列 | 转化方式 | 优先级 |
|---|---|---|
| JKP value/profitability/investment themes | 用 `valuation`、`fina_indicator`、`cashflow`、`balancesheet` 做 PIT 因子组 | 高 |
| JKP momentum/reversal/low-risk themes | 接入 [[动量与101量价因子]]，补行业/市值中性化和分层回测 | 高 |
| 101 Alpha formula grammar | 先实现一组通用算子：`rank`、`ts_rank`、`delta`、`corr`、`decay_linear` | 高 |
| A 股 factor pipeline | 补中性化、滚动线性合成、walk-forward 合成因子 | 中 |
| CogAlpha-style agentic mining | 通过 [[CogAlpha因子研究落地路线]] 先落地 skill、agent catalog、quality gate 和 fitness 规范；后续再做 workspace 小 CogAlpha runner | 高 |
| LightGBM/SHAP | 只在可解释因子池稳定后引入，不作为第一优先 | 中 |
| Quantpedia low-frequency equity themes | 提炼事件、季节性、行业轮动、低风险、质量动量主题 | 中 |

## 本轮得到的经验

1. 公开 Alpha101 仓库可以启发公式，但不能照搬成策略；本轮 [[动量与101量价因子]] 已证明很多强 IC 公式直接 TopN 多头会失败。
2. JKP 的价值在“主题覆盖度”：避免我们只盯业绩预告、低波和资金流，漏掉投资、盈利能力、发行/回购、资产增长等低频因子。
3. A 股项目样例的工程流程值得借鉴：因子清洗、中性化、IC、分层回测、滚动合成、ML baseline；但数据口径必须回到 Vortex 的 PIT 和本地 data registry。
4. Quantpedia 更像策略主题库，不是可直接复现的因子库；适合把“日频权益、周频权益、月频权益”策略分类后转成实验队列。
5. CogAlpha 最值得学的是闭环，不是某个公式：代码化 alpha、质量检查、评测反馈、失败原因回写和进化搜索，比单次 LLM 生成更接近研究员工作流；本轮已明确应沉淀为 `cogalpha-factor-mining` skill。
6. GitHub 暂未找到 CogAlpha 官方实现；相近项目能提供工程参考，但不能替代 Vortex 自己的 PIT、成本、容量和可交易性验证。

## 下一步

1. 基于 JKP 主题做 Vortex 因子地图：value、profitability、investment、momentum、reversal、low risk、issuance、seasonality。
2. 给 `vortex.research` 增加公式算子层之前，先用 workspace 脚本验证 10-20 个 PIT 安全公式。
3. 对本轮强 IC 但多头失败的低波/拥挤反转因子，补行业/市值中性化。
4. 在候选池稳定后再考虑滚动 OLS 或 LightGBM 合成，不提前引入强模型。
5. 基于 [[CogAlpha因子研究落地路线]] 执行 Phase 0：先用 skill 和 agent catalog 统一研究语言，再决定是否做 workspace 级小实验。
