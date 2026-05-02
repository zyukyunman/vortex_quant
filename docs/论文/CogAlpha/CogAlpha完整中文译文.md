---
tags: [vortex, vortex/research-domain, vortex/factor-methodology]
aliases: [CogAlpha完整中文译文, Cognitive Alpha Mining 完整中文译文]
created: 2026-05-02
updated: 2026-05-02
status: learning_translation
source: arxiv:2511.18850v3
---

# CogAlpha 完整中文译文

关联：[[CogAlpha学习资料]]、[[CogAlpha课堂讲义]]、[[CogAlpha与Vortex落地讨论]]

> 译文说明：本文是用于 Vortex 内部学习的纯中文译文，按论文结构完整覆盖主要正文、附录、实验设置和结论。为便于学习，公式渲染异常处保留指标含义和计算口径说明；图表不复制原图，只翻译图意和表意。

---

## 题名、作者与摘要

**题名**：通过 LLM 驱动的代码化进化进行认知式 Alpha 挖掘

**英文题名**：Cognitive Alpha Mining via LLM-Driven Code-Based Evolution

**作者与机构**：论文作者来自中国移动九天研究院、香港大学计算与数据科学学院、Grace Investment Machine 等机构。

### 摘要译文

从高维、极低信噪比的金融数据中发现有效预测信号，也就是 alpha，仍然是一个困难且开放的问题。尽管深度学习、遗传编程以及近来的大语言模型因子生成方法已经取得进展，现有方法仍然只探索了巨大 alpha 搜索空间中的狭窄区域。神经网络模型往往产生不透明、脆弱的模式；符号化或公式化方法则经常生成冗余、缺乏经济基础且泛化较差的表达式。

虽然这些方法形态不同，但它们共享一个关键局限：没有一种方法能够进行广泛、结构化、类似人类研究员的探索，同时兼顾逻辑一致性和创造性跳跃。为弥补这一缺口，论文提出认知式 Alpha 挖掘框架 CogAlpha。该框架结合代码级 alpha 表达、LLM 驱动的推理以及进化搜索。

CogAlpha 将 LLM 视为自适应认知代理，通过多阶段 prompt 和金融反馈，迭代地改进、变异和重组 alpha 候选。这样的协同设计能够带来更深层的思考、更丰富的结构多样性，以及具有经济解释性的 alpha 发现，同时显著扩展有效搜索空间。论文在三个股票市场的五个股票数据集上进行实验，结果显示 CogAlpha 能持续发现预测准确性、鲁棒性和泛化性优于现有方法的 alpha。论文结果强调，将进化优化与 LLM 推理对齐，有望实现自动化、可解释的 alpha 发现。

---

## 1. 引言

Alpha mining 是从股票等金融市场中发现预测性金融信号，即 alpha，用于预测未来资产收益的过程。金融市场具有高维度、波动率随时间变化和低信噪比等特点，因此，识别可解释、可靠、多样化，并能支持持续盈利和有效风险管理的 alpha，仍然十分困难。

过去几十年，alpha mining 经历了几次主要转变：从早期人工构造，到机器学习驱动的自动化，再到近年使用 LLM 的生成式与推理式探索。

最早阶段，alpha 因子由金融专家基于经济直觉和经验观察手工设计。典型例子包括 Fama-French 因子和各种文献中记录的金融异象。这类人工构造的 alpha 可解释性强，理论基础较好。但设计过程劳动密集、效率低；随着市场复杂度和数据丰富度提升，人工方法难以扩展，类似策略拥挤后边际收益下降。

为了提升效率，研究者开始使用机器学习模型进行 alpha 发现。一类方法直接使用神经网络，从市场数据中隐式提取复杂、非线性的 alpha 结构。这类方法预测能力强，能捕捉高维非线性依赖，但也有天然弱点：模型经常像黑箱，难以追踪决策逻辑，也难以评估其在市场状态变化下的鲁棒性；因此，一旦面对 regime shift 或未见过的模式，性能容易衰减。

另一类公式化方法试图寻找显式数学表达式形式的 alpha。许多基于遗传编程和强化学习的框架被提出，用于自动搜索符号公式空间。它们的优点是表达透明、容易复现和评估；缺点是生成公式往往过度复杂、冗余，且缺少坚实的经济或金融解释，导致真实交易环境中的泛化性和稳定性有限。

虽然神经网络和公式搜索差异很大，但它们共同的问题是：搜索过程效率低，探索范围窄。它们都难以模拟人类研究员那种既保持逻辑一致，又能进行跳跃式创造的推理。这使得算法探索和真正概念创新之间仍然存在关键差距。

近年，LLM 被引入 alpha mining，因为它们具有知识整合、抽象和生成式推理能力。LLM 可以综合金融知识，并大规模提出新的公式化表达。但现有 LLM alpha mining 方法多数仍然依赖公式堆叠和模式重复，而不是真正的推理或结构创新。因此，生成因子容易冗余，也容易受到拥挤效应影响，限制其在动态市场环境中的可持续性。

论文认为，关键研究缺口在于：如何让 LLM 从简单的模式复制器，进化为真正的认知式思考者。更具体地说，需要一种框架，使 LLM 能进行更深层思考、更丰富的结构多样性，以及有经济基础的探索，从而提升所发现 alpha 的长期稳定性和鲁棒性。这会推动该领域从暴力搜索或浅层公式生成，走向知识驱动、可解释的 alpha 发现范式。

为弥补这一缺口，论文提出 CogAlpha。名字包含两层含义：Cognitive 和 Alpha。Cognitive 指利用上一代和不同 agent 的迭代反馈，支持自适应生成，从而超越浅层模式识别，走向类似人类的分析推理。Alpha 对应量化金融中发现盈利信号的核心目标。通过整合能诱导 LLM 深度思考的进化搜索过程、七层 agent hierarchy 和多 agent 质量检查器，CogAlpha 体现了走向认知式 Alpha 挖掘的思路。

论文结构如下：第 2 节回顾 LLM 驱动 alpha mining 和 LLM 深度思考相关工作；第 3 节详细介绍 CogAlpha，包括七层 agent hierarchy、多 agent quality checker 和 thinking evolution；第 4 节说明实验设置，并在五个股票数据集上报告实验结果，重点是 CSI300；第 5 节总结并提出未来研究方向。

论文贡献可以概括为三点：

1. 提出 Cognitive Alpha Mining 概念，为自动化、稳健、可解释的 alpha 发现打开新方向，并通过 CogAlpha 框架形式化。
2. 提出 CogAlpha 方法：结合诱导 LLM 深度思考的进化搜索、七层 agent hierarchy 和多 agent quality checker。
3. 在三个股票市场的五个数据集上进行广泛实验，证明 CogAlpha 的有效性；论文报告称其生成 alpha 相比 baseline 有更强预测表现、更高稳定性和更好解释性。

---

## 2. 相关工作

### 2.1 基于 LLM 的 Alpha Mining

Alpha mining 是量化金融的基础任务，目标是在股票市场中发现预测信号，也就是 alpha 因子。过去方法主要依赖人类专家、遗传编程、强化学习或深度学习来探索庞大的因子空间。这些方法都有内在局限：可能效率不高，可能产生过度复杂的解，也可能解释性不足。

近年，LLM 凭借广泛世界知识和强推理能力被引入 alpha mining。例如，AutoAlpha 使用 LLM 评估和选择较优 alpha 候选；agentic 框架也被纳入以增强适应性和自动化。AlphaAgent 提出基于 agent 的架构，并用正则化探索策略挖掘抗衰减 alpha；AlphaJungle 提出 LLM 驱动的蒙特卡洛树搜索框架，让 LLM 进行多步公式改进；RD-Agent(Q) 提出数据中心反馈闭环和因子-模型联合优化，使因子能在动态市场中持续适应。

但论文认为，现有 LLM alpha mining 仍依赖公式化搜索表示，这限制了探索范围，使其停留在因子空间较浅区域，也未能充分发挥 LLM 在推理和代码生成上的优势。CogAlpha 则让 LLM 直接执行代码化进化，从而探索更广、更深的搜索空间。

### 2.2 LLM 思维进化

为了进一步挖掘 LLM 潜力，许多方法试图增强其思考和推理能力。近期研究探索了将遗传算法、进化算法与 LLM 结合。例如，Mind Evolution 使用进化搜索策略扩展大模型推理时计算；WizardLM 自动生成大量开放领域、多主题、多难度指令来增强 LLM；EvoPrompt 将进化算法与 LLM 结合，通过初始化、选择、交叉、变异和评估等算子优化 prompt；FunSearch 使用 LLM 引导的进化搜索发现数学启发式，在构造新数学对象和算法发现方面表现突出；AlphaEvolve 进一步扩大这一思想，提出自主进化式代码流水线，让 LLM 生成代码变体，并由 evaluator 迭代评估与改进。

此外，LLM 与进化方法也被用于文本生成和代码生成。尽管已有这些进展，现有工作没有专门聚焦于从高波动金融市场中提取有效信号。为此，论文提出 CogAlpha：结合进化搜索诱导 LLM 深度思考，并与七层 agent hierarchy 和多 agent quality checker 协作，生成稳健、可解释的 alpha 因子。

---

## 3. 方法

CogAlpha 的目标是模拟类似人类的推理，发现更复杂、更有逻辑、更可解释的 alpha 解。它使用进化搜索策略诱导 LLM 深度思考，并结合七层 agent hierarchy 和多 agent quality checker 来执行 alpha mining。

CogAlpha 生成的每个 alpha 都附带详细注释，解释其逻辑、澄清底层想法，并给出对应公式。注释之后提供实现代码。论文在本节介绍 CogAlpha 的核心组件，以及每个部分在整体框架中的作用。

### 3.1 七层 Agent Hierarchy

可用原始因子只有开盘价、最高价、最低价、收盘价和成交量，也就是 OHLCV。基于这五类输入，论文设计七层 agent hierarchy，以尽可能全面探索 alpha。该 hierarchy 包含 21 个不同 agent。从宏观层面到微观层面，这些 agent 被组织为七个层级。每个 agent 专注于一个独立的 alpha 发现方向，并根据其指定探索策略独立生成一组 alpha 因子。

七层含义如下：

| 层级 | 中文译名 | 对应 agent | 探索方向 |
|---|---|---|---|
| I | 市场结构与周期层 | AgentMarketCycle、AgentVolatilityRegime | 长期趋势、市场阶段、周期状态转换、波动 regime |
| II | 极端风险与脆弱性层 | AgentTailRisk、AgentCrashPredictor | 尾部风险、崩盘前兆、系统脆弱性和压力积累 |
| III | 价格-成交量动态层 | AgentLiquidity、AgentOrderImbalance、AgentPriceVolumeCoherence、AgentVolumeStructure | 流动性、订单失衡、价量一致性、成交结构 |
| IV | 价格-波动行为层 | AgentDailyTrend、AgentReversal、AgentRangeVol、AgentLagResponse、AgentVolAsymmetry | 趋势延续、短期反转、波动聚集、非对称价格动态 |
| V | 多尺度复杂性层 | AgentDrawdown、AgentFractal | 跨尺度不规则性、分形粗糙度、回撤-恢复几何、长记忆 |
| VI | 稳定性与状态门控层 | AgentRegimeGating、AgentStability | 时间稳定性、不同市场状态下的信号启停门控 |
| VII | 几何与融合层 | AgentBarShape、AgentCreative、AgentComposite、AgentHerding | K 线几何、多因子融合、非线性变换、羊群行为 |

论文图 1 的图意：七层 agent hierarchy 从 OHLCV 数据产生初始 alpha；multi-agent quality checker 验证每段 alpha 代码的有效性和质量；filtering 模块用五个预测力指标评估所有 alpha 代码；thinking evolution 模块在每轮迭代中通过 LLM 更深层推理，持续改进和重组合格候选。

### 3.2 多样化引导

为了沿每个 alpha 发现方向进行更精确、更全面的探索，论文将原始 guidance 生成扩展为五种改写模式：light、moderate、creative、divergent 和 concrete。

| 模式 | 中文解释 | 作用 |
|---|---|---|
| light | 轻微改写 | 尽量保持原意一致，用于语言变化下的一致性测试 |
| moderate | 中度改写 | 自然改写并略微丰富表达，保持同一分析焦点 |
| creative | 创造性改写 | 加入研究式解释深度，激发同一概念边界内的替代理路 |
| divergent | 发散式改写 | 从相关但新的分析视角重写，生成互补假设 |
| concrete | 具体化改写 | 把抽象描述转成可度量形式，如公式、比率、统计操作 |

这五种改写风格共同扩大语义覆盖，并加深因子推理，同时不偏离原始分析意图。

### 3.3 多 Agent 质量检查器

为验证生成 alpha 代码的有效性和质量，论文设计 Multi-Agent Quality Checker，由 Judge Agent、Logic Improvement Agent、Code Quality Agent 和 Code Repair Agent 组成。通过检查的 alpha 代码会存入 candidate pool；未通过的无效代码会被送回多 agent 系统修复。多次修复或改进仍失败的代码会被丢弃。

流程如下：

1. Code Quality Agent 首先检测语法错误、格式不一致和运行时 bug。
2. 如果发现问题，Code Repair Agent 根据反馈修复 alpha 代码。
3. Judge Agent 判断 alpha 是否逻辑一致、技术正确、有经济意义。
4. 如果需要改进，Logic Improvement Agent 改善未通过 Judge Agent 的代码。
5. 代码通过全部质量检查后会被执行。
6. 数值稳定性检查会检测 runtime error、NaN 比例、overflow/underflow、每日 distinct values。
7. 失败代码会被拒绝，或送回前面 agent 继续修复。
8. 成功运行后，会执行信息泄露单元测试。
9. 通过测试的代码被视为 qualified，存入 candidate pool。

附录进一步说明，quality checker 会检查未定义变量、无效库调用、类型不匹配、不稳定数值操作、rolling window 使用是否正确、是否存在未来函数如 `shift(-1)`，以及隐含时间错位等问题。

### 3.4 适应度评估

通过 Multi-Agent Quality Checker 后，每个 alpha 会用五个预测力指标评估：

| 指标 | 中文解释 |
|---|---|
| IC | Information Coefficient，alpha 与未来收益的线性相关 |
| ICIR | IC 的 information ratio，用于衡量 IC 时间稳定性 |
| RankIC | alpha 排名与未来收益排名的单调相关 |
| RankICIR | RankIC 的时间稳定性 |
| MI | Mutual Information，衡量 alpha 与未来收益之间可能的非线性依赖 |

前四个指标衡量 alpha 与目标收益之间的线性或单调关系，MI 捕捉非线性依赖。

论文设置阈值识别 qualified alphas 和 elite alphas。若某 alpha 的五个评价指标全部超过同代所有 alpha 的 65 分位，则归为 qualified alpha；超过 80 分位则归为 elite alpha。为避免异常值支配，论文给每个指标设置最低边界：IC 和 RankIC 不低于 0.005，ICIR 和 RankICIR 不低于 0.05，MI 不低于 0.02。对于 elite factors，最低边界更高：IC 和 RankIC 不低于 0.01，ICIR 和 RankICIR 不低于 0.1，MI 不低于 0.02。

Qualified alphas 组成新的 parent pool，进入下一轮迭代；elite alphas 会被保留并存入最终候选池。此外，每代会把上一代前两个 elite alphas 带入下一代，以保留高质量解。

### 3.5 自适应生成

每次 fitness evaluation 后，系统会得到一组 valid alphas 和一组 invalid alphas，它们背后的有效或无效原因不同。为了让 agent 能持续从上一代学习，论文把有效和无效 alpha 的信息纳入 prompt。

每一代随机选择两个 valid alphas 和两个表现最差的 invalid alphas 作为引导样本。每个样本先被分析和总结，解释它为什么有效或无效。随后，这些 fitness 结果和分析摘要会被合并进下一代生成 prompt，系统据此生成新的 alpha。

### 3.6 思维进化

为了引导 LLM 对 alpha search 进行更深入推理，论文使用 Thinking Evolution 增强其 alpha mining 能力。所有 qualified alphas 都会经历这一过程。

Thinking Evolution 在自然语言空间中实现类似遗传算法的优化过程：候选 alpha 代码通过文本 prompt 表达的 mutation 和 crossover 操作进行演化。该模块包含两个 agent：

1. Mutation Agent：对给定 alpha 代码进行小幅修改，引入变化。
2. Crossover Agent：结合两个现有 alpha 生成一个新 alpha。

论文执行三种进化方式：

1. only mutation；
2. only crossover；
3. crossover 后接 mutation。

每次进化之后，新 alpha 都会再次经过 Multi-Agent Quality Checker。这个过程持续到所有 generations 完成。

---

## 4. 实验

本节先描述实验设置并与 baseline 比较，然后展示生成 alpha 的可解释性和进化过程，最后研究不同指标阈值对方法的敏感性。

### 4.1 实验设置

#### 数据集

论文主要实验在 CSI300 上进行。CSI300 由中国市场 300 只大盘 A 股组成。论文主要使用 10 日收益作为预测目标，买卖价格使用开盘价。数据按时间顺序切分为：

| 用途 | 区间 |
|---|---|
| 训练 | 2011/01/01-2019/12/31 |
| 验证 | 2020/01/01-2020/12/31 |
| 测试 | 2021/01/01-2024/12/01 |

论文还测试了四个数据集：CSI500、S&P500、HSI、HSCI，覆盖中国、美国和香港三个市场。附录还在 CSI300 上测试 30 日收益目标。

#### 模型

论文中所有 agent 默认基于 gpt-oss-120b。七层 agent hierarchy 中的任务 agent 和 thinking-evolution agent，会从 `{0.7, 0.8, 0.9, 1.0, 1.1, 1.2}` 中随机选择 temperature，以鼓励多样性。Multi-agent quality checker 中的 agent temperature 固定为 0.8。最大 token 长度设为 4096。默认使用 LightGBM 训练 CogAlpha 生成的 alpha。

#### 训练设置

初始池大小为 80，即任务 agent 至少生成 80 个 alpha。Parent pool 大小为 32，表示过滤后最多保留 32 个 alpha 进入下一代。Children pool 是 parent pool 的三倍，因此 evolution agent 至少生成 96 个 alpha。

默认情况下，每个任务 agent 负责一个完整演化周期：24 代，包含 3 个内部子周期，每个子周期 8 代。因此，每个任务 agent 会启动 3 次 evolutionary search。每隔 2 代，任务 agent 新生成的 alpha 会被过滤并注入 parent pool。每一代都会保留上一代 top two elite alphas。NaN 超过 30% 或未通过 quality checker 的 alpha 会被丢弃。所有实验在 NVIDIA H100 GPU 上完成。

#### 评价

论文用四个预测力指标评估 alpha 组合：IC、ICIR、RankIC 和 RankICIR。IC 衡量 alpha 值与后续总收益之间的线性相关，反映整体预测力；ICIR 衡量 IC 的稳定性和时间一致性；RankIC 和 RankICIR 类似，但衡量的是 alpha 与后续总收益之间的单调关系，而不是线性相关。

此外，论文还使用两个收益表现指标：IR 和 AER。IR 评估风险调整后的超额收益，AER 衡量给定期间的年化超额累计收益。

### 4.2 与 Baseline 比较

论文将 CogAlpha 与 21 个 benchmark 方法比较。Baseline 包括：

- 7 个常用机器学习模型：Linear Regression、MLP、Random Forest、LightGBM、XGBoost、CatBoost、AdaBoost；
- 4 个代表性深度学习模型：GRU、LSTM、CNN、Transformer；
- 2 个 alpha library：Alpha158、Alpha360；
- 2 个自动 alpha mining 方法：AutoAlpha、AlphaAgent；
- 6 个 LLM：Llama3-8B、Llama3-70B、GPT-OSS-20B、GPT-OSS-120B、GPT-4.1、o3。

对能生成 alpha 因子的方法和 LLM，论文使用 20 个生成 alpha 构造多因子组合后评估。

论文报告：传统机器学习方法总体优于深度学习方法；传统机器学习、现有 alpha library 和 LLM 公式挖掘方法之间没有明显性能差距；Alpha158 和 Alpha360 说明更多 alpha 不一定带来更高 IC 或 RankIC。开源 LLM 中，大模型通常比小模型有更强 alpha mining 能力。两个闭源模型表现较差，reasoning model o3 在所评估 LLM 中表现最差。总体上，CogAlpha 在所有评价指标上持续优于 baseline；唯一例外是 Random Forest 的 RankICIR 可能更高，原因可能是其 RankIC 标准差很低。

### 4.3 消融实验

论文评估 CogAlpha 各组件的作用：Adaptive Generation（A）、Diversified Guidance（G）、Seven-Level Agent Hierarchy（H）和 Thinking Evolution（E）。附录表 3 显示，这四个部分在一定程度上都能提高 alpha mining 的有效性和表现。

表 3 的核心含义：

| 配置 | IC | RankIC | AER | IR |
|---|---:|---:|---:|---:|
| Agent | 0.0300 | 0.0318 | 0.0789 | 0.8015 |
| Agent_E | 0.0219 | 0.0420 | 0.0808 | 0.8999 |
| Agent_EA | 0.0315 | 0.0491 | 0.0825 | 1.0145 |
| Agent_EAG | 0.0414 | 0.0501 | 0.1245 | 1.4668 |
| Agent_EAGH / CogAlpha | 0.0591 | 0.0814 | 0.1639 | 1.8999 |

论文据此说明：完整框架优于逐步加入组件前的版本。

### 4.4 生成 Alpha 的可解释性

论文分析生成 alpha 的可解释性。CogAlpha 生成的每个 alpha 都带有详细注释，解释逻辑、说明底层想法，并给出对应公式，然后提供实现代码。

论文示例是一个 liquidity impact alpha。它衡量单位成交量对应的价格上升，即 `(high - close)` 相对于 traded volume 的冲击。较大的正值表示股价明显上升但成交量较低，意味着流动性较薄，预期短期收益较高。该设计可解释为单位成交量的价格冲击。在市场微观结构理论中，低成交量下的大价格变化反映流动性约束、订单簿失衡，以及小额交易即可显著移动价格的市场状态。这类状态可能暗示短期反转或动量效应，与 Kyle 和 Amihud 关于价格冲击和非流动性收益关系的研究一致。

### 4.5 Alpha 的进化

为展示 CogAlpha 的进化能力，论文展示了流动性相关 alpha 如何在多轮迭代中演化。每个 alpha 使用 IC 和 RankIC 评估。表现差的 alpha 会被自动过滤，较强的 alpha 被保留并继续演化。

第一版 alpha 是初始人工设计版本，衡量单位成交量对应的价格上升。其指标为 IC 0.0090、RankIC 0.0061。通过 mutation，模型生成了一个替代表达：使用完整日内价格区间，而不是收盘差异。这个版本捕捉更广泛的日内流动性行为，但指标下降到 IC 0.0073、RankIC 0.0021，因此在后续轮次中被丢弃。

经过多轮 evolution 后，CogAlpha 产生更精细版本：用 dollar volume 对日内绝对价格变动归一化，并加入变换以确保有界性和鲁棒性。演化后的 alpha 提升到 IC 0.0141、RankIC 0.0087，说明进化机制能有效改进量化因子。

完整 evolution cycle 后，CogAlpha 能生成大量预测力较强的单因子 alpha，其中许多绝对 IC 超过 0.05，绝对 RankIC 超过 0.07。论文认为，这证明框架能够自动探索并优化因子空间，获得更高表现和更可解释的 alpha。

### 4.6 不同设置下的泛化

为测试 CogAlpha 泛化能力，论文在五个数据集、两种训练方法和两个预测 horizon 下进行实验。数据集包括 CSI300、CSI500、S&P500、HSI、HSCI；训练方法包括 LightGBM 和 Ridge；horizon 包括 10 日和 30 日。论文报告称，CogAlpha 在不同设置下表现稳定。

表 5 的核心结论包括：

- CSI300 10 日：CogAlpha + LightGBM 的 IC 0.0591、RankIC 0.0814。
- CSI300 30 日：CogAlpha + LightGBM 的 IC 0.0886、RankIC 0.1243。
- CSI500 10 日：CogAlpha + Ridge 的 IC 0.0455、RankIC 0.0738。
- S&P500 10 日：CogAlpha + Ridge 的 IC 0.0217、RankIC 0.0226。
- HSI 10 日：CogAlpha + Ridge 的 IC 0.0327、RankIC 0.0400。
- HSCI 10 日：CogAlpha + Ridge 的 IC 0.0562、RankIC 0.0495。

论文用这些结果说明：CogAlpha 不只在单一市场、单一 horizon、单一模型上有效，而具有一定跨市场泛化性。

### 4.7 不同 Fitness 阈值

论文分析 alpha 过滤阈值设置的敏感性。为了保持所选 alpha 的质量，论文测试三组阈值：

| Qualified 阈值 | Elite 阈值 |
|---:|---:|
| 65 | 80 |
| 80 | 90 |
| 85 | 95 |

每组中，前者是进入下一代的 qualified factor 分位数阈值，后者是直接存入最终候选池的 elite factor 分位数阈值。为保证过滤质量，论文还基于因子得分的经验分布设置每个预测指标的最低值：IC 和 RankIC 不低于 0.005，ICIR 和 RankICIR 不低于 0.05，MI 不低于 0.02。Elite factor 的最低值更高。

论文报告称 `(65, 80)` 阈值组合整体表现更好。原因可能是该配置下 parent pool 更大，有助于进化搜索探索更广阔的 alpha 空间，并降低过早收敛到局部最优的风险。

---

## 5. 结论

论文研究如何从高波动、低信噪比的金融市场中提取可解释、可靠的 alpha 信号。论文提出 Cognitive Alpha Mining 概念，为自动化、稳健、可解释的 alpha 发现打开新方向，并进一步提出 CogAlpha：一个由深度思考 LLM 驱动的多 agent 框架。论文通过大量实验展示方法有效性。未来工作中，作者计划将方法用于实盘交易环境，以进一步验证其实践表现。

---

## 限制

CogAlpha 框架仅用于学术用途，不提供任何金融意见。回测模拟完全在 Qlib 框架内实现和执行，可能无法完全复现实盘交易环境。由于 LLM 输出具有内在随机性，每次运行精确复现相同 alpha 会比较困难。实验执行时间还受数据集规模影响，较大数据集可能带来更长处理时间。

---

## 伦理声明

论文使用的所有数据集均来自公开来源，并且公开可得。

论文使用 ChatGPT-5.2 进行语法检查和建议，但所有编辑均由作者人工验证，最终提交稿未直接包含 AI 生成内容。

CogAlpha 框架及相关代码的使用者需要自行获取金融数据，并在具体场景中独立评估生成因子和模型的风险。用户必须谨慎对待 agent 生成的代码、数据和模型，并进行全面验证。CogAlpha 不提供金融建议，也不用于替代合格金融专业人士在金融产品创建、评估和批准中的专业判断。

---

## 附录 A：方法

### A.1 七层 Agent Hierarchy

论文附录用金字塔图表示七层 agent hierarchy，从宏观结构推理到微观层级融合。七层说明如下：

| 层级 | 名称 | 描述 |
|---|---|---|
| I | Market Structure & Cycle Layer | 探索长期趋势、市场阶段和周期状态转换等大尺度时间结构 |
| II | Extreme Risk & Fragility Layer | 建模尾部风险、崩盘前兆和系统脆弱性 |
| III | Price-Volume Dynamics Layer | 捕捉价格与交易活动之间的互动，包括流动性、订单失衡和价量一致性 |
| IV | Price-Volatility Behavior Layer | 分析趋势延续、短期反转、波动聚集和非对称价格动态 |
| V | Multi-Scale Complexity Layer | 衡量跨尺度不规则性、分形粗糙度、回撤恢复几何和长记忆 |
| VI | Stability & Regime-Gating Layer | 评估时间稳定性，并构造不同市场条件下的自适应门控 |
| VII | Geometric & Fusion Layer | 关注 K 线几何和多因子融合，将独立信号组合成连贯复合因子 |

各 agent 的重点：

- AgentMarketCycle：探索长期周期转换和价格动态阶段变化。
- AgentVolatilityRegime：检测平静和剧烈波动状态之间的转换。
- AgentTailRisk：量化下行敏感性和尾部事件暴露。
- AgentCrashPredictor：追踪波动压缩、流动性枯竭和结构脆弱性，识别崩盘早期信号。
- AgentLiquidity：通过价格冲击和换手波动衡量市场深度与交易摩擦。
- AgentOrderImbalance：从日频 OHLCV 模式中推断单边参与形成的方向压力。
- AgentPriceVolumeCoherence：研究价格和成交量变化之间的同步与背离。
- AgentVolumeStructure：分析成交活动的统计形状、集中度和参与节奏。
- AgentDailyTrend：建模方向延续和多日动量强度。
- AgentReversal：捕捉短期过度反应后的均值回复。
- AgentRangeVol：研究日内价格区间的压缩-扩张周期。
- AgentLagResponse：研究波动、成交量和收益之间的滞后反馈。
- AgentVolAsymmetry：衡量上涨和下跌价格变动之间的非对称波动。
- AgentDrawdown：评估累计损失的深度、持续时间和恢复几何。
- AgentFractal：通过跨 horizon 变化和结构不规则性评估多尺度粗糙度和长记忆。
- AgentRegimeGating：根据波动、趋势或流动性状态构造信号激活门控。
- AgentStability：量化收益或衍生信号的时间一致性和平滑性。
- AgentComposite：融合多个独立因子，强调信号之间的协同和正交性。
- AgentCreative：使用非线性变换、重新参数化或软门控生成新特征表达。
- AgentBarShape：将 K 线实体、影线和对称性编码为连续、可解释的量化描述。
- AgentHerding：检测 OHLCV 动态中的群体拥挤和方向一致性。

### A.2 多样化引导

附录进一步定义五种改写模式：

1. Light：最小改写，保持几乎相同含义，同时改善清晰度和语言流畅度。
2. Moderate：自然改写，加入轻微丰富或风格变化，用于捕捉细微语义差异。
3. Creative：加入表达性、研究导向的改写，增加解释深度，激发新的分析角度。
4. Divergent：从新的但相关的分析视角探索性重写，鼓励更广泛假设生成。
5. Concrete：更具体、更面向实现，引入统计公式、比率或示例计算，把概念因子想法连接到实践实现。

### A.3 多 Agent Quality Checker

Quality Checker 依次执行以下检查：

1. Code Quality Agent：对 LLM 生成代码做第一遍审查，检测语法错误、未定义变量、格式不一致、无效库调用和潜在运行失败。
2. Code Repair Agent：若发现问题，自主修复导入语句、错误表达式、类型不匹配和不稳定数值操作，使因子至少语法可行、运行可行。
3. Judge Agent：在代码语法干净后，从语义层面评估因子是否逻辑一致、技术正确、经济有意义。
4. Logic Improvement Agent：对逻辑弱或不一致的因子进行改进，包括重构公式、调整窗口、替换可疑变换、消除冗余操作，并增强金融解释性。
5. Execution and Numerical Stability Check：在受限沙箱中执行代码，检测 runtime error、NaN 传播、overflow/underflow、无效对数和不稳定归一化。
6. Temporal Leakage Unit Test：执行特定领域的信息泄露测试，检测未来偏移、rolling window 错位和隐含时间违反。
7. Output：通过全部检查的代码组成安全、可执行、无泄露的 alpha 因子池，为下一阶段 Thinking Evolution 奠定可信计算基础。

### A.4 Fitness Evaluation

五个预测力指标阈值会随数据集变化。例如 CSI300 中，为防止异常值支配，IC 和 RankIC 最低 0.005，ICIR 和 RankICIR 最低 0.05，MI 最低 0.02；elite factors 的最低要求更高。S&P500 中 MI 边界设为 0.012，因为在更有效市场中挖掘 alpha 更困难。

论文还指出，MI 所代表的非线性关系暗示市场可能并非完全有效，某些信息可能没有完全反映在价格中，因此为因子投资提供机会。非线性因子模型更能捕捉市场复杂模式，尤其当市场并非完全有效时，可能带来超额收益机会。

---

## 附录 B：实验

### B.1 数据集

CSI300 和 CSI500 数据来自 Qlib。CSI300 使用 10 日收益和 30 日收益作为目标，CSI500 使用 10 日收益。S&P500、HSI 和 HSCI 用于跨市场泛化测试。HSI 和 HSCI 数据来自 Yahoo Finance。所有回测在 Qlib 框架内实现和执行。

### B.2 回测

论文使用 top-k/drop-n 排名组合构建方法：每天选择预测收益最高的 top 股票，同时限制每日组合换手。每个交易日，组合保留此前选择且排名仍高的股票，并最多替换 n 个持仓。所有交易在开盘价执行。开仓成本设为 0.05%，平仓成本设为 0.15%，每笔交易最低手续费 5 元人民币。

### B.3 指标

论文使用五个因子预测力指标：IC、ICIR、RankIC、RankICIR 和 MI。假设某时点有多只资产，alpha 预测值对应后续一段期间的总收益。IC 衡量截面上因子值和未来收益的线性相关，并在时间上取平均。ICIR 衡量 IC 序列的均值相对于标准差的稳定性。RankIC 衡量因子排名与未来收益排名的单调相关。RankICIR 衡量 RankIC 的时间稳定性。MI 衡量因子值与未来收益之间的非线性依赖。

论文还使用两个收益表现指标：

- AER：Annualized Excess Return，年化超额收益。先计算每日组合收益扣除 benchmark 收益和交易成本后的超额收益，再年化。
- IR：Information Ratio，信息比率。用日超额收益均值相对于标准差的比例，并按交易期数年化。

### B.4 训练设置

初始池 80，parent pool 32，children pool 约为 parent pool 的三倍。每个任务 agent 默认进行 24 代演化，包含 3 个子周期，每个子周期 8 代。每两代会注入新的任务 agent 生成 alpha。上一代 top two elite alphas 总是带入下一代。NaN 超过 30% 或质量检查失败的 alpha 会被丢弃。

训练使用 rolling training，rolling step 为 126。模型包括 LGBMRegressor 和 Ridge。LightGBM 学习率 0.0001，叶子数 32，最大深度 12，`reg_alpha` 和 `reg_lambda` 为 1.0，1000 棵树，feature fraction 和 bagging fraction 为 0.8。Ridge 正则强度 alpha 为 10。

论文还采用两个停止条件：默认固定演化步数；同时加入 plateau early stopping，即跟踪 elite-pool 表现，如果连续窗口改善不足，则终止该 island 或 run 的演化。

### B.5 计算成本

在 CSI300 上，生成单个 alpha 通常需要 5 到 9 秒，完成一代约 1 小时。深度学习模型在 GPU 上训练：CNN、GRU、LSTM 约 20 分钟，Transformer 约 40 分钟。传统机器学习模型在 CPU 上：AdaBoost 约 6 小时，Random Forest 约 40 分钟，LightGBM 约 2 分钟，线性模型约 5 到 10 秒。主实验使用单张 H100 GPU，进化过程使用本地 gpt-oss-120b，无 API 成本。

### B.6 LLM 随机性

由于大模型输出具有内在随机性，每次运行结果可能变化。但因子挖掘与其他实验不同：好因子可以累积和存储。因此，论文展示的实验结果反映一次完整因子挖掘后的成果。

### B.7 消融实验

论文测试 Adaptive Generation、Diversified Guidance、Seven-Level Agent Hierarchy 和 Thinking Evolution 的效果。表 3 显示四个部分均能在一定程度上提升 alpha mining 效果。

### B.8 超参数设计与分析

论文超参数设计受 Mind Evolution 和 Verbalized Sampling 启发。由于框架有 21 个异质 agent，论文使用黄金比例随机选择 13 个 agent 构造初始因子池。每个选中 agent 生成约 5-6 个 alpha，初始池约 80 个。随后再次用黄金比例形成 32 个 parent pool。考虑三种 evolution operator，children pool 约为 parent pool 三倍。

论文在 AgentMarketCycle 上测试不同配置，记为 `P_G_H`：P 是 parent pool size，G 是每周期 generations 数，H 是子周期长度。CSI300 10 日结果显示，`P32_G24_H2` 综合表现最佳。但其他配置也能发现有效 alpha。论文认为不存在跨时期、跨市场条件的通用最优超参数，保持超参数配置多样性有助于更全面发现 alpha。

### B.9 Alpha 进化示例

论文展示流动性相关 alpha 的进化。第一版是人工设计的单位成交量价格上升，IC 0.0090、RankIC 0.0061。Mutation 后使用完整价格区间，指标下降，后续被淘汰。多轮优化后，因子用 dollar volume 归一化绝对日内价格变动，并使用有界变换，IC 提升到 0.0141、RankIC 提升到 0.0087。

论文还列举几个 elite alpha：

| alpha 名称 | 训练原始指标 | 测试指标 |
|---|---|---|
| factor_lownorm_slopecos_30d_low | IC -0.0498，RankIC -0.0791 | Ridge 测试 IC 0.0507，RankIC 0.0704 |
| factor_pressure_drawdown_fisher_10d | IC -0.0473，RankIC -0.0668 | LightGBM 测试 IC 0.0491，RankIC 0.0690 |
| factor_herd_drawdown_synergy_gate_ema10 | IC -0.0552，RankIC -0.0742 | LightGBM 测试 IC 0.0503，RankIC 0.0663 |

这些示例说明，CogAlpha 能生成具有较强预测力的单因子，并能在 evolution 中逐步改进。

### B.10 不同 Fitness 阈值

论文测试 `(65,80)`、`(80,90)`、`(85,95)` 三组阈值。结果显示 `(65,80)` 表现更好，可能因为 parent pool 较大，让进化搜索能探索更广 alpha 空间，并降低局部最优风险。

### B.11 不同设置下的泛化

论文测试不同数据集、训练方法和 horizon。CSI300/CSI500 训练 2011-2019、验证 2020、测试 2021-2024；S&P500 训练 2007-2014、验证 2015、测试 2016-2020；HSI/HSCI 训练 2011-2019、验证 2020、测试 2021-2025。论文报告 CogAlpha 在这些设置下表现稳定。

---

## 附录 C：Prompt 设计

附录 C 提供 prompt 设计方向，包括七层 agent hierarchy、multi-agent quality checker 和 thinking evolution 的 prompt。HTML 中部分 prompt 内容被省略，论文说明更多 agent prompt 细节会在 GitHub 仓库展示。

---

## 译者给 Vortex 的边界提醒

1. 论文结果是作者在 Qlib 环境中的报告，不等同于 Vortex A 股实盘结论。
2. CogAlpha 的核心价值是研究闭环，而不是某个具体 alpha 公式。
3. Vortex 若吸收该框架，应先做小规模、可审计、PIT-safe 的学习实验。
4. 在没有质量检查、沙箱和完整 lineage 前，不应运行任意 LLM 生成代码。
