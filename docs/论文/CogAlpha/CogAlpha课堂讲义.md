---
tags: [vortex, vortex/research-domain, vortex/factor-methodology]
aliases: [CogAlpha课堂讲义, CogAlpha论文中文精读, Cognitive Alpha Mining 中文精读, CogAlpha]
created: 2026-05-02
updated: 2026-05-02
status: lecture_note
source: arxiv:2511.18850v3
---

# CogAlpha 课堂讲义

关联：[[CogAlpha学习资料]]、[[CogAlpha完整中文译文]]、[[CogAlpha与Vortex落地讨论]]、[[因子研究档案]]

> 这份讲义按“我当老师，你跟着学”的方式写。完整论文意思见 [[CogAlpha完整中文译文]]；这里重点讲为什么重要、怎么理解、怎么和 Vortex 因子研究流程对应。

---

## 先导课：这篇论文到底在讲什么

### 先用人话讲

CogAlpha 不是“让 LLM 随机写几个因子”。如果只是让 ChatGPT 写 Alpha101 风格公式，我们很快会得到一堆看似复杂、实际重复、甚至偷看未来的东西。论文真正想做的是：把 LLM 放进一个像研究员团队一样的闭环里。

这个闭环是：

```text
不同研究视角提出代码化 alpha
  -> 多 agent 质量检查挡掉坏代码和坏逻辑
  -> 用 IC / RankIC / ICIR / MI 等指标评估
  -> 把好因子和坏因子的原因反馈给下一代
  -> mutation / crossover 继续进化
```

### 论文原意

金融数据高维、低信噪比，传统手工因子慢，深度学习黑箱，公式搜索容易重复且缺经济意义，普通 LLM 生成又容易停留在浅层公式拼接。CogAlpha 的目标是让 LLM 从“模式复制器”变成“认知式研究员”。

### 关键机制

1. **代码化**：alpha 不是一句想法，而是可执行代码。
2. **多视角**：七层 agent hierarchy 覆盖周期、风险、价量、波动、稳定性、形态和融合。
3. **强门禁**：quality checker 检查语法、逻辑、数值稳定、未来函数。
4. **可度量**：fitness evaluation 用多指标筛选。
5. **会反思**：adaptive generation 把成功和失败案例写回 prompt。
6. **会进化**：thinking evolution 用 mutation/crossover 改良 alpha。

### 对 Vortex 的启发

我们现在做因子研究，最容易陷入“测了一个因子，写个报告，结束”。CogAlpha 提醒我们：失败因子也应该变成下一轮生成的训练材料；好因子也不应该直接上策略，而要继续变异、组合、降风险和做可交易性验证。

### 你应该记住什么

- CogAlpha 的核心是闭环，不是 LLM。
- 代码化 alpha 让想法可执行、可审计、可复现。
- 质量检查比生成更重要。
- elite alpha 只是候选，不是策略。

---

## 第一课：Alpha mining 为什么难

### 先用人话讲

金融市场不是一个干净的机器学习数据集。信号弱、噪声大、市场状态变来变去，一个今天有效的模式明天可能消失。更麻烦的是，大家都在挖 alpha，越容易发现的信号越容易拥挤。

### 论文原意

论文认为，alpha mining 的困难来自高维数据、时间变化的波动率和低信噪比。早期人工因子有解释性但效率低；机器学习能捕捉复杂关系但黑箱且脆弱；公式化搜索可解释但容易冗余和缺乏经济基础；LLM 生成虽然有知识和推理能力，但现有方法仍然容易变成公式堆叠和模式重复。

### 关键机制

这节给 CogAlpha 铺垫了三个判断：

1. 单纯靠人，太慢。
2. 单纯靠模型，太黑箱。
3. 单纯靠公式搜索，太窄。

CogAlpha 要结合三者：人类式推理、机器可执行代码、量化指标反馈。

### 对 Vortex 的启发

Vortex 的因子研究也要避免“强 IC 崇拜”。一个因子可能 IC 好，但多头失败；可能样本内好，但换市场失败；可能没有未来函数，但容量或交易成本不可接受。所以因子研究必须是多门槛流程，而不是单指标排名。

### 你应该记住什么

- 金融 alpha 的难点不只是预测，而是稳定、可解释、可交易。
- 普通 LLM 生成因子不等于研究闭环。
- 好框架必须能记录失败，并从失败里学习。

---

## 第二课：为什么代码化 alpha 是关键

### 先用人话讲

如果 alpha 只是“低波反转可能有效”这种文字，它无法自动评测。只有变成代码，才能跑数据、查 NaN、查未来函数、算 IC、做回测、做 lineage。

### 论文原意

CogAlpha 用 code-level alpha representation。每个 alpha 包含注释、公式和实现代码。LLM 不只是写想法，而是生成可执行对象。随后所有候选都进入 quality checker 和 fitness evaluation。

### 关键机制

代码化带来四个好处：

1. **可执行**：可以直接算因子值。
2. **可检查**：可以做语法、运行、PIT、NaN 检查。
3. **可进化**：mutation/crossover 可以改代码结构。
4. **可归档**：可以记录 hash、父代、变异来源和失败原因。

### 对 Vortex 的启发

Vortex 已有 `factor_ops.py` 和 `alpha101_registry.py`，说明我们已经有一点“代码化 alpha”底座。后续如果学 CogAlpha，不应直接让 LLM 写任意 Python，而应先限定在安全算子和明确字段里。

### 你应该记住什么

- 代码化不是为了炫技，是为了审计和闭环。
- 没有代码，就没有自动 fitness。
- 没有质量检查的代码化，会放大 LLM 幻觉风险。

---

## 第三课：七层 Agent Hierarchy

### 先用人话讲

这像把一个研究团队分成 21 个研究员：有的人看市场周期，有的人看尾部风险，有的人看流动性，有的人看价量背离，有的人看 K 线形态，有的人负责把信号融合。这样做是为了避免所有 LLM 都只写“动量、反转、均线”。

### 论文原意

论文基于 OHLCV 设计七层、21 个 agent，从宏观到微观覆盖市场结构、极端风险、价量动态、价格波动行为、多尺度复杂性、稳定性门控、几何与融合。

### 关键机制

| 层级       | 核心问题              |
| -------- | ----------------- |
| 市场结构与周期  | 市场现在处于什么阶段？       |
| 极端风险与脆弱性 | 是否有崩盘或压力积累前兆？     |
| 价格-成交量动态 | 价格变化和交易活跃度是否匹配？   |
| 价格-波动行为  | 动量、反转、波动聚集是否提供信号？ |
| 多尺度复杂性   | 回撤、分形、长记忆是否有信息？   |
| 稳定性与状态门控 | 信号何时应该打开或关闭？      |
| 几何与融合    | K 线形态和多因子组合是否有协同？ |

### 对 Vortex 的启发

我们可以把现有因子映射到这些 agent：

- 小市值低波反转：价格-波动行为 + 稳定性；
- 资金流拥挤反转：价量动态 + 羊群行为；
- 峰岭谷成交量结构：价量动态 + 多尺度复杂性；
- 业绩预告漂移：可看作事件层外部信息，未来可与低波/流动性门控 crossover。

### 你应该记住什么

- 七层 agent 是搜索空间设计，不是模型结构。
- 它强迫 LLM 从多个金融研究视角出发。
- Vortex 可以先学“分类地图”，不必马上实现 21 个 agent。

---

## 第四课：Diversified Guidance

### 先用人话讲

同一个研究想法，用不同说法会让 LLM 走出不同公式路径。比如“低成交量下价格大幅波动”可以写成流动性冲击、订单簿脆弱、价格冲击成本、非流动性溢价、短期反转压力。Diversified Guidance 就是系统化地让这些表达都被探索。

### 论文原意

CogAlpha 对 guidance 做五种改写：light、moderate、creative、divergent、concrete。它们分别用于保持一致、丰富表达、加入研究深度、扩展相关视角、转成可实现公式。

### 关键机制

这一步解决 prompt mode collapse：如果 prompt 太单一，LLM 输出会重复；如果 prompt 太发散，又会偏离研究目标。五种改写在一致性和发散性之间做平衡。

### 对 Vortex 的启发

未来我们做一个因子，不要只写一个公式。应该给同一假设设计多个实现：

- 原始表达；
- 行业/市值中性版；
- 波动门控版；
- 流动性过滤版；
- 有界变换版；
- 与另一个因子 crossover 的版本。

### 你应该记住什么

- Prompt 多样性是因子多样性的一部分。
- Concrete 改写很重要，因为它把金融语言落到可计算表达。
- 多样化不是乱改，而是在同一研究意图内扩展。

---

## 第五课：Multi-Agent Quality Checker

### 先用人话讲

这是整篇论文最该学的地方。LLM 生成因子最大风险不是“写不出东西”，而是“写出看起来对、实际错的东西”。Quality Checker 就是门卫：代码错、逻辑错、未来函数、NaN 太多、经济意义瞎编，全部挡掉。

### 论文原意

Quality Checker 由 Code Quality Agent、Code Repair Agent、Judge Agent、Logic Improvement Agent 组成。通过后还要运行代码、检查数值稳定、检查信息泄露。无法修复或多次失败的代码被丢弃。

### 关键机制

检查分三层：

1. **代码层**：语法、变量、库调用、运行错误。
2. **逻辑层**：rolling window、变换、公式结构、经济含义。
3. **金融安全层**：NaN、overflow、未来函数、时间错位。

### 对 Vortex 的启发

如果 Vortex 以后做小 CogAlpha，第一个要做的不是 generator，而是 checker。没有 checker 的 LLM alpha mining 是危险的，因为它会用很多“聪明”的方式制造假 alpha。

### 你应该记住什么

- Quality Checker 是 CogAlpha 的安全底座。
- Code Repair 不能无限修；修不好要丢弃。
- Judge Agent 不能替代量化指标，只能做逻辑门禁。

---

## 第六课：Fitness Evaluation

### 先用人话讲

质量检查只能说明“这个因子能算、没明显偷看未来、逻辑还行”。但它有没有预测力，要靠指标。CogAlpha 用 IC、ICIR、RankIC、RankICIR、MI 来衡量。

### 论文原意

通过 checker 的 alpha 会按五个预测力指标评估。超过同代 65 分位并满足绝对下限的是 qualified alpha；超过 80 分位的是 elite alpha。Qualified 进入下一代 parent pool，elite 存入最终候选池，上一代 top two elite 会被保留到下一代。

### 关键机制

这里有两个筛选维度：

1. **相对阈值**：同代 65/80 分位，适应不同 generation 的整体质量。
2. **绝对下限**：避免某代整体很差时，矮子里拔将军。

### 对 Vortex 的启发

Vortex 当前已有 RankIC、多空、候选准入。若借鉴 CogAlpha，后续可以加：

- IC / ICIR；
- MI；
- generation 内相对排名；
- elite pool；
- invalid reason；
- horizon 维度的稳定性。

### 你应该记住什么

- 合格因子不是单指标第一，而是多指标过线。
- 65/80 阈值是为了平衡探索和质量。
- Elite alpha 也只是候选池，不是交易信号。

---

## 第七课：Adaptive Generation

### 先用人话讲

这一步让系统会“复盘”。每一代结束后，它不只是保留好因子，还会看坏因子为什么坏。然后把好坏案例的原因告诉下一代 LLM。

### 论文原意

每代随机选择两个 valid alphas 和两个最差 invalid alphas。先分析总结它们为什么有效或无效，再把 fitness 结果和分析摘要合并到下一代 prompt 中，用于生成新 alpha。

### 关键机制

Adaptive Generation 让系统有研究记忆：

- 好因子告诉模型什么方向值得走；
- 坏因子告诉模型什么错误不要重复；
- 失败原因变成下一代 prompt 的约束。

### 对 Vortex 的启发

这正对应我们的 [[因子研究档案]]。坏因子不是垃圾，而是“负样本”。如果我们记录得足够结构化，未来 LLM/研究员可以少走弯路。

### 你应该记住什么

- 失败因子也有价值。
- 研究档案不是写给人看的日志，也可以变成下一代生成上下文。
- Adaptive Generation 是 CogAlpha 从“一次生成”变成“持续学习”的关键。

---

## 第八课：Thinking Evolution

### 先用人话讲

这就是因子的繁殖和变异。一个好想法先不要急着定稿，可以小改窗口、小改归一化、小改非线性变换，也可以和另一个好想法组合。坏变体淘汰，好变体留下。

### 论文原意

Thinking Evolution 在自然语言空间中实现遗传式优化。它包括 Mutation Agent 和 Crossover Agent。三种演化方式是 mutation only、crossover only、crossover followed by mutation。每次演化后，新 alpha 都要重新经过 Quality Checker。

### 关键机制

Mutation 用于局部搜索，Crossover 用于组合两个假设。论文示例中，流动性 alpha 从单位成交量价格上升，演化到用 dollar volume 归一化绝对价格变动并做有界变换，IC 和 RankIC 都提升。

### 对 Vortex 的启发

我们可以人工先做小规模 thinking evolution：

- 对强 IC 但多头差的因子做低波/行业/市值门控；
- 对拥挤反转因子加流动性过滤；
- 对事件因子和价量因子做 crossover；
- 对极端值敏感因子加有界变换。

### 你应该记住什么

- Evolution 是系统化改因子，不是乱调参。
- 每个新变体必须重新过 checker 和 fitness。
- Crossover 最适合把“预测腿”和“风控腿”组合。

---

## 第九课：实验结果怎么读

### 先用人话讲

论文数字看起来很强，但我们不能被数字带跑。要先问：数据是什么？回测环境是什么？成本怎么设？是否有真实交易约束？能不能复现？是否只在 Qlib 里成立？

### 论文原意

论文主要在 CSI300 上实验，训练 2011-2019，验证 2020，测试 2021-2024，主要预测 10 日收益。还在 CSI500、S&P500、HSI、HSCI 做泛化测试。默认 agent 用 gpt-oss-120b，默认训练模型为 LightGBM。论文报告 CogAlpha 相比 21 个 baseline 在多数指标上更强。

### 关键机制

要看懂实验，重点不是“CogAlpha 赢了”，而是：

1. 它和机器学习、深度学习、alpha library、LLM baseline 都比了；
2. 它做了消融，说明 A/G/H/E 组件都有贡献；
3. 它展示了跨市场泛化；
4. 它承认 Qlib 回测不等于实盘；
5. 它承认 LLM 随机性导致精确复现困难。

### 对 Vortex 的启发

任何 CogAlpha 生成的因子，进入 Vortex 后都要重新走：

```text
PIT 检查
  -> 多 horizon IC/RankIC
  -> 分层与多空
  -> 成本/容量/涨跌停/ST/停牌
  -> 样本外
  -> 组合贡献
  -> shadow
```

### 你应该记住什么

- 论文数字只能证明“值得研究”，不能证明“可实盘”。
- Qlib 回测不是 A 股实盘约束。
- LLM 生成的随机性要求我们更重视归档和复现。

---

## 第十课：我们是否要走通 CogAlpha

### 先用人话讲

我的判断：不要一上来复刻完整 CogAlpha。我们应该先做“小 CogAlpha”。

### 小 CogAlpha 是什么

```text
人工/LLM 提出少量代码化 alpha
  -> 规则版质量检查
  -> Vortex 多周期评测
  -> 记录好/坏/失败原因
  -> 手工 mutation/crossover
  -> 再评测
```

### 为什么不是大改

完整 CogAlpha 需要：

- 21 个 agent；
- 多代 evolution；
- 大模型运行环境；
- 安全沙箱；
- 大量 prompt；
- lineage 系统；
- 质量检查器；
- 计算资源。

Vortex 现在更需要先把“研究闭环”学会，而不是立刻做重型自动化。

### 你应该记住什么

- 学 CogAlpha 的正确顺序是：先讲懂，再翻译，再映射，再小实验。
- Vortex 不缺想法，缺的是安全、可复现、可审计的闭环。
- 下一步如果做工程，也应该从 checker 和 lineage 开始，而不是 generator。
