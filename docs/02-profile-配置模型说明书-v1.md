# 设计文档导航：Profile 配置模型说明书

这份根目录文档已迁移到分层目录中维护。

请优先阅读：

1. [架构设计/02-profile-配置模型说明书-v1.md](架构设计/02-profile-配置模型说明书-v1.md)
2. [产品原型/02-profile-配置体验原型-v1.md](产品原型/02-profile-配置体验原型-v1.md)

说明：

1. 架构文档负责解释 profile 的建模、解析、继承、preset / pack 分层与运行时装配。
2. 产品文档负责解释用户为什么需要 profile、应该看到哪些字段、以及如何理解 profile / preset / pack 的关系。

---

## 二、核心结论

### 2.1 `profile` 是对外配置契约，不是内部设置转储

用户看到的 `profile`，本质上是：

1. 我在哪个业务上下文里工作。
2. 这个上下文默认使用什么口径。
3. 这次动作允许我改哪些参数，不允许我改哪些参数。

所以 `profile` 不应该直接暴露：

1. Parquet 物理路径
2. DuckDB 文件位置
3. Qlib 目录细节
4. 密钥、凭证、连接串
5. 某个内部类名

这些都属于内部解析结果，不属于用户配置契约。

### 2.2 `profile` 应该统一命名，但按领域分型

从 CLI 看，用户永远写 `--profile xxx`，这是统一体验。

但从配置模型看，不应该只有一个“大一统 Profile”。

更合理的做法是按领域分成 4 类：

1. `DataProfile`
2. `ResearchProfile`
3. `StrategyProfile`
4. `TradeProfile`

这样做的原因是：

1. 数据同步关心 provider、market、freq、quality。
2. 研究关心 snapshot、feature、label、workflow。
3. 回测关心 signal、portfolio、cost、slippage。
4. 交易关心 account、gateway、retry、reconcile。

如果把这些内容都塞进一个 profile，使用者会看到一堆与自己无关的字段，最后还是不敢改。

### 2.3 `profile` 负责“环境与口径”，`snapshot` 负责“版本固定”

这两个对象不能混。

1. `profile` 说明“应该怎样工作”。
2. `snapshot` 说明“这次实际用了哪一版数据”。

例如：

1. `cn_alpha_daily` 是一个 `ResearchProfile`
2. `2026-04-04` 是一个 research snapshot

前者是长期稳定配置，后者是当天生成的具体版本。

---

## 三、使用者脑海里的正确模型

从使用者角度，最理想的理解方式不是“系统里有很多目录和类”，而是下面这条链：

```text
DataProfile
  -> 生成 Snapshot
  -> 被 ResearchProfile 消费
  -> 产出 SignalSnapshot
  -> 被 StrategyProfile 消费
  -> 产出 TargetPortfolio
  -> 被 TradeProfile 消费
  -> 产出 ExecutionReport / ReconciliationReport
```

这条链的意义是：

1. `DataProfile` 决定数据口径和快照产出方式。
2. `ResearchProfile` 决定研究怎么吃快照、怎么产出信号。
3. `StrategyProfile` 决定怎么把信号编译成可回测、可执行的目标组合。
4. `TradeProfile` 决定怎么把目标组合送进模拟盘或实盘。

这比“一个 profile 管全世界”更符合真实工作分工。

---

## 四、Profile 家族设计

### 4.1 所有 Profile 都应共享的基础字段

不管是哪一类 profile，建议都保留一组统一基础字段。

建议字段：

1. `name`：profile 唯一名称。
2. `type`：profile 类型，取值为 `data/research/strategy/trade`。
3. `extends`：是否继承另一个 profile。
4. `description`：这份 profile 的用途说明。
5. `owner`：责任人或团队。
6. `tags`：检索标签，例如 `cn`、`daily`、`paper`。
7. `enabled`：是否启用。

这组字段的作用，不是运行逻辑本身，而是：

1. 帮助使用者识别配置。
2. 支持继承和模板化。
3. 支持 CLI 和未来 UI 做检索、筛选、校验。

### 4.2 DataProfile

`DataProfile` 是“怎么生成权威快照”的配置对象。

这里要先修正一个非常重要的点。

如果站在运营者视角，`DataProfile` 不应该直接暴露一整套技术参数；否则它虽然形式上叫 profile，本质上仍然只是“可编辑的大配置文件”。

对运营者可见的 `DataProfile`，建议优先收敛到下面这组字段：

1. `name`：这条数据线叫什么。
2. `type`：固定为 `data`。
3. `preset`：选择哪套内置数据方案，例如 `cn_stock_daily_core`。
4. `history_start`：第一次建库从哪天开始。
5. `schedule`：这条数据线什么时候自动跑。
6. `publish`：更新完成后是否自动发布 research snapshot。
7. `notify`：运行结果发给谁。

也就是说，运营者真正面对的应该是“选方案 + 调少量差异”，而不是“理解 provider、calendar、quality、pit、storage 的全套工程细节”。

如果从平台内部“preset 展开后的视角”看，`DataProfile` 最终会解析出下面这些技术字段：

它应该承载的用户可见参数：

1. `market`：市场，例如 `cn_stock`、`us_stock`。
2. `universe`：默认标的范围，例如 `all_a`、`hs300`。
3. `freqs`：支持的频率，例如 `1d`、`1m`。
4. `provider`：默认数据源，例如 `tushare`。
5. `datasets`：需要同步的数据集列表。
6. `timezone`：时区。
7. `calendar`：交易日历。
8. `quality_policy`：质量门禁策略。
9. `pit_policy`：财务 PIT 策略。
10. `snapshot_policy`：快照发布时间、命名与保留策略。

但这组字段更适合作为：

1. preset 的展开结果
2. 平台内部模板字段
3. `ResolvedProfile` 的运行时装配输入

而不适合作为运营者日常直接维护的最小配置。

它不应该直接暴露给用户的字段：

1. Parquet 根目录
2. DuckDB 连接细节
3. provider token 实际值
4. 内部缓存目录

这些字段应该由环境配置或系统解析层补全。

进一步说，后续应把这些技术字段继续下沉到 `preset / pack` 分层，而不是继续停留在用户可见 profile 顶层。

### 4.3 ResearchProfile

`ResearchProfile` 是“怎么基于 snapshot 做研究”的配置对象。

它应该承载的用户可见参数：

1. `data_profile` 或 `snapshot_family`：默认依赖哪条数据线。
2. `market`：研究市场。
3. `universe`：研究股票池。
4. `freq`：研究频率。
5. `feature_set`：使用哪组特征视图。
6. `label_spec`：标签定义，例如 `next_5d_return`。
7. `qlib_workflow`：Qlib workflow 模板名。
8. `experiment_namespace`：实验归档命名空间。
9. `signal_output`：信号输出格式。

它不应该直接暴露给用户的字段：

1. Qlib 物理目录
2. MLflow URI
3. 实验工件物理路径
4. 中间缓存路径

### 4.4 StrategyProfile

`StrategyProfile` 是“怎么把信号变成可验证的策略结果”的配置对象。

它应该承载的用户可见参数：

1. `research_profile` 或 `signal_source`：信号来源。
2. `benchmark`：基准。
3. `rebalance_rule`：调仓规则。
4. `portfolio_constraints`：组合约束。
5. `cost_model`：手续费模型。
6. `slippage_model`：滑点模型。
7. `risk_pack`：风控规则包。
8. `backtest_engine`：默认回测引擎，例如 `backtrader`。
9. `report_template`：回测报告模板。

它不应该暴露给用户的字段：

1. Backtrader 具体 adapter 类路径
2. 内部 broker stub 实现名
3. 低层订单模拟对象细节

### 4.5 TradeProfile

`TradeProfile` 是“怎么把目标组合送去执行”的配置对象。

它应该承载的用户可见参数：

1. `strategy_profile` 或 `portfolio_source`：组合来源。
2. `mode`：`paper` 或 `live`。
3. `gateway`：例如 `miniqmt`。
4. `account`：账户别名。
5. `trading_window`：允许交易时间窗。
6. `order_policy`：拆单、限价、市价等规则。
7. `retry_policy`：失败重试策略。
8. `risk_pack`：执行前风控规则包。
9. `reconcile_policy`：收盘后对账策略。

它不应该暴露给用户的字段：

1. miniQMT 主机地址细节
2. 认证密钥原文
3. 网关 session 内部状态
4. RPC 连接对象

---

## 五、用户可见字段和内部派生字段必须分开

这是 profile 设计里最容易做错的地方。

### 5.1 用户可见字段

用户可见字段应该满足两个条件：

1. 业务上真的有意义。
2. 用户真的可能主动修改。

例如：

1. `market`
2. `universe`
3. `freq`
4. `rebalance_rule`
5. `cost_model`
6. `quality_policy`
7. `snapshot_policy`

### 5.2 内部派生字段

内部派生字段的特征是：

1. 主要服务于运行时装配。
2. 变动频率高。
3. 对业务使用者没有解释价值。

例如：

1. `duckdb_path`
2. `qlib_uri`
3. `artifact_root`
4. `provider_credentials_ref`
5. `resolved_storage_backend`
6. `resolved_gateway_endpoint`

这些字段应该由系统在加载 profile 时自动补成“已解析配置对象（resolved profile）”，而不是让用户直接写。

### 5.3 为什么一定要分开

如果不分开，最终一定会出现两个后果：

1. 用户不敢改 profile，因为看不懂哪些字段能改。
2. 工程团队也不敢重构底层，因为一改内部路径就会破坏用户配置。

---

## 六、怎么让配置是可选的，而不是可填的

这是 `profile` 设计里最重要的产品问题。

如果用户每次都要面对几十个字段，即使这些字段 technically 都有默认值，使用体验仍然是“要填很多配置”。

所以这里要把一个观念讲清楚：

1. 好的配置系统，不是“允许填很多东西”。
2. 好的配置系统，是“绝大多数字段用户根本不需要碰”。

### 6.1 配置的目标不是完整描述世界，而是只表达差异

从使用者角度，最理想的状态是：

1. 新手只需要选一个现成 profile。
2. 熟练用户只需要覆盖少数字段。
3. 高级用户才需要定义完整 profile。

换句话说，配置应该表达“我和默认模板有什么不同”，而不是“我把整个系统重新写一遍”。

### 6.2 采用四层配置来源

建议运行时配置始终按 4 层合成：

1. **系统默认层**：代码内置默认值，解决“最小可运行”。
2. **领域模板层**：团队维护的 base profile，解决“市场/场景标准化”。
3. **实例 profile 层**：某个具体 profile 文件，解决“这条工作线的个性化差异”。
4. **运行时覆盖层**：CLI/API 临时 override，解决“今天这次只改一点点”。

优先级从低到高依次覆盖。

这套设计的价值是：

1. 默认值集中，不用每个 YAML 都写一遍。
2. 模板和实例分开，不会满地复制粘贴。
3. CLI 临时参数不会污染长期配置。

### 6.3 用户真正应该手动填写的字段必须很少

建议把字段分成 4 类：

1. **必选字段**：用户必须明确声明，否则系统不知道他要干什么。
2. **常用可选字段**：用户偶尔会改，但大多数时候使用默认值。
3. **高级可选字段**：只给高级用户或平台维护者使用。
4. **禁止直填字段**：只能由系统派生，用户不应该接触。

以 `DataProfile` 为例：

1. 必选字段通常只有 `name`、`type`、`extends` 或少量业务差异字段。
2. 常用可选字段可能是 `datasets`、`universe`、`snapshot_policy.publish_time`。
3. 高级可选字段可能是某些质量阈值。
4. 禁止直填字段则包括路径、凭证、内部 backend 细节。

### 6.4 CLI 也要配合“少填”原则

如果 CLI 仍然要求用户每次都写 `--provider --storage --calendar --timezone --qlib-uri`，那 profile 再优雅也没有意义。

所以 CLI 也要遵守同样原则：

1. 大多数命令只需要 `--profile`。
2. 次常用场景加上 `--snapshot` 或 `--as-of`。
3. 极少数场景再允许 `--set key=value` 形式的小覆盖。

例如：

```bash
vortex data sync --profile cn_stock_daily
vortex research run --profile cn_alpha_daily --snapshot latest
vortex strategy backtest --profile dividend_classic --set benchmark=000905.SH
```

这里的设计重点不是让 override 很强，而是让 override 很克制。

### 6.5 推荐引入“最新成功快照”这类可解析别名

为了让用户不必每次都手写具体日期，可以允许少量稳定别名：

1. `latest`
2. `latest_success`
3. `latest_research`

但系统必须在输出里打印最终解析结果，例如：

1. `snapshot=latest_success`
2. `resolved_snapshot=2026-04-04`

这样可以同时兼顾：

1. 入口简洁
2. 运行结果可复现

### 6.6 推荐支持 profile alias，但不要支持任意自由拼装

为了让用户记忆负担更小，可以支持 profile alias，例如：

1. `cn_daily`
2. `alpha_daily`
3. `dividend_paper`

但不建议让用户在 CLI 上临时拼装一大堆配置，例如：

```bash
vortex research run --market cn --freq 1d --provider tushare --universe all_a --feature-set alpha158 ...
```

这种形式看似灵活，实际上会把系统重新带回脚本化。

### 6.7 推荐引入 profile preset + patch 模式

很多时候用户不是想“新建一份完整 profile”，而是想“在已有模板上改一点”。

所以建议支持两种使用方式：

1. `preset`：团队提供的稳定模板。
2. `patch`：用户只写变化部分。

例如：

```yaml
name: dividend_classic_smallcap
type: strategy
extends: dividend_classic

portfolio_constraints:
  max_names: 50

benchmark: 000905.SH
```

这比复制一整份 `dividend_classic.yaml` 更健康。

### 6.8 从产品体验上，推荐增加“先看解析结果，再执行”能力

如果系统支持：

```bash
vortex profile resolve --profile cn_alpha_daily
```

那么用户就能先看到：

1. 最终用了哪个 base profile
2. 哪些字段来自默认值
3. 哪些字段来自 override
4. 最终解析出来的 snapshot family、provider、calendar、risk pack 是什么

这会明显降低用户对配置系统的恐惧感。

### 6.9 一句话总结“可选而不是可填”

不是让用户拥有填写所有字段的权力，
而是让用户在大多数时候只做一个动作：**选模板**；
少数时候做第二个动作：**覆盖差异**。

### 6.10 最小 profile 应该可以短到什么程度

如果这套设计是对的，那么很多 profile 文件应该短到只表达差异。

例如，一个研究 profile 完全可以只有：

```yaml
name: cn_alpha_daily
type: research
extends: cn_alpha_base
```

再例如，一个策略 profile 也完全可以只写：

```yaml
name: dividend_classic_smallcap
type: strategy
extends: dividend_classic

benchmark: 000905.SH
```

甚至一个交易 profile 也可以只有：

```yaml
name: dividend_live
type: trade
extends: dividend_classic_live

account: cn_equity_live_02
```

如果未来用户仍然普遍需要手写 30 个字段，说明 profile 系统设计仍然失败，还没有真正做到“可选而不是可填”。

### 6.11 对 DataProfile 的进一步收敛

对 `DataProfile` 来说，真正的问题不是“还能不能再少写两个字段”，而是“运营者到底该不该直接接触技术参数”。

这里建议进一步收敛为两层：

1. **运营者可见层**：只保留 `preset`、`history_start`、`schedule`、`publish`、`notify` 这类业务动作字段。
2. **平台内部层**：由 preset 展开出 `provider`、`datasets`、`quality_policy`、`pit_policy`、`snapshot_policy`、`storage pack` 等技术字段。

这样做的意义是：

1. 运营者不需要先理解技术栈，才能开始使用系统。
2. 平台可以独立演进质量门禁、存储后端和发布策略，而不破坏用户 profile。
3. 同一类运营动作可以通过更换 preset 迁移到不同市场、不同数据源、不同质量包。

这也是为什么后面推荐引入 `preset / pack` 分层，而不是继续把所有技术字段平铺到用户 profile 里。

---

## 七、配置文件组织方式

### 7.1 建议目录结构

```text
profiles/
  data/
    cn_stock_daily.yaml
    cn_stock_daily_extended.yaml
  research/
    cn_alpha_base.yaml
    cn_alpha_daily.yaml
  strategy/
    dividend_classic.yaml
  trade/
    dividend_classic_paper.yaml
    dividend_classic_live.yaml

presets/
  data/
    cn_stock_daily_core.yaml
    cn_stock_daily_extended.yaml

packs/
  quality/
    cn_daily_default.yaml
  pit/
    cn_financial_strict.yaml
  publish/
    research_daily.yaml
  storage/
    parquet_duckdb_default.yaml
```

这样组织的原因是：

1. 使用者能一眼看出 profile 属于哪个领域。
2. CLI 可以据此做类型校验。
3. 后续 UI 也容易按领域列出 profile。
4. `preset` 和 `pack` 被明确隔离后，用户可见配置和平台内部装配不会继续混在一起。

### 7.2 建议支持 `extends`

很多 profile 之间只有少量差异，不应该整份复制。

例如：

1. `cn_stock_base` 定义通用市场、日历、时区。
2. `cn_stock_daily` 在它基础上增加 `tushare`、`1d`、默认 datasets。
3. `cn_alpha_daily` 继承研究基础模板，只覆盖特征集和标签定义。

`extends` 的价值是：

1. 降低重复。
2. 提高一致性。
3. 让“模板 -> 实例”结构更稳定。

### 7.3 推荐的 preset / pack 分层

这一步是把“profile 太多技术参数”问题真正解决掉的关键。

建议区分：

1. `profile`：运营者真正选择和修改的工作模板。
2. `preset`：某条数据线的一揽子技术方案。
3. `pack`：更细粒度的能力包，例如质量包、PIT 包、发布包、存储包。

它们的关系建议是：

1. 运营者在 `profile` 中选择 `preset`。
2. `preset` 决定默认 market、provider、datasets 和 pack 组合。
3. `pack` 再分别定义质量规则、PIT 规则、发布规则、存储规则。

例如：

1. `cn_stock_daily` 这个 profile 选择 `cn_stock_daily_core`。
2. `cn_stock_daily_core` 这个 preset 内部引用 `cn_daily_default` 质量包。
3. 同一个运营 profile 未来也可以通过更换 preset 切到扩展数据线，而不需要用户重新理解底层细节。

### 7.4 对运营者可见的 profile 文件，建议补中文注释

如果用户会直接打开 YAML，那么 YAML 本身就应该承担部分解释职责。

建议原则：

1. 运营者可见的 profile 文件应该有中文注释。
2. 注释重点解释“这个字段是做什么的”“平时要不要改”“改了会影响什么”。
3. 不要把解释只放在源码或聊天里。

---

## 八、从 CLI 看，Profile 应该怎么被使用

从用户角度，命令应该保持统一：

```bash
vortex data sync --profile cn_stock_daily --as-of 2026-04-04
vortex research run --profile cn_alpha_daily --snapshot 2026-04-04
vortex strategy backtest --profile dividend_classic --snapshot 2026-04-04
vortex trade paper rebalance --profile dividend_classic_paper --snapshot 2026-04-04
```

这里有一个很重要的设计点：

1. 参数名统一叫 `--profile`
2. 但命令空间决定了 profile 类型

也就是说：

1. `vortex data *` 只能接受 `DataProfile`
2. `vortex research *` 只能接受 `ResearchProfile`
3. `vortex strategy *` 只能接受 `StrategyProfile`
4. `vortex trade *` 只能接受 `TradeProfile`

这能防止一种很常见的错误：

1. 用户拿 `strategy profile` 去跑 `data sync`
2. 或者拿 `data profile` 去跑 `trade live`

系统应该在入口层就给出明确报错，而不是运行到一半才失败。

---

## 九、建议的配置样例

下面样例的重点不是 YAML 语法，而是字段边界。

这里要特别强调：

1. 这些是“参考模板”，不是推荐用户日常从零手写的最小输入。
2. 真正的日常使用应尽量基于 `extends` 只写差异。
3. 下面之所以写得完整，是为了把字段边界一次讲清楚。

### 9.1 DataProfile 示例

如果面向运营者，推荐优先看到的是这个最小示例：

```yaml
name: cn_stock_daily
type: data
preset: cn_stock_daily_core
history_start: 2018-01-01
schedule: weeknights_2200
publish: research_daily
notify: data_team
```

这个示例表达的是：

1. 运营者选择一条数据线。
2. 系统按 preset 决定 provider、datasets、quality、pit、publish 细节。
3. 用户只关心这条线从哪里开始、何时自动跑、跑完要不要发布和通知。

如果面向平台内部模板或解析后的展开视图，才会出现下面这种更完整的技术字段：

```yaml
name: cn_stock_daily
type: data
extends: cn_stock_base
description: A股日频研究快照生产线
owner: data-team
tags: [cn, stock, daily]
enabled: true

market: cn_stock
universe: all_a
freqs: [1d, 1m]
provider: tushare
datasets:
  - instruments
  - trading_calendar
  - bar_1d
  - adj_factor
  - daily_basic
  - income
  - balance_sheet
  - cashflow
  - fina_indicator
  - disclosure_calendar

timezone: Asia/Shanghai
calendar: xshg

quality_policy:
  mode: fail_closed
  required_rules:
    - required_columns
    - primary_key_unique
    - trading_calendar_consistency
    - adj_factor_sanity

pit_policy:
  mode: strict
  ann_date_field: ann_date
  disclosure_dataset: disclosure_calendar

snapshot_policy:
  publish_time: "22:00"
  selector: latest_success
  retain_days: 365
```

### 9.2 ResearchProfile 示例

```yaml
name: cn_alpha_daily
type: research
description: A股日频因子研究模板
owner: research-team
tags: [cn, alpha, qlib]
enabled: true

data_profile: cn_stock_daily
market: cn_stock
universe: all_a
freq: 1d

feature_set: alpha158_cn_v1
label_spec:
  name: next_5d_return
  horizon: 5
  transform: rank

qlib_workflow: qlib_alpha_daily
experiment_namespace: cn_alpha_daily

signal_output:
  format: signal_snapshot
  topk: 100
```

### 9.3 StrategyProfile 示例

```yaml
name: dividend_classic
type: strategy
description: 高股息经典策略回测配置
owner: strategy-team
tags: [cn, dividend, backtest]
enabled: true

research_profile: cn_alpha_daily
benchmark: 000300.SH

rebalance_rule:
  frequency: monthly
  trading_day: 1

portfolio_constraints:
  max_names: 30
  max_weight_per_name: 0.08
  min_cash_ratio: 0.01

cost_model:
  commission_bps: 8
  tax_bps: 10

slippage_model:
  type: fixed_bps
  bps: 10

risk_pack: cn_equity_v1
backtest_engine: backtrader
report_template: default_backtest_v1
```

### 9.4 TradeProfile 示例

```yaml
name: dividend_classic_paper
type: trade
description: 高股息经典策略模拟盘执行配置
owner: execution-team
tags: [cn, dividend, paper]
enabled: true

strategy_profile: dividend_classic
mode: paper
gateway: miniqmt
account: cn_equity_paper_01

trading_window:
  start: "09:30"
  end: "14:55"

order_policy:
  order_type: limit
  split_policy: twap_like

retry_policy:
  max_retries: 2

risk_pack: cn_execution_v1

reconcile_policy:
  enabled: true
  run_after_close: true
```

---

## 十、工程上建议再区分一个 ResolvedProfile

用户写的是 profile 文件，但系统运行时不应该直接拿原始 YAML 硬跑。

建议增加一个解析后的对象：`ResolvedProfile`。

它负责：

1. 处理 `extends`。
2. 合并环境变量和密钥引用。
3. 校验字段完整性。
4. 生成内部运行时需要的派生字段。

也就是说，工程上应该区分两层：

1. `Profile`：对外配置契约
2. `ResolvedProfile`：对内运行时装配对象

这一步很重要，因为它把“用户可理解的配置”和“系统可执行的配置”分开了。

---

## 十一、Profile 系统类接口设计

前面已经把配置模型讲清楚了，这一节开始回答“代码里到底有哪些类”。

这里建议把 profile 系统放在：

```text
vortex/config/profile/
```

而不是 `vortex/profile/`。

原因是：

1. `profile` 是跨数据、研究、回测、交易的配置基础设施。
2. 它本身不是一个业务域，而是一个配置装配系统。

### 11.1 建议目录结构

```text
vortex/config/profile/
  __init__.py
  exceptions.py
  types.py
  models.py
  defaults.py
  store.py
  loader.py
  merger.py
  validator.py
  resolver.py
  overrides.py
  service.py
```

### 11.2 核心模型类

文件建议：`vortex/config/profile/models.py`

1. `BaseProfile`
2. `DataProfile`
3. `ResearchProfile`
4. `StrategyProfile`
5. `TradeProfile`
6. `ResolvedProfile`

职责：

1. `BaseProfile` 放公共字段。
2. 4 个领域 profile 放用户可见字段。
3. `ResolvedProfile` 放运行时派生字段和装配结果。

### 11.3 存储与加载类

文件建议：`vortex/config/profile/store.py`

1. `ProfileStore`

职责：

1. 根据名字和类型找到 profile 文件。
2. 列出当前可用 profiles。
3. 对外隐藏具体文件系统布局。

文件建议：`vortex/config/profile/loader.py`

1. `ProfileLoader`

职责：

1. 从 `ProfileStore` 读取原始 YAML。
2. 反序列化为对应的 `BaseProfile` 子类。
3. 不处理 `extends` 合并，不做运行时派生。

### 11.4 合并与默认值类

文件建议：`vortex/config/profile/defaults.py`

1. `ProfileDefaultsProvider`

职责：

1. 提供系统默认层。
2. 按 profile 类型和市场补默认值。

文件建议：`vortex/config/profile/merger.py`

1. `ProfileMerger`

职责：

1. 处理 `extends` 链。
2. 合并默认层、模板层、实例层、override 层。
3. 输出“未解析但已展开”的 profile 对象。

### 11.5 校验与解析类

文件建议：`vortex/config/profile/validator.py`

1. `ProfileValidator`

职责：

1. 做字段级校验。
2. 做跨字段语义校验。
3. 做命令空间与 profile 类型匹配校验。

文件建议：`vortex/config/profile/resolver.py`

1. `ProfileResolver`
2. `SnapshotSelector`

职责：

1. `ProfileResolver` 把 profile 转成 `ResolvedProfile`。
2. `SnapshotSelector` 把 `latest_success` 这类别名解析成具体 snapshot。

### 11.6 运行时 override 类

文件建议：`vortex/config/profile/overrides.py`

1. `RuntimeOverride`
2. `OverrideParser`

职责：

1. `RuntimeOverride` 描述一次运行时覆盖。
2. `OverrideParser` 把 CLI 的 `--set key=value` 解析成结构化对象。

### 11.7 门面服务类

文件建议：`vortex/config/profile/service.py`

1. `ProfileService`

职责：

1. 对 CLI/API 提供统一入口。
2. 串联 `store -> loader -> merger -> validator -> resolver`。
3. 对外只暴露少量稳定方法。

### 11.8 推荐的最小方法签名

```python
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RuntimeOverride:
  values: dict[str, Any]


class ProfileStore:
  def list_profiles(self, profile_type: str | None = None) -> list[str]: ...
  def exists(self, name: str, profile_type: str | None = None) -> bool: ...
  def load_text(self, name: str, profile_type: str | None = None) -> str: ...


class ProfileLoader:
  def load(self, name: str, profile_type: str | None = None) -> "BaseProfile": ...


class ProfileDefaultsProvider:
  def get_defaults(self, profile_type: str, market: str | None = None) -> dict[str, Any]: ...


class ProfileMerger:
  def expand(
    self,
    profile: "BaseProfile",
    override: RuntimeOverride | None = None,
  ) -> "BaseProfile": ...


class ProfileValidator:
  def validate(self, profile: "BaseProfile", command_scope: str | None = None) -> None: ...


class SnapshotSelector:
  def resolve(self, snapshot_ref: str | None, profile: "BaseProfile") -> str | None: ...


class ProfileResolver:
  def resolve(
    self,
    profile: "BaseProfile",
    snapshot_ref: str | None = None,
  ) -> "ResolvedProfile": ...


class ProfileService:
  def prepare(
    self,
    name: str,
    profile_type: str,
    command_scope: str,
    snapshot_ref: str | None = None,
    override: RuntimeOverride | None = None,
  ) -> "ResolvedProfile": ...
```

### 11.9 类之间怎么衔接

一条标准调用链建议固定为：

1. CLI/API 把 `--profile`、`--snapshot`、`--set` 交给 `ProfileService`。
2. `ProfileService` 通过 `ProfileStore` 找到 profile 文件。
3. `ProfileLoader` 把 YAML 读成 profile 对象。
4. `ProfileMerger` 展开 `extends`，再叠加默认值与 runtime override。
5. `ProfileValidator` 检查字段合法性和命令空间匹配性。
6. `SnapshotSelector` 解析 snapshot 别名。
7. `ProfileResolver` 产出 `ResolvedProfile`。
8. 业务模块只消费 `ResolvedProfile`，不再关心 YAML 合并细节。

这里最关键的边界是：

1. 业务模块不应该自己读 YAML。
2. 业务模块不应该自己解析 `extends`。
3. 业务模块不应该自己猜 `latest_success` 对应哪一天。

### 11.10 为什么需要 `ProfileService` 这个门面

如果没有门面层，很快就会出现：

1. CLI 自己写一套 profile 加载逻辑。
2. API 再写一套。
3. 调度器再复制一套。

最后虽然都叫 profile，实际行为却不一致。

所以 `ProfileService` 的价值是：

1. 让所有入口复用同一条解析链。
2. 保证 CLI、API、调度系统看到的是同一份 `ResolvedProfile`。

---

## 十二、第一版最小实现建议

如果下一步开始写代码，我建议不要一上来把 4 类 profile 全部实现完。

第一版最小闭环应当是：

1. 先实现 `DataProfile` 与 `ResearchProfile`。
2. 先把 `ProfileLoader`、`ProfileValidator`、`ProfileResolver` 做出来。
3. 先让 `vortex data sync` 和 `vortex research run` 走通 profile 加载与解析流程。

原因很直接：

1. 数据和研究是当前最先落地的主线。
2. `StrategyProfile` 和 `TradeProfile` 依赖前两者稳定后再落更合理。
3. 这样最容易尽快验证 `profile + snapshot` 这套用户心智模型是不是顺手。

---

## 十三、结论

从使用者角度看，`profile` 不应该是“系统参数大仓库”，而应该是：

1. 对外稳定的配置入口
2. 按领域分型的工作模板
3. 与 `snapshot` 明确分工的长期配置对象

如果这件事做对了，后面 CLI、API、调度、UI 都会变简单；
如果这件事做错了，后面所有模块都会重新退化成“脚本 + 参数 + 路径”的组合。
