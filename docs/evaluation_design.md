# 因子评测系统重构设计

> 状态：设计阶段，待审核后实现
> 目标：将 `scripts/run_factor_test.py` 中的评测逻辑下沉到 `vortex/evaluation/`，按因子角色分类评测

---

## 一、现状问题

```
scripts/run_factor_test.py     ← 500 行，IC/多空/权重/回测全在一个脚本
vortex/analysis/analyzer.py    ← FactorAnalyzer 把所有因子一视同仁
vortex/factor/base.py          ← BaseFactor.category 只是字符串标签，没有任何逻辑意义
```

**具体问题：**

| # | 问题 | 影响 |
| --- | --- | --- |
| 1 | 评测逻辑在脚本层，不可复用 | 新策略无法复用评测流程 |
| 2 | 所有因子用同一套 IC + 多空评测 | 过滤型因子（如连续分红年数）算 IC 没有意义 |
| 3 | category 仅做标签 | 无法按类别设定不同准入标准或评测方法 |
| 4 | 权重优化逻辑零散 | 和评测耦合在一起，不易扩展 |

---

## 二、核心抽象

### 2.1 因子角色 FactorRole

当前 `BaseFactor` 有 `category`（value / quality / cashflow），但缺少"这个因子在策略中扮演什么角色"的概念。同一个 category 的因子，角色可能不同：

| 角色 | 含义 | 当前因子举例 | 评测方式 |
| --- | --- | --- | --- |
| `SCORING` | 参与综合打分排序 | dividend_yield, ep, fcf_yield, delta_roe | IC + 多空 + 权重优化 |
| `FILTER` | 硬门槛过滤 | consecutive_div_years ≥ 5, fcf_ttm > 0 | 通过率 + 覆盖度 + 条件 IC |
| `TIMING` | 择时/情绪/动量 | (暂无，预留) | 分域 IC + 状态划分 |
| `RISK` | 风险约束 | debt_to_assets, roe_stability | 尾部分布 + 条件覆盖 |

```python
class FactorRole(str, Enum):
    SCORING = "scoring"
    FILTER  = "filter"
    TIMING  = "timing"
    RISK    = "risk"
```

**设计要点：**

- `role` 是因子在策略中的用途，`category` 是经济学分类，二者正交
- 一个因子可以在不同策略中扮演不同角色（例如 `roe_ttm` 在 A 策略是 SCORING，在 B 策略是 FILTER）
- 因此 `role` **不应该写死在因子类上**，而是由策略或评测配置指定

### 2.2 评测配置 EvalSpec

每个因子的评测需要一份配置，描述"用什么方式评测这个因子"：

```python
@dataclass
class EvalSpec:
    """单个因子的评测规格"""
    factor_name: str                    # "dividend_yield"
    role: FactorRole                    # SCORING / FILTER / ...
    horizons: list[int] = (1, 5, 20)   # IC 计算周期
    ls_horizon: int = 5                 # 多空组合的 forward days
    ls_groups: int = 5                  # 多空分组数
    threshold: float | None = None     # FILTER 角色的门槛值
    threshold_op: str = ">="           # 门槛比较运算符
```

**为什么不把 horizons 放在 Evaluator 上？** 因为同一个 ScoringEvaluator 可能评测两个因子，但一个用 `[1,5,20]` 短线周期，另一个用 `[20,60,120]` 长线周期。周期是因子属性，不是评测器属性。

### 2.3 评测结果 EvalResult

所有评测器返回统一格式的结果：

```python
@dataclass
class EvalResult:
    """单因子评测结果"""
    factor_name: str
    role: FactorRole
    passed: bool                        # 是否通过准入
    metrics: dict[str, float]           # 指标集合，key 含义由 role 决定
    detail: pd.DataFrame | None = None  # 可选的明细数据（IC 时序等）
    reason: str = ""                    # 人可读结论
```

**`metrics` 的 key 约定：**

| 角色 | metrics 典型 key |
| --- | --- |
| SCORING | `mean_ic_5d`, `icir_20d`, `positive_rate_20d`, `ls_return_5d`, `ls_sharpe` |
| FILTER | `pass_rate`, `coverage`, `conditional_ic` |
| TIMING | `ic_bull`, `ic_bear`, `regime_count` |
| RISK | `tail_coverage`, `breach_rate` |

---

## 三、类图

```
┌─────────────────────────────────────────────────────┐
│                   EvalPipeline                      │
│  (对外唯一入口，编排整个评测流程)                     │
│                                                     │
│  specs: list[EvalSpec]                              │
│  analyzer: FactorAnalyzer                           │
│  _registry: dict[FactorRole, BaseEvaluator]         │
│                                                     │
│  + run(dates) → list[EvalResult]                    │
│  + summary() → pd.DataFrame                        │
│  + admission_report() → pd.DataFrame               │
└───────────────┬─────────────────────────────────────┘
                │ dispatch by role
                ▼
┌───────────────────────────────┐
│     BaseEvaluator (ABC)       │
│                               │
│  + evaluate(                  │
│      factor_name,             │
│      spec: EvalSpec,          │
│      analyzer: FactorAnalyzer,│
│      dates: list[str]         │
│    ) → EvalResult             │
│                               │
│  + default_admission(         │
│      result: EvalResult       │
│    ) → bool                   │
└───────┬───────┬───────┬───────┘
        │       │       │
   ┌────┴──┐ ┌──┴───┐ ┌─┴──────┐
   │Scoring│ │Filter│ │  Risk  │
   │Evalu- │ │Evalu-│ │ Evalu- │
   │ator   │ │ator  │ │ ator   │
   └───────┘ └──────┘ └────────┘
```

---

## 四、各评测器职责

### 4.1 ScoringEvaluator

**适用于：** 参与综合打分的因子（dividend_yield, ep, fcf_yield, delta_roe, roe_ttm, opcfd）

**评测内容：**

```
1. 多周期 IC         ← 复用 FactorAnalyzer.ic_report_multi_horizon()
2. 多空组合收益      ← 复用 FactorAnalyzer.long_short_report()
3. IC 衰减判断       ← 新增：比较短/中/长 horizon 的 IC 衰减速度
4. 准入判断          ← |mean_ic| > 阈值 AND icir > 阈值 AND positive_rate > 阈值
```

**准入默认标准（可覆盖）：**

```python
SCORING_ADMISSION = {
    "min_abs_ic": 0.03,
    "min_icir": 0.3,
    "min_positive_rate": 0.50,
    "min_periods": 6,
}
```

### 4.2 FilterEvaluator

**适用于：** 硬门槛过滤因子（consecutive_div_years ≥ 5, fcf_ttm > 0, payout_ratio_3y ∈ [20%, 90%]）

**评测内容：**

```
1. 通过率(pass_rate)  ← 全市场/指定域中满足门槛的股票占比
2. 覆盖度(coverage)   ← 有因子值（非 NaN）的股票占比
3. 条件 IC            ← 在通过门槛的子集上，计算该因子的排序 IC
4. 门槛敏感度         ← 缩放门槛 ±20%，观察 pass_rate 变化
```

**准入默认标准：**

```python
FILTER_ADMISSION = {
    "min_coverage": 0.80,       # 因子覆盖度 ≥ 80%
    "pass_rate_range": (0.10, 0.70),  # 通过率不能太低（空集）也不能太高（无区分度）
}
```

### 4.3 RiskEvaluator

**适用于：** 风险约束因子（debt_to_assets, roe_stability）

**评测内容：**

```
1. 尾部覆盖           ← 极端值（>95pct 或 <5pct）的股票数量和占比
2. 违约率(breach_rate) ← 持仓中触碰风险阈值的频率
3. 条件 IC            ← 在尾部极端区域，该因子是否仍有预测力
```

### 4.4 TimingEvaluator（预留）

**适用于：** 择时/动量/情绪因子（当前策略暂无，但架构预留）

**评测内容：**

```
1. 分域 IC            ← 牛市/熊市/震荡市分别计算 IC
2. 状态翻转检测       ← 因子值极端变化时，后续收益是否显著
```

---

## 五、模块结构

```
vortex/
  evaluation/                       ← 新增 package
    __init__.py                     ← 导出 EvalPipeline, EvalSpec, EvalResult
    spec.py                         ← EvalSpec, FactorRole, EvalResult (数据类)
    base.py                         ← BaseEvaluator 抽象基类
    scoring.py                      ← ScoringEvaluator
    filter.py                       ← FilterEvaluator
    risk.py                         ← RiskEvaluator (简版)
    pipeline.py                     ← EvalPipeline (编排 + 汇总)
```

**不新增的模块：**

- `reporter.py` — 暂不独立，报告逻辑放在 `EvalPipeline.summary()` 即可
- `timing.py` — 当前无择时因子，不实现空壳

**修改的现有模块：**

- `vortex/models.py` — 新增 `FactorRole` 枚举
- `vortex/analysis/analyzer.py` — 不改动，保持纯粹的底层计算工具
- `vortex/factor/base.py` — 不改动，`role` 不放在因子类上

---

## 六、核心接口设计

### 6.1 BaseEvaluator

```python
# vortex/evaluation/base.py

from abc import ABC, abstractmethod
from .spec import EvalSpec, EvalResult

class BaseEvaluator(ABC):
    """因子评测策略基类"""

    @abstractmethod
    def evaluate(
        self,
        spec: EvalSpec,
        analyzer: FactorAnalyzer,
        dates: list[str],
    ) -> EvalResult:
        """
        执行评测

        Parameters
        ----------
        spec : EvalSpec
            本因子的评测规格（含 factor_name, role, horizons 等）
        analyzer : FactorAnalyzer
            底层计算器（IC、多空等）
        dates : list[str]
            截面日期列表

        Returns
        -------
        EvalResult
            标准化评测结果
        """
        ...

    def default_admission(self, result: EvalResult) -> bool:
        """
        默认准入判断

        子类可覆盖此方法以自定义准入标准。
        """
        return result.passed
```

### 6.2 EvalPipeline

```python
# vortex/evaluation/pipeline.py

class EvalPipeline:
    """
    因子评测管线

    职责：
    1. 接收一组 EvalSpec
    2. 按 role 分派到对应 Evaluator
    3. 汇总结果

    用法:
        pipeline = EvalPipeline(analyzer)
        pipeline.add(EvalSpec("dividend_yield", FactorRole.SCORING, horizons=[1,5,20]))
        pipeline.add(EvalSpec("consecutive_div_years", FactorRole.FILTER, threshold=5))
        results = pipeline.run(dates)
        print(pipeline.summary(results))
    """

    def __init__(self, analyzer: FactorAnalyzer):
        self.analyzer = analyzer
        self._evaluators: dict[FactorRole, BaseEvaluator] = {
            FactorRole.SCORING: ScoringEvaluator(),
            FactorRole.FILTER:  FilterEvaluator(),
            FactorRole.RISK:    RiskEvaluator(),
        }
        self._specs: list[EvalSpec] = []

    def add(self, spec: EvalSpec) -> "EvalPipeline":
        """添加一个因子评测规格，支持链式调用"""
        self._specs.append(spec)
        return self

    def register_evaluator(self, role: FactorRole, evaluator: BaseEvaluator):
        """注册自定义评测器（扩展点）"""
        self._evaluators[role] = evaluator

    def run(self, dates: list[str]) -> list[EvalResult]:
        """
        执行全部评测

        按 role 分组 → 分派给对应 evaluator → 收集 EvalResult
        """
        results = []
        for spec in self._specs:
            evaluator = self._evaluators.get(spec.role)
            if evaluator is None:
                raise ValueError(f"未注册角色 {spec.role} 的评测器")
            result = evaluator.evaluate(spec, self.analyzer, dates)
            results.append(result)
        return results

    def summary(self, results: list[EvalResult]) -> pd.DataFrame:
        """将评测结果汇总为一张表"""
        rows = []
        for r in results:
            row = {"factor": r.factor_name, "role": r.role.value, "passed": r.passed}
            row.update(r.metrics)
            rows.append(row)
        return pd.DataFrame(rows)

    def admission_report(self, results: list[EvalResult]) -> pd.DataFrame:
        """只输出准入判断结果"""
        rows = []
        for r in results:
            rows.append({
                "factor": r.factor_name,
                "role": r.role.value,
                "passed": r.passed,
                "reason": r.reason,
            })
        return pd.DataFrame(rows)
```

---

## 七、数据流

```
                          ┌──────────────────┐
 调用方 (脚本/策略)        │  EvalPipeline     │
   │                      │                  │
   │  add(EvalSpec)  ×N   │  specs: [...]    │
   │─────────────────────→│                  │
   │                      │                  │
   │  run(dates)          │   for spec:      │
   │─────────────────────→│    ┌─────────┐   │
   │                      │    │dispatch │   │
   │                      │    │by role  │   │
   │                      │    └────┬────┘   │
   │                      │         │        │
   │                      │    ┌────▼─────┐  │
   │                      │    │Evaluator │  │
   │                      │    │.evaluate │  │
   │                      │    └────┬─────┘  │
   │                      │         │        │
   │                      │    uses │        │
   │                      │    ┌────▼──────┐ │
   │                      │    │ Factor    │ │
   │                      │    │ Analyzer  │ │
   │                      │    │ (现有)    │ │
   │                      │    └───────────┘ │
   │                      │                  │
   │  ← list[EvalResult]  │                  │
   │←─────────────────────│                  │
   │                      └──────────────────┘
   │
   │  summary(results)
   │──→ pd.DataFrame
```

---

## 八、与现有代码的关系

### 不改动

| 模块 | 原因 |
| --- | --- |
| `vortex/analysis/analyzer.py` | 保持纯粹的底层计算工具，Evaluator 复用它 |
| `vortex/factor/*.py` | 因子类不需要知道自己被怎么评测 |
| `vortex/core/factorhub.py` | 注册/计算接口不变 |

### 小改动

| 模块 | 改动 |
| --- | --- |
| `vortex/models.py` | 新增 `FactorRole` 枚举（4 行） |
| `vortex/strategy/base.py` | BaseStrategy 新增 `eval_specs()` 方法（返回空列表，子类覆盖） |
| `vortex/strategy/dividend.py` | 实现 `eval_specs()`，声明全部因子的角色/门槛/数据来源 |

### 新增

| 模块 | 说明 |
| --- | --- |
| `vortex/evaluation/__init__.py` | 包导出 |
| `vortex/evaluation/spec.py` | EvalSpec + EvalResult 数据类 |
| `vortex/evaluation/base.py` | BaseEvaluator 抽象基类 |
| `vortex/evaluation/scoring.py` | ScoringEvaluator |
| `vortex/evaluation/filter.py` | FilterEvaluator |
| `vortex/evaluation/risk.py` | RiskEvaluator |
| `vortex/evaluation/pipeline.py` | EvalPipeline 编排器 |
| `vortex/evaluation/weight_tuner.py` | WeightTuner 权重优化（独立入口） |

### 重构

| 模块 | 改动 |
| --- | --- |
| `scripts/run_factor_test.py` | 瘦身为 CLI 薄层：解析参数 → 构造 EvalSpec → 调 EvalPipeline → 打印结果 |

---

## 九、调用示例

### 9.1 脚本层（重构后的 run_factor_test.py）

```python
from vortex.evaluation import EvalPipeline, WeightTuner

# ── Step 1: 从策略自动读取因子规格 ──
strategy = DividendQualityFCFStrategy(ds, fh, bus)
specs = strategy.eval_specs()

# ── Step 2: 跑评测 ──
pipeline = EvalPipeline(analyzer)
for s in specs:
    pipeline.add(s)
results = pipeline.run(dates)

# ── Step 3: 看报告 ──
print(pipeline.summary(results))
print(pipeline.admission_report(results))
# → 人工审核，确认哪些因子留下

# ── Step 4: 权重优化 (独立步骤，确认因子后才跑) ──
passed = [r.factor_name for r in results if r.role == FactorRole.SCORING and r.passed]
tuner = WeightTuner(analyzer)
weights_comparison = tuner.compare(passed, dates, horizons=[20, 60, 120])
print(weights_comparison)
```

### 9.2 策略层（查询通过的因子）

```python
passed_scoring = [
    r.factor_name
    for r in results
    if r.role == FactorRole.SCORING and r.passed
]
# → ["dividend_yield", "ep", "fcf_yield"]
```

### 9.3 完整工作流（评测 → 审核 → 权重优化 → 回测）

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  EvalPipeline │ ──→ │  人工审核    │ ──→ │ WeightTuner  │ ──→ │ BacktestEngine│
│              │     │  看报告      │     │              │     │              │
│  输出:       │     │  决定:       │     │  输出:       │     │  输出:       │
│  EvalResult[]│     │  留哪些因子  │     │  weights     │     │  NAV + 指标  │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
     (自动)              (人工)               (自动)               (自动)
```

---

## 十、设计决策记录

| # | 决策 | 理由 | 备选方案 |
| --- | --- | --- | --- |
| D1 | `role` 放在 EvalSpec 而非 BaseFactor | 同一因子在不同策略中角色可能不同 | 放 BaseFactor 上作 default_role |
| D2 | Evaluator 复用现有 FactorAnalyzer | 避免重复造轮子，FactorAnalyzer 已稳定 | 在 Evaluator 内自行计算 IC |
| D3 | EvalResult.metrics 用 dict 而非固定字段 | 不同 role 的指标集不同 | 每个 role 一个 Result 子类 |
| D4 | 不改 BaseFactor.category | category 是经济学分类，role 是策略用途，二者正交 | 合并为一个字段 |
| D5 | TimingEvaluator 暂不实现 | 当前无择时因子，避免过度设计 | 实现空壳 |
| D6 | 权重优化放 `evaluation/weight_tuner.py`，与 pipeline 独立 | 评测 → 人工审核 → 权重优化，是两个独立决策点 | 自动串联进 pipeline |
| D7 | EvalSpec 从策略 `eval_specs()` 方法自动生成 | 因子定义散落在策略多处，集中声明避免遗漏 | 手写或 YAML 配置 |
| D8 | EvalSpec 新增 `data_source` 字段 | 数据审计：知道因子算的对不对，先得知道数据从哪来 | 只写在因子类 docstring 里 |
| D9 | FilterEvaluator 自行计算通过率/覆盖度 | 简单统计不值得扩展 FactorAnalyzer | 扩展 FactorAnalyzer |

---

## 十一、实现优先级

| 阶段 | 内容 | 影响范围 |
| --- | --- | --- |
| P0 | `spec.py` + `base.py` + `pipeline.py` | 骨架搭建 |
| P1 | `scoring.py` | 现有 6 个打分因子能跑通 |
| P2 | `filter.py` | 3 个过滤因子能跑通 |
| P3 | `risk.py` | 2 个风险因子能跑通 |
| P4 | `weight_tuner.py` | 权重优化独立模块 |
| P5 | `BaseStrategy.eval_specs()` + `DividendQualityFCFStrategy.eval_specs()` | 策略声明因子规格 |
| P6 | 重构 `scripts/run_factor_test.py` | 脚本瘦身 |
| P7 | 测试 `tests/test_evaluation.py` | 回归保障 |

---

## 十二、开放问题（已解决）

> 以下三个问题在评审中已明确方向，记录结论。

### Q1: EvalSpec 从哪来？→ 从策略配置自动生成

**结论：** EvalSpec 不应该手写，应由策略类声明"我用了哪些因子、每个因子的角色和门槛"，评测系统自动读取。

**现状：** 策略里已经有这些信息，只是散落在不同地方：

- 打分因子列表 → `dividend.py` 的 `SCORING_FACTORS`
- 过滤门槛 → `build_filter_pipeline()` 里硬编码的 `FactorThresholdFilter`
- 权重 → `DEFAULT_WEIGHTS`

**方案：** 在策略基类上新增一个方法 `eval_specs()`，返回该策略使用的全部因子定义：

```python
# vortex/strategy/base.py (新增方法)

class BaseStrategy(ABC):
    ...
    def eval_specs(self) -> list[EvalSpec]:
        """
        声明策略使用的因子及其角色，供评测管线自动读取。

        子类必须实现，返回该策略涉及的全部因子评测规格。
        """
        return []
```

```python
# vortex/strategy/dividend.py (实现示例)

class DividendQualityFCFStrategy(BaseStrategy):
    ...
    def eval_specs(self) -> list[EvalSpec]:
        return [
            # ── 打分因子 ──
            EvalSpec(
                factor_name="dividend_yield",
                role=FactorRole.SCORING,
                horizons=[1, 5, 20, 60, 120],
                data_source="valuation.dv_ttm / close",
                description="当期股息率",
            ),
            EvalSpec(
                factor_name="ep",
                role=FactorRole.SCORING,
                horizons=[1, 5, 20, 60, 120],
                data_source="valuation.pe_ttm → 取倒数",
                description="盈利收益率 = 1/PE_TTM",
            ),
            ...
            # ── 过滤因子 ──
            EvalSpec(
                factor_name="consecutive_div_years",
                role=FactorRole.FILTER,
                threshold=self.scfg.min_consecutive_dividend_years,
                threshold_op=">=",
                data_source="dividend 分红公告表 → 按 end_date 统计连续年数",
                description="连续现金分红年数",
            ),
            EvalSpec(
                factor_name="fcf_ttm",
                role=FactorRole.FILTER,
                threshold=0,
                threshold_op=">",
                data_source="cashflow.free_cashflow 近四季度滚动合计",
                description="近一年自由现金流",
            ),
            # ── 风险因子 ──
            EvalSpec(
                factor_name="debt_to_assets",
                role=FactorRole.RISK,
                data_source="fina_indicator.debt_to_assets",
                description="资产负债率",
            ),
        ]
```

**EvalSpec 新增字段：**

```python
@dataclass
class EvalSpec:
    factor_name: str
    role: FactorRole
    horizons: list[int] = (1, 5, 20)
    ls_horizon: int = 5
    ls_groups: int = 5
    threshold: float | None = None
    threshold_op: str = ">="
    # ── 新增 ──
    data_source: str = ""     # 数据来源描述，方便审计
    description: str = ""     # 因子含义
```

**调用方式变化：**

```python
# 旧：手写 specs
specs = [EvalSpec("dividend_yield", ...), ...]

# 新：从策略自动读取
strategy = DividendQualityFCFStrategy(ds, fh, bus)
specs = strategy.eval_specs()
pipeline = EvalPipeline(analyzer)
for s in specs:
    pipeline.add(s)
results = pipeline.run(dates)
```

---

### Q2: 权重优化放哪？→ 同一个 evaluation/ 包，但独立模块

**结论：** 权重优化放在 `vortex/evaluation/` 内，但和评测管线完全独立。工作流是：

```
评测报告 → 人工审核 → 确定因子增减 → 权重优化
  (EvalPipeline)                      (WeightTuner)
       ↓                                   ↓
  EvalResult[]                    Dict[str, float]
       ↓                                   ↓
  人看报告，决策                     喂给策略去回测
```

**理由：** 评测和权重是两个独立决策点，你要先看评测报告，决定哪些因子留下，然后才做权重优化。不应该自动串联。

**模块结构更新：**

```
vortex/evaluation/
  __init__.py
  spec.py              ← EvalSpec, FactorRole, EvalResult
  base.py              ← BaseEvaluator 抽象基类
  scoring.py           ← ScoringEvaluator
  filter.py            ← FilterEvaluator
  risk.py              ← RiskEvaluator
  pipeline.py          ← EvalPipeline (评测编排)
  weight_tuner.py      ← WeightTuner (权重优化，独立入口) ← 新增
```

**WeightTuner 接口：**

```python
class WeightTuner:
    """
    权重优化器 — 输入因子列表，输出最优权重

    与 EvalPipeline 完全解耦：
    - EvalPipeline 负责"这些因子行不行"
    - WeightTuner 负责"行的那些因子怎么配权"

    用法:
        # 1. 先跑评测
        results = pipeline.run(dates)
        # 2. 人工确认通过的因子
        passed = ["dividend_yield", "ep", "fcf_yield"]
        # 3. 再做权重优化
        tuner = WeightTuner(analyzer)
        weights = tuner.optimize(passed, dates, horizon=20)
    """

    def __init__(self, analyzer: FactorAnalyzer):
        self.analyzer = analyzer

    def optimize(
        self,
        factor_names: list[str],
        dates: list[str],
        horizon: int = 20,
        method: str = "ic",    # "ic" | "icir" | "equal"
    ) -> dict[str, float]:
        """计算因子权重"""
        ...

    def compare(
        self,
        factor_names: list[str],
        dates: list[str],
        horizons: list[int] = [20, 60, 120],
        methods: list[str] = ["ic", "icir", "equal"],
    ) -> pd.DataFrame:
        """对比多种配权方案，输出表格供人选择"""
        ...
```

**注意：** 这个 `WeightTuner` 会取代现有 `scripts/run_factor_test.py` 中的
`compute_optimal_weights()` 函数，但不会取代 `vortex/core/weight_optimizer.py`
中的 `ICWeightOptimizer` 等类 —— 后者是策略运行时用的实时配权器，前者是研究阶段的离线调优工具。

```
WeightTuner (evaluation/)    ← 研究阶段：离线分析，人看报告
WeightOptimizer (core/)      ← 运行阶段：策略实时配权
```

---

### Q3: FactorAnalyzer 是什么？

**一句话：** FactorAnalyzer 是一个底层计算工具，负责"给我一个因子名和一组日期，我告诉你它的 IC 是多少、多空收益是多少"。

**类比：**

| 层次 | 角色 | 类比 |
| --- | --- | --- |
| `FactorAnalyzer` | 底层计算器 | 计算器 |
| `ScoringEvaluator` | 评测策略 | 使用计算器的会计 |
| `EvalPipeline` | 流程编排 | 安排多个会计干活的经理 |

**FactorAnalyzer 做的事：**

```python
analyzer = FactorAnalyzer(ds, fh)

# 1. 算 IC：给一个因子 + 日期列表 → 返回每个日期的 IC 值
ic_series = analyzer.calc_ic("dividend_yield", dates, forward_days=20)
# → pd.Series: {"20250131": 0.08, "20250228": 0.05, ...}

# 2. 汇总 IC 报告：给一组因子 → 返回每个因子的 IC 均值、ICIR、正IC率
ic_report = analyzer.ic_report(["dividend_yield", "ep"], dates, forward_days=20)
# → pd.DataFrame:
#   factor           mean_ic  icir  positive_rate
#   dividend_yield   0.071    0.50  0.68
#   ep               0.036    0.23  0.58

# 3. 算多空收益：前20%做多 vs 后20%做空
ls_report = analyzer.long_short_report(["dividend_yield"], dates, forward_days=5)
# → pd.DataFrame:
#   factor           long_short_5d  sharpe
#   dividend_yield   0.0085         1.32
```

**它不做什么：**

- 不判断因子"好不好"（准入判断是 Evaluator 的事）
- 不区分因子角色（所有因子一视同仁算 IC）
- 不做权重优化

**评测系统和它的关系：** Evaluator 调用 FactorAnalyzer 的方法获取数据，然后自己做判断逻辑。FactorAnalyzer 不需要改动。

FilterEvaluator 需要的"通过率""覆盖度"计算，不在 FactorAnalyzer 中 → **直接在 FilterEvaluator 内部实现**，因为这些都是简单的统计（count / len），不值得扩展 FactorAnalyzer。
