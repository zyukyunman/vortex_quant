---
name: volume-peak-ridge-valley
description: '高频成交量峰岭谷因子构建与检验。Use when: 从分钟级成交量提取微观结构因子、计算量峰/量岭/量谷、高频因子IC检验、多空组合回测、eruption爆发点识别。'
argument-hint: '描述您需要的峰岭谷因子计算或检验任务，如因子构建、IC分析、多空回测等'
tags: [vortex, vortex/skill, vortex/research-domain]
obsidian_links:
  - "[[因子研究与评测全流程说明]]"
  - "[[factor-evaluation skill]]"
  - "[[研究域设计说明书]]"
---

# 高频成交量的峰、岭、谷因子

基于开源证券《市场微观结构研究系列（27）》的因子复现方法论。从分钟级成交量数据中提取市场微观结构信息，构建 alpha 因子。

## 适用场景

- 从分钟级行情构建日频截面因子
- 量峰(Peak)、量岭(Ridge)、量谷(Valley) 的识别与因子化
- 因子 IC / 多空组合 / 稳定性的标准化检验流程
- 与动量、价值等因子的融合使用

## 核心概念

### 量峰（Peak）—— 孤立放量点

某一分钟出现放量（成交量 > 阈值），但**前后相邻分钟都是缩量**。通常由非持续性短期交易行为引起，如单笔大单或偶发情绪波动。

### 量岭（Ridge）—— 连续放量段

**连续 3 分钟及以上**出现放量，代表资金持续流入。这是最关键的因子信号——连续放量往往意味着机构或大资金在持续买入，是股价上涨的积极信号。

### 量谷（Valley）—— 缩量区间

成交量低于**历史均值 − 1 个标准差**。代表市场交投清淡，通常出现在盘整期或变盘前夜。

## 因子构建流程

### Step 1: 计算基准阈值

使用过去 20 个交易日的**同一分钟**成交量，计算滚动均值和标准差：

```python
import pandas as pd
import numpy as np

def calc_eruption_threshold(
    volume_min: pd.DataFrame,
    lookback: int = 20,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    计算放量阈值

    Parameters
    ----------
    volume_min : pd.DataFrame
        分钟级成交量，index=(trade_date, minute), columns=ts_code
    lookback : int
        滚动窗口天数，默认20

    Returns
    -------
    threshold_upper : 放量阈值 (mean + 1*std)
    threshold_lower : 缩量阈值 (mean - 1*std)
    """
    rolling_mean = volume_min.rolling(lookback).mean()
    rolling_std = volume_min.rolling(lookback).std()
    threshold_upper = rolling_mean + rolling_std
    threshold_lower = rolling_mean - rolling_std
    return threshold_upper, threshold_lower
```

### Step 2: 识别爆发点（eruption）

```python
def identify_eruption(
    volume: pd.Series,
    threshold: pd.Series,
) -> pd.Series:
    """当某分钟成交量 > 均值 + 1*std 时，标记为 eruption"""
    return volume > threshold
```

### Step 3: 分类判断——峰、岭、谷

```python
def classify_volume_pattern(eruption: pd.Series) -> pd.Series:
    """
    对每分钟的 eruption 标记进行分类

    规则:
    - eruption=True 但前后都是 False → peak（量峰）
    - 连续 >=3 分钟 eruption=True → ridge（量岭）
    - eruption=False 的缩量区间 → valley（量谷）
    """
    label = pd.Series('normal', index=eruption.index)

    # 量岭：连续3分钟及以上放量
    consecutive = eruption & eruption.shift(1) & eruption.shift(2)
    # 标记连续放量段中的所有分钟（包含起始2分钟）
    ridge_mask = consecutive | consecutive.shift(-1) | consecutive.shift(-2)
    label[ridge_mask & eruption] = 'ridge'

    # 量峰：孤立放量点（前后都不是放量）
    isolated = eruption & ~eruption.shift(1, fill_value=False) & ~eruption.shift(-1, fill_value=False)
    label[isolated] = 'peak'

    # 量谷：缩量区间（需另外用 threshold_lower 判断）
    # valley_mask = volume < threshold_lower
    # label[valley_mask] = 'valley'

    return label
```

### Step 4: 聚合为日频因子

将分钟级标记聚合到每日每股票的因子值：

```python
def aggregate_daily_factors(
    labels: pd.DataFrame,
    volume: pd.DataFrame,
    vwap: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    聚合分钟级标记为日频因子

    核心因子:
    - ridge_minutes: 量岭总分钟数（最强因子，IC_5d ≈ 0.31）
    - peak_minutes: 量峰总分钟数
    - valley_minutes: 量谷总分钟数

    可选衍生因子:
    - valley_vwap_percentile: 量谷区间 VWAP 百分位
    - valley_relative_vwap: 量谷相对 VWAP
    - peak_ridge_turnover_ratio: 峰岭换手率占比
    - valley_ridge_vwap_ratio: 谷岭 VWAP 比值
    """
    factors = {}
    factors['ridge_minutes'] = (labels == 'ridge').sum()
    factors['peak_minutes'] = (labels == 'peak').sum()
    factors['valley_minutes'] = (labels == 'valley').sum()

    # 衍生因子（需要 vwap 数据）
    if vwap is not None:
        ridge_vol = volume[labels == 'ridge']
        valley_vol = volume[labels == 'valley']
        factors['peak_ridge_turnover_ratio'] = (
            volume[labels.isin(['peak', 'ridge'])].sum()
            / volume.sum()
        )
        # ... 更多衍生因子

    return pd.DataFrame(factors)
```

## 因子表现基准（2024年A股实证）

| 因子 | IC_1d | IC_5d | 方向 | 5日多空收益 | 夏普比率 |
|------|-------|-------|------|-----------|---------|
| **ridge_minutes** | **0.155** | **0.308** | 正向 | **+10.72%** | **2.15** |
| peak_minutes | 0.031 | 0.141 | 正向 | +1.59% | 0.87 |
| valley_vwap_percentile | 0.259 | 0.050 | 正向 | +2.58% | 1.12 |
| valley_relative_vwap | -0.465 | -0.265 | 负向 | — | — |
| peak_ridge_turnover_ratio | -0.181 | -0.268 | 负向 | — | — |
| valley_ridge_vwap_ratio | -0.464 | -0.240 | 负向 | — | — |

**核心结论**: `ridge_minutes`（量岭分钟数）是最优因子，IC_5d = 0.308，5日多空收益 10.72%，夏普 2.15。

## 因子检验标准流程

### IC 分析

```python
from scipy.stats import spearmanr

def calc_ic_series(
    factor: pd.DataFrame,
    forward_ret: pd.DataFrame,
    method: str = 'spearman',
) -> pd.Series:
    """
    计算截面 IC 时间序列

    Parameters
    ----------
    factor : 因子值，index=trade_date, columns=ts_code
    forward_ret : 未来N日收益率，同结构
    method : 'spearman'(RankIC) 或 'pearson'

    Returns
    -------
    ic_series : 每日 IC 值
    """
    ic_list = []
    for date in factor.index:
        f = factor.loc[date].dropna()
        r = forward_ret.loc[date].dropna()
        common = f.index.intersection(r.index)
        if len(common) < 30:
            continue
        if method == 'spearman':
            ic, _ = spearmanr(f[common], r[common])
        else:
            ic = f[common].corr(r[common])
        ic_list.append({'date': date, 'ic': ic})
    return pd.DataFrame(ic_list).set_index('date')['ic']
```

### IC 评价指标

| 指标 | 计算 | 优秀标准 |
|------|------|---------|
| IC_mean | IC 序列均值 | > 0.03 |
| IC_std | IC 序列标准差 | 越小越好 |
| ICIR | IC_mean / IC_std | > 0.5 |
| 正IC比例 | 正IC月份 / 总月份 | > 60% |

### 多空组合

```python
def build_long_short_portfolio(
    factor: pd.DataFrame,
    forward_ret: pd.DataFrame,
    n_groups: int = 5,
) -> pd.DataFrame:
    """
    构建多空组合

    每期按因子值分 n_groups 组，多头 = top组，空头 = bottom组
    多空收益 = 多头 - 空头
    """
    ls_returns = []
    for date in factor.index:
        f = factor.loc[date].dropna()
        r = forward_ret.loc[date].reindex(f.index).dropna()
        common = f.index.intersection(r.index)
        if len(common) < n_groups * 10:
            continue
        ranked = f[common].rank(pct=True)
        long_mask = ranked >= (1 - 1 / n_groups)
        short_mask = ranked <= (1 / n_groups)
        long_ret = r[common][long_mask].mean()
        short_ret = r[common][short_mask].mean()
        ls_returns.append({
            'date': date,
            'long': long_ret,
            'short': short_ret,
            'long_short': long_ret - short_ret,
        })
    return pd.DataFrame(ls_returns).set_index('date')
```

## 数据需求

| 数据 | 频率 | 说明 |
|------|------|------|
| 分钟成交量 | 1min | Tushare `stk_mins` 或券商数据 |
| 分钟 VWAP | 1min | 可选，用于衍生因子 |
| 日频收益率 | 日频 | 用于 IC / 多空检验 |

**注意**: 分钟级数据量大（全A约 5000 只 × 240 分钟 × 242 天/年），需考虑存储和计算资源。

## 使用建议

1. **优先使用 `ridge_minutes`**: 预测力最强、稳定性最好
2. **结合市场环境**: 趋势行情中效果更佳
3. **与其他因子融合**: 可与动量、价值因子正交化后合成
4. **控制换手率**: 5 日 IC 较高，建议周频或双周频调仓
5. **注意极端行情**: 在极端市场环境下因子可能失效

## 局限性

- 需要分钟级数据，计算资源要求较高
- 极端市场（暴涨暴跌）可能失效
- 历史回测结果不代表未来收益
- 建议与其他因子组合使用，避免单因子风险
