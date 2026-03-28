---
description: "因子回测模板。Use when: 测试单因子有效性、生成因子回测报告、计算IC/收益率/换手率。输入因子定义，自动输出完整回测分析。"
name: "因子回测分析"
argument-hint: "描述因子计算逻辑，如：20日动量因子 = 过去20日收益率"
agent: "agent"
tools: [read, search, edit, execute]
---

# 因子回测分析任务

根据用户提供的因子定义，完成以下回测分析流程：

## 输入要求

请用户提供：
1. **因子名称**: 如"动量因子"、"市值因子"
2. **因子计算逻辑**: 具体的计算公式或描述
3. **回测参数（可选）**:
   - 回测区间（默认：最近3年）
   - 股票池（默认：全A股剔除ST）
   - 调仓频率（默认：月度）
   - 分组数量（默认：5组）

## 输出内容

### 1. 因子构建代码
```python
# 完整可运行的因子计算函数
# 使用 Tushare 数据源
# 包含数据获取、清洗、因子计算
```

### 2. 因子检验指标

| 指标 | 说明 | 判断标准 |
|------|------|----------|
| IC均值 | 因子预测能力 | |IC| > 0.03 有效 |
| IC_IR | IC稳定性 | > 0.5 较好 |
| IC胜率 | IC为正的比例 | > 50% |
| 多头年化收益 | Top组收益 | 对比基准 |
| 多空年化收益 | Top组-Bottom组 | 显著为正 |
| 最大回撤 | 多空组合回撤 | < 30% |
| 月度换手率 | 持仓变动比例 | < 50% 可接受 |

### 3. 可视化图表

生成以下图表：
- IC时序图（带12个月移动平均）
- 分组累计收益曲线
- 多空净值曲线
- 因子收益分布直方图

### 4. 结论与建议

- 因子是否有效的判断
- 适用的市场环境
- 与其他因子的相关性提示
- 改进方向建议

## 代码模板

```python
import numpy as np
import pandas as pd
import tushare as ts
from scipy import stats
import matplotlib.pyplot as plt

# ============ 配置 ============
TUSHARE_TOKEN = 'your_token'
START_DATE = '20210101'
END_DATE = '20240101'
N_GROUPS = 5
REBALANCE_FREQ = 'M'  # M=月度, W=周度

# ============ 因子计算 ============
def calc_factor(data: pd.DataFrame) -> pd.Series:
    """
    因子计算函数
    
    Parameters:
    -----------
    data : DataFrame
        包含计算因子所需的原始数据
        
    Returns:
    --------
    factor : Series
        因子值，index为股票代码
    """
    # TODO: 根据用户输入实现具体计算逻辑
    pass

# ============ 因子检验 ============
def calc_ic(factor: pd.DataFrame, returns: pd.DataFrame) -> pd.Series:
    """计算IC序列"""
    ic_list = []
    for date in factor.index[:-1]:
        f = factor.loc[date].dropna()
        # 获取下期收益
        next_dates = returns.index[returns.index > date]
        if len(next_dates) == 0:
            continue
        next_date = next_dates[0]
        r = returns.loc[next_date].reindex(f.index).dropna()
        common = f.index.intersection(r.index)
        if len(common) > 30:
            ic = stats.spearmanr(f[common], r[common])[0]
            ic_list.append({'date': date, 'ic': ic})
    return pd.DataFrame(ic_list).set_index('date')['ic']

def group_backtest(factor: pd.DataFrame, returns: pd.DataFrame, n_groups: int = 5):
    """分组回测"""
    group_returns = {i: [] for i in range(1, n_groups + 1)}
    
    for date in factor.index[:-1]:
        f = factor.loc[date].dropna()
        next_dates = returns.index[returns.index > date]
        if len(next_dates) == 0:
            continue
        next_date = next_dates[0]
        r = returns.loc[next_date].reindex(f.index).dropna()
        common = f.index.intersection(r.index)
        
        if len(common) > n_groups * 10:
            # 分组
            f_sorted = f[common].sort_values()
            group_size = len(f_sorted) // n_groups
            for i in range(1, n_groups + 1):
                start_idx = (i - 1) * group_size
                end_idx = i * group_size if i < n_groups else len(f_sorted)
                stocks = f_sorted.index[start_idx:end_idx]
                group_ret = r[stocks].mean()
                group_returns[i].append({'date': next_date, 'return': group_ret})
    
    # 转换为DataFrame
    result = {}
    for i in range(1, n_groups + 1):
        df = pd.DataFrame(group_returns[i]).set_index('date')['return']
        result[f'G{i}'] = df
    return pd.DataFrame(result)

# ============ 绩效计算 ============
def calc_metrics(ic_series: pd.Series, group_returns: pd.DataFrame):
    """计算因子检验指标"""
    metrics = {
        'IC均值': ic_series.mean(),
        'IC标准差': ic_series.std(),
        'IC_IR': ic_series.mean() / ic_series.std(),
        'IC胜率': (ic_series > 0).mean(),
        'IC偏度': ic_series.skew(),
    }
    
    # 多头组（最高分组）
    top_group = group_returns.iloc[:, -1]
    metrics['多头年化收益'] = (1 + top_group).prod() ** (252 / len(top_group)) - 1
    
    # 多空组合
    long_short = group_returns.iloc[:, -1] - group_returns.iloc[:, 0]
    metrics['多空年化收益'] = (1 + long_short).prod() ** (252 / len(long_short)) - 1
    
    # 最大回撤
    cumret = (1 + long_short).cumprod()
    drawdown = cumret / cumret.cummax() - 1
    metrics['最大回撤'] = drawdown.min()
    
    return metrics

# ============ 可视化 ============
def plot_results(ic_series: pd.Series, group_returns: pd.DataFrame):
    """绑制回测结果图表"""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # 1. IC时序图
    ax1 = axes[0, 0]
    ic_series.plot(ax=ax1, alpha=0.5, label='IC')
    ic_series.rolling(12).mean().plot(ax=ax1, label='IC MA12', linewidth=2)
    ax1.axhline(y=0, color='black', linestyle='--', alpha=0.3)
    ax1.set_title('IC时序图')
    ax1.legend()
    
    # 2. 分组累计收益
    ax2 = axes[0, 1]
    (1 + group_returns).cumprod().plot(ax=ax2)
    ax2.set_title('分组累计收益')
    ax2.legend(title='分组')
    
    # 3. 多空净值曲线
    ax3 = axes[1, 0]
    long_short = group_returns.iloc[:, -1] - group_returns.iloc[:, 0]
    (1 + long_short).cumprod().plot(ax=ax3)
    ax3.set_title('多空净值曲线')
    
    # 4. IC分布
    ax4 = axes[1, 1]
    ic_series.hist(ax=ax4, bins=30, edgecolor='black')
    ax4.axvline(x=ic_series.mean(), color='red', linestyle='--', label=f'均值={ic_series.mean():.3f}')
    ax4.set_title('IC分布')
    ax4.legend()
    
    plt.tight_layout()
    plt.savefig('factor_backtest_report.png', dpi=150)
    plt.show()

# ============ 主程序 ============
if __name__ == '__main__':
    # 1. 获取数据
    # 2. 计算因子
    # 3. 计算IC
    # 4. 分组回测
    # 5. 计算指标
    # 6. 可视化
    pass
```

## 执行步骤

1. **确认因子定义**: 解析用户输入，明确计算逻辑
2. **填充代码模板**: 实现 `calc_factor` 函数
3. **运行回测**: 如果用户有数据环境，执行代码
4. **分析结果**: 解读各项指标，给出专业判断
5. **提供建议**: 因子改进方向或组合使用建议
