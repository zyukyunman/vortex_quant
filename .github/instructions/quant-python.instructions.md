---
description: "Python量化代码规范。Use when: 编写量化策略代码、因子计算、回测系统、数据处理脚本。规范包括命名、注释、结构、错误处理等。"
applyTo: ["**/*.py", "**/strategies/**", "**/factors/**", "**/backtest/**"]
---

# Python 量化代码规范

## 命名规范

### 文件命名
```python
# 好的命名
momentum_factor.py      # 因子文件
dual_ma_strategy.py     # 策略文件
data_utils.py           # 工具文件

# 避免
factor1.py              # 无意义数字
test.py                 # 测试时临时文件要及时删除
```

### 变量命名
```python
# 数据相关
df_daily = ...          # DataFrame 加 df_ 前缀
price_close = ...       # 价格数据明确类型
returns_daily = ...     # 收益率数据

# 因子相关
factor_momentum = ...   # 因子加 factor_ 前缀
ic_series = ...         # IC序列
weights = ...           # 权重向量

# 回测相关
nav = ...               # 净值 (Net Asset Value)
pnl = ...               # 盈亏 (Profit and Loss)
position = ...          # 持仓
turnover = ...          # 换手率
```

### 函数命名
```python
# 动词开头，描述行为
def calc_factor():      # 计算因子
def get_price_data():   # 获取数据
def run_backtest():     # 执行回测
def plot_nav():         # 绑制净值图
```

## 函数规范

### 必须有 Docstring
```python
def calc_momentum_factor(prices: pd.DataFrame, lookback: int = 20) -> pd.DataFrame:
    """
    计算动量因子
    
    动量因子 = 过去 lookback 天的累计收益率
    经济学逻辑：趋势延续效应，强者恒强
    
    Parameters:
    -----------
    prices : pd.DataFrame
        收盘价数据，index为日期，columns为股票代码
    lookback : int
        回看天数，默认20天
        
    Returns:
    --------
    pd.DataFrame
        因子值，与输入同结构
        
    Examples:
    ---------
    >>> factor = calc_momentum_factor(close_prices, lookback=20)
    """
    returns = prices.pct_change(lookback)
    return returns
```

### 参数类型标注
```python
from typing import Optional, Union, List, Dict
import pandas as pd
import numpy as np

def backtest_strategy(
    signals: pd.DataFrame,
    prices: pd.DataFrame,
    initial_capital: float = 1_000_000,
    commission: float = 0.001,
    slippage: float = 0.001,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Union[pd.Series, float]]:
    """回测函数示例"""
    pass
```

## 代码结构

### 单文件结构
```python
"""
momentum_factor.py
动量因子计算模块

Author: xxx
Date: 2024-01-01
"""

# ============ 导入 ============
# 标准库
import logging
from datetime import datetime
from typing import Optional

# 第三方库
import numpy as np
import pandas as pd

# 本地模块
from utils.data_utils import get_price_data
from vortex.config.settings import TUSHARE_TOKEN

# ============ 常量 ============
DEFAULT_LOOKBACK = 20
MIN_STOCKS = 30

# ============ 日志 ============
logger = logging.getLogger(__name__)

# ============ 主要函数 ============
def calc_factor():
    """主要的因子计算函数"""
    pass

# ============ 辅助函数 ============
def _preprocess_data():
    """内部辅助函数用下划线开头"""
    pass

# ============ 测试代码 ============
if __name__ == '__main__':
    # 测试代码放这里
    pass
```

## 数据处理规范

### DataFrame 操作
```python
# 好的做法
df = df.copy()                          # 避免修改原数据
df = df.dropna(subset=['close'])        # 明确指定列
df = df[df['volume'] > 0]               # 过滤无效数据
df = df.sort_index()                    # 确保时间排序

# 避免
df.dropna(inplace=True)                 # inplace 操作难以追踪
df['new_col'] = ...                     # 链式赋值可能警告
```

### 时间处理
```python
# 统一使用字符串格式 'YYYYMMDD'
start_date = '20200101'
end_date = '20231231'

# 转换
date_str = pd.to_datetime(date_str).strftime('%Y%m%d')

# 比较
if trade_date >= start_date:
    pass
```

## 错误处理

### 数据校验
```python
def calc_factor(prices: pd.DataFrame) -> pd.DataFrame:
    # 输入校验
    if prices.empty:
        logger.warning("输入数据为空")
        return pd.DataFrame()
    
    if prices.isnull().all().all():
        raise ValueError("所有数据都是空值")
    
    # 数量校验
    n_stocks = prices.shape[1]
    if n_stocks < MIN_STOCKS:
        logger.warning(f"股票数量不足: {n_stocks} < {MIN_STOCKS}")
```

### 异常捕获
```python
def get_data_from_tushare(ts_code: str) -> pd.DataFrame:
    try:
        df = pro.daily(ts_code=ts_code)
        return df
    except Exception as e:
        logger.error(f"获取数据失败 {ts_code}: {e}")
        return pd.DataFrame()
```

## 性能优化

### 向量化优先
```python
# 好的做法 - 向量化
returns = prices.pct_change()
factor = returns.rolling(20).mean()

# 避免 - 循环
for i in range(len(prices)):
    for stock in stocks:
        # ... 逐个计算
```

### 避免重复计算
```python
class FactorCalculator:
    def __init__(self):
        self._cache = {}
    
    def calc_factor(self, name: str, params: dict):
        cache_key = f"{name}_{hash(frozenset(params.items()))}"
        if cache_key not in self._cache:
            self._cache[cache_key] = self._do_calc(name, params)
        return self._cache[cache_key]
```

## 注释规范

### 关键逻辑必须注释
```python
# 剔除涨跌停股票（无法成交）
# 涨停判断：涨幅 >= 9.9%（考虑精度误差）
mask_limit_up = (df['pct_chg'] >= 9.9)
mask_limit_down = (df['pct_chg'] <= -9.9)
df = df[~(mask_limit_up | mask_limit_down)]

# T+1 处理：信号产生后，次日开盘执行
# 因此用 shift(1) 将持仓信号后移一天
position = signal.shift(1)
```

### 公式注释
```python
# 夏普比率 = (年化收益 - 无风险收益) / 年化波动率
# SR = (R_p - R_f) / σ_p * sqrt(252)
sharpe = (ann_return - risk_free) / ann_vol
```

## 日志规范

```python
import logging

# 配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 使用
logger.info(f"开始回测: {start_date} ~ {end_date}")
logger.debug(f"当前持仓: {len(position)} 只股票")
logger.warning(f"数据缺失: {missing_dates}")
logger.error(f"计算失败: {e}")
```

## 配置管理

```python
# config/settings.py
from dataclasses import dataclass

@dataclass
class BacktestConfig:
    initial_capital: float = 1_000_000
    commission: float = 0.001      # 佣金率
    slippage: float = 0.001        # 滑点
    stamp_tax: float = 0.0005      # 印花税（卖出）
    
# 使用
config = BacktestConfig()
config.commission = 0.0003  # 可调整
```

## 测试规范

```python
# tests/test_factor.py
import pytest
import pandas as pd
import numpy as np
from factors.momentum import calc_momentum_factor

class TestMomentumFactor:
    
    @pytest.fixture
    def sample_prices(self):
        """生成测试数据"""
        dates = pd.date_range('2020-01-01', periods=100)
        stocks = ['000001', '000002', '000003']
        data = np.random.randn(100, 3) * 0.02 + 1
        return pd.DataFrame(data, index=dates, columns=stocks).cumprod()
    
    def test_basic_calculation(self, sample_prices):
        """测试基本计算"""
        factor = calc_momentum_factor(sample_prices, lookback=20)
        assert factor.shape == sample_prices.shape
        assert not factor.iloc[20:].isnull().all().all()
    
    def test_empty_input(self):
        """测试空输入"""
        factor = calc_momentum_factor(pd.DataFrame())
        assert factor.empty
```
