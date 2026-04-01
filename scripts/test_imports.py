#!/usr/bin/env python3
"""验证所有模块导入正常"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

print("测试模块导入...")

from vortex.config.settings import Settings, setup_logging
print("  ✓ config.settings")

from vortex.models import Signal, SelectionResult
print("  ✓ app.models")

from vortex.factor.base import BaseFactor, zscore, rank_pct
print("  ✓ app.factor.base")

from vortex.factor.value import (
    DividendYield, DividendYield3Y, PayoutRatio3Y,
    EP, DP, ConsecutiveDividendYears,
)
print("  ✓ app.factor.value (6 factors)")

from vortex.factor.quality import (
    RoeTTM, DeltaROE, ROEStability, OPCFD,
    DebtToAssets, NetProfitYoY,
)
print("  ✓ app.factor.quality (6 factors)")

from vortex.factor.cashflow import FCFYield, OCFtoOP, FCF_TTM, OCF3YPositive
print("  ✓ app.factor.cashflow (4 factors)")

from vortex.core.data.datastore import DataStore
print("  ✓ app.core.datastore")

from vortex.core.factorhub import FactorHub
print("  ✓ app.core.factorhub")

from vortex.core.signalbus import SignalBus
print("  ✓ app.core.signalbus")

from vortex.strategy.base import BaseStrategy
print("  ✓ app.strategy.base")

from vortex.strategy.dividend import DividendQualityFCFStrategy
print("  ✓ app.strategy.dividend")

print("\n所有模块导入成功! ✅")

# 测试 Settings
cfg = Settings()
print(f"\n配置检查:")
print(f"  数据目录: {cfg.data_dir}")
print(f"  选股数量: {cfg.top_n}")
print(f"  买入门槛: {cfg.dividend_buy_threshold:.0%}")
print(f"  卖出门槛: {cfg.dividend_sell_threshold:.0%}")

# 测试 zscore
import pandas as pd
s = pd.Series([1, 2, 3, 4, 5, 100], index=["a", "b", "c", "d", "e", "f"])
z = zscore(s)
print(f"\nzscore 测试: {z.round(2).tolist()}")
print("  (极端值 100 被 MAD 缩尾)")

# 测试 FactorHub 注册
print("\nFactorHub 因子注册测试:")
# 不初始化 DataStore (需要 token), 只测试注册
class FakeDS:
    cfg = cfg
fh = FactorHub(FakeDS())
fh.register_all_defaults()
for info in fh.list_factors():
    print(f"  {info['name']:25s} [{info['category']:10s}] dir={info['direction']:+d}  {info['description']}")

print(f"\n共注册 {len(fh.factors)} 个因子 ✅")
