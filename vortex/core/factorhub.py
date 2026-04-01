"""
factorhub.py
L2 因子层 — 因子注册、计算、缓存管理

职责:
  - 注册所有可用因子
  - 统一计算入口
  - 缓存到 Parquet
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from vortex.core.data.datastore import DataStore
from vortex.factor.base import BaseFactor

logger = logging.getLogger(__name__)


class FactorHub:
    """因子计算与管理中心"""

    def __init__(self, ds: DataStore):
        self.ds = ds
        self.factors: Dict[str, BaseFactor] = {}
        self._cache: Dict[str, pd.Series] = {}  # (factor_name, date) → Series

    def register(self, factor: BaseFactor):
        """注册因子"""
        self.factors[factor.name] = factor
        logger.debug("注册因子: %s", factor)

    def register_all_defaults(self):
        """注册所有默认因子（策略所需的全部因子）"""
        from vortex.factor.value import (
            DividendYield, DividendYield3Y, PayoutRatio3Y,
            EP, DP, ConsecutiveDividendYears, RoeOverPb,
        )
        from vortex.factor.quality import (
            RoeTTM, DeltaROE, ROEStability, OPCFD,
            DebtToAssets, NetProfitYoY,
        )
        from vortex.factor.cashflow import (
            FCFYield, OCFtoOP, FCF_TTM, OCF3YPositive,
        )

        for cls in [
            DividendYield, DividendYield3Y, PayoutRatio3Y,
            EP, DP, ConsecutiveDividendYears, RoeOverPb,
            RoeTTM, DeltaROE, ROEStability, OPCFD,
            DebtToAssets, NetProfitYoY,
            FCFYield, OCFtoOP, FCF_TTM, OCF3YPositive,
        ]:
            self.register(cls())

        logger.info("已注册 %d 个因子", len(self.factors))

    def compute(self, factor_name: str, date: str) -> pd.Series:
        """计算单因子截面值"""
        cache_key = f"{factor_name}_{date}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        if factor_name not in self.factors:
            raise KeyError(f"因子 '{factor_name}' 未注册")

        factor = self.factors[factor_name]
        logger.info("计算因子: %s @ %s", factor_name, date)
        result = factor.compute(self.ds, date)

        self._cache[cache_key] = result
        return result

    def compute_all(self, date: str) -> Dict[str, pd.Series]:
        """计算所有注册因子"""
        results = {}
        for name in self.factors:
            try:
                results[name] = self.compute(name, date)
                logger.info(
                    "  ✓ %s: %d 只股票", name, len(results[name])
                )
            except Exception as e:
                logger.error("  ✗ %s 计算失败: %s", name, e)
                results[name] = pd.Series(dtype=float)
        return results

    def get_factor_matrix(self, date: str, factor_names: Optional[List[str]] = None) -> pd.DataFrame:
        """
        获取因子矩阵 (index=ts_code, columns=factor_name)

        如果 factor_names 不指定则用全部已注册因子。
        """
        if factor_names is None:
            factor_names = list(self.factors.keys())

        factors = {}
        for name in factor_names:
            factors[name] = self.compute(name, date)

        df = pd.DataFrame(factors)
        logger.info("因子矩阵: %d 只股票 × %d 个因子", len(df), len(df.columns))
        return df

    def list_factors(self) -> List[Dict]:
        """列出所有已注册因子的信息"""
        info = []
        for name, f in self.factors.items():
            info.append({
                "name": f.name,
                "category": f.category,
                "direction": f.direction,
                "description": f.description,
            })
        return info
