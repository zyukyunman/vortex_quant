#!/usr/bin/env python3
"""验证重构后的模块导入和基本功能"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# --- 1. filters.py ---
from vortex.strategy.filters import (
    FilterPipeline, FilterContext, StockFilter,
    NonSTFilter, MinListedDaysFilter, IndustryExcludeFilter,
    FactorThresholdFilter, FactorRangeFilter, QuantileCutoffFilter,
)
print("[OK] filters.py imports")

# --- 2. weight_optimizer.py ---
from vortex.core.weight_optimizer import (
    WeightOptimizer, FixedWeightOptimizer, EqualWeightOptimizer,
    ICWeightOptimizer, ICIRWeightOptimizer,
)
print("[OK] weight_optimizer.py imports")

# --- 3. dividend.py ---
from vortex.strategy.dividend import (
    DividendQualityFCFStrategy, build_filter_pipeline,
    EXCLUDED_INDUSTRIES, DEFAULT_WEIGHTS, SCORING_FACTORS,
)
print("[OK] dividend.py imports")

# --- 4. FixedWeightOptimizer ---
fw = FixedWeightOptimizer({"a": 0.3, "b": 0.2, "c": 0.5})
w = fw.optimize(["a", "b", "c"], "20260328")
assert abs(sum(w.values()) - 1.0) < 1e-6
print(f"[OK] FixedWeightOptimizer: {w}")

# --- 5. EqualWeightOptimizer ---
ew = EqualWeightOptimizer()
w = ew.optimize(["x", "y", "z"], "20260328")
assert abs(w["x"] - 1/3) < 1e-6
print(f"[OK] EqualWeightOptimizer: {w}")

# --- 6. DEFAULT_WEIGHTS normalization ---
fw2 = FixedWeightOptimizer(DEFAULT_WEIGHTS)
w2 = fw2.optimize(SCORING_FACTORS, "20260328")
assert abs(sum(w2.values()) - 1.0) < 1e-6
print(f"[OK] DEFAULT_WEIGHTS: { {k: round(v,4) for k,v in w2.items()} }")

# --- 7. FilterPipeline ---
from vortex.config.settings import Settings
cfg = Settings()
pipeline = build_filter_pipeline(cfg)
print(f"[OK] FilterPipeline: {len(pipeline.filters)} filters")
for f in pipeline.filters:
    print(f"     - {f.name}")

# --- 8. FactorThresholdFilter unit test ---
import pandas as pd
ft = FactorThresholdFilter("test_factor", op="gte", threshold=5)
pool = {"A", "B", "C", "D"}
factor_data = {"test_factor": pd.Series({"A": 10, "B": 3, "C": 5, "D": 8})}
ctx = FilterContext(date="20260328", df_basic=pd.DataFrame(), settings=cfg)
result = ft.apply(pool, factor_data, ctx)
assert result == {"A", "C", "D"}, f"expected A,C,D but got {result}"
print(f"[OK] FactorThresholdFilter: {pool} → {result}")

# --- 9. QuantileCutoffFilter unit test ---
qf = QuantileCutoffFilter("score", max_quantile=0.75)
pool2 = {"A", "B", "C", "D"}
factor_data2 = {"score": pd.Series({"A": 1, "B": 2, "C": 3, "D": 100})}
result2 = qf.apply(pool2, factor_data2, ctx)
assert "D" not in result2, f"D should be excluded, got {result2}"
print(f"[OK] QuantileCutoffFilter: {pool2} → {result2}")

print("\n=== ALL TESTS PASSED ===")
