"""test_weight_optimizer.py — 权重优化器单元测试"""
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from vortex.core.weight_optimizer import (
    EqualWeightOptimizer,
    FixedWeightOptimizer,
    ICIRWeightOptimizer,
    ICWeightOptimizer,
)


class TestFixedWeightOptimizer:
    def test_basic(self):
        weights = {"a": 0.3, "b": 0.5, "c": 0.2}
        opt = FixedWeightOptimizer(weights)
        result = opt.optimize(["a", "b", "c"], "20250630")
        assert abs(result["a"] - 0.3) < 1e-6
        assert abs(result["b"] - 0.5) < 1e-6
        assert abs(result["c"] - 0.2) < 1e-6

    def test_subset(self):
        weights = {"a": 0.3, "b": 0.5, "c": 0.2}
        opt = FixedWeightOptimizer(weights)
        result = opt.optimize(["a", "c"], "20250630")
        # 只取 a 和 c，归一化
        total = 0.3 + 0.2
        assert abs(result["a"] - 0.3 / total) < 1e-6

    def test_name(self):
        opt = FixedWeightOptimizer({"a": 1.0})
        assert opt.name == "fixed"


class TestEqualWeightOptimizer:
    def test_equal(self):
        opt = EqualWeightOptimizer()
        result = opt.optimize(["a", "b", "c", "d"], "20250630")
        assert len(result) == 4
        for v in result.values():
            assert abs(v - 0.25) < 1e-6

    def test_name(self):
        opt = EqualWeightOptimizer()
        assert opt.name == "equal"


class TestICWeightOptimizer:
    def test_fallback_to_equal(self):
        """无历史 IC 数据时 fallback 到等权"""
        mock_ds = MagicMock()
        mock_ds.data_dir = Path("/tmp/nonexistent_data")
        mock_fh = MagicMock()
        opt = ICWeightOptimizer(ds=mock_ds, fh=mock_fh, lookback_months=12)
        # Mock _get_rebalance_dates to return empty list (fallback)
        with patch.object(opt, "_get_rebalance_dates", return_value=[]):
            result = opt.optimize(["a", "b", "c"], "20250630")
        for v in result.values():
            assert abs(v - 1.0 / 3) < 1e-6

    def test_name(self):
        mock_ds = MagicMock()
        mock_fh = MagicMock()
        opt = ICWeightOptimizer(ds=mock_ds, fh=mock_fh)
        assert opt.name == "ic_weighted"


class TestICIRWeightOptimizer:
    def test_fallback(self):
        mock_ds = MagicMock()
        mock_ds.data_dir = Path("/tmp/nonexistent_data")
        mock_fh = MagicMock()
        opt = ICIRWeightOptimizer(ds=mock_ds, fh=mock_fh)
        with patch.object(opt, "_get_rebalance_dates", return_value=[]):
            result = opt.optimize(["a", "b"], "20250630")
        for v in result.values():
            assert abs(v - 0.5) < 1e-6

    def test_name(self):
        mock_ds = MagicMock()
        mock_fh = MagicMock()
        opt = ICIRWeightOptimizer(ds=mock_ds, fh=mock_fh)
        assert opt.name == "ic_ir_weighted"
