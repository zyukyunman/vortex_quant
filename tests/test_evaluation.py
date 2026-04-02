"""test_evaluation.py — 因子评测模块单元测试"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from vortex.evaluation.spec import EvalSpec, EvalResult, FactorRole
from vortex.evaluation.scoring import ScoringEvaluator
from vortex.evaluation.filter import FilterEvaluator
from vortex.evaluation.risk import RiskEvaluator
from vortex.evaluation.pipeline import EvalPipeline
from vortex.evaluation.weight_tuner import WeightTuner


# ================================================================
#  Fixtures
# ================================================================

@pytest.fixture
def mock_analyzer():
    """模拟 FactorAnalyzer"""
    analyzer = MagicMock()
    analyzer.ds = MagicMock()
    analyzer.fh = MagicMock()

    # ic_report_multi_horizon 返回值
    def mock_ic_multi(factor_names, dates, forward_days_list):
        rows = []
        for name in factor_names:
            row = {"factor": name}
            for h in forward_days_list:
                row[f"mean_ic_{h}d"] = 0.05
                row[f"ic_std_{h}d"] = 0.10
                row[f"icir_{h}d"] = 0.50
                row[f"positive_rate_{h}d"] = 0.60
                row[f"n_periods_{h}d"] = 12
            row["best_horizon"] = forward_days_list[-1]
            rows.append(row)
        return pd.DataFrame(rows)

    analyzer.ic_report_multi_horizon = MagicMock(side_effect=mock_ic_multi)

    # long_short_report 返回值
    def mock_ls_report(factor_names, dates, forward_days, n_groups):
        rows = []
        for name in factor_names:
            rows.append({
                "factor": name,
                f"long_short_{forward_days}d": 0.03,
                "long_mean": 0.02,
                "short_mean": -0.01,
                "sharpe": 0.8,
                "n_periods": len(dates),
            })
        return pd.DataFrame(rows)

    analyzer.long_short_report = MagicMock(side_effect=mock_ls_report)

    # calc_ic 返回值
    def mock_calc_ic(name, dates, forward_days):
        return pd.Series(
            [0.05, 0.04, 0.06, 0.03],
            index=["20250131", "20250228", "20250331", "20250430"],
        )

    analyzer.calc_ic = MagicMock(side_effect=mock_calc_ic)

    # ic_report 返回值 (for WeightTuner)
    def mock_ic_report(factor_names, dates, forward_days):
        rows = []
        for name in factor_names:
            rows.append({
                "factor": name,
                "mean_ic": 0.05,
                "ic_std": 0.10,
                "icir": 0.50,
                "ic_positive_rate": 0.65,
                "n_periods": 12,
            })
        return pd.DataFrame(rows)

    analyzer.ic_report = MagicMock(side_effect=mock_ic_report)

    # fh.compute 返回值 (for FilterEvaluator)
    index = [f"00{i:04d}.SZ" for i in range(100)]
    basic_df = pd.DataFrame({"ts_code": index, "name": [f"stock_{i}" for i in range(100)]})
    analyzer.ds.get_stock_basic = MagicMock(return_value=basic_df)

    def mock_compute(name, date):
        vals = np.random.RandomState(42).rand(100) * 10
        return pd.Series(vals, index=index)

    analyzer.fh.compute = MagicMock(side_effect=mock_compute)

    return analyzer


@pytest.fixture
def sample_dates():
    return ["20250131", "20250228", "20250331", "20250430"]


# ================================================================
#  EvalSpec / FactorRole
# ================================================================

class TestSpec:
    def test_factor_role_values(self):
        assert FactorRole.SCORING.value == "scoring"
        assert FactorRole.FILTER.value == "filter"
        assert FactorRole.RISK.value == "risk"
        assert FactorRole.TIMING.value == "timing"

    def test_eval_spec_defaults(self):
        spec = EvalSpec("dividend_yield", FactorRole.SCORING)
        assert spec.factor_name == "dividend_yield"
        assert spec.role == FactorRole.SCORING
        assert spec.horizons == (1, 5, 20)
        assert spec.factor_family == ""
        assert spec.threshold is None
        assert spec.data_source == ""

    def test_eval_spec_custom(self):
        spec = EvalSpec(
            "debt_to_assets", FactorRole.FILTER,
            threshold=70, threshold_op="<=",
            data_source="fina_indicator.debt_to_assets",
        )
        assert spec.threshold == 70
        assert spec.threshold_op == "<="
        assert spec.data_source == "fina_indicator.debt_to_assets"

    def test_eval_result_defaults(self):
        result = EvalResult("test_factor", FactorRole.SCORING, passed=True)
        assert result.metrics == {}
        assert result.detail is None
        assert result.reason == ""


# ================================================================
#  ScoringEvaluator
# ================================================================

class TestScoringEvaluator:
    def test_evaluate_passing(self, mock_analyzer, sample_dates):
        spec = EvalSpec("dividend_yield", FactorRole.SCORING)
        evaluator = ScoringEvaluator()
        result = evaluator.evaluate(spec, mock_analyzer, sample_dates)

        assert result.factor_name == "dividend_yield"
        assert result.role == FactorRole.SCORING
        assert result.passed is True
        assert "mean_ic_1d" in result.metrics
        assert "mean_ic_5d" in result.metrics
        assert "mean_ic_20d" in result.metrics
        assert result.detail is not None

    def test_evaluate_failing(self, mock_analyzer, sample_dates):
        """IC 太低 → 不通过"""
        def weak_ic(factor_names, dates, forward_days_list):
            rows = []
            for name in factor_names:
                row = {"factor": name}
                for h in forward_days_list:
                    row[f"mean_ic_{h}d"] = 0.01  # 太低
                    row[f"ic_std_{h}d"] = 0.10
                    row[f"icir_{h}d"] = 0.10  # 太低
                    row[f"positive_rate_{h}d"] = 0.45  # 太低
                    row[f"n_periods_{h}d"] = 12
                row["best_horizon"] = forward_days_list[-1]
                rows.append(row)
            return pd.DataFrame(rows)

        mock_analyzer.ic_report_multi_horizon = MagicMock(side_effect=weak_ic)
        spec = EvalSpec("weak_factor", FactorRole.SCORING)
        evaluator = ScoringEvaluator()
        result = evaluator.evaluate(spec, mock_analyzer, sample_dates)

        assert result.passed is False
        assert "未通过" in result.reason

    def test_custom_admission(self, mock_analyzer, sample_dates):
        """自定义准入标准"""
        spec = EvalSpec("dividend_yield", FactorRole.SCORING)
        evaluator = ScoringEvaluator(admission={
            "min_abs_ic": 0.10,  # 非常高的标准
            "min_icir": 1.0,
            "min_positive_rate": 0.80,
            "min_periods": 6,
        })
        result = evaluator.evaluate(spec, mock_analyzer, sample_dates)
        assert result.passed is False


# ================================================================
#  FilterEvaluator
# ================================================================

class TestFilterEvaluator:
    def test_evaluate_with_threshold(self, mock_analyzer, sample_dates):
        spec = EvalSpec(
            "debt_to_assets", FactorRole.FILTER,
            threshold=5.0, threshold_op=">=",
        )
        evaluator = FilterEvaluator()
        result = evaluator.evaluate(spec, mock_analyzer, sample_dates)

        assert result.factor_name == "debt_to_assets"
        assert result.role == FactorRole.FILTER
        assert "coverage" in result.metrics
        assert "pass_rate" in result.metrics

    def test_evaluate_no_threshold(self, mock_analyzer, sample_dates):
        """无门槛 → 只评覆盖度"""
        spec = EvalSpec(
            "some_filter", FactorRole.FILTER,
            threshold=None,
        )
        evaluator = FilterEvaluator()
        result = evaluator.evaluate(spec, mock_analyzer, sample_dates)
        assert "coverage" in result.metrics


# ================================================================
#  RiskEvaluator
# ================================================================

class TestRiskEvaluator:
    def test_evaluate(self, mock_analyzer, sample_dates):
        spec = EvalSpec("roe_stability", FactorRole.RISK)
        evaluator = RiskEvaluator()
        result = evaluator.evaluate(spec, mock_analyzer, sample_dates)

        assert result.factor_name == "roe_stability"
        assert result.role == FactorRole.RISK
        assert result.passed is True  # risk 总是通过
        assert "coverage" in result.metrics
        assert "mean" in result.metrics
        assert "std" in result.metrics


# ================================================================
#  EvalPipeline
# ================================================================

class TestEvalPipeline:
    def test_add_and_run(self, mock_analyzer, sample_dates):
        pipeline = EvalPipeline(mock_analyzer)
        pipeline.add(EvalSpec("dividend_yield", FactorRole.SCORING))
        pipeline.add(EvalSpec("debt_to_assets", FactorRole.FILTER, threshold=70, threshold_op="<="))
        pipeline.add(EvalSpec("roe_stability", FactorRole.RISK))

        results = pipeline.run(sample_dates)
        assert len(results) == 3
        assert results[0].role == FactorRole.SCORING
        assert results[1].role == FactorRole.FILTER
        assert results[2].role == FactorRole.RISK

    def test_add_many(self, mock_analyzer, sample_dates):
        pipeline = EvalPipeline(mock_analyzer)
        specs = [
            EvalSpec("dividend_yield", FactorRole.SCORING),
            EvalSpec("ep", FactorRole.SCORING),
        ]
        pipeline.add_many(specs)
        results = pipeline.run(sample_dates)
        assert len(results) == 2

    def test_summary(self, mock_analyzer, sample_dates):
        pipeline = EvalPipeline(mock_analyzer)
        pipeline.add(EvalSpec("dividend_yield", FactorRole.SCORING))
        results = pipeline.run(sample_dates)
        summary = pipeline.summary(results)
        assert "factor" in summary.columns
        assert "role" in summary.columns
        assert "passed" in summary.columns

    def test_admission_report(self, mock_analyzer, sample_dates):
        pipeline = EvalPipeline(mock_analyzer)
        pipeline.add(EvalSpec("dividend_yield", FactorRole.SCORING))
        results = pipeline.run(sample_dates)
        report = pipeline.admission_report(results)
        assert list(report.columns) == ["factor", "role", "passed", "reason"]

    def test_save_report(self, mock_analyzer, sample_dates, tmp_path):
        pipeline = EvalPipeline(mock_analyzer)
        pipeline.add(EvalSpec("dividend_yield", FactorRole.SCORING))
        results = pipeline.run(sample_dates)

        out_dir = tmp_path / "eval_output"
        pipeline.save_report(results, out_dir, run_config={"test": True})

        assert (out_dir / "config.json").exists()
        assert (out_dir / "summary.csv").exists()
        assert (out_dir / "admission.csv").exists()

        config = json.loads((out_dir / "config.json").read_text())
        assert config["run_config"]["test"] is True
        assert config["specs"][0]["factor_family"] == ""


# ================================================================
#  WeightTuner
# ================================================================

class TestWeightTuner:
    def test_optimize_ic(self, mock_analyzer, sample_dates):
        tuner = WeightTuner(mock_analyzer)
        weights = tuner.optimize(
            ["dividend_yield", "ep", "roe_ttm"],
            sample_dates, horizon=20, method="ic",
        )
        assert len(weights) == 3
        assert abs(sum(weights.values()) - 1.0) < 1e-6

    def test_optimize_icir(self, mock_analyzer, sample_dates):
        tuner = WeightTuner(mock_analyzer)
        weights = tuner.optimize(
            ["dividend_yield", "ep"],
            sample_dates, horizon=20, method="icir",
        )
        assert len(weights) == 2
        assert abs(sum(weights.values()) - 1.0) < 1e-6

    def test_optimize_equal(self, mock_analyzer, sample_dates):
        tuner = WeightTuner(mock_analyzer)
        weights = tuner.optimize(
            ["dividend_yield", "ep", "roe_ttm"],
            sample_dates, method="equal",
        )
        for v in weights.values():
            assert abs(v - 1.0 / 3) < 1e-6

    def test_compare(self, mock_analyzer, sample_dates):
        tuner = WeightTuner(mock_analyzer)
        comparison = tuner.compare(
            ["dividend_yield", "ep"],
            sample_dates,
            horizons=[20],
            methods=["ic", "equal"],
        )
        assert "factor" in comparison.columns
        assert len(comparison) == 2

    def test_save_report(self, mock_analyzer, sample_dates, tmp_path):
        tuner = WeightTuner(mock_analyzer)
        weights = {"dividend_yield": 0.6, "ep": 0.4}
        out_dir = tmp_path / "weight_output"
        tuner.save_report(
            weights, out_dir,
            method="icir", horizon=20,
            run_config={"test": True},
        )
        assert (out_dir / "weights.json").exists()
        data = json.loads((out_dir / "weights.json").read_text())
        assert data["method"] == "icir"
        assert data["weights"]["dividend_yield"] == 0.6


# ================================================================
#  reporter (HTML)
# ================================================================

class TestReporter:
    def test_generate_eval_html(self, mock_analyzer, sample_dates, tmp_path):
        from vortex.evaluation.reporter import generate_eval_html

        pipeline = EvalPipeline(mock_analyzer)
        specs = [
            EvalSpec("dividend_yield", FactorRole.SCORING, description="近12月股息率"),
            EvalSpec("debt_to_assets", FactorRole.FILTER, threshold=5.0, description="资产负债率过滤"),
            EvalSpec("roe_stability", FactorRole.RISK, description="ROE稳定性"),
        ]
        pipeline.add_many(specs)
        results = pipeline.run(sample_dates)

        html_path = tmp_path / "test_eval.html"
        out = generate_eval_html(results, specs, html_path)
        assert out.exists()
        content = out.read_text(encoding="utf-8")
        assert "因子准入总览" in content
        assert "近12月股息率" in content
        assert "dividend_yield" in content

    def test_generate_weight_html(self, tmp_path):
        from vortex.evaluation.reporter import generate_weight_html

        weights = {"dividend_yield": 0.49, "ep": 0.33, "delta_roe": 0.18}
        comparison = pd.DataFrame({
            "factor": ["dividend_yield", "ep", "delta_roe"],
            "ic_20d": [0.49, 0.33, 0.18],
            "equal": [0.333, 0.333, 0.333],
        })

        html_path = tmp_path / "test_weight.html"
        out = generate_weight_html(
            weights, comparison, html_path,
            method="icir", horizon=20,
        )
        assert out.exists()
        content = out.read_text(encoding="utf-8")
        assert "最终权重" in content
        assert "股息率" in content


# ================================================================
#  Strategy eval_specs
# ================================================================

class TestStrategyEvalSpecs:
    def test_base_strategy_default(self):
        from vortex.strategy.base import BaseStrategy
        # BaseStrategy 是ABC，不能直接实例化，用 mock
        strategy = MagicMock(spec=BaseStrategy)
        strategy.eval_specs = BaseStrategy.eval_specs
        result = strategy.eval_specs(strategy)
        assert result == []

    def test_dividend_strategy_specs(self):
        from vortex.strategy.dividend import DividendQualityFCFStrategy, SCORING_FACTORS
        ds = MagicMock()
        fh = MagicMock()
        bus = MagicMock()
        # mock scfg
        scfg = MagicMock()
        scfg.min_consecutive_dividend_years = 5
        scfg.payout_ratio_range = (0.20, 0.90)
        scfg.min_listed_days = 250
        ds.cfg = scfg

        strategy = DividendQualityFCFStrategy(ds, fh, bus)
        specs = strategy.eval_specs()

        assert len(specs) > 0
        roles = {s.role for s in specs}
        assert FactorRole.SCORING in roles
        assert FactorRole.FILTER in roles
        assert FactorRole.RISK in roles

        scoring_specs = [s for s in specs if s.role == FactorRole.SCORING]
        scoring_names = {s.factor_name for s in scoring_specs}
        for f in SCORING_FACTORS:
            assert f in scoring_names, f"Missing scoring spec for {f}"

        # 每个 spec 都有 data_source
        for s in specs:
            assert s.data_source != "", f"{s.factor_name} missing data_source"

        # 打分因子应显式标注类别，供自动 horizon 选择使用
        for s in scoring_specs:
            assert s.factor_family != "", f"{s.factor_name} missing factor_family"
