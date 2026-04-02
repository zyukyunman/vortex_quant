"""test_horizon_policy.py — 自动 horizon 选择规则测试"""
from __future__ import annotations

from vortex.evaluation.horizon_policy import (
    apply_scoring_horizon_policy,
    collect_scoring_horizons,
    collect_scoring_ls_horizons,
    infer_factor_family,
    recommend_weight_horizon,
)
from vortex.evaluation.spec import EvalSpec, FactorRole


class TestHorizonPolicy:
    def test_infer_factor_family(self):
        assert infer_factor_family("dividend_yield") == "dividend"
        assert infer_factor_family("fcf_yield") == "quality"
        assert infer_factor_family("delta_roe") == "quality"
        assert infer_factor_family("momentum_20d") == "momentum"
        assert infer_factor_family("netprofit_yoy") == "growth"

    def test_apply_auto_policy_by_freq_and_family(self):
        specs = [
            EvalSpec("dividend_yield", FactorRole.SCORING, factor_family="dividend"),
            EvalSpec("delta_roe", FactorRole.SCORING, factor_family="quality"),
            EvalSpec("debt_to_assets", FactorRole.FILTER, factor_family="risk"),
        ]

        resolved = apply_scoring_horizon_policy(specs, freq="SA")

        assert resolved[0].horizons == (20, 60, 120, 250)
        assert resolved[0].ls_horizon == 20
        assert resolved[1].horizons == (20, 60, 120, 250)
        assert resolved[1].ls_horizon == 20
        assert resolved[2].horizons == (1, 5, 20)

        assert collect_scoring_horizons(resolved) == [20, 60, 120, 250]
        assert collect_scoring_ls_horizons(resolved) == [20]

    def test_manual_override_applies_to_all_scoring_specs(self):
        specs = [
            EvalSpec("dividend_yield", FactorRole.SCORING, factor_family="dividend"),
            EvalSpec("ep", FactorRole.SCORING, factor_family="value"),
        ]

        resolved = apply_scoring_horizon_policy(
            specs,
            freq="Q",
            forward_days_list=[5, 20, 60],
            ls_horizon=5,
        )

        assert resolved[0].horizons == (5, 20, 60)
        assert resolved[1].horizons == (5, 20, 60)
        assert resolved[0].ls_horizon == 5
        assert resolved[1].ls_horizon == 5

    def test_recommend_weight_horizon(self):
        assert recommend_weight_horizon("M") == 20
        assert recommend_weight_horizon("Q") == 60
        assert recommend_weight_horizon("SA") == 120