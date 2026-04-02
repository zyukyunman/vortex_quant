"""
scoring.py
打分因子评测器 — IC + 多空 + 准入判断

适用于参与综合打分排序的因子:
  dividend_yield, ep, fcf_yield, roe_ttm, delta_roe, opcfd
"""
from __future__ import annotations

import logging
from typing import Dict, List

import numpy as np
import pandas as pd

from vortex.evaluation.base import BaseEvaluator
from vortex.evaluation.spec import EvalResult, EvalSpec, FactorRole

logger = logging.getLogger(__name__)

# 默认准入标准
SCORING_ADMISSION = {
    "min_abs_ic": 0.03,
    "min_icir": 0.3,
    "min_positive_rate": 0.50,
    "min_periods": 6,
}


class ScoringEvaluator(BaseEvaluator):
    """
    打分因子评测器

    评测内容:
      1. 多周期 IC (复用 FactorAnalyzer.ic_report_multi_horizon)
      2. 多空组合收益 (复用 FactorAnalyzer.long_short_report)
      3. IC 衰减判断
      4. 准入判断
    """

    def __init__(self, admission: Dict[str, float] | None = None):
        self.admission = admission or SCORING_ADMISSION.copy()

    def evaluate(
        self,
        spec: EvalSpec,
        analyzer,
        dates: List[str],
    ) -> EvalResult:
        """评测单个打分因子"""
        factor_name = spec.factor_name
        horizons = list(spec.horizons)

        logger.info("[ScoringEvaluator] 评测 %s, horizons=%s", factor_name, horizons)

        # 1. 多周期 IC
        ic_df = analyzer.ic_report_multi_horizon(
            factor_names=[factor_name],
            dates=dates,
            forward_days_list=horizons,
        )

        # 2. 多空组合
        ls_df = analyzer.long_short_report(
            factor_names=[factor_name],
            dates=dates,
            forward_days=spec.ls_horizon,
            n_groups=spec.ls_groups,
        )

        # 3. 汇总指标
        metrics: Dict[str, float] = {}
        if not ic_df.empty:
            row = ic_df.iloc[0]
            for h in horizons:
                for key in ["mean_ic", "ic_std", "icir", "positive_rate", "n_periods"]:
                    col = f"{key}_{h}d"
                    if col in row.index:
                        metrics[col] = float(row[col]) if pd.notna(row[col]) else 0.0
            if "best_horizon" in row.index:
                metrics["best_horizon"] = float(row["best_horizon"]) if pd.notna(row["best_horizon"]) else 0.0

        if not ls_df.empty:
            ls_row = ls_df.iloc[0]
            ls_col = f"long_short_{spec.ls_horizon}d"
            for key in [ls_col, "long_mean", "short_mean", "sharpe", "n_periods"]:
                if key in ls_row.index:
                    val = ls_row[key]
                    k = f"ls_{key}" if key != ls_col else key
                    metrics[k] = float(val) if pd.notna(val) else 0.0

        # 4. IC 衰减分析
        ic_values = []
        for h in horizons:
            col = f"mean_ic_{h}d"
            ic_values.append((h, metrics.get(col, 0.0)))
        if len(ic_values) >= 2:
            first_ic = abs(ic_values[0][1])
            last_ic = abs(ic_values[-1][1])
            if first_ic > 0:
                metrics["ic_decay_ratio"] = round(last_ic / first_ic, 4)

        # 5. IC 时序明细 (用最常用的 horizon)
        primary_horizon = horizons[1] if len(horizons) > 1 else horizons[0]
        ic_series = analyzer.calc_ic(factor_name, dates, forward_days=primary_horizon)
        detail = pd.DataFrame({"date": ic_series.index, "ic": ic_series.values})

        # 6. 准入判断
        passed, reason = self._check_admission(metrics, horizons)

        return EvalResult(
            factor_name=factor_name,
            role=FactorRole.SCORING,
            passed=passed,
            metrics=metrics,
            detail=detail,
            reason=reason,
        )

    def _check_admission(
        self, metrics: Dict[str, float], horizons: List[int]
    ) -> tuple[bool, str]:
        """准入判断：至少一个 horizon 满足标准"""
        reasons = []
        any_passed = False

        for h in horizons:
            abs_ic = abs(metrics.get(f"mean_ic_{h}d", 0.0))
            icir = abs(metrics.get(f"icir_{h}d", 0.0))
            pos_rate = metrics.get(f"positive_rate_{h}d", 0.0)
            n_periods = metrics.get(f"n_periods_{h}d", 0)

            if n_periods < self.admission["min_periods"]:
                continue

            checks = [
                abs_ic >= self.admission["min_abs_ic"],
                icir >= self.admission["min_icir"],
                pos_rate >= self.admission["min_positive_rate"],
            ]
            if sum(checks) >= 2:
                any_passed = True
                reasons.append(
                    f"{h}d: |IC|={abs_ic:.4f} ICIR={icir:.3f} "
                    f"正IC率={pos_rate:.0%}"
                )

        if any_passed:
            return True, "通过: " + "; ".join(reasons)
        return False, "未通过: 所有周期均不满足准入标准"
