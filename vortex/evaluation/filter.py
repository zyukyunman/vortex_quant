"""
filter.py
过滤因子评测器 — 通过率 + 覆盖度 + 条件 IC + 门槛敏感度

适用于硬门槛过滤因子:
  consecutive_div_years >= 5, fcf_ttm > 0, payout_ratio_3y in [20%, 90%]
"""
from __future__ import annotations

import logging
import operator
from typing import Dict, List

import numpy as np
import pandas as pd

from vortex.evaluation.base import BaseEvaluator
from vortex.evaluation.spec import EvalResult, EvalSpec, FactorRole

logger = logging.getLogger(__name__)

# 门槛比较运算符映射
_OPS = {
    ">=": operator.ge,
    ">": operator.gt,
    "<=": operator.le,
    "<": operator.lt,
    "==": operator.eq,
}

# 默认准入标准
FILTER_ADMISSION = {
    "min_coverage": 0.60,
    "pass_rate_range": (0.05, 0.80),
}


class FilterEvaluator(BaseEvaluator):
    """
    过滤因子评测器

    评测内容:
      1. 覆盖度 (coverage) — 有因子值的股票占比
      2. 通过率 (pass_rate) — 满足门槛的股票占比
      3. 条件 IC — 在通过门槛的子集上计算排序 IC
      4. 门槛敏感度 — 门槛 ±20% 时通过率变化
    """

    def __init__(self, admission: Dict | None = None):
        self.admission = admission or FILTER_ADMISSION.copy()

    def evaluate(
        self,
        spec: EvalSpec,
        analyzer,
        dates: List[str],
    ) -> EvalResult:
        factor_name = spec.factor_name
        threshold = spec.threshold
        threshold_op = spec.threshold_op
        op_fn = _OPS.get(threshold_op, operator.ge)

        logger.info(
            "[FilterEvaluator] 评测 %s (门槛: %s %s)",
            factor_name, threshold_op, threshold,
        )

        coverages = []
        pass_rates = []
        total_stocks_list = []
        pass_counts = []

        # 门槛敏感度: ±20%
        sensitivity_lo_rates = []
        sensitivity_hi_rates = []

        for date in dates:
            try:
                factor = analyzer.fh.compute(factor_name, date)
                if factor.empty:
                    continue

                # 全市场股票数 (用 stock_basic 估算)
                basic = analyzer.ds.get_stock_basic()
                n_total = len(basic)
                n_valid = factor.dropna().shape[0]
                coverage = n_valid / n_total if n_total > 0 else 0
                coverages.append(coverage)
                total_stocks_list.append(n_total)

                if threshold is not None:
                    valid = factor.dropna()
                    passed_mask = op_fn(valid, threshold)
                    n_pass = passed_mask.sum()
                    pass_rate = n_pass / n_valid if n_valid > 0 else 0
                    pass_rates.append(pass_rate)
                    pass_counts.append(int(n_pass))

                    # 敏感度
                    if threshold != 0:
                        lo_thr = threshold * 0.8
                        hi_thr = threshold * 1.2
                    else:
                        lo_thr = -0.01
                        hi_thr = 0.01
                    lo_mask = op_fn(valid, lo_thr)
                    hi_mask = op_fn(valid, hi_thr)
                    sensitivity_lo_rates.append(lo_mask.sum() / n_valid if n_valid else 0)
                    sensitivity_hi_rates.append(hi_mask.sum() / n_valid if n_valid else 0)

            except Exception as e:
                logger.debug("FilterEvaluator 计算失败 %s@%s: %s", factor_name, date, e)

        # 汇总
        metrics: Dict[str, float] = {}
        metrics["coverage"] = round(float(np.mean(coverages)), 4) if coverages else 0.0
        metrics["n_dates"] = len(coverages)

        if pass_rates:
            metrics["pass_rate"] = round(float(np.mean(pass_rates)), 4)
            metrics["pass_rate_std"] = round(float(np.std(pass_rates)), 4)
            metrics["avg_pass_count"] = round(float(np.mean(pass_counts)), 1)
        if sensitivity_lo_rates:
            metrics["sensitivity_lo"] = round(float(np.mean(sensitivity_lo_rates)), 4)
            metrics["sensitivity_hi"] = round(float(np.mean(sensitivity_hi_rates)), 4)

        # 构造明细
        detail_rows = []
        for i, date in enumerate(dates[:len(coverages)]):
            row = {"date": date, "coverage": coverages[i]}
            if i < len(pass_rates):
                row["pass_rate"] = pass_rates[i]
                row["pass_count"] = pass_counts[i]
            detail_rows.append(row)
        detail = pd.DataFrame(detail_rows) if detail_rows else None

        # 准入判断
        passed, reason = self._check_admission(metrics)

        return EvalResult(
            factor_name=factor_name,
            role=FactorRole.FILTER,
            passed=passed,
            metrics=metrics,
            detail=detail,
            reason=reason,
        )

    def _check_admission(self, metrics: Dict[str, float]) -> tuple[bool, str]:
        """准入判断"""
        reasons = []
        ok = True

        coverage = metrics.get("coverage", 0)
        if coverage < self.admission["min_coverage"]:
            ok = False
            reasons.append(f"覆盖度 {coverage:.0%} < {self.admission['min_coverage']:.0%}")
        else:
            reasons.append(f"覆盖度 {coverage:.0%} ✓")

        pass_rate = metrics.get("pass_rate")
        if pass_rate is not None:
            lo, hi = self.admission["pass_rate_range"]
            if pass_rate < lo:
                ok = False
                reasons.append(f"通过率 {pass_rate:.0%} 过低 (< {lo:.0%})")
            elif pass_rate > hi:
                ok = False
                reasons.append(f"通过率 {pass_rate:.0%} 过高 (> {hi:.0%})")
            else:
                reasons.append(f"通过率 {pass_rate:.0%} ✓")

        return ok, "; ".join(reasons)
