"""
risk.py
风险约束因子评测器 — 尾部分布 + 违约率

适用于风险约束因子:
  debt_to_assets, roe_stability
"""
from __future__ import annotations

import logging
from typing import Dict, List

import numpy as np
import pandas as pd

from vortex.evaluation.base import BaseEvaluator
from vortex.evaluation.spec import EvalResult, EvalSpec, FactorRole

logger = logging.getLogger(__name__)


class RiskEvaluator(BaseEvaluator):
    """
    风险因子评测器

    评测内容:
      1. 尾部覆盖 — 极端值 (>95pct / <5pct) 的股票数量和占比
      2. 分布统计 — 均值、中位数、标准差、偏度
      3. 覆盖度 — 有因子值的股票占比
    """

    def evaluate(
        self,
        spec: EvalSpec,
        analyzer,
        dates: List[str],
    ) -> EvalResult:
        factor_name = spec.factor_name

        logger.info("[RiskEvaluator] 评测 %s", factor_name)

        coverages = []
        means = []
        medians = []
        stds = []
        tail_high_ratios = []
        tail_low_ratios = []
        n_stocks_list = []

        for date in dates:
            try:
                factor = analyzer.fh.compute(factor_name, date)
                if factor.empty:
                    continue

                basic = analyzer.ds.get_stock_basic()
                n_total = len(basic)
                valid = factor.dropna()
                n_valid = len(valid)

                if n_valid < 20:
                    continue

                coverages.append(n_valid / n_total if n_total else 0)
                means.append(float(valid.mean()))
                medians.append(float(valid.median()))
                stds.append(float(valid.std()))
                n_stocks_list.append(n_valid)

                # 尾部统计
                q95 = valid.quantile(0.95)
                q05 = valid.quantile(0.05)
                tail_high_ratios.append(float((valid > q95).mean()))
                tail_low_ratios.append(float((valid < q05).mean()))

            except Exception as e:
                logger.debug("RiskEvaluator 失败 %s@%s: %s", factor_name, date, e)

        metrics: Dict[str, float] = {}
        if coverages:
            metrics["coverage"] = round(float(np.mean(coverages)), 4)
            metrics["mean"] = round(float(np.mean(means)), 4)
            metrics["median"] = round(float(np.mean(medians)), 4)
            metrics["std"] = round(float(np.mean(stds)), 4)
            metrics["tail_high_ratio"] = round(float(np.mean(tail_high_ratios)), 4)
            metrics["tail_low_ratio"] = round(float(np.mean(tail_low_ratios)), 4)
            metrics["avg_n_stocks"] = round(float(np.mean(n_stocks_list)), 0)
            metrics["n_dates"] = len(coverages)

        # 明细
        detail_rows = []
        for i in range(len(coverages)):
            detail_rows.append({
                "date": dates[i] if i < len(dates) else "",
                "coverage": coverages[i],
                "mean": means[i],
                "median": medians[i],
                "std": stds[i],
                "n_stocks": n_stocks_list[i],
            })
        detail = pd.DataFrame(detail_rows) if detail_rows else None

        # 风险因子始终 passed=True (它们不需要准入，只需展示分布)
        passed = True
        reason = (
            f"覆盖度={metrics.get('coverage', 0):.0%}, "
            f"均值={metrics.get('mean', 0):.2f}, "
            f"中位数={metrics.get('median', 0):.2f}"
        )

        return EvalResult(
            factor_name=factor_name,
            role=FactorRole.RISK,
            passed=passed,
            metrics=metrics,
            detail=detail,
            reason=reason,
        )
