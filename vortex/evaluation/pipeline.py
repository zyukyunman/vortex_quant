"""
pipeline.py
因子评测管线 — 编排 + 汇总 + 报告输出

对外唯一入口，将 EvalSpec 按 role 分派给对应 Evaluator。
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from vortex.evaluation.base import BaseEvaluator
from vortex.evaluation.filter import FilterEvaluator
from vortex.evaluation.risk import RiskEvaluator
from vortex.evaluation.scoring import ScoringEvaluator
from vortex.evaluation.spec import EvalResult, EvalSpec, FactorRole

logger = logging.getLogger(__name__)


class EvalPipeline:
    """
    因子评测管线

    职责:
      1. 接收一组 EvalSpec
      2. 按 role 分派到对应 Evaluator
      3. 汇总结果、保存报告

    用法:
        pipeline = EvalPipeline(analyzer)
        pipeline.add(EvalSpec("dividend_yield", FactorRole.SCORING))
        pipeline.add(EvalSpec("consecutive_div_years", FactorRole.FILTER, threshold=5))
        results = pipeline.run(dates)
        pipeline.save_report(results, output_dir="data/reports/eval_20260402")
    """

    def __init__(self, analyzer):
        self.analyzer = analyzer
        self._evaluators: Dict[FactorRole, BaseEvaluator] = {
            FactorRole.SCORING: ScoringEvaluator(),
            FactorRole.FILTER: FilterEvaluator(),
            FactorRole.RISK: RiskEvaluator(),
        }
        self._specs: List[EvalSpec] = []

    def add(self, spec: EvalSpec) -> "EvalPipeline":
        """添加因子评测规格，支持链式调用"""
        self._specs.append(spec)
        return self

    def add_many(self, specs: List[EvalSpec]) -> "EvalPipeline":
        """批量添加评测规格"""
        self._specs.extend(specs)
        return self

    def register_evaluator(self, role: FactorRole, evaluator: BaseEvaluator):
        """注册自定义评测器"""
        self._evaluators[role] = evaluator

    def run(self, dates: List[str]) -> List[EvalResult]:
        """
        执行全部评测

        按 spec 顺序逐个评测 → 收集 EvalResult。
        """
        results = []
        for spec in self._specs:
            evaluator = self._evaluators.get(spec.role)
            if evaluator is None:
                logger.warning("未注册角色 %s 的评测器，跳过 %s", spec.role, spec.factor_name)
                continue
            logger.info("评测 %s [%s]...", spec.factor_name, spec.role.value)
            result = evaluator.evaluate(spec, self.analyzer, dates)
            results.append(result)
            logger.info(
                "  → %s: %s", "✓ 通过" if result.passed else "✗ 未通过", result.reason
            )
        return results

    def summary(self, results: List[EvalResult]) -> pd.DataFrame:
        """将评测结果汇总为一张宽表"""
        rows = []
        for r in results:
            row = {"factor": r.factor_name, "role": r.role.value, "passed": r.passed}
            row.update(r.metrics)
            row["reason"] = r.reason
            rows.append(row)
        return pd.DataFrame(rows)

    def admission_report(self, results: List[EvalResult]) -> pd.DataFrame:
        """只输出准入判断结果"""
        rows = []
        for r in results:
            rows.append({
                "factor": r.factor_name,
                "role": r.role.value,
                "passed": r.passed,
                "reason": r.reason,
            })
        return pd.DataFrame(rows)

    def save_report(
        self,
        results: List[EvalResult],
        output_dir: str | Path,
        run_config: Optional[Dict] = None,
    ) -> Path:
        """
        保存评测结果到指定目录

        目录结构:
          output_dir/
            config.json       — 运行配置
            summary.csv       — 汇总表
            admission.csv     — 准入判断
            specs.json        — 评测规格
            detail_{factor}.csv — 各因子明细

        Parameters
        ----------
        results : list[EvalResult]
        output_dir : 输出目录路径
        run_config : 额外配置信息 (可选)

        Returns
        -------
        Path
            输出目录
        """
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        # 1. 保存配置
        config = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "n_factors": len(results),
            "n_specs": len(self._specs),
        }
        if run_config:
            config["run_config"] = run_config
        # 保存 specs 信息
        specs_data = []
        for s in self._specs:
            specs_data.append({
                "factor_name": s.factor_name,
                "role": s.role.value,
                "horizons": list(s.horizons),
                "ls_horizon": s.ls_horizon,
                "factor_family": s.factor_family,
                "threshold": s.threshold,
                "threshold_op": s.threshold_op,
                "data_source": s.data_source,
                "description": s.description,
            })
        config["specs"] = specs_data
        (out / "config.json").write_text(
            json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        # 2. 汇总表
        summary_df = self.summary(results)
        summary_df.to_csv(out / "summary.csv", index=False, encoding="utf-8-sig")

        # 3. 准入判断
        admission_df = self.admission_report(results)
        admission_df.to_csv(out / "admission.csv", index=False, encoding="utf-8-sig")

        # 4. 各因子明细
        for r in results:
            if r.detail is not None and not r.detail.empty:
                fname = f"detail_{r.factor_name}.csv"
                r.detail.to_csv(out / fname, index=False, encoding="utf-8-sig")

        logger.info("评测报告已保存: %s", out)
        return out
