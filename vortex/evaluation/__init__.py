"""
evaluation
因子评测与权重优化

模块:
  spec.py         — EvalSpec, FactorRole, EvalResult 数据类
  base.py         — BaseEvaluator 抽象基类
  scoring.py      — ScoringEvaluator (IC + 多空)
  filter.py       — FilterEvaluator (通过率 + 覆盖度)
  risk.py         — RiskEvaluator (尾部分布)
  pipeline.py     — EvalPipeline 编排器
  weight_tuner.py — WeightTuner 权重优化
  reporter.py     — HTML 报告生成
"""
from vortex.evaluation.spec import EvalSpec, EvalResult, FactorRole
from vortex.evaluation.pipeline import EvalPipeline
from vortex.evaluation.weight_tuner import WeightTuner
from vortex.evaluation.reporter import generate_eval_html, generate_weight_html

__all__ = [
    "EvalSpec",
    "EvalResult",
    "FactorRole",
    "EvalPipeline",
    "WeightTuner",
    "generate_eval_html",
    "generate_weight_html",
]
