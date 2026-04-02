"""
weight_tuner.py
权重优化器 — 输入因子列表，输出最优权重

与 EvalPipeline 完全解耦:
  - EvalPipeline 负责"这些因子行不行"
  - WeightTuner 负责"行的那些因子怎么配权"

注意区分:
  WeightTuner (evaluation/)  ← 研究阶段: 离线分析，人看报告
  WeightOptimizer (core/)    ← 运行阶段: 策略实时配权
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class WeightTuner:
    """
    因子权重优化器

    用法:
        tuner = WeightTuner(analyzer)
        weights = tuner.optimize(["dividend_yield", "ep"], dates, horizon=20)
        comparison = tuner.compare(["dividend_yield", "ep"], dates)
    """

    def __init__(self, analyzer):
        self.analyzer = analyzer

    def optimize(
        self,
        factor_names: List[str],
        dates: List[str],
        horizon: int = 20,
        method: str = "ic",
        min_ic: float = 0.0,
        min_positive_rate: float = 0.45,
        min_periods: int = 6,
    ) -> Dict[str, float]:
        """
        计算因子权重

        Parameters
        ----------
        factor_names : 参与配权的因子列表
        dates : 截面日期
        horizon : IC 周期 (交易日)
        method : 配权方法 ("ic" / "icir" / "equal")
        min_ic : IC 准入下限 (仅 ic/icir 方法)
        min_positive_rate : 正 IC 率准入下限
        min_periods : 最少样本期

        Returns
        -------
        dict[str, float]
            因子权重，归一化后之和 = 1.0
        """
        if method == "equal":
            n = len(factor_names)
            return {f: 1.0 / n for f in factor_names} if n > 0 else {}

        # IC / ICIR 方法: 先算 IC 报告
        ic_report = self.analyzer.ic_report(
            factor_names=factor_names,
            dates=dates,
            forward_days=horizon,
        )

        weights = {}
        for _, row in ic_report.iterrows():
            name = row["factor"]
            mean_ic = row.get("mean_ic", 0.0)
            pos_rate = row.get("ic_positive_rate", 0.0)
            icir = row.get("icir", 0.0)
            n_periods = row.get("n_periods", 0)

            if n_periods < min_periods:
                continue
            if pos_rate < min_positive_rate:
                continue

            if method == "ic":
                if abs(mean_ic) > min_ic:
                    weights[name] = abs(mean_ic)
            elif method == "icir":
                if abs(icir) > 0.1:
                    weights[name] = abs(icir)

        # 归一化
        if not weights:
            logger.warning("%s 方法无有效因子，退化到等权", method)
            return {f: 1.0 / len(factor_names) for f in factor_names}

        total = sum(weights.values())
        normalized = {f: w / total for f, w in weights.items()}
        # 补齐缺失因子为 0
        for f in factor_names:
            normalized.setdefault(f, 0.0)
        return normalized

    def compare(
        self,
        factor_names: List[str],
        dates: List[str],
        horizons: List[int] | None = None,
        methods: List[str] | None = None,
    ) -> pd.DataFrame:
        """
        对比多种配权方案

        Returns
        -------
        pd.DataFrame
            index=因子名, columns=各方案权重
        """
        if horizons is None:
            horizons = [20, 60, 120]
        if methods is None:
            methods = ["ic", "icir", "equal"]

        all_weights = {}
        for method in methods:
            for horizon in horizons:
                label = f"{method}_{horizon}d" if method != "equal" else "equal"
                weights = self.optimize(factor_names, dates, horizon=horizon, method=method)
                all_weights[label] = weights
                if method == "equal":
                    break  # equal 不受 horizon 影响

        rows = []
        for f in factor_names:
            row = {"factor": f}
            for label, w in all_weights.items():
                row[label] = w.get(f, 0.0)
            rows.append(row)

        return pd.DataFrame(rows)

    def save_report(
        self,
        weights: Dict[str, float],
        output_dir: str | Path,
        method: str = "",
        horizon: int = 0,
        comparison: Optional[pd.DataFrame] = None,
        run_config: Optional[Dict] = None,
    ) -> Path:
        """
        保存权重优化结果

        目录结构:
          output_dir/
            weights.json      — 最优权重
            comparison.csv    — 多方案对比 (可选)
            config.json       — 运行配置
        """
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        # 权重
        weights_data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "method": method,
            "horizon": horizon,
            "weights": {k: round(v, 4) for k, v in weights.items()},
        }
        if run_config:
            weights_data["run_config"] = run_config

        (out / "weights.json").write_text(
            json.dumps(weights_data, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        # 对比表
        if comparison is not None:
            comparison.to_csv(out / "comparison.csv", index=False, encoding="utf-8-sig")

        logger.info("权重报告已保存: %s", out)
        return out
