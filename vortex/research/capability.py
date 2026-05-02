"""研究环境与模型能力评估。

该模块用于在本地优先的约束下判断当前机器适合哪类量化研究：
传统因子、表格机器学习、深度学习或大模型推理。它只做能力评估，
不自动安装依赖，也不把硬件能力当成策略有效性的证明。
"""
from __future__ import annotations

from dataclasses import dataclass
import importlib.util
import os
import platform
import subprocess


@dataclass(frozen=True)
class ResearchCapabilityReport:
    """本机研究能力摘要。"""

    machine: str
    processor: str
    cpu_count: int
    memory_gb: float | None
    packages: dict[str, bool]
    table_ml_level: str
    deep_learning_level: str
    llm_level: str
    recommendations: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "machine": self.machine,
            "processor": self.processor,
            "cpu_count": self.cpu_count,
            "memory_gb": self.memory_gb,
            "packages": self.packages,
            "table_ml_level": self.table_ml_level,
            "deep_learning_level": self.deep_learning_level,
            "llm_level": self.llm_level,
            "recommendations": list(self.recommendations),
        }


def assess_research_capability() -> ResearchCapabilityReport:
    """评估当前机器适合的研究形态。"""

    packages = {
        name: _has_package(name)
        for name in ("numpy", "pandas", "scipy", "sklearn", "torch", "lightgbm", "xgboost", "duckdb", "pyarrow")
    }
    memory_gb = _memory_gb()
    cpu_count = os.cpu_count() or 1
    table_ml_level = classify_table_ml_support(memory_gb, packages)
    deep_learning_level = classify_deep_learning_support(memory_gb, packages)
    llm_level = classify_llm_support(memory_gb, packages)
    recommendations = build_recommendations(
        table_ml_level=table_ml_level,
        deep_learning_level=deep_learning_level,
        llm_level=llm_level,
        packages=packages,
    )
    return ResearchCapabilityReport(
        machine=platform.machine(),
        processor=platform.processor(),
        cpu_count=cpu_count,
        memory_gb=memory_gb,
        packages=packages,
        table_ml_level=table_ml_level,
        deep_learning_level=deep_learning_level,
        llm_level=llm_level,
        recommendations=tuple(recommendations),
    )


def classify_table_ml_support(memory_gb: float | None, packages: dict[str, bool]) -> str:
    """判断表格机器学习支持程度。"""

    has_array_stack = packages.get("numpy", False) and packages.get("pandas", False) and packages.get("scipy", False)
    has_ml = packages.get("sklearn", False) or packages.get("lightgbm", False) or packages.get("xgboost", False)
    if not has_array_stack:
        return "not_ready"
    if has_ml and (memory_gb is None or memory_gb >= 16):
        return "ready"
    if has_ml:
        return "limited"
    return "needs_dependencies"


def classify_deep_learning_support(memory_gb: float | None, packages: dict[str, bool]) -> str:
    """判断深度学习训练支持程度。"""

    if not packages.get("torch", False):
        return "needs_dependencies"
    if memory_gb is not None and memory_gb < 32:
        return "small_experiments_only"
    return "ready_for_medium_experiments"


def classify_llm_support(memory_gb: float | None, packages: dict[str, bool]) -> str:
    """判断本地大模型支持程度。"""

    if memory_gb is not None and memory_gb < 32:
        return "inference_only_small_models"
    if packages.get("torch", False):
        return "local_inference_possible"
    return "needs_runtime"


def build_recommendations(
    *,
    table_ml_level: str,
    deep_learning_level: str,
    llm_level: str,
    packages: dict[str, bool],
) -> list[str]:
    """根据能力等级生成研究建议。"""

    recommendations: list[str] = []
    if table_ml_level == "needs_dependencies":
        recommendations.append("当前适合先做因子工程、IC、多空和线性/排序模型；若进入表格 ML，优先补 sklearn 或 LightGBM。")
    elif table_ml_level in {"ready", "limited"}:
        recommendations.append("可以做 walk-forward 表格机器学习，例如 Ridge/Logistic、随机森林、GBDT 或 LightGBM。")
    if deep_learning_level == "small_experiments_only":
        recommendations.append("深度学习仅建议小样本实验，不应把本机作为大规模训练环境。")
    elif deep_learning_level == "needs_dependencies":
        recommendations.append("当前未安装 torch，不建议把深度学习列为近期主线。")
    if llm_level == "inference_only_small_models":
        recommendations.append("16GB 内存更适合小模型推理或调用外部大模型做研究辅助，不适合本地训练大模型。")
    if not packages.get("duckdb", False) or not packages.get("pyarrow", False):
        recommendations.append("量化研究数据面依赖 DuckDB/PyArrow，应先补齐再跑批量实验。")
    return recommendations


def _has_package(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def _memory_gb() -> float | None:
    if platform.system() == "Darwin":
        try:
            output = subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip()
            return int(output) / (1024**3)
        except (FileNotFoundError, subprocess.CalledProcessError, ValueError):
            return None
    if hasattr(os, "sysconf"):
        try:
            pages = os.sysconf("SC_PHYS_PAGES")
            page_size = os.sysconf("SC_PAGE_SIZE")
            return float(pages * page_size) / (1024**3)
        except (ValueError, OSError, AttributeError):
            return None
    return None

