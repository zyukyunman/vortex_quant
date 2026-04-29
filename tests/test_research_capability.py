from __future__ import annotations

from vortex.research.capability import (
    build_recommendations,
    classify_deep_learning_support,
    classify_llm_support,
    classify_table_ml_support,
)


def test_table_ml_requires_ml_dependency():
    packages = {"numpy": True, "pandas": True, "scipy": True, "sklearn": False, "lightgbm": False, "xgboost": False}

    assert classify_table_ml_support(16, packages) == "needs_dependencies"


def test_table_ml_ready_with_sklearn_and_enough_memory():
    packages = {"numpy": True, "pandas": True, "scipy": True, "sklearn": True}

    assert classify_table_ml_support(16, packages) == "ready"


def test_deep_learning_is_small_only_on_low_memory_machine():
    assert classify_deep_learning_support(16, {"torch": True}) == "small_experiments_only"


def test_llm_level_for_16gb_machine_is_small_inference():
    assert classify_llm_support(16, {"torch": False}) == "inference_only_small_models"


def test_recommendations_prioritize_factor_research_without_ml_stack():
    recommendations = build_recommendations(
        table_ml_level="needs_dependencies",
        deep_learning_level="needs_dependencies",
        llm_level="inference_only_small_models",
        packages={"duckdb": True, "pyarrow": True},
    )

    assert any("因子工程" in item for item in recommendations)
    assert any("未安装 torch" in item for item in recommendations)
