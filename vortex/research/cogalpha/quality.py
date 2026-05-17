"""Fail-closed quality gates for CogAlpha candidates."""
from __future__ import annotations

from dataclasses import dataclass
import re

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

from vortex.research.cogalpha.agent_catalog import is_registered_agent
from vortex.research.cogalpha.schema import AlphaCandidate, QualityCheckResult, QualityIssue


DEFAULT_ALLOWED_OPERATORS: tuple[str, ...] = (
    "abs",
    "clip",
    "correlation",
    "covariance",
    "cs_rank",
    "cs_zscore",
    "decay_linear",
    "delay",
    "delta",
    "log",
    "neutralize_by_group",
    "rank",
    "safe_div",
    "scale",
    "signed_power",
    "ts_mean",
    "ts_max",
    "ts_rank",
    "ts_std",
    "ts_sum",
    "where",
)


FORBIDDEN_EXPRESSION_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"shift\s*\(\s*-", "negative_shift"),
    (r"\bfuture_return\b", "future_return"),
    (r"\bforward_return\b", "forward_return"),
    (r"\btarget\b", "target_field"),
    (r"\blabel\b", "label_field"),
    (r"\beval\s*\(", "eval_call"),
    (r"\bexec\s*\(", "exec_call"),
    (r"__", "dunder_access"),
    (r"\bimport\b", "import_statement"),
    (r"\bsubprocess\b", "subprocess_access"),
    (r"\bos\.", "os_access"),
    (r"\bsys\.", "sys_access"),
    (r"\bopen\s*\(", "file_open"),
    (r"\bread\s*\(", "file_read"),
    (r"center\s*=\s*True", "centered_window"),
)


@dataclass(frozen=True)
class QualityGateRule:
    """Thresholds for CogAlpha quality checks."""

    min_coverage: float = 0.70
    min_distinct_ratio: float = 0.05
    min_valid_dates: int = 20
    min_valid_symbols: int = 30
    allowed_expression_types: tuple[str, ...] = ("formula", "field")


def check_candidate_metadata(
    candidate: AlphaCandidate,
    *,
    available_fields: set[str] | frozenset[str],
    allowed_operators: tuple[str, ...] = DEFAULT_ALLOWED_OPERATORS,
    rule: QualityGateRule | None = None,
) -> QualityCheckResult:
    """Check candidate metadata before any factor values are evaluated."""

    gate_rule = rule or QualityGateRule()
    issues: list[QualityIssue] = []

    if not candidate.alpha_id.strip():
        issues.append(QualityIssue("missing_alpha_id", "alpha_id must be non-empty"))
    if not candidate.name.strip():
        issues.append(QualityIssue("missing_name", "name must be non-empty"))
    if not candidate.hypothesis.strip():
        issues.append(QualityIssue("missing_hypothesis", "hypothesis must be non-empty"))
    if not candidate.expression.strip():
        issues.append(QualityIssue("missing_expression", "expression must be non-empty"))
    if candidate.expression_type not in gate_rule.allowed_expression_types:
        issues.append(QualityIssue("unsupported_expression_type", f"unsupported expression_type: {candidate.expression_type}"))
    if not is_registered_agent(candidate.agent):
        issues.append(QualityIssue("unknown_agent", f"unknown CogAlpha agent: {candidate.agent}"))
    if candidate.direction not in {"positive", "negative", "unknown"}:
        issues.append(QualityIssue("invalid_direction", f"invalid direction: {candidate.direction}"))
    if not candidate.required_fields:
        issues.append(QualityIssue("missing_required_fields", "required_fields must be non-empty"))
    if not candidate.horizons or any(horizon <= 0 for horizon in candidate.horizons):
        issues.append(QualityIssue("invalid_horizon", "horizons must contain positive integers"))
    if any(window <= 0 for window in candidate.lookback_windows):
        issues.append(QualityIssue("invalid_lookback_window", "lookback windows must be positive"))

    unknown_fields = sorted(set(candidate.required_fields) - set(available_fields))
    if unknown_fields:
        issues.append(QualityIssue("field_not_available", f"fields are not available: {unknown_fields}"))

    issues.extend(_expression_issues(candidate, available_fields=set(available_fields), allowed_operators=set(allowed_operators)))
    return QualityCheckResult(candidate.alpha_id, tuple(issues))


def check_factor_output(
    candidate: AlphaCandidate,
    factor: pd.DataFrame,
    *,
    rule: QualityGateRule | None = None,
) -> QualityCheckResult:
    """Check a computed factor dataframe before fitness evaluation."""

    gate_rule = rule or QualityGateRule()
    issues: list[QualityIssue] = []
    metrics: dict[str, object] = {}

    if not isinstance(factor, pd.DataFrame):
        return QualityCheckResult(candidate.alpha_id, (QualityIssue("factor_not_dataframe", "factor must be a pandas DataFrame"),))
    if factor.empty:
        return QualityCheckResult(candidate.alpha_id, (QualityIssue("factor_empty", "factor must be non-empty"),))
    if not factor.index.is_monotonic_increasing:
        issues.append(QualityIssue("factor_index_not_sorted", "factor index must be sorted ascending"))
    non_numeric = [column for column in factor.columns if not is_numeric_dtype(factor[column])]
    if non_numeric:
        issues.append(QualityIssue("factor_non_numeric", f"factor columns must be numeric: {non_numeric}"))
        return QualityCheckResult(candidate.alpha_id, tuple(issues))

    values = factor.to_numpy(dtype="float64")
    finite_mask = np.isfinite(values)
    total_cells = int(values.size)
    finite_cells = int(finite_mask.sum())
    coverage = finite_cells / total_cells if total_cells else 0.0
    inf_cells = int(np.isinf(values).sum())
    valid_symbols_by_date = pd.DataFrame(finite_mask, index=factor.index, columns=factor.columns).sum(axis=1)
    valid_dates = int((valid_symbols_by_date >= gate_rule.min_valid_symbols).sum())
    avg_valid_symbols = float(valid_symbols_by_date.mean()) if not valid_symbols_by_date.empty else 0.0
    distinct_ratio = _mean_distinct_ratio(factor)

    metrics.update(
        {
            "coverage": coverage,
            "finite_cells": finite_cells,
            "total_cells": total_cells,
            "inf_cells": inf_cells,
            "valid_dates": valid_dates,
            "avg_valid_symbols": avg_valid_symbols,
            "distinct_ratio": distinct_ratio,
        }
    )

    if inf_cells > 0:
        issues.append(QualityIssue("factor_has_inf", "factor contains inf values"))
    if coverage < gate_rule.min_coverage:
        issues.append(QualityIssue("coverage_too_low", f"coverage {coverage:.4f} below {gate_rule.min_coverage:.4f}"))
    if valid_dates < gate_rule.min_valid_dates:
        issues.append(QualityIssue("valid_dates_too_low", f"valid_dates {valid_dates} below {gate_rule.min_valid_dates}"))
    if distinct_ratio < gate_rule.min_distinct_ratio:
        issues.append(QualityIssue("distinct_ratio_too_low", f"distinct_ratio {distinct_ratio:.4f} below {gate_rule.min_distinct_ratio:.4f}"))

    return QualityCheckResult(candidate.alpha_id, tuple(issues), metrics)


def run_quality_gate(
    candidate: AlphaCandidate,
    *,
    available_fields: set[str] | frozenset[str],
    factor: pd.DataFrame | None = None,
    allowed_operators: tuple[str, ...] = DEFAULT_ALLOWED_OPERATORS,
    rule: QualityGateRule | None = None,
) -> QualityCheckResult:
    """Run metadata checks and, if provided, factor output checks."""

    metadata = check_candidate_metadata(
        candidate,
        available_fields=available_fields,
        allowed_operators=allowed_operators,
        rule=rule,
    )
    if factor is None:
        return metadata

    output = check_factor_output(candidate, factor, rule=rule)
    return QualityCheckResult(
        candidate.alpha_id,
        metadata.issues + output.issues,
        {**dict(metadata.metrics), **dict(output.metrics)},
    )


def _expression_issues(
    candidate: AlphaCandidate,
    *,
    available_fields: set[str],
    allowed_operators: set[str],
) -> tuple[QualityIssue, ...]:
    issues: list[QualityIssue] = []
    expression = candidate.expression
    for pattern, code in FORBIDDEN_EXPRESSION_PATTERNS:
        if re.search(pattern, expression):
            issues.append(QualityIssue(code, f"forbidden expression pattern detected: {code}"))

    identifiers = set(re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", expression))
    allowed_identifiers = available_fields | allowed_operators | {"True", "False", "nan"}
    unknown_identifiers = sorted(identifiers - allowed_identifiers)
    if candidate.expression_type == "formula" and unknown_identifiers:
        issues.append(QualityIssue("unknown_identifier", f"expression uses undeclared identifiers: {unknown_identifiers}"))

    undeclared_expression_fields = sorted((identifiers & available_fields) - set(candidate.required_fields))
    if candidate.expression_type == "formula" and undeclared_expression_fields:
        issues.append(QualityIssue("field_not_declared", f"expression fields are not declared: {undeclared_expression_fields}"))

    undeclared_fields = sorted(set(candidate.required_fields) - available_fields)
    if undeclared_fields:
        issues.append(QualityIssue("undeclared_required_field", f"required fields unavailable in expression context: {undeclared_fields}"))
    return tuple(issues)


def _mean_distinct_ratio(factor: pd.DataFrame) -> float:
    ratios: list[float] = []
    for _, row in factor.iterrows():
        clean = row.replace([np.inf, -np.inf], np.nan).dropna()
        if clean.empty:
            continue
        ratios.append(float(clean.nunique() / len(clean)))
    return float(np.mean(ratios)) if ratios else 0.0
