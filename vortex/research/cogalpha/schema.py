"""Schemas for safe CogAlpha factor research."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal


CogAlphaDecision = Literal["generated", "invalid", "rejected", "qualified", "elite"]
Direction = Literal["positive", "negative", "unknown"]
ExpressionType = Literal["formula", "field"]
IssueSeverity = Literal["error", "warning"]


@dataclass(frozen=True)
class LineageRecord:
    """Trace how a CogAlpha candidate was created."""

    generation: int = 0
    parents: tuple[str, ...] = ()
    guidance_type: str = "concrete"
    mutation_type: str | None = None
    crossover_type: str | None = None
    prompt_hash: str | None = None
    code_hash: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "generation": self.generation,
            "parents": list(self.parents),
            "guidance_type": self.guidance_type,
            "mutation_type": self.mutation_type,
            "crossover_type": self.crossover_type,
            "prompt_hash": self.prompt_hash,
            "code_hash": self.code_hash,
        }


@dataclass(frozen=True)
class AlphaCandidate:
    """A structured, auditable CogAlpha candidate.

    The expression is metadata in this phase. Vortex only evaluates factor
    values that have already been produced by a trusted builder or dataframe
    source.
    """

    alpha_id: str
    name: str
    agent: str
    hypothesis: str
    expression: str
    required_fields: tuple[str, ...]
    horizons: tuple[int, ...]
    expression_type: ExpressionType = "formula"
    direction: Direction = "unknown"
    lookback_windows: tuple[int, ...] = ()
    created_by: str = "cogalpha-factor-mining"
    lineage: LineageRecord = field(default_factory=LineageRecord)
    metadata: Mapping[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {
            "alpha_id": self.alpha_id,
            "name": self.name,
            "agent": self.agent,
            "hypothesis": self.hypothesis,
            "expression_type": self.expression_type,
            "expression": self.expression,
            "required_fields": list(self.required_fields),
            "lookback_windows": list(self.lookback_windows),
            "horizons": list(self.horizons),
            "direction": self.direction,
            "created_by": self.created_by,
            "lineage": self.lineage.to_dict(),
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True)
class QualityIssue:
    """One quality gate issue."""

    code: str
    message: str
    severity: IssueSeverity = "error"

    @property
    def blocking(self) -> bool:
        return self.severity == "error"

    def to_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
        }


@dataclass(frozen=True)
class QualityCheckResult:
    """Quality gate result for one candidate."""

    candidate_id: str
    issues: tuple[QualityIssue, ...] = ()
    metrics: Mapping[str, object] = field(default_factory=dict)

    @property
    def has_blocking_issues(self) -> bool:
        return any(issue.blocking for issue in self.issues)

    @property
    def passed(self) -> bool:
        return not self.has_blocking_issues

    @property
    def status(self) -> str:
        return "passed" if self.passed else "failed"

    @property
    def blocking_codes(self) -> tuple[str, ...]:
        return tuple(issue.code for issue in self.issues if issue.blocking)

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_id": self.candidate_id,
            "status": self.status,
            "issues": [issue.to_dict() for issue in self.issues],
            "metrics": dict(self.metrics),
        }


@dataclass(frozen=True)
class FitnessStats:
    """CogAlpha fitness summary for the primary horizon."""

    primary_horizon: int
    pearson_ic_mean: float
    pearson_icir: float
    pearson_positive_rate: float
    pearson_count: int
    rank_ic_mean: float
    rank_icir: float
    rank_positive_rate: float
    rank_count: int
    long_short_mean: float
    long_short_sharpe: float
    coverage: float
    distinct_ratio: float
    group_monotonicity: float
    max_abs_correlation: float = 0.0

    def to_dict(self) -> dict[str, object]:
        return {
            "primary_horizon": self.primary_horizon,
            "pearson_ic_mean": self.pearson_ic_mean,
            "pearson_icir": self.pearson_icir,
            "pearson_positive_rate": self.pearson_positive_rate,
            "pearson_count": self.pearson_count,
            "rank_ic_mean": self.rank_ic_mean,
            "rank_icir": self.rank_icir,
            "rank_positive_rate": self.rank_positive_rate,
            "rank_count": self.rank_count,
            "long_short_mean": self.long_short_mean,
            "long_short_sharpe": self.long_short_sharpe,
            "coverage": self.coverage,
            "distinct_ratio": self.distinct_ratio,
            "group_monotonicity": self.group_monotonicity,
            "max_abs_correlation": self.max_abs_correlation,
        }


@dataclass(frozen=True)
class CogAlphaEvaluationResult:
    """Quality and fitness decision for one CogAlpha candidate."""

    candidate: AlphaCandidate
    quality: QualityCheckResult
    decision: CogAlphaDecision
    fitness: FitnessStats | None = None
    score: float = 0.0
    rejection_reasons: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate": self.candidate.to_dict(),
            "quality": self.quality.to_dict(),
            "decision": self.decision,
            "fitness": self.fitness.to_dict() if self.fitness else None,
            "score": self.score,
            "rejection_reasons": list(self.rejection_reasons),
        }
