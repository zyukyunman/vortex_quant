"""策略目标达成审查。

每轮策略实验结束后都应先通过该模块判断是否真正达成年化/回撤目标。
如果未达标，模块会给出下一步研究动作，避免把阶段性失败包装成最终策略。
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GoalCriteria:
    """策略硬目标阈值。"""

    min_annual_return: float = 0.20
    max_drawdown_floor: float = -0.05
    require_pit_safe: bool = True
    require_cost_included: bool = True
    require_no_future_leakage: bool = True
    require_out_of_sample: bool = True


@dataclass(frozen=True)
class ExperimentQuality:
    """回测可信度口径。"""

    pit_safe: bool
    adjusted_prices: bool
    cost_included: bool
    no_future_leakage: bool
    out_of_sample_checked: bool


@dataclass(frozen=True)
class StrategyGoalInput:
    """单次策略实验的审查输入。"""

    annual_return: float
    max_drawdown: float
    sharpe: float | None = None
    calmar: float | None = None
    quality: ExperimentQuality | None = None


@dataclass(frozen=True)
class StrategyCandidateCriteria:
    """满仓 alpha 策略候选分级阈值。

    该标准不把最大回撤 5% 作为一票否决，因为 alpha 策略可在组合层通过
    其他品种、现金或对冲控制总组合风险。它更强调收益效率、可信度和稳定性。
    """

    min_annual_return: float = 0.20
    max_drawdown_floor: float = -0.15
    min_sharpe: float = 1.50
    min_calmar: float = 2.50
    min_positive_year_rate: float = 0.70
    require_pit_safe: bool = True
    require_cost_included: bool = True
    require_no_future_leakage: bool = True


@dataclass(frozen=True)
class StrategyCandidateInput:
    """满仓 alpha 策略候选审查输入。"""

    annual_return: float
    max_drawdown: float
    sharpe: float
    calmar: float
    positive_year_rate: float | None = None
    quality: ExperimentQuality | None = None


@dataclass(frozen=True)
class GoalReviewResult:
    """目标审查结果。"""

    status: str
    achieved: bool
    failures: tuple[str, ...]
    next_actions: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "achieved": self.achieved,
            "failures": list(self.failures),
            "next_actions": list(self.next_actions),
        }


@dataclass(frozen=True)
class StrategyCandidateReviewResult:
    """策略候选分级结果。"""

    grade: str
    worth_owning: bool
    failures: tuple[str, ...]
    next_actions: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "grade": self.grade,
            "worth_owning": self.worth_owning,
            "failures": list(self.failures),
            "next_actions": list(self.next_actions),
        }


def review_strategy_goal(
    result: StrategyGoalInput,
    criteria: GoalCriteria | None = None,
) -> GoalReviewResult:
    """审查策略是否达成目标；未达标时返回下一步研究动作。"""

    criteria = criteria or GoalCriteria()
    failures: list[str] = []
    quality = result.quality

    if quality is None:
        failures.append("missing_quality_check")
    else:
        failures.extend(_quality_failures(quality, criteria))

    if result.annual_return < criteria.min_annual_return:
        failures.append("annual_return_below_target")
    if result.max_drawdown <= criteria.max_drawdown_floor:
        failures.append("max_drawdown_above_limit")

    if any(_is_invalid_failure(item) for item in failures):
        status = "invalid_experiment"
    elif failures:
        status = "not_achieved"
    else:
        status = "achieved"

    return GoalReviewResult(
        status=status,
        achieved=status == "achieved",
        failures=tuple(failures),
        next_actions=tuple(select_next_research_actions(failures)),
    )


def review_strategy_candidate(
    result: StrategyCandidateInput,
    criteria: StrategyCandidateCriteria | None = None,
) -> StrategyCandidateReviewResult:
    """按满仓 alpha 候选标准审查策略是否值得拥有。"""

    criteria = criteria or StrategyCandidateCriteria()
    failures: list[str] = []
    quality = result.quality
    if quality is None:
        failures.append("missing_quality_check")
    else:
        quality_criteria = GoalCriteria(
            min_annual_return=criteria.min_annual_return,
            max_drawdown_floor=criteria.max_drawdown_floor,
            require_pit_safe=criteria.require_pit_safe,
            require_cost_included=criteria.require_cost_included,
            require_no_future_leakage=criteria.require_no_future_leakage,
            require_out_of_sample=False,
        )
        failures.extend(_quality_failures(quality, quality_criteria))
    if result.annual_return < criteria.min_annual_return:
        failures.append("annual_return_below_candidate_floor")
    if result.max_drawdown <= criteria.max_drawdown_floor:
        failures.append("drawdown_above_candidate_floor")
    if result.sharpe < criteria.min_sharpe:
        failures.append("sharpe_below_candidate_floor")
    effective_calmar = result.calmar if result.max_drawdown < 0 else float("inf")
    if effective_calmar < criteria.min_calmar:
        failures.append("calmar_below_candidate_floor")
    if (
        result.positive_year_rate is not None
        and result.positive_year_rate < criteria.min_positive_year_rate
    ):
        failures.append("positive_year_rate_below_candidate_floor")

    if any(_is_invalid_failure(item) for item in failures):
        grade = "invalid"
    elif failures:
        grade = "reject"
    elif result.annual_return >= 0.30 and result.sharpe >= 2.0 and effective_calmar >= 3.0:
        grade = "S"
    elif result.annual_return >= 0.20 and result.sharpe >= 1.5 and effective_calmar >= 2.5:
        grade = "A"
    else:
        grade = "B"

    return StrategyCandidateReviewResult(
        grade=grade,
        worth_owning=grade in {"S", "A"},
        failures=tuple(failures),
        next_actions=tuple(select_candidate_next_actions(grade, failures)),
    )


def select_next_research_actions(failures: list[str] | tuple[str, ...]) -> list[str]:
    """根据失败原因生成继续执行动作。"""

    failure_set = set(failures)
    actions: list[str] = []
    if any(_is_invalid_failure(item) for item in failure_set):
        actions.append("先修正实验可信度：复权、PIT、成本、未来函数和样本外检查。")
    if "annual_return_below_target" in failure_set:
        actions.append("停止重复旧参数网格，转向新 alpha：日频事件、行业轮动、盈利惊喜、资金流和价格延迟。")
    if "max_drawdown_above_limit" in failure_set:
        actions.append("补市场状态和行业状态数据，评估风险预算、现金管理或对冲机制。")
    if "out_of_sample_missing" in failure_set:
        actions.append("运行 walk-forward，冻结参数并输出样本外稳定性。")
    if not actions:
        actions.append("归档达标策略，生成可复现报告和 lineage。")
    return actions


def select_candidate_next_actions(
    grade: str,
    failures: list[str] | tuple[str, ...],
) -> list[str]:
    """根据候选分级生成后续动作。"""

    failure_set = set(failures)
    actions: list[str] = []
    if any(_is_invalid_failure(item) for item in failure_set):
        actions.append("先修正实验可信度：复权、PIT、成本和未来函数。")
    if "drawdown_above_candidate_floor" in failure_set:
        actions.append("不要在 alpha 层强行压回撤，改在组合层做仓位、对冲或多资产风险预算。")
    if grade in {"S", "A"}:
        actions.append("归档为值得拥有的 alpha 候选，继续做 walk-forward、容量和成本压力测试。")
    elif not actions:
        actions.append("不进入候选池，转向新 alpha 或重新检查信号口径。")
    return actions


def _quality_failures(quality: ExperimentQuality, criteria: GoalCriteria) -> list[str]:
    failures: list[str] = []
    if criteria.require_pit_safe and not quality.pit_safe:
        failures.append("pit_not_safe")
    if not quality.adjusted_prices:
        failures.append("prices_not_adjusted")
    if criteria.require_cost_included and not quality.cost_included:
        failures.append("cost_not_included")
    if criteria.require_no_future_leakage and not quality.no_future_leakage:
        failures.append("future_leakage_risk")
    if criteria.require_out_of_sample and not quality.out_of_sample_checked:
        failures.append("out_of_sample_missing")
    return failures


def _is_invalid_failure(failure: str) -> bool:
    return failure in {
        "missing_quality_check",
        "pit_not_safe",
        "prices_not_adjusted",
        "cost_not_included",
        "future_leakage_risk",
    }
