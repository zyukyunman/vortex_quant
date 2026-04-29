from __future__ import annotations

from vortex.research.goal_review import (
    ExperimentQuality,
    StrategyCandidateInput,
    StrategyGoalInput,
    review_strategy_candidate,
    review_strategy_goal,
)


def test_goal_review_accepts_strategy_only_when_all_targets_met():
    result = review_strategy_goal(
        StrategyGoalInput(
            annual_return=0.35,
            max_drawdown=-0.04,
            quality=ExperimentQuality(
                pit_safe=True,
                adjusted_prices=True,
                cost_included=True,
                no_future_leakage=True,
                out_of_sample_checked=True,
            ),
        )
    )

    assert result.achieved
    assert result.status == "achieved"


def test_goal_review_uses_current_20pct_annual_return_target():
    result = review_strategy_goal(
        StrategyGoalInput(
            annual_return=0.19,
            max_drawdown=-0.04,
            quality=ExperimentQuality(
                pit_safe=True,
                adjusted_prices=True,
                cost_included=True,
                no_future_leakage=True,
                out_of_sample_checked=True,
            ),
        )
    )

    assert not result.achieved
    assert "annual_return_below_target" in result.failures


def test_goal_review_rejects_high_return_high_drawdown_strategy():
    result = review_strategy_goal(
        StrategyGoalInput(
            annual_return=0.40,
            max_drawdown=-0.25,
            quality=ExperimentQuality(
                pit_safe=True,
                adjusted_prices=True,
                cost_included=True,
                no_future_leakage=True,
                out_of_sample_checked=True,
            ),
        )
    )

    assert not result.achieved
    assert "max_drawdown_above_limit" in result.failures
    assert any("市场状态" in action for action in result.next_actions)


def test_goal_review_marks_missing_quality_as_invalid():
    result = review_strategy_goal(StrategyGoalInput(annual_return=0.35, max_drawdown=-0.04))

    assert result.status == "invalid_experiment"
    assert "missing_quality_check" in result.failures


def test_goal_review_requires_out_of_sample_check():
    result = review_strategy_goal(
        StrategyGoalInput(
            annual_return=0.35,
            max_drawdown=-0.04,
            quality=ExperimentQuality(
                pit_safe=True,
                adjusted_prices=True,
                cost_included=True,
                no_future_leakage=True,
                out_of_sample_checked=False,
            ),
        )
    )

    assert result.status == "not_achieved"
    assert "out_of_sample_missing" in result.failures
    assert any("walk-forward" in action for action in result.next_actions)


def test_strategy_candidate_review_accepts_full_exposure_alpha_with_higher_drawdown():
    result = review_strategy_candidate(
        StrategyCandidateInput(
            annual_return=0.36,
            max_drawdown=-0.11,
            sharpe=2.2,
            calmar=3.5,
            positive_year_rate=0.90,
            quality=ExperimentQuality(
                pit_safe=True,
                adjusted_prices=True,
                cost_included=True,
                no_future_leakage=True,
                out_of_sample_checked=False,
            ),
        )
    )

    assert result.worth_owning
    assert result.grade == "S"
    assert any("walk-forward" in action for action in result.next_actions)


def test_strategy_candidate_review_rejects_low_efficiency_alpha():
    result = review_strategy_candidate(
        StrategyCandidateInput(
            annual_return=0.22,
            max_drawdown=-0.14,
            sharpe=0.8,
            calmar=1.2,
            positive_year_rate=0.6,
            quality=ExperimentQuality(
                pit_safe=True,
                adjusted_prices=True,
                cost_included=True,
                no_future_leakage=True,
                out_of_sample_checked=True,
            ),
        )
    )

    assert not result.worth_owning
    assert result.grade == "reject"
    assert "sharpe_below_candidate_floor" in result.failures
    assert "calmar_below_candidate_floor" in result.failures


def test_strategy_candidate_review_treats_zero_drawdown_as_calmar_pass():
    result = review_strategy_candidate(
        StrategyCandidateInput(
            annual_return=0.25,
            max_drawdown=0.0,
            sharpe=2.0,
            calmar=0.0,
            positive_year_rate=1.0,
            quality=ExperimentQuality(
                pit_safe=True,
                adjusted_prices=True,
                cost_included=True,
                no_future_leakage=True,
                out_of_sample_checked=True,
            ),
        )
    )

    assert result.worth_owning
    assert result.grade == "A"
    assert "calmar_below_candidate_floor" not in result.failures
