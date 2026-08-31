import numpy as np

from bundesliga.scoring import (
    blend_outcome_probabilities,
    expected_points_tip,
    matchday_points_distribution,
    outcome_probabilities,
    probability_at_least,
    score_matrix,
    target_points_tips,
    tip_points,
    tip_points_distribution,
)


def test_score_matrix_is_normalized_and_outcomes_sum_to_one():
    matrix = score_matrix(1.55, 1.10, rho=-0.10, max_goals=10)
    assert np.isclose(matrix.sum(), 1.0)
    assert np.isclose(sum(outcome_probabilities(matrix)), 1.0)


def test_tip_points_rules():
    assert tip_points((2, 1), (2, 1)) == 4
    assert tip_points((2, 0), (3, 1)) == 3
    assert tip_points((1, 0), (4, 2)) == 2
    assert tip_points((1, 1), (2, 1)) == 0


def test_expected_points_tip_returns_valid_score():
    matrix = score_matrix(1.4, 1.0, rho=-0.08)
    tip = expected_points_tip(matrix)
    assert 0 <= tip[0] <= 5
    assert 0 <= tip[1] <= 5


def test_bookmaker_blend_reaches_external_outcome_probabilities_at_full_weight():
    matrix = score_matrix(1.55, 1.10, rho=-0.10, max_goals=10)
    blended = blend_outcome_probabilities(matrix, (0.2, 0.3, 0.5), 1.0)
    assert np.allclose(outcome_probabilities(blended), (0.2, 0.3, 0.5))
    assert np.isclose(blended.sum(), 1.0)


def test_tip_and_matchday_points_distributions_are_normalized():
    matrices = [
        score_matrix(1.6, 1.0, rho=-0.08),
        score_matrix(1.1, 1.2, rho=-0.08),
    ]
    single = tip_points_distribution(matrices[0], (2, 1))
    total = matchday_points_distribution(matrices, [(2, 1), (1, 1)])
    assert np.isclose(single.sum(), 1.0)
    assert np.isclose(total.sum(), 1.0)
    assert len(total) == 9


def test_target_points_portfolio_does_not_reduce_its_own_objective():
    matrices = [
        score_matrix(1.2 + index * 0.08, 1.0, rho=-0.08)
        for index in range(9)
    ]
    baseline_tips = [expected_points_tip(matrix) for matrix in matrices]
    baseline_probability = probability_at_least(
        matchday_points_distribution(matrices, baseline_tips), 24
    )
    contest_tips, contest_probability = target_points_tips(matrices, 24)
    assert len(contest_tips) == 9
    assert contest_probability + 1e-12 >= baseline_probability
    assert all(0 <= home <= 5 and 0 <= away <= 5 for home, away in contest_tips)


def test_target_points_rejects_impossible_matchday_target():
    matrix = score_matrix(1.4, 1.0, rho=-0.08)
    with np.testing.assert_raises(ValueError):
        target_points_tips([matrix], 5)
