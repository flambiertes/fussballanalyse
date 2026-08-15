import numpy as np

from bundesliga.scoring import (
    blend_outcome_probabilities,
    expected_points_tip,
    outcome_probabilities,
    score_matrix,
    tip_points,
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
