from itertools import product

import numpy as np
import pytest

from bundesliga.contest import ContestConfig, _sample, contest_tips, prize_shares
from bundesliga.scoring import score_matrix


def test_prize_share_matches_exhaustive_ties_and_wins():
    probabilities = np.array([0.2, 0.0, 0.3, 0.1, 0.4])
    expected = np.zeros(5)
    for opponents in product(range(5), repeat=3):
        probability = np.prod([probabilities[score] for score in opponents])
        for own_score in range(5):
            if max(opponents) <= own_score:
                expected[own_score] += probability / (1 + opponents.count(own_score))
    assert np.allclose(prize_shares(probabilities, 3), expected, atol=1e-14)
    assert np.allclose(prize_shares(np.array([0., 0., 1.]), 10000), [0, 0, 1 / 10001])


def test_tiny_tie_probability_preserves_almost_certain_win():
    distribution = np.array([0.9, 0.1, 1e-25])
    assert np.isclose(prize_shares(distribution, 10000)[-1], 1.0)
    rare_win = prize_shares(np.array([0.2, 1e-300, 0.8]), 100)[1]
    assert np.isclose(rare_win / (0.2 ** 100), 1.0)


def test_opponents_and_own_tip_use_the_same_actual_results():
    # Crowd always tips 0:1. Our 1:0 wins outright on the same 30% of results
    # where the crowd loses; those events must never be sampled independently.
    matrix = np.array([[0.0, 0.7], [0.3, 0.0]])
    own_points = [np.array([[0, 0, 4, 0]])]
    crowd = [[np.array([[1, 0, 0, 0, 0], [0, 0, 0, 0, 1],
                        [1, 0, 0, 0, 0], [1, 0, 0, 0, 0]])]]
    config = ContestConfig(opponents=(100,), crowd_concentrations=(1.,))
    points, utilities = _sample([matrix], own_points, crowd, config, 1024, np.random.default_rng(42))
    own_scores = points[0, 0]
    own_shares = utilities[0, np.arange(1024), own_scores]
    assert np.array_equal(own_shares, (own_scores == 4).astype(float))
    assert 0.25 < own_shares.mean() < 0.35


def test_contest_can_select_plausible_underdog_against_concentrated_crowd():
    matrix = np.array([[0.0, 0.7], [0.3, 0.0]])
    config = ContestConfig(
        opponents=(100,), crowd_concentrations=(10.,), max_tip_goals=1,
        training_samples=1024, evaluation_samples=2048,
    )
    tips, report = contest_tips([matrix], config)
    assert tips == [(1, 0)]
    assert report['changed_tendencies'] == 1
    assert report['expected_points'] < report['baseline_expected_points']
    scenario = report['scenarios'][0]
    assert scenario['simulated_prize_share'] > scenario['baseline_simulated_prize_share']
    assert report['experimental'] is True


def test_evaluation_sample_does_not_affect_selected_tips():
    matrices = [score_matrix(1.8, 1.0, -.08), score_matrix(1.3, 1.6, -.08)]
    kwargs = dict(training_samples=512, opponents=(100,), crowd_concentrations=(2.5,))
    tips_a, report_a = contest_tips(matrices, ContestConfig(evaluation_samples=256, **kwargs))
    tips_b, report_b = contest_tips(matrices, ContestConfig(evaluation_samples=512, **kwargs))
    assert tips_a == tips_b
    assert report_a['training_log_objective'] == report_b['training_log_objective']


@pytest.mark.parametrize('matrix', [
    np.full((6, 6), np.nan), np.full((6, 6), -1.), np.ones((6, 6)), np.ones((1, 1)),
])
def test_invalid_probabilities_are_rejected(matrix):
    with pytest.raises(ValueError):
        contest_tips([matrix])


def test_invalid_simulation_config_is_rejected():
    with pytest.raises(ValueError):
        contest_tips([score_matrix(1.8, 1., -.08)], ContestConfig(training_samples=0))
