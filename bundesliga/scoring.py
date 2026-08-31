from __future__ import annotations

import math

import numpy as np


def dixon_coles_tau(home_goals: int, away_goals: int, lambda_home: float, lambda_away: float, rho: float) -> float:
    if home_goals == 0 and away_goals == 0:
        return 1.0 - lambda_home * lambda_away * rho
    if home_goals == 0 and away_goals == 1:
        return 1.0 + lambda_home * rho
    if home_goals == 1 and away_goals == 0:
        return 1.0 + lambda_away * rho
    if home_goals == 1 and away_goals == 1:
        return 1.0 - rho
    return 1.0


def poisson_probabilities(lam: float, max_goals: int) -> np.ndarray:
    probabilities = np.empty(max_goals + 1, dtype=float)
    probabilities[0] = math.exp(-lam)
    for goals in range(1, max_goals + 1):
        probabilities[goals] = probabilities[goals - 1] * lam / goals
    return probabilities


def score_matrix(lambda_home: float, lambda_away: float, rho: float, max_goals: int = 10) -> np.ndarray:
    home = poisson_probabilities(lambda_home, max_goals)
    away = poisson_probabilities(lambda_away, max_goals)
    matrix = np.outer(home, away)
    for gh, ga in ((0, 0), (0, 1), (1, 0), (1, 1)):
        matrix[gh, ga] *= max(dixon_coles_tau(gh, ga, lambda_home, lambda_away, rho), 1e-9)
    total = matrix.sum()
    if total <= 0:
        raise ValueError("Ungueltige Ergebnis-Wahrscheinlichkeiten")
    return matrix / total


def outcome_probabilities(matrix: np.ndarray) -> tuple[float, float, float]:
    prob_home = float(np.tril(matrix, k=-1).sum())
    prob_draw = float(np.trace(matrix))
    prob_away = float(np.triu(matrix, k=1).sum())
    return prob_home, prob_draw, prob_away


def blend_outcome_probabilities(
    matrix: np.ndarray,
    external_probabilities: tuple[float, float, float] | None,
    weight: float,
) -> np.ndarray:
    """Mischt 1X2-Wahrscheinlichkeiten geometrisch und erhaelt die Score-Verteilung je Ausgang."""
    if external_probabilities is None or weight <= 0.0:
        return matrix
    weight = float(np.clip(weight, 0.0, 1.0))
    model = np.asarray(outcome_probabilities(matrix), dtype=float)
    external = np.asarray(external_probabilities, dtype=float)
    if external.shape != (3,) or not np.all(np.isfinite(external)) or np.any(external <= 0):
        return matrix
    external /= external.sum()
    blended = np.exp((1.0 - weight) * np.log(model) + weight * np.log(external))
    blended /= blended.sum()

    result = matrix.copy()
    home_mask = np.tril(np.ones_like(result, dtype=bool), k=-1)
    draw_mask = np.eye(result.shape[0], result.shape[1], dtype=bool)
    away_mask = np.triu(np.ones_like(result, dtype=bool), k=1)
    for mask, old_probability, new_probability in zip(
        (home_mask, draw_mask, away_mask), model, blended
    ):
        result[mask] *= new_probability / max(old_probability, 1e-12)
    return result / result.sum()


def tip_points(tip: tuple[int, int], actual: tuple[int, int]) -> int:
    """Standard im vorhandenen Tippspiel: 4 exakt, 3 Differenz, 2 Tendenz."""
    if tip == actual:
        return 4
    tip_diff = tip[0] - tip[1]
    actual_diff = actual[0] - actual[1]
    if tip_diff == actual_diff:
        return 3
    if int(np.sign(tip_diff)) == int(np.sign(actual_diff)):
        return 2
    return 0


def expected_points_tip(matrix: np.ndarray, max_tip_goals: int = 5) -> tuple[int, int]:
    outcomes = [
        ((gh, ga), float(matrix[gh, ga]))
        for gh in range(matrix.shape[0])
        for ga in range(matrix.shape[1])
    ]
    candidates = [
        (gh, ga)
        for gh in range(max_tip_goals + 1)
        for ga in range(max_tip_goals + 1)
    ]
    return max(
        candidates,
        key=lambda tip: (
            sum(tip_points(tip, actual) * probability for actual, probability in outcomes),
            matrix[tip[0], tip[1]],
            -sum(tip),
            -abs(tip[0] - tip[1]),
        ),
    )


def tip_points_distribution(matrix: np.ndarray, tip: tuple[int, int]) -> np.ndarray:
    """Probability of receiving 0, 1, 2, 3, or 4 points for one tip."""
    distribution = np.zeros(5, dtype=float)
    for home_goals in range(matrix.shape[0]):
        for away_goals in range(matrix.shape[1]):
            points = tip_points(tip, (home_goals, away_goals))
            distribution[points] += matrix[home_goals, away_goals]
    return distribution


def matchday_points_distribution(
    matrices: list[np.ndarray],
    tips: list[tuple[int, int]],
) -> np.ndarray:
    """Distribution of total matchday points, assuming independent matches."""
    if len(matrices) != len(tips):
        raise ValueError("Fuer jede Ergebnismatrix wird genau ein Tipp benoetigt")
    total = np.array([1.0])
    for matrix, tip in zip(matrices, tips):
        total = np.convolve(total, tip_points_distribution(matrix, tip))
    return total


def probability_at_least(distribution: np.ndarray, target_points: int) -> float:
    if target_points <= 0:
        return 1.0
    if target_points >= len(distribution):
        return 0.0
    return float(distribution[target_points:].sum())


def target_points_tips(
    matrices: list[np.ndarray],
    target_points: int = 24,
    max_tip_goals: int = 5,
) -> tuple[list[tuple[int, int]], float]:
    """Choose a fixed tip portfolio that maximizes P(matchday points >= target).

    The exact discrete objective does not decompose by match. We therefore use
    deterministic multi-start coordinate ascent. A Bundesliga matchday has only
    nine matches, so all candidate scorelines can be evaluated cheaply on every
    coordinate. The returned probability uses the model's usual conditional
    independence assumption.
    """
    if not matrices:
        return [], float(target_points <= 0)
    maximum_points = 4 * len(matrices)
    if not 1 <= target_points <= maximum_points:
        raise ValueError(
            f"target_points muss zwischen 1 und {maximum_points} liegen"
        )

    candidates = [
        (home_goals, away_goals)
        for home_goals in range(max_tip_goals + 1)
        for away_goals in range(max_tip_goals + 1)
    ]
    point_distributions = [
        np.asarray([tip_points_distribution(matrix, tip) for tip in candidates])
        for matrix in matrices
    ]

    def convolve(distributions: list[np.ndarray]) -> np.ndarray:
        total = np.array([1.0])
        for distribution in distributions:
            total = np.convolve(total, distribution)
        return total

    def best_index(values: np.ndarray, match_index: int) -> int:
        best_value = float(np.max(values))
        tied = np.flatnonzero(np.isclose(values, best_value, rtol=1e-12, atol=1e-15))
        distributions = point_distributions[match_index]
        matrix = matrices[match_index]
        return int(
            max(
                tied,
                key=lambda index: (
                    float(np.dot(np.arange(5), distributions[index])),
                    float(matrix[candidates[index]]),
                    -sum(candidates[index]),
                    -abs(candidates[index][0] - candidates[index][1]),
                ),
            )
        )

    # Different convex utilities supply useful, reproducible starting portfolios.
    point_values = np.asarray([
        [
            [
                [tip_points(tip, (home_goals, away_goals))
                 for away_goals in range(matrix.shape[1])]
                for home_goals in range(matrix.shape[0])
            ]
            for tip in candidates
        ]
        for matrix in matrices
    ])
    starts: list[list[int]] = []
    for risk_factor in (0.0, 0.25, 0.5, 1.0, 1.5, 2.0, 3.0):
        start = []
        for match_index, matrix in enumerate(matrices):
            utility = point_values[match_index]
            if risk_factor > 0:
                utility = np.exp(risk_factor * utility)
            values = np.sum(utility * matrix[None, :, :], axis=(1, 2))
            start.append(best_index(values, match_index))
        starts.append(start)
    modal_start = []
    for matrix in matrices:
        candidate_matrix = matrix[: max_tip_goals + 1, : max_tip_goals + 1]
        modal_tip = tuple(
            int(value)
            for value in np.unravel_index(np.argmax(candidate_matrix), candidate_matrix.shape)
        )
        modal_start.append(candidates.index(modal_tip))
    starts.append(modal_start)

    best_portfolio: list[int] | None = None
    best_objective = (-1.0, -1.0)
    for initial in starts:
        portfolio = initial.copy()
        for _ in range(20):
            changed = False
            # Both directions reduce dependence on the order of the fixtures.
            for match_index in list(range(len(matrices))) + list(
                range(len(matrices) - 1, -1, -1)
            ):
                other_distributions = [
                    point_distributions[index][portfolio[index]]
                    for index in range(len(matrices))
                    if index != match_index
                ]
                other_total = convolve(other_distributions)
                values = np.asarray(
                    [
                        probability_at_least(
                            np.convolve(other_total, distribution), target_points
                        )
                        for distribution in point_distributions[match_index]
                    ]
                )
                replacement = best_index(values, match_index)
                if replacement != portfolio[match_index]:
                    portfolio[match_index] = replacement
                    changed = True
            if not changed:
                break

        selected = [
            point_distributions[index][candidate_index]
            for index, candidate_index in enumerate(portfolio)
        ]
        total_distribution = convolve(selected)
        target_probability = probability_at_least(total_distribution, target_points)
        expected_points = float(
            sum(
                np.dot(np.arange(5), distribution)
                for distribution in selected
            )
        )
        objective = (target_probability, expected_points)
        if objective > best_objective:
            best_objective = objective
            best_portfolio = portfolio.copy()

    assert best_portfolio is not None
    return [candidates[index] for index in best_portfolio], best_objective[0]


def evaluate_prediction(
    matrix: np.ndarray,
    tip: tuple[int, int],
    actual: tuple[int, int],
) -> dict[str, float | int]:
    prob_home, prob_draw, prob_away = outcome_probabilities(matrix)
    actual_outcome = 0 if actual[0] > actual[1] else (1 if actual[0] == actual[1] else 2)
    outcome_probs = np.array([prob_home, prob_draw, prob_away])
    target = np.zeros(3)
    target[actual_outcome] = 1.0
    gh = min(actual[0], matrix.shape[0] - 1)
    ga = min(actual[1], matrix.shape[1] - 1)
    return {
        "tip_points": tip_points(tip, actual),
        "log_loss": float(-math.log(max(matrix[gh, ga], 1e-12))),
        "brier_score": float(np.square(outcome_probs - target).sum()),
    }
