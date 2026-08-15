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
