"""Keep at least seven baseline tips and allow bounded, model-supported risk."""
from __future__ import annotations

from dataclasses import dataclass
import numpy as np

from .half_life_study import TIPS, POINTS_TABLE, fast_tip
from .scoring import outcome_probabilities


@dataclass(frozen=True)
class LimitedRiskConfig:
    max_changes: int = 2
    min_base_probability: float = 0.20
    min_probability_edge: float = 0.05
    expected_points_budget: float = 1.0
    max_added_draws: int = 1


def tendency(tip: tuple[int, int]) -> int:
    return 0 if tip[0] > tip[1] else (1 if tip[0] == tip[1] else 2)


def limited_risk_tips(base_matrices: list[np.ndarray], signal_matrices: list[np.ndarray],
                      match_ids: list[str], config: LimitedRiskConfig | None = None
                      ) -> tuple[list[tuple[int, int]], dict]:
    """Selection accepts predictions only; actual scores cannot enter this API."""
    config = config or LimitedRiskConfig()
    if len(base_matrices) != 9 or len(signal_matrices) != 9 or len(match_ids) != 9:
        raise ValueError("Expected one complete nine-match round")
    if len(set(match_ids)) != 9:
        raise ValueError("Duplicate match IDs")
    if (not 0 <= config.max_changes <= 2 or config.expected_points_budget < 0
            or not 0 <= config.min_base_probability <= 1
            or not 0 <= config.min_probability_edge <= 1
            or not 0 <= config.max_added_draws <= 1):
        raise ValueError("Invalid risk limits")
    for matrix in [*base_matrices, *signal_matrices]:
        if (matrix.shape != (11, 11) or not np.isfinite(matrix).all()
                or np.any(matrix < 0) or not np.isclose(matrix.sum(), 1)):
            raise ValueError("Expected normalized 11x11 score matrices")
    baseline = [fast_tip(m) for m in base_matrices]
    candidates = []
    for i, (base, signal, tip) in enumerate(zip(base_matrices, signal_matrices, baseline)):
        probabilities = np.array(outcome_probabilities(base))
        signals = np.array(outcome_probabilities(signal))
        expected = POINTS_TABLE @ base.ravel()
        best_ep = float(expected[TIPS.index(tip)])
        for outcome in range(3):
            edge = float(signals[outcome] - probabilities[outcome])
            if (outcome == tendency(tip) or probabilities[outcome] < config.min_base_probability
                    or edge < config.min_probability_edge):
                continue
            ids = [j for j, t in enumerate(TIPS) if tendency(t) == outcome]
            best = max(ids, key=lambda j: (expected[j], base[TIPS[j]], -sum(TIPS[j]),
                                           -abs(TIPS[j][0] - TIPS[j][1])))
            cost = max(0.0, best_ep - float(expected[best]))
            candidates.append(dict(index=i, match_id=match_ids[i], tip=TIPS[best],
                                   base_probability=float(probabilities[outcome]),
                                   signal_probability=float(signals[outcome]),
                                   edge=edge, expected_points_cost=cost, adds_draw=outcome == 1))
    tips = list(baseline)
    changes = []
    used = set()
    total_cost = 0.0
    added_draws = 0
    for option in sorted(candidates, key=lambda x: (-x["edge"], x["expected_points_cost"],
                                                   x["match_id"], x["tip"])):
        if len(changes) >= config.max_changes:
            break
        if (option["index"] in used
                or total_cost + option["expected_points_cost"] > config.expected_points_budget + 1e-12
                or added_draws + int(option["adds_draw"]) > config.max_added_draws):
            continue
        tips[option["index"]] = option["tip"]
        used.add(option["index"])
        total_cost += option["expected_points_cost"]
        added_draws += int(option["adds_draw"])
        changes.append(option)
    return tips, dict(baseline_tips=baseline, changes=changes, changes_count=len(changes),
                      expected_points_cost=total_cost, added_draws=added_draws)
