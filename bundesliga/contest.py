"""Experimental prize-oriented selection without access to other players' tips.

The crowd is an assumption, never observed CHECK24 data. Conditional on each
simulated set of results, a random opponent's score distribution is convolved
exactly. This preserves the dependence between our score and opponents' scores.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np

from .scoring import expected_points_tip, tip_points


@dataclass(frozen=True)
class ContestConfig:
    # Deliberately fixed scenarios, not fitted to the winning review screenshots.
    opponents: tuple[int, ...] = (100, 1000, 10000)
    crowd_concentrations: tuple[float, ...] = (1.0, 2.5, 5.0)
    training_samples: int = 8192
    evaluation_samples: int = 32768
    seed: int = 20260908
    max_passes: int = 4
    max_tip_goals: int = 5

    def validate(self) -> None:
        if not self.opponents or any(not isinstance(n, int) or n < 1 for n in self.opponents):
            raise ValueError("opponents muss positive ganze Zahlen enthalten")
        if not self.crowd_concentrations or any(
            not np.isfinite(beta) or beta < 0 for beta in self.crowd_concentrations
        ):
            raise ValueError("crowd_concentrations muss endlich und nichtnegativ sein")
        for name in ("training_samples", "evaluation_samples", "max_passes", "max_tip_goals"):
            value = getattr(self, name)
            if not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} muss eine positive ganze Zahl sein")


def prize_shares(distribution: np.ndarray, opponents: int) -> np.ndarray:
    """Expected 1/(1 + tied opponents) if nobody beats our score, for every score.

    Given actual results, opponents are iid draws from the assumed crowd.
    For L=P(opponent score<s), E=P(score=s), U=L+E, the share is
    (U**(N+1)-L**(N+1))/((N+1)*E). Equal splitting / a uniform tie lottery
    is an explicit modelling convention, not a claim about CHECK24 rules.
    """
    upper = np.clip(np.cumsum(distribution, axis=-1), 0.0, 1.0)
    shares = np.power(upper, opponents)
    mask = (distribution > 0) & (upper > 0)
    ratio = np.zeros_like(upper)
    # Use E/U directly. Computing L/U as (U-E)/U loses tiny tie masses to
    # cancellation and can incorrectly turn an almost certain win into zero.
    np.divide(distribution, upper, out=ratio, where=mask)
    ratio = np.clip(ratio, 0.0, 1.0)
    with np.errstate(divide="ignore", invalid="ignore"):
        tie_factor = -np.expm1((opponents + 1) * np.log1p(-ratio[mask]))
    # Divide the small terms before multiplying by U**N to avoid underflow.
    shares[mask] *= tie_factor / ((opponents + 1) * ratio[mask])
    return np.clip(shares, 0.0, 1.0)


def _tables(matrices: list[np.ndarray], config: ContestConfig):
    candidates = np.asarray([
        (h, a) for h in range(config.max_tip_goals + 1)
        for a in range(config.max_tip_goals + 1)
    ])
    points, expectations, candidate_probs = [], [], []
    for matrix in matrices:
        outcomes = list(np.ndindex(matrix.shape))
        table = np.asarray([
            [tip_points(tuple(tip), actual) for actual in outcomes]
            for tip in candidates
        ], dtype=np.int16)
        points.append(table)
        expectations.append(table @ matrix.ravel())
        candidate_probs.append(np.asarray([matrix[tuple(tip)] for tip in candidates]))
    return candidates, points, np.asarray(expectations), np.asarray(candidate_probs)


def _crowds(points, expectations, candidate_probs, config):
    """Q(tip) proportional to sqrt(P(exact score)) * exp(beta * E(points)).

    Higher beta concentrates the hypothetical crowd on conventional points tips.
    Each profile is a separate homogeneous crowd scenario. Match-level draws are
    independent; fan loyalties and correlations across a person's tips are absent.
    """
    all_distributions = []
    all_tip_probs = []
    for beta in config.crowd_concentrations:
        log_q = 0.5 * np.log(np.maximum(candidate_probs, 1e-300)) + beta * expectations
        q = np.exp(log_q - log_q.max(axis=1, keepdims=True))
        q[candidate_probs == 0] = 0
        q /= q.sum(axis=1, keepdims=True)
        # Per match and actual result, probabilities of the opponent scoring 0..4.
        all_distributions.append([
            np.stack([q[i] @ (table == score) for score in range(5)], axis=1)
            for i, table in enumerate(points)
        ])
        all_tip_probs.append(q)
    return all_distributions, np.mean(all_tip_probs, axis=0)


def _sample(matrices, points, crowds, config, count, rng):
    outcomes = [rng.choice(matrix.size, size=count, p=matrix.ravel()) for matrix in matrices]
    selected_points = np.asarray([table[:, actual] for table, actual in zip(points, outcomes)])
    utilities = []
    for crowd in crowds:
        total = np.ones((count, 1))
        for probabilities, actual in zip(crowd, outcomes):
            single = probabilities[actual]
            updated = np.zeros((count, total.shape[1] + 4))
            for score in range(5):
                updated[:, score:score + total.shape[1]] += total * single[:, score, None]
            total = updated
        total /= total.sum(axis=1, keepdims=True)
        utilities.extend(prize_shares(total, n) for n in config.opponents)
    return selected_points, np.asarray(utilities)


def _objective(means):
    # Equal-weight geometric mean prevents small assumed fields dominating solely
    # because their absolute prize shares are larger. It is not a worst-case bound.
    return np.mean(np.log(np.maximum(means, 1e-300)), axis=0)


def contest_tips(
    matrices: list[np.ndarray], config: ContestConfig | None = None,
) -> tuple[list[tuple[int, int]], dict[str, object]]:
    """Choose a whole portfolio using assumed crowds and independent validation.

    Multi-start coordinate search approximates the optimum on a simulation sample.
    The evaluation sample is never used to pick tips or tune scenario parameters.
    Returned numbers describe hypothetical scenarios, not actual winning chances.
    """
    config = config or ContestConfig()
    config.validate()
    if not matrices or len(matrices) > 9:
        raise ValueError("contest_tips benoetigt zwischen 1 und 9 Spiele")
    for matrix in matrices:
        if (
            matrix.ndim != 2 or min(matrix.shape) <= config.max_tip_goals
            or not np.all(np.isfinite(matrix)) or np.any(matrix < 0)
            or not np.isclose(matrix.sum(), 1.0, rtol=1e-9, atol=1e-12)
            or matrix[:config.max_tip_goals + 1, :config.max_tip_goals + 1].sum() <= 0
        ):
            raise ValueError("Ungueltige Ergebnismatrix oder zu kleiner Ergebnisraum")
    candidates, points, expectations, candidate_probs = _tables(matrices, config)
    crowds, mean_crowd = _crowds(points, expectations, candidate_probs, config)
    seed_train, seed_eval = np.random.SeedSequence(config.seed).spawn(2)
    sampled_points, utilities = _sample(
        matrices, points, crowds, config, config.training_samples,
        np.random.default_rng(seed_train),
    )
    baseline = np.asarray([
        list(map(tuple, candidates)).index(expected_points_tip(matrix, config.max_tip_goals))
        for matrix in matrices
    ])
    starts = [baseline, np.argmax(candidate_probs, axis=1)]
    for discount in (0.5, 1.0):
        starts.append(np.argmax(
            np.log(np.maximum(candidate_probs, 1e-300))
            - discount * np.log(np.maximum(mean_crowd, 1e-300)), axis=1,
        ))
    sample_ids = np.arange(config.training_samples)
    match_ids = np.arange(len(matrices))

    def values(total):
        return utilities[:, sample_ids, total].mean(axis=1)

    best = baseline.copy()
    best_value = float(_objective(values(sampled_points[match_ids, best].sum(axis=0))))
    seen = set()
    for start in starts:
        if tuple(start) in seen:
            continue
        seen.add(tuple(start))
        portfolio = start.copy()
        total = sampled_points[match_ids, portfolio].sum(axis=0)
        current_value = float(_objective(values(total)))
        for pass_index in range(config.max_passes):
            changed = False
            order = range(len(matrices)) if pass_index % 2 == 0 else reversed(range(len(matrices)))
            for i in order:
                other = total - sampled_points[i, portfolio[i]]
                alternatives = other[None, :] + sampled_points[i]
                means = utilities[:, sample_ids[None, :], alternatives].mean(axis=2)
                objectives = _objective(means)
                replacement = int(np.argmax(objectives))
                if objectives[replacement] > current_value + 1e-12:
                    portfolio[i] = replacement
                    total = alternatives[replacement]
                    current_value = float(objectives[replacement])
                    changed = True
            if not changed:
                break
        if current_value > best_value + 1e-12:
            best, best_value = portfolio.copy(), current_value

    evaluation_points, evaluation_utilities = _sample(
        matrices, points, crowds, config, config.evaluation_samples,
        np.random.default_rng(seed_eval),
    )
    evaluation_ids = np.arange(config.evaluation_samples)
    own_total = evaluation_points[match_ids, best].sum(axis=0)
    baseline_total = evaluation_points[match_ids, baseline].sum(axis=0)
    own_shares = evaluation_utilities[:, evaluation_ids, own_total]
    baseline_shares = evaluation_utilities[:, evaluation_ids, baseline_total]
    scenarios = []
    for i, (beta, n) in enumerate(
        (beta, n) for beta in config.crowd_concentrations for n in config.opponents
    ):
        difference = own_shares[i] - baseline_shares[i]
        scenarios.append({
            "assumed_opponents": n,
            "assumed_crowd_concentration": beta,
            "simulated_prize_share": float(own_shares[i].mean()),
            "baseline_simulated_prize_share": float(baseline_shares[i].mean()),
            "paired_difference_standard_error": float(
                difference.std(ddof=1) / np.sqrt(config.evaluation_samples)
            ) if config.evaluation_samples > 1 else None,
        })
    tips = [tuple(map(int, candidates[i])) for i in best]
    return tips, {
        "experimental": True,
        "crowd_source": "assumed; no observed participant data",
        "tie_convention": "equal prize share / uniform lottery",
        "search": "multi-start coordinate ascent; no global optimum guarantee",
        "config": asdict(config),
        "expected_points": float(expectations[match_ids, best].sum()),
        "baseline_expected_points": float(expectations[match_ids, baseline].sum()),
        "changed_tendencies": int(np.sum(
            np.sign(candidates[best, 0] - candidates[best, 1])
            != np.sign(candidates[baseline, 0] - candidates[baseline, 1])
        )),
        "training_log_objective": best_value,
        "evaluation_log_objective": float(_objective(own_shares.mean(axis=1))),
        "baseline_evaluation_log_objective": float(_objective(baseline_shares.mean(axis=1))),
        "scenarios": scenarios,
    }
