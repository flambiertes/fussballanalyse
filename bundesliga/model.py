from __future__ import annotations

from dataclasses import asdict

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from .config import ModelConfig
from .features import feature_adjustments
from .scoring import (
    blend_outcome_probabilities,
    expected_points_tip,
    outcome_probabilities,
    score_matrix,
)


MODEL_VERSION = "dynamic-dixon-coles-0.2"


class DynamicDixonColes:
    def __init__(self, config: ModelConfig | None = None):
        self.config = config or ModelConfig()
        self.teams: list[str] = []
        self.team_index: dict[str, int] = {}
        self.attack = np.array([])
        self.defense = np.array([])
        self.intercept = 0.0
        self.home_advantage = 0.0
        self.rho = 0.0
        self.as_of: pd.Timestamp | None = None
        self.training_matches = pd.DataFrame()
        self.external_team_parameters: dict[str, tuple[float, float]] = {}
        self.optimization_result = None

    def fit(self, matches: pd.DataFrame, as_of: str | pd.Timestamp) -> "DynamicDixonColes":
        as_of = pd.Timestamp(as_of)
        finished = matches.dropna(subset=["home_goals", "away_goals"]).copy()
        finished["match_date"] = pd.to_datetime(finished["match_date"])
        cutoff = as_of - pd.Timedelta(days=365.25 * self.config.lookback_years)
        training = finished[(finished["match_date"] < as_of) & (finished["match_date"] >= cutoff)].copy()
        if len(training) < self.config.min_training_matches:
            raise ValueError(
                f"Nur {len(training)} Trainingsspiele vor {as_of.date()}, "
                f"mindestens {self.config.min_training_matches} erforderlich."
            )

        teams = sorted(set(training["home_team"]) | set(training["away_team"]))
        team_index = {team: index for index, team in enumerate(teams)}
        n = len(teams)
        home_idx = training["home_team"].map(team_index).to_numpy(int)
        away_idx = training["away_team"].map(team_index).to_numpy(int)
        goals_home = training["home_goals"].to_numpy(float)
        goals_away = training["away_goals"].to_numpy(float)
        days_ago = (as_of - training["match_date"]).dt.total_seconds().to_numpy() / 86400.0
        weights = 0.5 ** (days_ago / self.config.half_life_days)
        weights *= len(weights) / max(weights.sum(), 1e-12)
        counts = np.bincount(np.concatenate([home_idx, away_idx]), minlength=n).astype(float)
        ridge_weights = 1.0 / np.sqrt(np.maximum(counts, 1.0))

        mean_home = max(float(np.average(goals_home, weights=weights)), 0.2)
        mean_away = max(float(np.average(goals_away, weights=weights)), 0.2)
        x0 = np.zeros(2 * n + 3)
        x0[2 * n] = np.log(mean_away)
        x0[2 * n + 1] = np.log(mean_home / mean_away)
        x0[2 * n + 2] = -0.05

        def objective(params: np.ndarray) -> tuple[float, np.ndarray]:
            attack = params[:n]
            defense = params[n:2 * n]
            intercept = params[2 * n]
            home_advantage = params[2 * n + 1]
            rho = params[2 * n + 2]
            raw_home = intercept + home_advantage + attack[home_idx] + defense[away_idx]
            raw_away = intercept + attack[away_idx] + defense[home_idx]
            log_home = np.clip(raw_home, -5.0, 4.0)
            log_away = np.clip(raw_away, -5.0, 4.0)
            lambda_home = np.exp(log_home)
            lambda_away = np.exp(log_away)

            dc_log = np.zeros(len(training))
            dc_home = np.zeros(len(training))
            dc_away = np.zeros(len(training))
            dc_rho = np.zeros(len(training))
            masks = {
                "00": (goals_home == 0) & (goals_away == 0),
                "01": (goals_home == 0) & (goals_away == 1),
                "10": (goals_home == 1) & (goals_away == 0),
                "11": (goals_home == 1) & (goals_away == 1),
            }
            tau = np.ones(len(training))
            tau[masks["00"]] = 1 - lambda_home[masks["00"]] * lambda_away[masks["00"]] * rho
            tau[masks["01"]] = 1 + lambda_home[masks["01"]] * rho
            tau[masks["10"]] = 1 + lambda_away[masks["10"]] * rho
            tau[masks["11"]] = 1 - rho
            if np.any(tau <= 1e-8):
                return 1e12 + float(np.square(np.minimum(tau, 0)).sum()) * 1e9, np.zeros_like(params)
            dc_log = np.log(tau)
            dc_home[masks["00"]] = -lambda_home[masks["00"]] * lambda_away[masks["00"]] * rho / tau[masks["00"]]
            dc_away[masks["00"]] = dc_home[masks["00"]]
            dc_rho[masks["00"]] = -lambda_home[masks["00"]] * lambda_away[masks["00"]] / tau[masks["00"]]
            dc_home[masks["01"]] = lambda_home[masks["01"]] * rho / tau[masks["01"]]
            dc_rho[masks["01"]] = lambda_home[masks["01"]] / tau[masks["01"]]
            dc_away[masks["10"]] = lambda_away[masks["10"]] * rho / tau[masks["10"]]
            dc_rho[masks["10"]] = lambda_away[masks["10"]] / tau[masks["10"]]
            dc_rho[masks["11"]] = -1.0 / tau[masks["11"]]

            log_likelihood = weights * (
                goals_home * log_home - lambda_home
                + goals_away * log_away - lambda_away + dc_log
            )
            ridge = self.config.ridge * np.sum((attack ** 2 + defense ** 2) * ridge_weights)
            value = -float(log_likelihood.sum()) + ridge

            active_home = ((raw_home > -5.0) & (raw_home < 4.0)).astype(float)
            active_away = ((raw_away > -5.0) & (raw_away < 4.0)).astype(float)
            residual_home = weights * (goals_home - lambda_home + dc_home) * active_home
            residual_away = weights * (goals_away - lambda_away + dc_away) * active_away
            grad_attack = np.zeros(n)
            grad_defense = np.zeros(n)
            np.add.at(grad_attack, home_idx, -residual_home)
            np.add.at(grad_defense, away_idx, -residual_home)
            np.add.at(grad_attack, away_idx, -residual_away)
            np.add.at(grad_defense, home_idx, -residual_away)
            grad_attack += 2 * self.config.ridge * attack * ridge_weights
            grad_defense += 2 * self.config.ridge * defense * ridge_weights
            gradient = np.concatenate(
                [
                    grad_attack,
                    grad_defense,
                    [-residual_home.sum() - residual_away.sum()],
                    [-residual_home.sum()],
                    [-float(np.sum(weights * dc_rho))],
                ]
            )
            return value, gradient

        bounds = (
            [(-2.5, 2.5)] * (2 * n)
            + [(-1.5, 1.5), (-0.3, 0.8), (-0.25, 0.15)]
        )
        result = minimize(
            objective,
            x0,
            jac=True,
            method="L-BFGS-B",
            bounds=bounds,
            options={"maxiter": 1200, "ftol": 1e-9, "maxls": 40},
        )
        if not result.success:
            raise RuntimeError(f"Dixon-Coles-Fit nicht konvergiert: {result.message}")

        self.teams = teams
        self.team_index = team_index
        self.attack = result.x[:n]
        self.defense = result.x[n:2 * n]
        self.intercept = float(result.x[2 * n])
        self.home_advantage = float(result.x[2 * n + 1])
        self.rho = float(result.x[2 * n + 2])
        self.as_of = as_of
        self.training_matches = training
        self.optimization_result = result
        return self

    def team_parameters(self, team: str) -> tuple[float, float]:
        if team in self.external_team_parameters:
            return self.external_team_parameters[team]
        index = self.team_index.get(team)
        if index is None:
            return 0.0, 0.0
        return float(self.attack[index]), float(self.defense[index])

    def set_external_team_parameters(
        self,
        parameters: dict[str, tuple[float, float]],
    ) -> None:
        self.external_team_parameters = dict(parameters)

    def predict(
        self,
        home_team: str,
        away_team: str,
        market_values: pd.DataFrame | None = None,
        bookmaker_probabilities: dict[str, float] | pd.Series | None = None,
    ) -> dict[str, object]:
        if self.as_of is None:
            raise RuntimeError("Modell muss vor predict() mit fit() trainiert werden")
        home_attack, home_defense = self.team_parameters(home_team)
        away_attack, away_defense = self.team_parameters(away_team)
        home_adjustment, away_adjustment, features = feature_adjustments(
            self.training_matches, home_team, away_team, self.as_of,
            self.config, market_values,
        )
        log_home = self.intercept + self.home_advantage + home_attack + away_defense + home_adjustment
        log_away = self.intercept + away_attack + home_defense + away_adjustment
        lambda_home = float(np.exp(np.clip(log_home, -4.0, 3.0)))
        lambda_away = float(np.exp(np.clip(log_away, -4.0, 3.0)))
        matrix = score_matrix(lambda_home, lambda_away, self.rho, self.config.max_goals)
        book_probabilities = None
        if bookmaker_probabilities is not None:
            values = tuple(
                float(bookmaker_probabilities.get(column, np.nan))
                for column in ("book_prob_home", "book_prob_draw", "book_prob_away")
            )
            if np.all(np.isfinite(values)):
                book_probabilities = values
        matrix = blend_outcome_probabilities(
            matrix, book_probabilities, self.config.bookmaker_weight
        )
        prob_home, prob_draw, prob_away = outcome_probabilities(matrix)
        tip_home, tip_away = expected_points_tip(matrix)
        features.update(
            {
                "book_prob_home": book_probabilities[0] if book_probabilities else np.nan,
                "book_prob_draw": book_probabilities[1] if book_probabilities else np.nan,
                "book_prob_away": book_probabilities[2] if book_probabilities else np.nan,
                "bookmaker_weight": self.config.bookmaker_weight if book_probabilities else 0.0,
            }
        )
        return {
            "lambda_home": lambda_home,
            "lambda_away": lambda_away,
            "prob_home": prob_home,
            "prob_draw": prob_draw,
            "prob_away": prob_away,
            "tip_home": tip_home,
            "tip_away": tip_away,
            "score_matrix": matrix,
            "features": features,
        }

    def metadata(self) -> dict[str, object]:
        return {
            "model_version": MODEL_VERSION,
            "as_of": self.as_of.isoformat() if self.as_of is not None else None,
            "intercept": self.intercept,
            "home_advantage": self.home_advantage,
            "rho": self.rho,
            "training_matches": len(self.training_matches),
            "config": asdict(self.config),
        }
