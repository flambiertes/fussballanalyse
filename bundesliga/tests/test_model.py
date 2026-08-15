from __future__ import annotations

import numpy as np
import pandas as pd

from bundesliga.config import ModelConfig
from bundesliga.model import DynamicDixonColes
from bundesliga.predict import select_upcoming_matchdays


def synthetic_matches() -> pd.DataFrame:
    rng = np.random.default_rng(12)
    teams = ["Stark", "Mittel A", "Mittel B", "Schwach"]
    attack = {"Stark": 0.45, "Mittel A": 0.1, "Mittel B": 0.0, "Schwach": -0.4}
    defense = {"Stark": -0.25, "Mittel A": 0.0, "Mittel B": 0.1, "Schwach": 0.35}
    rows = []
    date = pd.Timestamp("2020-01-01")
    for cycle in range(35):
        for home in teams:
            for away in teams:
                if home == away:
                    continue
                lam_h = np.exp(0.15 + 0.2 + attack[home] + defense[away])
                lam_a = np.exp(0.15 + attack[away] + defense[home])
                rows.append(
                    {
                        "match_date": date,
                        "home_team": home,
                        "away_team": away,
                        "home_goals": rng.poisson(lam_h),
                        "away_goals": rng.poisson(lam_a),
                    }
                )
                date += pd.Timedelta(days=1)
    return pd.DataFrame(rows)


def test_model_fits_and_ranks_strong_team_higher():
    matches = synthetic_matches()
    config = ModelConfig(min_training_matches=100, lookback_years=10, half_life_days=2000)
    model = DynamicDixonColes(config).fit(matches, matches["match_date"].max() + pd.Timedelta(days=1))
    prediction = model.predict("Stark", "Schwach")
    assert prediction["lambda_home"] > prediction["lambda_away"]
    assert prediction["prob_home"] > prediction["prob_away"]
    assert -0.25 <= model.rho <= 0.15


def test_selects_exact_number_of_upcoming_matchdays():
    upcoming = pd.DataFrame(
        {
            "matchday": [1, 1, 2, 2, 3, 3, 4],
            "match_date": pd.date_range("2026-08-28", periods=7, freq="D"),
        }
    )
    selected = select_upcoming_matchdays(upcoming, 3)
    assert sorted(selected["matchday"].unique()) == [1, 2, 3]
    assert len(selected) == 6
