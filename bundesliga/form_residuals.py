"""Opponent-adjusted form from archived, genuinely pre-match expectations."""
from __future__ import annotations

import numpy as np
import pandas as pd


def residual_form(history: pd.DataFrame, team: str, as_of: pd.Timestamp,
                  limit: int = 8, half_life: float = 3.5,
                  lookback_years: float = 6.0) -> float:
    if history.empty:
        return 0.0
    # Both filters matter: a forecast may exist for a postponed, unplayed match.
    past = history.loc[
        (pd.to_datetime(history.match_date) < as_of)
        & (pd.to_datetime(history.match_date) >= as_of - pd.Timedelta(days=365.25 * lookback_years))
        # Some historical kickoffs have only a date (midnight). Same-date
        # forecasts are valid: the fitting cutoff excludes all that day's scores.
        & (pd.to_datetime(history.as_of) <= pd.to_datetime(history.match_date))
        & (history.home_team.eq(team) | history.away_team.eq(team))
    ].dropna(subset=["home_goals", "away_goals", "base_home", "base_away"])
    past = past.sort_values(["match_date", "match_id"]).tail(limit)
    if past.empty:
        return 0.0
    sign = np.where(past.home_team.eq(team), 1.0, -1.0)
    residual = sign * ((past.home_goals - past.away_goals)
                       - (past.base_home - past.base_away))
    residual = np.clip(residual / np.sqrt(past.base_home + past.base_away), -3, 3)
    weights = 0.5 ** (np.arange(len(past) - 1, -1, -1) / half_life)
    return float(np.dot(residual, weights) / (weights.sum() + 3.0))
