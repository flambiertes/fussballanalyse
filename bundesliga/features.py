from __future__ import annotations

import math

import numpy as np
import pandas as pd

from .config import ModelConfig


def _team_view(matches: pd.DataFrame, team: str) -> pd.DataFrame:
    home = matches[matches["home_team"].eq(team)].copy()
    home["goals_for"] = home["home_goals"]
    home["goals_against"] = home["away_goals"]
    home["venue"] = "home"
    home["opponent"] = home["away_team"]
    away = matches[matches["away_team"].eq(team)].copy()
    away["goals_for"] = away["away_goals"]
    away["goals_against"] = away["home_goals"]
    away["venue"] = "away"
    away["opponent"] = away["home_team"]
    return pd.concat([home, away], ignore_index=True).sort_values("match_date")


def _performance_score(team_matches: pd.DataFrame, limit: int, half_life: float) -> float:
    recent = team_matches.tail(limit).copy()
    if recent.empty:
        return 0.0
    goal_diff = recent["goals_for"].to_numpy(float) - recent["goals_against"].to_numpy(float)
    points = np.where(goal_diff > 0, 3.0, np.where(goal_diff == 0, 1.0, 0.0))
    # Punkte dominieren, Tordifferenz trennt deutliche von knappen Resultaten.
    raw = (points - 1.35) / 1.65 + 0.12 * np.clip(goal_diff, -3, 3)
    age = np.arange(len(recent) - 1, -1, -1, dtype=float)
    weights = 0.5 ** (age / max(half_life, 0.1))
    shrinkage = 3.0
    return float(np.sum(raw * weights) / (weights.sum() + shrinkage))


def recent_form(matches: pd.DataFrame, team: str, config: ModelConfig, venue: str | None = None) -> float:
    view = _team_view(matches, team)
    if venue is not None:
        view = view[view["venue"].eq(venue)]
    return _performance_score(view, config.form_matches, config.form_half_life_matches)


def h2h_score(
    matches: pd.DataFrame,
    home_team: str,
    away_team: str,
    as_of: pd.Timestamp,
    config: ModelConfig,
) -> float:
    direct = matches[
        (matches["home_team"].isin([home_team, away_team]))
        & (matches["away_team"].isin([home_team, away_team]))
    ].sort_values("match_date").tail(config.h2h_matches)
    if direct.empty:
        return 0.0
    scores = []
    weights = []
    for row in direct.itertuples(index=False):
        home_perspective = row.home_goals - row.away_goals
        team_diff = home_perspective if row.home_team == home_team else -home_perspective
        scores.append(float(np.clip(team_diff, -3, 3)) / 3.0)
        days = max((as_of - pd.Timestamp(row.match_date)).days, 0)
        weights.append(0.5 ** (days / config.h2h_half_life_days))
    # Direkte Duelle sind eine kleine Stichprobe und werden stark zu null gezogen.
    return float(np.dot(scores, weights) / (sum(weights) + 4.0))


def market_log_ratio(market_values: pd.DataFrame, home_team: str, away_team: str) -> float:
    if market_values.empty:
        return 0.0
    values = market_values.set_index("team")["squad_value_eur"]
    if home_team not in values.index or away_team not in values.index:
        return 0.0
    home = float(values.loc[home_team])
    away = float(values.loc[away_team])
    if home <= 0 or away <= 0:
        return 0.0
    return float(np.clip(math.log(home / away), -2.5, 2.5))


def season_match_count(matches: pd.DataFrame, team: str, as_of: pd.Timestamp) -> int:
    season_start_year = as_of.year if as_of.month >= 7 else as_of.year - 1
    season_start = pd.Timestamp(year=season_start_year, month=7, day=1)
    current = matches[
        matches["match_date"].ge(season_start)
        & matches["match_date"].lt(as_of)
        & (matches["home_team"].eq(team) | matches["away_team"].eq(team))
    ]
    return len(current)


def feature_adjustments(
    matches: pd.DataFrame,
    home_team: str,
    away_team: str,
    as_of: pd.Timestamp,
    config: ModelConfig,
    market_values: pd.DataFrame | None = None,
) -> tuple[float, float, dict[str, float]]:
    home_form = recent_form(matches, home_team, config)
    away_form = recent_form(matches, away_team, config)
    home_venue_form = recent_form(matches, home_team, config, venue="home")
    away_venue_form = recent_form(matches, away_team, config, venue="away")
    h2h = h2h_score(matches, home_team, away_team, as_of, config)
    market = market_log_ratio(
        market_values if market_values is not None else pd.DataFrame(), home_team, away_team
    )
    home_season_matches = season_match_count(matches, home_team, as_of)
    away_season_matches = season_match_count(matches, away_team, as_of)
    decay_matches = max(config.market_decay_matches, 1)
    home_market_share = max(0.0, 1.0 - home_season_matches / decay_matches)
    away_market_share = max(0.0, 1.0 - away_season_matches / decay_matches)
    market_decay = 0.5 * (home_market_share + away_market_share)

    form_home_adj = config.form_weight * (home_form - 0.5 * away_form)
    form_away_adj = config.form_weight * (away_form - 0.5 * home_form)
    venue_home_adj = config.venue_form_weight * home_venue_form
    venue_away_adj = config.venue_form_weight * away_venue_form
    h2h_adj = config.h2h_weight * h2h
    market_adj = 0.5 * config.market_value_weight * market * market_decay
    home_adjustment = form_home_adj + venue_home_adj + h2h_adj + market_adj
    away_adjustment = form_away_adj + venue_away_adj - h2h_adj - market_adj
    details = {
        "home_form": home_form,
        "away_form": away_form,
        "home_venue_form": home_venue_form,
        "away_venue_form": away_venue_form,
        "h2h": h2h,
        "market_log_ratio": market,
        "market_decay": market_decay,
        "home_season_matches": float(home_season_matches),
        "away_season_matches": float(away_season_matches),
        "home_log_adjustment": home_adjustment,
        "away_log_adjustment": away_adjustment,
    }
    return home_adjustment, away_adjustment, details
