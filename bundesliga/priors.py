from __future__ import annotations

from dataclasses import replace

import pandas as pd

from .config import ModelConfig
from .model import DynamicDixonColes


def lower_league_priors(
    target_model: DynamicDixonColes,
    lower_matches: pd.DataFrame,
    target_matches: pd.DataFrame,
    current_teams: set[str],
    as_of: pd.Timestamp,
    config: ModelConfig,
) -> dict[str, tuple[float, float]]:
    """Uebertraegt die aktuelle Zweitliga-Staerke frisch aufgestiegener Teams.

    Ein Team gilt als Aufsteiger, wenn es in den letzten 450 Tagen D2 spielte
    und danach weniger als ``promotion_prior_matches`` D1-Partien absolviert
    hat. Der D2-Prior wird ueber diese ersten D1-Spiele linear ausgeblendet.
    Die Abbildung in das Erstliga-Niveau ist explizit konfiguriert und damit
    backtestbar.
    """
    lower_before = lower_matches[
        lower_matches["match_date"].lt(as_of)
        & lower_matches["home_goals"].notna()
        & lower_matches["away_goals"].notna()
    ].copy()
    if len(lower_before) < config.min_training_matches:
        return {}
    lower_config = replace(
        config,
        use_lower_league_priors=False,
        form_weight=0.0,
        venue_form_weight=0.0,
        h2h_weight=0.0,
        market_value_weight=0.0,
    )
    lower_model = DynamicDixonColes(lower_config).fit(lower_before, as_of)
    history_columns = [
        "match_date", "home_team", "away_team", "home_goals", "away_goals"
    ]
    target_before = target_matches[
        target_matches["match_date"].lt(as_of)
        & target_matches["home_goals"].notna()
        & target_matches["away_goals"].notna()
    ][history_columns]
    priors: dict[str, tuple[float, float]] = {}
    for team in current_teams:
        lower_history = lower_before[
            lower_before["home_team"].eq(team) | lower_before["away_team"].eq(team)
        ].sort_values("match_date")
        if lower_history.empty:
            continue
        lower_last_date = pd.Timestamp(lower_history.iloc[-1]["match_date"])
        if (as_of - lower_last_date).days > 450:
            continue
        target_since_promotion = target_before[
            (target_before["match_date"] > lower_last_date)
            & (target_before["home_team"].eq(team) | target_before["away_team"].eq(team))
        ]
        target_games = len(target_since_promotion)
        if target_games >= config.promotion_prior_matches:
            continue
        if team not in lower_model.team_index:
            continue
        lower_attack, lower_defense = lower_model.team_parameters(team)
        mapped_attack = config.lower_league_scale * lower_attack + config.promotion_attack_penalty
        mapped_defense = config.lower_league_scale * lower_defense + config.promotion_defense_penalty
        target_attack, target_defense = target_model.team_parameters(team)
        blend = 1.0 - target_games / max(config.promotion_prior_matches, 1)
        priors[team] = (
            blend * mapped_attack + (1.0 - blend) * target_attack,
            blend * mapped_defense + (1.0 - blend) * target_defense,
        )
    target_model.set_external_team_parameters(priors)
    return priors
