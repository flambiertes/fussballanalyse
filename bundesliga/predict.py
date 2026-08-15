from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import asdict, replace
from pathlib import Path

import pandas as pd

from .config import DB_PATH, ModelConfig
from .database import (
    consensus_bookmaker_probabilities,
    latest_market_values,
    load_matches,
    save_predictions,
)
from .model import MODEL_VERSION, DynamicDixonColes
from .priors import lower_league_priors


LIVE_CONFIG = replace(
    ModelConfig(),
    form_weight=0.10,
    h2h_weight=0.05,
    market_value_weight=0.05,
    bookmaker_weight=1.00,
    bookmaker_snapshot_type="captured",
    use_lower_league_priors=True,
)


def select_upcoming_matchdays(
    upcoming: pd.DataFrame,
    matchdays: int | None,
) -> pd.DataFrame:
    """Begrenzt kommende Spiele auf die naechsten N Spieltage; None bedeutet alle."""
    if matchdays is None:
        return upcoming
    if matchdays < 1:
        raise ValueError("matchdays muss mindestens 1 sein")
    if upcoming["matchday"].notna().any():
        next_matchdays = sorted(upcoming["matchday"].dropna().unique())[:matchdays]
        return upcoming[upcoming["matchday"].isin(next_matchdays)].copy()
    dates = pd.to_datetime(upcoming["match_date"])
    round_starts = dates.dt.normalize() - pd.to_timedelta((dates.dt.weekday - 1) % 7, unit="D")
    next_rounds = sorted(round_starts.unique())[:matchdays]
    return upcoming[round_starts.isin(next_rounds)].copy()


def predict_upcoming(
    competition: str,
    season: int,
    as_of: str | pd.Timestamp | None = None,
    config: ModelConfig | None = None,
    next_matchday_only: bool = True,
    matchdays: int | None = None,
    db_path: Path | str = DB_PATH,
    persist: bool = True,
) -> tuple[pd.DataFrame, dict[str, object]]:
    config = config or LIVE_CONFIG
    as_of = pd.Timestamp(as_of or pd.Timestamp.now())
    all_matches = load_matches(competition=competition, path=db_path)
    training = all_matches[
        all_matches["home_goals"].notna()
        & all_matches["away_goals"].notna()
        & (all_matches["match_date"] < as_of)
    ].copy()
    upcoming = all_matches[
        all_matches["season"].eq(season)
        & all_matches["home_goals"].isna()
        & (all_matches["match_date"] >= as_of)
    ].copy()
    if upcoming.empty:
        raise ValueError(f"Keine kommenden Spiele fuer {competition} {season}/{str(season + 1)[-2:]}")
    if matchdays is not None:
        upcoming = select_upcoming_matchdays(upcoming, matchdays)
    elif next_matchday_only:
        upcoming = select_upcoming_matchdays(upcoming, 1)

    model = DynamicDixonColes(config).fit(training, as_of)
    promoted_priors = {}
    if competition == "D1" and config.use_lower_league_priors:
        lower_matches = load_matches(competition="D2", finished_only=True, path=db_path)
        current_teams = set(upcoming["home_team"]) | set(upcoming["away_team"])
        promoted_priors = lower_league_priors(
            model, lower_matches, all_matches, current_teams, as_of, config
        )
    markets = latest_market_values(as_of, db_path)
    upcoming_odds = consensus_bookmaker_probabilities(
        upcoming["match_id"],
        snapshot_type=config.bookmaker_snapshot_type,
        as_of=as_of,
        path=db_path,
    ).set_index("match_id")
    rows = []
    for match in upcoming.sort_values("match_date").itertuples(index=False):
        bookmaker = (
            upcoming_odds.loc[match.match_id]
            if match.match_id in upcoming_odds.index else None
        )
        prediction = model.predict(
            match.home_team, match.away_team, markets,
            bookmaker_probabilities=bookmaker,
        )
        rows.append(
            {
                "match_id": match.match_id,
                "competition": competition,
                "season": season,
                "matchday": match.matchday,
                "match_date": match.match_date,
                "as_of": as_of,
                "home_team": match.home_team,
                "away_team": match.away_team,
                "lambda_home": prediction["lambda_home"],
                "lambda_away": prediction["lambda_away"],
                "prob_home": prediction["prob_home"],
                "prob_draw": prediction["prob_draw"],
                "prob_away": prediction["prob_away"],
                "tip_home": prediction["tip_home"],
                "tip_away": prediction["tip_away"],
                **prediction["features"],
                "home_external_prior": match.home_team in model.external_team_parameters,
                "away_external_prior": match.away_team in model.external_team_parameters,
            }
        )
    result = pd.DataFrame(rows)
    run_id = f"live-{competition.lower()}-{uuid.uuid4().hex[:12]}"
    if persist:
        save_predictions(result, run_id, MODEL_VERSION, asdict(config), db_path)
    return result, {
        "run_id": run_id,
        "promoted_team_priors": sorted(promoted_priors),
        **model.metadata(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Kommenden Bundesliga-Spieltag tippen")
    parser.add_argument("--league", choices=["D1", "D2"], default="D1")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--as-of")
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--all", action="store_true", help="Alle statt nur des naechsten Spieltags")
    scope.add_argument("--matchdays", type=int, help="Die naechsten N Spieltage")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    predictions, metadata = predict_upcoming(
        args.league, args.season, as_of=args.as_of,
        next_matchday_only=not args.all and args.matchdays is None,
        matchdays=args.matchdays,
    )
    display = predictions.copy()
    display["tipp"] = display["tip_home"].astype(str) + ":" + display["tip_away"].astype(str)
    print(
        display[["match_date", "home_team", "away_team", "tipp", "prob_home", "prob_draw", "prob_away"]]
        .to_string(index=False)
    )
    print(json.dumps(metadata, indent=2, ensure_ascii=False))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        if args.output.suffix.lower() == ".xlsx":
            predictions.to_excel(args.output, index=False)
        else:
            predictions.to_csv(args.output, index=False)


if __name__ == "__main__":
    main()
