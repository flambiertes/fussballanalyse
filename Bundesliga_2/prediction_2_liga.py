import argparse
import json
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import requests
from tqdm.auto import tqdm

PROJECT_ROOT = Path(__file__).resolve().parent
SOCCERDATA_BASE_DIR = PROJECT_ROOT / "data" / "soccerdata"
SOCCERDATA_CONFIG_DIR = SOCCERDATA_BASE_DIR / "config"
SOCCERDATA_CONFIG_DIR.mkdir(parents=True, exist_ok=True)

custom_league_dict_path = SOCCERDATA_CONFIG_DIR / "league_dict.json"
custom_league_entry = {
    "GER-2. Bundesliga": {
        "MatchHistory": "D2",
        "ESPN": "ger.2",
        "season_start": "Aug",
        "season_end": "May",
    }
}
if custom_league_dict_path.exists():
    existing_league_dict = json.loads(custom_league_dict_path.read_text(encoding="utf-8"))
else:
    existing_league_dict = {}

merged_league_dict = {**existing_league_dict, **custom_league_entry}
if "GER-2. Bundesliga" in existing_league_dict:
    merged_league_dict["GER-2. Bundesliga"] = {
        **existing_league_dict["GER-2. Bundesliga"],
        **custom_league_entry["GER-2. Bundesliga"],
    }

custom_league_dict_path.write_text(
    json.dumps(merged_league_dict, indent=2),
    encoding="utf-8",
)

os.environ["SOCCERDATA_DIR"] = str(SOCCERDATA_BASE_DIR)
os.environ.setdefault("SOCCERDATA_LOGLEVEL", "INFO")

try:
    import soccerdata as sd
except ImportError:
    sd = None


DATA_DIR = "data"
CURRENT_SEASON = 2025

# Diese Defaults erlauben den direkten Start ueber den VS-Code-Play-Button.
DEFAULT_REFRESH_CURRENT = True
DEFAULT_SIMULATIONS = 1000
DEFAULT_RUN_BACKTEST = False
DEFAULT_BACKTEST_CUTOFF = 25
SOCCERDATA_DATA_DIR = os.path.join(DATA_DIR, "soccerdata_cache")
ESPN_API = "http://site.api.espn.com/apis/site/v2/sports/soccer"


@dataclass
class LeagueAverages:
    home_goals: float
    away_goals: float
    per_team_goals: float


def infer_current_season_start(today: datetime = None) -> int:
    today = today or datetime.today()
    return today.year if today.month >= 7 else today.year - 1


def parse_date(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return pd.NaT
    return pd.to_datetime(value, errors="coerce", dayfirst=True)


def parse_result(result: str) -> Tuple[float, float, bool]:
    result = (result or "").strip()
    match = re.match(r"^(\d+)\s*[:\-]\s*(\d+)$", result)
    if match:
        return float(match.group(1)), float(match.group(2)), True
    return np.nan, np.nan, False


def require_soccerdata():
    if sd is None:
        raise ImportError(
            "Das Paket 'soccerdata' ist nicht installiert. "
            "Bitte installiere es einmal mit: pip install soccerdata"
        )


def parse_matchday(value) -> float:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return np.nan
    if isinstance(value, (int, np.integer)):
        return int(value)
    text = str(value).strip()
    match = re.search(r"(\d+)", text)
    return int(match.group(1)) if match else np.nan


def find_column(df: pd.DataFrame, candidates: List[str]) -> str:
    lower_map = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        column = lower_map.get(candidate.lower())
        if column is not None:
            return column
    return ""


def normalize_schedule_dataframe(raw_df: pd.DataFrame, season: int) -> pd.DataFrame:
    df = raw_df.copy()
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()
    else:
        df = df.reset_index(drop=False)

    date_col = find_column(df, ["date", "game_date"])
    home_col = find_column(df, ["home_team", "home", "hometeam", "squad"])
    away_col = find_column(df, ["away_team", "away", "awayteam", "opponent"])
    round_col = find_column(df, ["round", "week", "wk", "gameweek"])
    home_score_col = find_column(df, ["home_score", "home goals", "fthg", "gf"])
    away_score_col = find_column(df, ["away_score", "away goals", "ftag", "ga"])
    score_col = find_column(df, ["score", "result"])
    game_id_col = find_column(df, ["game_id", "match_id", "id"])
    league_id_col = find_column(df, ["league_id"])

    if not date_col or not home_col or not away_col:
        raise ValueError(f"Unerwartete Spalten fuer Spielplan: {list(df.columns)}")

    normalized = pd.DataFrame(
        {
            "season": season,
            "date": df[date_col].apply(parse_date),
            "home": df[home_col].astype(str).str.strip(),
            "away": df[away_col].astype(str).str.strip(),
        }
    )

    if round_col:
        normalized["matchday"] = df[round_col].apply(parse_matchday)
    else:
        normalized["matchday"] = np.nan

    if home_score_col and away_score_col:
        normalized["goals_home"] = pd.to_numeric(df[home_score_col], errors="coerce")
        normalized["goals_away"] = pd.to_numeric(df[away_score_col], errors="coerce")
        normalized["played"] = normalized["goals_home"].notna() & normalized["goals_away"].notna()
    elif score_col:
        parsed = df[score_col].apply(parse_result)
        normalized[["goals_home", "goals_away", "played"]] = pd.DataFrame(parsed.tolist(), index=df.index)
    else:
        normalized["goals_home"] = np.nan
        normalized["goals_away"] = np.nan
        normalized["played"] = False

    if game_id_col:
        normalized["game_id"] = pd.to_numeric(df[game_id_col], errors="coerce")
    if league_id_col:
        normalized["league_id"] = df[league_id_col].astype(str)

    normalized["source"] = "real"
    normalized = normalized[
        normalized["home"].ne("")
        & normalized["away"].ne("")
        & normalized["home"].str.lower().ne("nan")
        & normalized["away"].str.lower().ne("nan")
    ].copy()
    normalized = normalized.sort_values(["date", "matchday", "home", "away"]).reset_index(drop=True)

    if normalized["matchday"].isna().any():
        normalized = assign_matchdays_fallback(normalized)

    normalized["matchday"] = normalized["matchday"].astype(int)
    return normalized


def assign_matchdays_fallback(matches: pd.DataFrame) -> pd.DataFrame:
    matches = matches.copy()
    matches = matches.sort_values(["date", "home", "away"]).reset_index(drop=True)
    matchdays = []
    current_md = 1
    teams_seen = set()

    for _, row in matches.iterrows():
        home = row["home"]
        away = row["away"]
        if home in teams_seen or away in teams_seen:
            current_md += 1
            teams_seen = set()
        teams_seen.add(home)
        teams_seen.add(away)
        matchdays.append(current_md)

    matches["matchday"] = matches["matchday"].fillna(pd.Series(matchdays, index=matches.index))
    return matches


def enrich_espn_schedule_with_scores(raw_df: pd.DataFrame, timeout: int = 15) -> pd.DataFrame:
    df = raw_df.copy()
    if isinstance(df.index, pd.MultiIndex):
        df = df.reset_index()
    else:
        df = df.reset_index(drop=False)

    required_cols = {"game_id", "league_id", "date"}
    if not required_cols.issubset(df.columns):
        return raw_df

    if "home_score" not in df.columns:
        df["home_score"] = np.nan
    if "away_score" not in df.columns:
        df["away_score"] = np.nan

    now_utc = pd.Timestamp.now(tz="UTC")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    for idx, row in tqdm(
        df.iterrows(),
        total=len(df),
        desc="ESPN-Ergebnisse laden",
        leave=False,
    ):
        match_date = parse_date(row.get("date"))
        game_id = row.get("game_id")
        league_id = row.get("league_id")

        if pd.isna(match_date) or pd.isna(game_id) or not league_id:
            continue
        if match_date > now_utc + pd.Timedelta(days=1):
            continue

        try:
            response = session.get(
                f"{ESPN_API}/{league_id}/summary?event={int(game_id)}",
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()

            competition = data.get("header", {}).get("competitions", [{}])[0]
            competitors = competition.get("competitors", [])
            if len(competitors) != 2:
                continue

            home = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
            away = next((c for c in competitors if c.get("homeAway") == "away"), competitors[-1])

            home_score = home.get("score")
            away_score = away.get("score")
            if str(home_score).isdigit() and str(away_score).isdigit():
                df.at[idx, "home_score"] = int(home_score)
                df.at[idx, "away_score"] = int(away_score)
        except Exception:
            continue

    if isinstance(raw_df.index, pd.MultiIndex):
        index_names = list(raw_df.index.names)
        if all(name in df.columns for name in index_names):
            df = df.set_index(index_names)
    return df


def update_existing_schedule_scores(schedule_df: pd.DataFrame, timeout: int = 15) -> pd.DataFrame:
    df = schedule_df.copy()
    if "game_id" not in df.columns or "league_id" not in df.columns:
        return df

    if "played" not in df.columns:
        df["played"] = False
    if "goals_home" not in df.columns:
        df["goals_home"] = np.nan
    if "goals_away" not in df.columns:
        df["goals_away"] = np.nan

    now_utc = pd.Timestamp.now(tz="UTC")
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})

    pending_mask = (~df["played"].fillna(False)) & df["date"].apply(parse_date).le(now_utc + pd.Timedelta(days=1))
    pending_games = df[pending_mask].copy()

    for idx, row in tqdm(
        pending_games.iterrows(),
        total=len(pending_games),
        desc="Offene Spiele aktualisieren",
        leave=False,
    ):
        game_id = row.get("game_id")
        league_id = row.get("league_id")
        if pd.isna(game_id) or not league_id:
            continue

        try:
            response = session.get(
                f"{ESPN_API}/{league_id}/summary?event={int(game_id)}",
                timeout=timeout,
            )
            response.raise_for_status()
            data = response.json()
            competition = data.get("header", {}).get("competitions", [{}])[0]
            competitors = competition.get("competitors", [])
            if len(competitors) != 2:
                continue

            home = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
            away = next((c for c in competitors if c.get("homeAway") == "away"), competitors[-1])
            home_score = home.get("score")
            away_score = away.get("score")

            if str(home_score).isdigit() and str(away_score).isdigit():
                df.at[idx, "goals_home"] = int(home_score)
                df.at[idx, "goals_away"] = int(away_score)
                df.at[idx, "played"] = True
        except Exception:
            continue

    return df


def merge_schedule_data(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["season", "home", "away", "matchday"]
    existing = existing_df.copy()
    current = new_df.copy()

    for column in ["date", "goals_home", "goals_away", "played", "source", "game_id", "league_id"]:
        if column not in existing.columns:
            existing[column] = np.nan if column not in ["played", "source"] else (False if column == "played" else "real")
        if column not in current.columns:
            current[column] = np.nan if column not in ["played", "source"] else (False if column == "played" else "real")

    merged = existing.merge(current, on=key_cols, how="outer", suffixes=("_old", "_new"))
    resolved = pd.DataFrame({col: merged[col] for col in key_cols})

    for column in ["date", "game_id", "league_id", "source"]:
        resolved[column] = merged[f"{column}_new"].combine_first(merged[f"{column}_old"])

    for column in ["goals_home", "goals_away"]:
        resolved[column] = merged[f"{column}_new"].combine_first(merged[f"{column}_old"])

    old_played = merged["played_old"].fillna(False)
    new_played = merged["played_new"].fillna(False)
    resolved["played"] = new_played | old_played | (resolved["goals_home"].notna() & resolved["goals_away"].notna())
    resolved = resolved.sort_values(["date", "matchday", "home", "away"]).reset_index(drop=True)
    return resolved


def fetch_schedule_from_soccerdata(season: int, force_refresh: bool = False) -> pd.DataFrame:
    require_soccerdata()
    os.makedirs(DATA_DIR, exist_ok=True)
    cache_path = os.path.join(DATA_DIR, f"2bundesliga_{season}_schedule.csv")
    existing_cache = None
    if os.path.exists(cache_path):
        existing_cache = pd.read_csv(cache_path, parse_dates=["date"])
        if not force_refresh:
            updated_cache = update_existing_schedule_scores(existing_cache)
            updated_cache.to_csv(cache_path, index=False)
            return updated_cache

    source_errors = []
    raw_df = None

    sources = [
        (
            "espn",
            lambda: sd.ESPN(
                leagues="GER-2. Bundesliga",
                seasons=season,
                data_dir=Path(SOCCERDATA_DATA_DIR),
            ).read_schedule(force_cache=not force_refresh)
        ),
        (
            "matchhistory",
            lambda: sd.MatchHistory(
                leagues="GER-2. Bundesliga",
                seasons=season,
                data_dir=Path(SOCCERDATA_DATA_DIR),
            ).read_games()
        ),
    ]

    for source_name, loader in sources:
        try:
            raw_df = loader()
            if raw_df is not None and not raw_df.empty:
                if source_name == "espn":
                    raw_df = enrich_espn_schedule_with_scores(raw_df)
                break
        except Exception as exc:
            source_errors.append(f"{source_name}: {exc}")
            raw_df = None

    if raw_df is None or raw_df.empty:
        raise RuntimeError(
            "Die aktuelle Saison konnte ueber soccerdata nicht geladen werden. "
            f"Fehler: {' | '.join(source_errors) if source_errors else 'keine Datenquelle lieferte Daten'}"
        )

    normalized = normalize_schedule_dataframe(raw_df, season)
    if existing_cache is not None:
        normalized = merge_schedule_data(existing_cache, normalized)
        normalized = update_existing_schedule_scores(normalized)
    normalized.to_csv(cache_path, index=False)
    return normalized


def archive_previous_season_if_missing(current_season: int, force_refresh: bool = False):
    previous_season = current_season - 1
    path = os.path.join(DATA_DIR, f"2bundesliga_{previous_season}.csv")
    if os.path.exists(path) and not force_refresh:
        return

    previous_schedule = fetch_schedule_from_soccerdata(previous_season, force_refresh=force_refresh)
    previous_played = previous_schedule[previous_schedule["played"]].copy()
    previous_played = previous_played[["season", "matchday", "home", "away", "goals_home", "goals_away"]]
    previous_played["goals_home"] = previous_played["goals_home"].astype(int)
    previous_played["goals_away"] = previous_played["goals_away"].astype(int)
    previous_played.to_csv(path, index=False)


def fetch_current_season_schedule(
    season: int = CURRENT_SEASON,
    force_refresh: bool = False,
) -> pd.DataFrame:
    archive_previous_season_if_missing(season, force_refresh=False)
    return fetch_schedule_from_soccerdata(season, force_refresh=force_refresh)


def load_historical_data(start_season: int = 1994, end_season: int = 2024) -> pd.DataFrame:
    frames = []
    for season in range(start_season, end_season + 1):
        path = os.path.join(DATA_DIR, f"2bundesliga_{season}.csv")
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path)
        df["season"] = season
        df["played"] = True
        df["source"] = "real"
        frames.append(df)

    if not frames:
        raise FileNotFoundError("Keine historischen CSV-Dateien gefunden.")

    return pd.concat(frames, ignore_index=True)


def compute_table(matches: pd.DataFrame) -> pd.DataFrame:
    played = matches[matches["played"]].copy()
    teams = np.sort(list(set(played["home"]) | set(played["away"])))
    if len(teams) == 0:
        return pd.DataFrame(columns=["team", "points", "goals_for", "goals_against", "gd", "played", "rank"])

    points = {team: 0 for team in teams}
    goals_for = {team: 0 for team in teams}
    goals_against = {team: 0 for team in teams}
    played_count = {team: 0 for team in teams}

    for _, row in played.iterrows():
        home = row["home"]
        away = row["away"]
        goals_home = int(row["goals_home"])
        goals_away = int(row["goals_away"])

        goals_for[home] += goals_home
        goals_against[home] += goals_away
        goals_for[away] += goals_away
        goals_against[away] += goals_home
        played_count[home] += 1
        played_count[away] += 1

        if goals_home > goals_away:
            points[home] += 3
        elif goals_away > goals_home:
            points[away] += 3
        else:
            points[home] += 1
            points[away] += 1

    table = pd.DataFrame(
        {
            "team": teams,
            "points": [points[team] for team in teams],
            "goals_for": [goals_for[team] for team in teams],
            "goals_against": [goals_against[team] for team in teams],
            "played": [played_count[team] for team in teams],
        }
    )
    table["gd"] = table["goals_for"] - table["goals_against"]
    table = table.sort_values(
        by=["points", "gd", "goals_for", "team"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    table["rank"] = np.arange(1, len(table) + 1)
    return table


def compute_league_averages(matches: pd.DataFrame) -> LeagueAverages:
    played = matches[matches["played"]].copy()
    if played.empty:
        return LeagueAverages(home_goals=1.4, away_goals=1.2, per_team_goals=1.3)

    home_goals = played["goals_home"].mean()
    away_goals = played["goals_away"].mean()
    return LeagueAverages(
        home_goals=float(home_goals),
        away_goals=float(away_goals),
        per_team_goals=float((home_goals + away_goals) / 2.0),
    )


def weighted_mean(values: pd.Series, weights: pd.Series, fallback: float) -> float:
    if values.empty or weights.sum() <= 0:
        return fallback
    return float(np.average(values, weights=weights))


def get_recent_form_factor(team_matches: pd.DataFrame, league_avg_points: float, recent_matches: int = 5) -> float:
    if team_matches.empty:
        return 1.0

    recent = team_matches.sort_values(["matchday", "date"]).tail(recent_matches).copy()
    recent["team_points"] = 0

    win = recent["goals_for"] > recent["goals_against"]
    draw = recent["goals_for"] == recent["goals_against"]

    recent.loc[win, "team_points"] = 3
    recent.loc[draw, "team_points"] = 1

    points_per_match = recent["team_points"].mean()
    delta = points_per_match - league_avg_points
    return float(np.clip(1.0 + 0.12 * delta, 0.8, 1.2))


def compute_team_strengths(
    matches: pd.DataFrame,
    simulated_weight: float = 0.35,
    shrinkage_matches: float = 5.0,
    recent_matches: int = 5,
) -> Tuple[pd.DataFrame, LeagueAverages]:
    played = matches[matches["played"]].copy()
    if played.empty:
        raise ValueError("Keine gespielten Partien zum Schätzen der Teamstärken vorhanden.")

    played["weight"] = np.where(played["source"].eq("sim"), simulated_weight, 1.0)

    teams = np.sort(list(set(played["home"]) | set(played["away"])))
    league = compute_league_averages(played)

    home_rows = played.rename(
        columns={
            "home": "team",
            "away": "opponent",
            "goals_home": "goals_for",
            "goals_away": "goals_against",
        }
    )[["team", "opponent", "goals_for", "goals_against", "weight", "matchday", "date"]].copy()
    home_rows["venue"] = "home"

    away_rows = played.rename(
        columns={
            "away": "team",
            "home": "opponent",
            "goals_away": "goals_for",
            "goals_home": "goals_against",
        }
    )[["team", "opponent", "goals_for", "goals_against", "weight", "matchday", "date"]].copy()
    away_rows["venue"] = "away"

    team_rows = pd.concat([home_rows, away_rows], ignore_index=True)
    league_avg_points = played.assign(
        points_home=np.select(
            [played["goals_home"] > played["goals_away"], played["goals_home"] == played["goals_away"]],
            [3, 1],
            default=0,
        ),
        points_away=np.select(
            [played["goals_away"] > played["goals_home"], played["goals_away"] == played["goals_home"]],
            [3, 1],
            default=0,
        ),
    )
    league_avg_points = float(
        (league_avg_points["points_home"].sum() + league_avg_points["points_away"].sum()) / (2 * len(played))
    )

    strength_rows = []
    for team in teams:
        team_matches = team_rows[team_rows["team"] == team].copy()
        home_matches = team_matches[team_matches["venue"] == "home"]
        away_matches = team_matches[team_matches["venue"] == "away"]

        total_games = len(team_matches)
        home_games = len(home_matches)
        away_games = len(away_matches)

        home_attack_raw = weighted_mean(home_matches["goals_for"], home_matches["weight"], league.home_goals)
        away_attack_raw = weighted_mean(away_matches["goals_for"], away_matches["weight"], league.away_goals)
        home_def_raw = weighted_mean(home_matches["goals_against"], home_matches["weight"], league.away_goals)
        away_def_raw = weighted_mean(away_matches["goals_against"], away_matches["weight"], league.home_goals)

        home_attack = (home_attack_raw * home_games + league.home_goals * shrinkage_matches) / (home_games + shrinkage_matches)
        away_attack = (away_attack_raw * away_games + league.away_goals * shrinkage_matches) / (away_games + shrinkage_matches)
        home_def = (home_def_raw * home_games + league.away_goals * shrinkage_matches) / (home_games + shrinkage_matches)
        away_def = (away_def_raw * away_games + league.home_goals * shrinkage_matches) / (away_games + shrinkage_matches)

        form_factor = get_recent_form_factor(team_matches, league_avg_points, recent_matches=recent_matches)

        strength_rows.append(
            {
                "team": team,
                "matches_played": total_games,
                "home_attack": home_attack / league.home_goals if league.home_goals else 1.0,
                "away_attack": away_attack / league.away_goals if league.away_goals else 1.0,
                "home_defense": home_def / league.away_goals if league.away_goals else 1.0,
                "away_defense": away_def / league.home_goals if league.home_goals else 1.0,
                "form_factor": form_factor,
            }
        )

    strengths = pd.DataFrame(strength_rows)
    return strengths, league


def expected_goals(
    home_team: str,
    away_team: str,
    strengths: pd.DataFrame,
    league: LeagueAverages,
) -> Tuple[float, float]:
    strength_map = strengths.set_index("team")
    home = strength_map.loc[home_team]
    away = strength_map.loc[away_team]

    lambda_home = league.home_goals * home["home_attack"] * away["away_defense"]
    lambda_away = league.away_goals * away["away_attack"] * home["home_defense"]

    lambda_home *= 0.75 * home["form_factor"] + 0.25 * away["form_factor"]
    lambda_away *= 0.75 * away["form_factor"] + 0.25 * home["form_factor"]

    return float(np.clip(lambda_home, 0.2, 4.0)), float(np.clip(lambda_away, 0.2, 4.0))


def simulate_match(lambda_home: float, lambda_away: float, rng: np.random.Generator) -> Tuple[int, int]:
    return int(rng.poisson(lambda_home)), int(rng.poisson(lambda_away))


def poisson_pmf(k: int, lam: float) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return float(np.exp(-lam) * (lam ** k) / math.factorial(k))


def match_prediction_table(
    season_matches: pd.DataFrame,
    max_goals: int = 8,
) -> pd.DataFrame:
    played = season_matches[season_matches["played"]].copy()
    remaining = season_matches[~season_matches["played"]].copy()
    if remaining.empty:
        return pd.DataFrame()

    strengths, league = compute_team_strengths(played)
    rows = []

    for _, row in remaining.sort_values(["matchday", "date", "home", "away"]).iterrows():
        lambda_home, lambda_away = expected_goals(row["home"], row["away"], strengths, league)
        probs_home = [poisson_pmf(goals, lambda_home) for goals in range(max_goals + 1)]
        probs_away = [poisson_pmf(goals, lambda_away) for goals in range(max_goals + 1)]

        home_win_prob = 0.0
        draw_prob = 0.0
        away_win_prob = 0.0
        best_score = None
        best_score_prob = -1.0

        for home_goals in range(max_goals + 1):
            for away_goals in range(max_goals + 1):
                prob = probs_home[home_goals] * probs_away[away_goals]
                if prob > best_score_prob:
                    best_score_prob = prob
                    best_score = f"{home_goals}:{away_goals}"
                if home_goals > away_goals:
                    home_win_prob += prob
                elif home_goals == away_goals:
                    draw_prob += prob
                else:
                    away_win_prob += prob

        rows.append(
            {
                "matchday": row["matchday"],
                "date": row["date"],
                "home": row["home"],
                "away": row["away"],
                "lambda_home": lambda_home,
                "lambda_away": lambda_away,
                "home_win_prob": home_win_prob,
                "draw_prob": draw_prob,
                "away_win_prob": away_win_prob,
                "most_likely_score": best_score,
                "most_likely_score_prob": best_score_prob,
            }
        )

    return pd.DataFrame(rows)


def simulate_season(
    season_matches: pd.DataFrame,
    n_sims: int = 10000,
    random_seed: int = 42,
    show_progress: bool = True,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    season_matches = season_matches.copy()
    season_matches["played"] = season_matches["played"].astype(bool)
    season_matches["source"] = season_matches["source"].fillna("real")

    played = season_matches[season_matches["played"]].copy()
    remaining = season_matches[~season_matches["played"]].copy()
    remaining = remaining.sort_values(["matchday", "date", "home", "away"]).reset_index(drop=True)

    if remaining.empty:
        final_table = compute_table(played)
        summary = final_table[["team", "points", "rank"]].copy()
        summary["top2_prob"] = (summary["rank"] <= 2).astype(float)
        summary["top3_prob"] = (summary["rank"] <= 3).astype(float)
        rank_matrix = final_table[["team", "rank"]].copy()
        rank_matrix["probability"] = 1.0
        rank_matrix = rank_matrix.pivot(index="team", columns="rank", values="probability").reset_index()
        points_distribution = final_table[["team", "points"]].copy()
        points_distribution["probability"] = 1.0
        return summary, pd.DataFrame(), rank_matrix, points_distribution

    rng = np.random.default_rng(random_seed)
    placements = []
    point_totals = []

    simulation_iterator = range(n_sims)
    if show_progress:
        simulation_iterator = tqdm(simulation_iterator, total=n_sims, desc="Saison simulieren")

    for sim_id in simulation_iterator:
        sim_played = played.copy()
        for matchday in sorted(remaining["matchday"].dropna().unique()):
            strengths, league = compute_team_strengths(sim_played)
            md_matches = remaining[remaining["matchday"] == matchday].copy()
            simulated_rows = []

            for _, row in md_matches.iterrows():
                lambda_home, lambda_away = expected_goals(row["home"], row["away"], strengths, league)
                goals_home, goals_away = simulate_match(lambda_home, lambda_away, rng)
                simulated_rows.append(
                    {
                        "season": row["season"],
                        "matchday": int(matchday),
                        "date": row["date"],
                        "home": row["home"],
                        "away": row["away"],
                        "goals_home": goals_home,
                        "goals_away": goals_away,
                        "played": True,
                        "source": "sim",
                    }
                )

            if simulated_rows:
                sim_played = pd.concat([sim_played, pd.DataFrame(simulated_rows)], ignore_index=True)

        final_table = compute_table(sim_played)
        final_table["simulation"] = sim_id
        placements.append(final_table[["simulation", "team", "rank"]])
        point_totals.append(final_table[["simulation", "team", "points"]])

    placement_df = pd.concat(placements, ignore_index=True)
    points_df = pd.concat(point_totals, ignore_index=True)

    summary = (
        placement_df.groupby("team")["rank"]
        .agg(
            avg_rank="mean",
            median_rank="median",
            top2_prob=lambda s: (s <= 2).mean(),
            top3_prob=lambda s: (s <= 3).mean(),
        )
        .reset_index()
    )

    expected_points = points_df.groupby("team")["points"].mean().reset_index(name="expected_points")
    summary = summary.merge(expected_points, on="team", how="left")

    current_table = compute_table(played)[["team", "points", "rank"]].rename(
        columns={"points": "current_points", "rank": "current_rank"}
    )
    summary = summary.merge(current_table, on="team", how="left")
    most_likely_rank = (
        placement_df.groupby(["team", "rank"])
        .size()
        .reset_index(name="count")
        .sort_values(["team", "count", "rank"], ascending=[True, False, True])
        .drop_duplicates("team")
        .rename(columns={"rank": "most_likely_rank"})
        [["team", "most_likely_rank"]]
    )
    summary = summary.merge(most_likely_rank, on="team", how="left")
    summary = summary.sort_values(["top2_prob", "top3_prob", "expected_points"], ascending=False).reset_index(drop=True)

    placement_distribution = (
        placement_df.groupby(["team", "rank"])
        .size()
        .div(n_sims)
        .reset_index(name="probability")
        .sort_values(["team", "rank"])
        .reset_index(drop=True)
    )

    rank_matrix = (
        placement_distribution.pivot(index="team", columns="rank", values="probability")
        .fillna(0.0)
        .reset_index()
    )
    rank_matrix.columns = ["team"] + [f"rank_{int(col)}" for col in rank_matrix.columns[1:]]

    points_distribution = (
        points_df.groupby(["team", "points"])
        .size()
        .div(n_sims)
        .reset_index(name="probability")
        .sort_values(["team", "points"])
        .reset_index(drop=True)
    )

    return summary, placement_distribution, rank_matrix, points_distribution


def backtest_cutoff(
    historical_matches: pd.DataFrame,
    cutoff_matchday: int = 25,
    n_sims: int = 2000,
    seasons: List[int] = None,
    show_progress: bool = True,
) -> pd.DataFrame:
    if seasons is None:
        seasons = sorted(historical_matches["season"].unique())

    rows = []
    season_iterator = seasons
    if show_progress:
        season_iterator = tqdm(seasons, desc="Backtest-Saisons")

    for season in season_iterator:
        season_df = historical_matches[historical_matches["season"] == season].copy()
        if season_df["matchday"].max() < 34 or cutoff_matchday >= season_df["matchday"].max():
            continue

        observed = season_df[season_df["matchday"] <= cutoff_matchday].copy()
        future = season_df[season_df["matchday"] > cutoff_matchday].copy()
        future["played"] = False
        simulation_input = pd.concat([observed, future], ignore_index=True)

        summary, _, _, _ = simulate_season(
            simulation_input,
            n_sims=n_sims,
            random_seed=season + cutoff_matchday,
            show_progress=False,
        )
        actual_table = compute_table(season_df)[["team", "rank"]].rename(columns={"rank": "actual_rank"})
        merged = summary.merge(actual_table, on="team", how="left")
        merged["season"] = season
        rows.append(merged)

    if not rows:
        return pd.DataFrame()

    backtest = pd.concat(rows, ignore_index=True)
    backtest["actual_top2"] = (backtest["actual_rank"] <= 2).astype(int)
    backtest["brier_top2"] = (backtest["top2_prob"] - backtest["actual_top2"]) ** 2
    return backtest


def get_latest_completed_matchday(season_matches: pd.DataFrame) -> int:
    if season_matches.empty or "matchday" not in season_matches.columns:
        return 0

    completed_matchdays = []
    grouped = season_matches.groupby("matchday", dropna=True)

    for matchday, matches in grouped:
        if matches.empty:
            continue
        if matches["played"].fillna(False).all():
            completed_matchdays.append(int(matchday))

    return max(completed_matchdays, default=0)


def save_prediction_outputs(
    summary: pd.DataFrame,
    placement_distribution: pd.DataFrame,
    rank_matrix: pd.DataFrame,
    points_distribution: pd.DataFrame,
    match_predictions: pd.DataFrame,
    output_prefix: str = "2_bundesliga_prediction",
) -> str:
    os.makedirs(DATA_DIR, exist_ok=True)
    output_path = os.path.join(DATA_DIR, f"{output_prefix}.xlsx")

    if "date" in match_predictions.columns:
        match_predictions = match_predictions.copy()
        match_predictions["date"] = pd.to_datetime(match_predictions["date"], errors="coerce")
        try:
            match_predictions["date"] = match_predictions["date"].dt.tz_localize(None)
        except TypeError:
            pass

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="summary", index=False)
        rank_matrix.to_excel(writer, sheet_name="rank_matrix", index=False)
        points_distribution.to_excel(writer, sheet_name="points_distribution", index=False)
        placement_distribution.to_excel(writer, sheet_name="rank_distribution", index=False)
        if not match_predictions.empty:
            match_predictions.to_excel(writer, sheet_name="match_predictions", index=False)

    return output_path


def main():
    parser = argparse.ArgumentParser(description="Monte-Carlo-Prognose fuer die 2. Bundesliga")
    parser.add_argument(
        "--refresh-current",
        action="store_true",
        default=DEFAULT_REFRESH_CURRENT,
        help="Aktuelle Saison erneut von worldfootball.net laden",
    )
    parser.add_argument("--season", type=int, default=infer_current_season_start(), help="Startjahr der Saison")
    parser.add_argument(
        "--simulations",
        type=int,
        default=DEFAULT_SIMULATIONS,
        help="Anzahl Simulationen",
    )
    parser.add_argument(
        "--backtest",
        action="store_true",
        default=DEFAULT_RUN_BACKTEST,
        help="Modell per historischen Saisons validieren",
    )
    parser.add_argument(
        "--cutoff",
        type=int,
        default=DEFAULT_BACKTEST_CUTOFF,
        help="Spieltag fuer den Backtest",
    )
    args = parser.parse_args()

    current = fetch_current_season_schedule(season=args.season, force_refresh=args.refresh_current)
    summary, distribution, rank_matrix, points_distribution = simulate_season(current, n_sims=args.simulations)
    match_predictions = match_prediction_table(current)
    latest_matchday = get_latest_completed_matchday(current)
    output_path = save_prediction_outputs(
        summary,
        distribution,
        rank_matrix,
        points_distribution,
        match_predictions,
        output_prefix=f"2_bundesliga_prediction_matchday_{latest_matchday}_{args.season}",
    )

    print(f"Prognose gespeichert: {output_path}")

    if args.backtest:
        historical = load_historical_data()
        backtest = backtest_cutoff(historical, cutoff_matchday=args.cutoff, n_sims=min(args.simulations, 2000))
        if backtest.empty:
            print("Backtest konnte nicht berechnet werden.")
            return

        backtest_path = os.path.join(DATA_DIR, f"2_bundesliga_backtest_md{args.cutoff}.xlsx")
        backtest.to_excel(backtest_path, index=False)
        print(f"Backtest gespeichert: {backtest_path}")
        print(f"Mittlerer Brier-Score Top-2: {backtest['brier_top2'].mean():.4f}")


if __name__ == "__main__":
    main()
