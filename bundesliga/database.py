from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .config import DB_PATH


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS matches (
    match_id TEXT PRIMARY KEY,
    competition TEXT NOT NULL,
    season INTEGER NOT NULL,
    match_date TEXT NOT NULL,
    matchday INTEGER,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    home_goals INTEGER,
    away_goals INTEGER,
    status TEXT NOT NULL DEFAULT 'FINISHED',
    source TEXT NOT NULL,
    source_match_id TEXT,
    ingested_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_matches_comp_date
ON matches(competition, match_date);

CREATE INDEX IF NOT EXISTS idx_matches_teams
ON matches(home_team, away_team);

CREATE TABLE IF NOT EXISTS market_values (
    team TEXT NOT NULL,
    as_of TEXT NOT NULL,
    squad_value_eur REAL NOT NULL,
    squad_size INTEGER,
    goalkeeper_value_eur REAL,
    defense_value_eur REAL,
    midfield_value_eur REAL,
    attack_value_eur REAL,
    source TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    PRIMARY KEY(team, as_of, source)
);

CREATE INDEX IF NOT EXISTS idx_market_team_date
ON market_values(team, as_of);

CREATE TABLE IF NOT EXISTS bookmaker_odds (
    odds_id TEXT PRIMARY KEY,
    match_id TEXT NOT NULL,
    bookmaker TEXT NOT NULL,
    snapshot_type TEXT NOT NULL,
    observed_at TEXT,
    home_odds REAL NOT NULL,
    draw_odds REAL NOT NULL,
    away_odds REAL NOT NULL,
    source TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    FOREIGN KEY(match_id) REFERENCES matches(match_id)
);

CREATE INDEX IF NOT EXISTS idx_odds_match_snapshot
ON bookmaker_odds(match_id, snapshot_type, observed_at);

CREATE TABLE IF NOT EXISTS predictions (
    run_id TEXT NOT NULL,
    model_version TEXT NOT NULL,
    config_json TEXT NOT NULL,
    as_of TEXT NOT NULL,
    match_id TEXT NOT NULL,
    lambda_home REAL NOT NULL,
    lambda_away REAL NOT NULL,
    prob_home REAL NOT NULL,
    prob_draw REAL NOT NULL,
    prob_away REAL NOT NULL,
    tip_home INTEGER NOT NULL,
    tip_away INTEGER NOT NULL,
    actual_home INTEGER,
    actual_away INTEGER,
    tip_points INTEGER,
    log_loss REAL,
    brier_score REAL,
    book_prob_home REAL,
    book_prob_draw REAL,
    book_prob_away REAL,
    bookmaker_weight REAL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(run_id, match_id),
    FOREIGN KEY(match_id) REFERENCES matches(match_id)
);
"""


def connect(path: Path | str = DB_PATH) -> sqlite3.Connection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize(path: Path | str = DB_PATH) -> None:
    with connect(path) as connection:
        connection.executescript(SCHEMA)
        market_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(market_values)")
        }
        if "squad_size" not in market_columns:
            connection.execute("ALTER TABLE market_values ADD COLUMN squad_size INTEGER")
        prediction_columns = {
            row[1] for row in connection.execute("PRAGMA table_info(predictions)")
        }
        for name in (
            "book_prob_home", "book_prob_draw", "book_prob_away", "bookmaker_weight",
        ):
            if name not in prediction_columns:
                connection.execute(f"ALTER TABLE predictions ADD COLUMN {name} REAL")


def make_match_id(
    competition: str,
    season: int,
    match_date: str,
    home_team: str,
    away_team: str,
) -> str:
    raw = "|".join([competition, str(season), match_date[:10], home_team, away_team])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def upsert_matches(matches: pd.DataFrame, path: Path | str = DB_PATH) -> int:
    if matches.empty:
        return 0
    initialize(path)
    required = {
        "competition", "season", "match_date", "home_team", "away_team",
        "home_goals", "away_goals", "status", "source",
    }
    missing = required - set(matches.columns)
    if missing:
        raise ValueError(f"Fehlende Match-Spalten: {sorted(missing)}")

    now = datetime.now(timezone.utc).isoformat()
    records = []
    for row in matches.to_dict("records"):
        date = pd.Timestamp(row["match_date"]).isoformat()
        source_match_id = _optional_text(row.get("source_match_id"))
        if row.get("match_id"):
            match_id = str(row["match_id"])
        elif source_match_id is not None:
            raw_source_id = f"{row['source']}|{source_match_id}"
            match_id = hashlib.sha1(raw_source_id.encode("utf-8")).hexdigest()[:20]
        else:
            match_id = make_match_id(
                str(row["competition"]), int(row["season"]), date,
                str(row["home_team"]), str(row["away_team"]),
            )
        records.append(
            (
                match_id, str(row["competition"]), int(row["season"]), date,
                _optional_int(row.get("matchday")), str(row["home_team"]),
                str(row["away_team"]), _optional_int(row.get("home_goals")),
                _optional_int(row.get("away_goals")), str(row.get("status", "FINISHED")),
                str(row["source"]), source_match_id, now,
            )
        )

    sql = """
    INSERT INTO matches VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(match_id) DO UPDATE SET
        match_date=excluded.match_date,
        matchday=COALESCE(excluded.matchday, matches.matchday),
        home_team=excluded.home_team,
        away_team=excluded.away_team,
        home_goals=COALESCE(excluded.home_goals, matches.home_goals),
        away_goals=COALESCE(excluded.away_goals, matches.away_goals),
        status=excluded.status,
        source=excluded.source,
        source_match_id=COALESCE(excluded.source_match_id, matches.source_match_id),
        ingested_at=excluded.ingested_at
    """
    with connect(path) as connection:
        connection.executemany(sql, records)
    return len(records)


def load_matches(
    competition: str | None = None,
    before: str | pd.Timestamp | None = None,
    seasons: Iterable[int] | None = None,
    finished_only: bool = False,
    path: Path | str = DB_PATH,
) -> pd.DataFrame:
    initialize(path)
    clauses: list[str] = []
    params: list[object] = []
    if competition:
        clauses.append("competition = ?")
        params.append(competition)
    if before is not None:
        clauses.append("match_date < ?")
        params.append(pd.Timestamp(before).isoformat())
    if seasons:
        season_values = [int(s) for s in seasons]
        clauses.append(f"season IN ({','.join('?' for _ in season_values)})")
        params.extend(season_values)
    if finished_only:
        clauses.append("home_goals IS NOT NULL AND away_goals IS NOT NULL")
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"SELECT * FROM matches {where} ORDER BY match_date, match_id"
    with connect(path) as connection:
        df = pd.read_sql_query(query, connection, params=params)
    if not df.empty:
        df["match_date"] = (
            pd.to_datetime(df["match_date"], format="mixed", utc=True)
            .dt.tz_localize(None)
        )
    return df


def upsert_market_values(values: pd.DataFrame, path: Path | str = DB_PATH) -> int:
    initialize(path)
    required = {"team", "as_of", "squad_value_eur", "source"}
    missing = required - set(values.columns)
    if missing:
        raise ValueError(f"Fehlende Marktwert-Spalten: {sorted(missing)}")
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for row in values.to_dict("records"):
        records.append(
            (
                str(row["team"]), pd.Timestamp(row["as_of"]).isoformat(),
                float(row["squad_value_eur"]), _optional_int(row.get("squad_size")),
                _optional_float(row.get("goalkeeper_value_eur")),
                _optional_float(row.get("defense_value_eur")),
                _optional_float(row.get("midfield_value_eur")),
                _optional_float(row.get("attack_value_eur")), str(row["source"]), now,
            )
        )
    sql = """
    INSERT INTO market_values(
      team, as_of, squad_value_eur, squad_size, goalkeeper_value_eur,
      defense_value_eur, midfield_value_eur, attack_value_eur, source, ingested_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(team, as_of, source) DO UPDATE SET
      squad_value_eur=excluded.squad_value_eur,
      squad_size=excluded.squad_size,
      goalkeeper_value_eur=excluded.goalkeeper_value_eur,
      defense_value_eur=excluded.defense_value_eur,
      midfield_value_eur=excluded.midfield_value_eur,
      attack_value_eur=excluded.attack_value_eur,
      ingested_at=excluded.ingested_at
    """
    with connect(path) as connection:
        connection.executemany(sql, records)
    return len(records)


def latest_market_values(as_of: str | pd.Timestamp, path: Path | str = DB_PATH) -> pd.DataFrame:
    initialize(path)
    query = """
    SELECT * FROM (
      SELECT mv.*,
             ROW_NUMBER() OVER (
               PARTITION BY team ORDER BY as_of DESC, ingested_at DESC, source DESC
             ) AS row_number
      FROM market_values mv
      WHERE as_of <= ?
    ) ranked
    WHERE row_number = 1
    """
    with connect(path) as connection:
        return pd.read_sql_query(query, connection, params=[pd.Timestamp(as_of).isoformat()])


def upsert_bookmaker_odds(odds: pd.DataFrame, path: Path | str = DB_PATH) -> int:
    """Speichert Dezimalquoten und ordnet sie bei Bedarf ueber die Paarung zu."""
    if odds.empty:
        return 0
    initialize(path)
    required = {
        "bookmaker", "snapshot_type", "home_odds", "draw_odds", "away_odds", "source",
    }
    missing = required - set(odds.columns)
    if missing:
        raise ValueError(f"Fehlende Quoten-Spalten: {sorted(missing)}")
    values = odds.copy()
    if "match_id" not in values or values["match_id"].isna().any():
        identity = {"competition", "season", "home_team", "away_team"}
        missing_identity = identity - set(values.columns)
        if missing_identity:
            raise ValueError(
                "Quoten benoetigen match_id oder Spielidentitaet; fehlend: "
                f"{sorted(missing_identity)}"
            )
        matches = load_matches(path=path)[
            ["match_id", "competition", "season", "home_team", "away_team"]
        ]
        if matches.duplicated(["competition", "season", "home_team", "away_team"]).any():
            raise ValueError("Spielpaarung ist innerhalb einer Saison nicht eindeutig")
        values = values.drop(columns=["match_id"], errors="ignore").merge(
            matches,
            on=["competition", "season", "home_team", "away_team"],
            how="left",
            validate="many_to_one",
        )
    if values["match_id"].isna().any():
        identity_columns = [
            column for column in ("competition", "season", "home_team", "away_team")
            if column in values
        ]
        unresolved = values.loc[values["match_id"].isna(), identity_columns]
        raise ValueError(f"Quoten konnten keinem Spiel zugeordnet werden:\n{unresolved.head()}")

    allowed_snapshots = {"opening", "closing", "captured"}
    unknown = set(values["snapshot_type"].astype(str)) - allowed_snapshots
    if unknown:
        raise ValueError(f"Unbekannte snapshot_type-Werte: {sorted(unknown)}")
    for column in ("home_odds", "draw_odds", "away_odds"):
        values[column] = pd.to_numeric(values[column], errors="raise")
        if (~np.isfinite(values[column]) | (values[column] <= 1.0)).any():
            raise ValueError(f"{column} muss eine endliche Dezimalquote > 1 sein")
    observed = values.get("observed_at", pd.Series(index=values.index, dtype=object))
    if (values["snapshot_type"].eq("captured") & observed.isna()).any():
        raise ValueError("captured-Quoten benoetigen observed_at")

    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for row in values.to_dict("records"):
        observed_at = row.get("observed_at")
        observed_text = (
            None if observed_at is None or pd.isna(observed_at)
            else pd.Timestamp(observed_at).isoformat()
        )
        identity = "|".join(
            [
                str(row["match_id"]), str(row["bookmaker"]), str(row["snapshot_type"]),
                observed_text or "", str(row["source"]),
            ]
        )
        odds_id = hashlib.sha1(identity.encode("utf-8")).hexdigest()[:24]
        rows.append(
            (
                odds_id, str(row["match_id"]), str(row["bookmaker"]),
                str(row["snapshot_type"]), observed_text, float(row["home_odds"]),
                float(row["draw_odds"]), float(row["away_odds"]), str(row["source"]), now,
            )
        )
    sql = """
    INSERT INTO bookmaker_odds(
      odds_id, match_id, bookmaker, snapshot_type, observed_at,
      home_odds, draw_odds, away_odds, source, ingested_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(odds_id) DO UPDATE SET
      home_odds=excluded.home_odds,
      draw_odds=excluded.draw_odds,
      away_odds=excluded.away_odds,
      ingested_at=excluded.ingested_at
    """
    with connect(path) as connection:
        connection.executemany(sql, rows)
    return len(rows)


def consensus_bookmaker_probabilities(
    match_ids: Iterable[str],
    snapshot_type: str = "opening",
    as_of: str | pd.Timestamp | None = None,
    path: Path | str = DB_PATH,
) -> pd.DataFrame:
    """Liefert margenbereinigte 1X2-Konsenswahrscheinlichkeiten je Spiel."""
    ids = [str(match_id) for match_id in match_ids]
    columns = [
        "match_id", "book_prob_home", "book_prob_draw", "book_prob_away",
        "bookmaker_count", "odds_snapshot_type",
    ]
    if not ids:
        return pd.DataFrame(columns=columns)
    initialize(path)
    placeholders = ",".join("?" for _ in ids)
    clauses = [f"match_id IN ({placeholders})", "snapshot_type = ?"]
    params: list[object] = [*ids, snapshot_type]
    if snapshot_type == "captured":
        if as_of is None:
            raise ValueError("captured-Quoten benoetigen einen as_of-Zeitpunkt")
        clauses.append("observed_at <= ?")
        params.append(pd.Timestamp(as_of).isoformat())
    query = f"SELECT * FROM bookmaker_odds WHERE {' AND '.join(clauses)}"
    with connect(path) as connection:
        raw = pd.read_sql_query(query, connection, params=params)
    if raw.empty:
        return pd.DataFrame(columns=columns)
    if snapshot_type == "captured":
        raw["observed_at"] = pd.to_datetime(raw["observed_at"], format="mixed")
        raw = raw.sort_values("observed_at").drop_duplicates(
            ["match_id", "bookmaker"], keep="last"
        )

    rows = []
    for match_id, group in raw.groupby("match_id"):
        average = group[group["bookmaker"].isin(["Average", "BbAv"])]
        usable = average.tail(1) if not average.empty else group[
            ~group["bookmaker"].isin(["Maximum", "BbMx"])
        ]
        inverse = 1.0 / usable[["home_odds", "draw_odds", "away_odds"]].to_numpy(float)
        fair = inverse / inverse.sum(axis=1, keepdims=True)
        consensus = fair.mean(axis=0)
        consensus /= consensus.sum()
        rows.append(
            {
                "match_id": match_id,
                "book_prob_home": consensus[0],
                "book_prob_draw": consensus[1],
                "book_prob_away": consensus[2],
                "bookmaker_count": len(usable),
                "odds_snapshot_type": snapshot_type,
            }
        )
    return pd.DataFrame(rows, columns=columns)


def save_predictions(
    predictions: pd.DataFrame,
    run_id: str,
    model_version: str,
    config: dict,
    path: Path | str = DB_PATH,
) -> int:
    if predictions.empty:
        return 0
    initialize(path)
    created_at = datetime.now(timezone.utc).isoformat()
    config_json = json.dumps(config, sort_keys=True)
    columns = [
        "match_id", "as_of", "lambda_home", "lambda_away", "prob_home",
        "prob_draw", "prob_away", "tip_home", "tip_away", "actual_home",
        "actual_away", "tip_points", "log_loss", "brier_score",
        "book_prob_home", "book_prob_draw", "book_prob_away", "bookmaker_weight",
    ]
    rows = []
    for row in predictions.to_dict("records"):
        rows.append(
            (
                run_id, model_version, config_json, pd.Timestamp(row["as_of"]).isoformat(),
                *[row.get(column) for column in columns if column != "as_of"], created_at,
            )
        )
    sql = """
    INSERT OR REPLACE INTO predictions(
      run_id, model_version, config_json, as_of, match_id,
      lambda_home, lambda_away, prob_home, prob_draw, prob_away,
      tip_home, tip_away, actual_home, actual_away, tip_points,
      log_loss, brier_score, book_prob_home, book_prob_draw, book_prob_away,
      bookmaker_weight, created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    with connect(path) as connection:
        connection.executemany(sql, rows)
    return len(rows)


def _optional_int(value):
    return None if value is None or pd.isna(value) else int(value)


def _optional_float(value):
    return None if value is None or pd.isna(value) else float(value)


def _optional_text(value):
    return None if value is None or pd.isna(value) else str(value)
