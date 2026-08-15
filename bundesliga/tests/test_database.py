from __future__ import annotations

import pandas as pd

from bundesliga.database import (
    consensus_bookmaker_probabilities,
    latest_market_values,
    load_matches,
    upsert_bookmaker_odds,
    upsert_market_values,
    upsert_matches,
)


def test_match_upsert_and_as_of_market_values(tmp_path):
    db = tmp_path / "test.sqlite"
    matches = pd.DataFrame(
        [
            {
                "competition": "D1",
                "season": 2024,
                "match_date": "2024-08-23",
                "matchday": 1,
                "home_team": "A",
                "away_team": "B",
                "home_goals": 2,
                "away_goals": 1,
                "status": "FINISHED",
                "source": "test",
            }
        ]
    )
    assert upsert_matches(matches, db) == 1
    assert upsert_matches(matches, db) == 1
    loaded = load_matches("D1", finished_only=True, path=db)
    assert len(loaded) == 1

    values = pd.DataFrame(
        [
            {"team": "A", "as_of": "2024-07-01", "squad_value_eur": 100, "source": "manual"},
            {"team": "A", "as_of": "2024-09-01", "squad_value_eur": 120, "source": "manual"},
            {"team": "B", "as_of": "2024-07-01", "squad_value_eur": 50, "source": "manual"},
        ]
    )
    upsert_market_values(values, db)
    snapshot = latest_market_values("2024-08-01", db).set_index("team")
    assert snapshot.loc["A", "squad_value_eur"] == 100
    assert snapshot.loc["B", "squad_value_eur"] == 50


def test_source_match_id_stays_stable_when_team_label_changes(tmp_path):
    db = tmp_path / "source-id.sqlite"
    base = {
        "competition": "D1",
        "season": 2026,
        "match_date": "2026-08-28T18:30:00",
        "matchday": 1,
        "home_team": "Bayern",
        "away_team": "Stuttgart",
        "home_goals": None,
        "away_goals": None,
        "status": "SCHEDULED",
        "source": "OpenLigaDB",
        "source_match_id": "12345",
    }
    upsert_matches(pd.DataFrame([base]), db)
    changed = {**base, "home_team": "Bayern München"}
    upsert_matches(pd.DataFrame([changed]), db)
    loaded = load_matches("D1", path=db)
    assert len(loaded) == 1
    assert loaded.loc[0, "home_team"] == "Bayern München"


def test_bookmaker_consensus_and_captured_as_of(tmp_path):
    db = tmp_path / "odds.sqlite"
    match = pd.DataFrame(
        [{
            "competition": "D1", "season": 2026, "match_date": "2026-08-28",
            "matchday": 1, "home_team": "A", "away_team": "B",
            "home_goals": None, "away_goals": None, "status": "SCHEDULED", "source": "test",
        }]
    )
    upsert_matches(match, db)
    odds = pd.DataFrame(
        [
            {
                "competition": "D1", "season": 2026, "home_team": "A", "away_team": "B",
                "bookmaker": "Test", "snapshot_type": "captured",
                "observed_at": "2026-08-20T10:00:00Z", "home_odds": 2.0,
                "draw_odds": 4.0, "away_odds": 4.0, "source": "test",
            },
            {
                "competition": "D1", "season": 2026, "home_team": "A", "away_team": "B",
                "bookmaker": "Test", "snapshot_type": "captured",
                "observed_at": "2026-08-21T10:00:00Z", "home_odds": 4.0,
                "draw_odds": 4.0, "away_odds": 2.0, "source": "test",
            },
        ]
    )
    assert upsert_bookmaker_odds(odds, db) == 2
    match_id = load_matches("D1", path=db).loc[0, "match_id"]
    early = consensus_bookmaker_probabilities(
        [match_id], "captured", "2026-08-20T12:00:00Z", db
    ).iloc[0]
    late = consensus_bookmaker_probabilities(
        [match_id], "captured", "2026-08-21T12:00:00Z", db
    ).iloc[0]
    assert early["book_prob_home"] == 0.5
    assert late["book_prob_away"] == 0.5
