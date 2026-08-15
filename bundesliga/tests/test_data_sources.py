from __future__ import annotations

import pytest

from bundesliga.data_sources import (
    canonical_team,
    parse_football_data_csv,
    parse_football_data_odds,
    parse_market_value,
    parse_odds_api_payload,
    parse_openligadb_matches,
    parse_transfermarkt_market_page,
    quarterly_snapshot_dates,
    season_code,
)


def test_parse_old_football_data_row_with_extra_column():
    raw = (
        b"Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,FTR,Odds\n"
        b"D1,08/02/03,Werder Bremen,Munich 1860,1,2,A,1.72,EXTRA\n"
    )
    matches = parse_football_data_csv(raw, "D1", 2002)
    assert len(matches) == 1
    assert matches.loc[0, "home_team"] == "SV Werder Bremen"
    assert matches.loc[0, "away_team"] == "TSV 1860 München"
    assert matches.loc[0, "home_goals"] == 1
    assert matches.loc[0, "away_goals"] == 2


def test_parse_football_data_opening_and_closing_odds():
    raw = (
        b"Div,Date,HomeTeam,AwayTeam,FTHG,FTAG,AvgH,AvgD,AvgA,B365CH,B365CD,B365CA\n"
        b"D1,05/08/2022,Ein Frankfurt,Bayern Munich,1,6,5.82,4.96,1.49,6,5,1.44\n"
    )
    odds = parse_football_data_odds(raw, "D1", 2022)
    assert len(odds) == 2
    opening = odds[odds["snapshot_type"].eq("opening")].iloc[0]
    closing = odds[odds["snapshot_type"].eq("closing")].iloc[0]
    assert opening["bookmaker"] == "Average"
    assert opening["home_team"] == "Eintracht Frankfurt"
    assert opening["away_odds"] == pytest.approx(1.49)
    assert closing["bookmaker"] == "Bet365"
    assert closing["draw_odds"] == pytest.approx(5.0)


def test_season_code_and_aliases():
    assert season_code(1999) == "9900"
    assert season_code(2025) == "2526"
    assert canonical_team("  Bayern\xa0Munich ") == "Bayern München"
    assert canonical_team("Bayer Leverkusen") == "Bayer 04 Leverkusen"
    assert canonical_team("FC Cologne") == "1. FC Köln"
    assert canonical_team("Borussia Monchengladbach") == "Borussia Mönchengladbach"
    assert canonical_team("FSV Mainz 05") == "1. FSV Mainz 05"


def test_openligadb_finished_result():
    payload = [
        {
            "matchID": 7,
            "matchDateTimeUTC": "2026-08-28T18:30:00Z",
            "matchIsFinished": True,
            "group": {"groupOrderID": 1},
            "team1": {"teamName": "Bayern Munich"},
            "team2": {"teamName": "Dortmund"},
            "matchResults": [
                {"resultTypeID": 1, "resultOrderID": 1, "pointsTeam1": 1, "pointsTeam2": 0},
                {"resultTypeID": 2, "resultOrderID": 2, "pointsTeam1": 2, "pointsTeam2": 1},
            ],
        }
    ]
    matches = parse_openligadb_matches(payload, "D1", 2026)
    assert matches.loc[0, "home_team"] == "Bayern München"
    assert matches.loc[0, "away_team"] == "Borussia Dortmund"
    assert matches.loc[0, "home_goals"] == 2
    assert matches.loc[0, "matchday"] == 1


def test_transfermarkt_stichtag_parser():
    html = """
    <table class="items">
      <thead><tr><th>#</th><th>wappen</th><th>Verein</th><th>Liga</th>
        <th>Wert 01.08.2025</th><th>Kadergröße 01.08.2025</th><th>Aktuell</th></tr></thead>
      <tbody>
        <tr><td>1</td><td></td><td>FC Bayern München</td><td>Bundesliga</td>
          <td>1,07 Mrd. €</td><td>27</td><td>1,10 Mrd. €</td></tr>
      </tbody>
    </table>
    """
    # Der Produktivparser verlangt mindestens zehn Vereine als Plausibilitaetscheck.
    html = html.replace("</tbody>", "".join(
        f"<tr><td>{i}</td><td></td><td>Team {i}</td><td>Bundesliga</td>"
        f"<td>{i},00 Mio. €</td><td>25</td><td>-</td></tr>"
        for i in range(2, 11)
    ) + "</tbody>")
    values = parse_transfermarkt_market_page(html, "2025-08-01")
    assert len(values) == 10
    assert values.loc[values["team"].eq("Bayern München"), "squad_value_eur"].iloc[0] == pytest.approx(1_070_000_000)
    assert values.loc[0, "squad_size"] == 27
    assert parse_market_value("329,85 Mio. €") == 329_850_000


def test_quarterly_dates_never_pass_end_date():
    dates = quarterly_snapshot_dates(2025, "2026-08-15")
    assert dates[0].strftime("%Y-%m-%d") == "2025-01-01"
    assert dates[-1].strftime("%Y-%m-%d") == "2026-08-15"


def test_odds_api_payload_becomes_timestamped_capture():
    payload = [
        {
            "home_team": "Bayern Munich",
            "away_team": "Dortmund",
            "bookmakers": [
                {
                    "key": "testbet",
                    "title": "TestBet",
                    "last_update": "2026-08-20T10:00:00Z",
                    "markets": [
                        {
                            "key": "h2h",
                            "outcomes": [
                                {"name": "Bayern Munich", "price": 1.7},
                                {"name": "Draw", "price": 4.2},
                                {"name": "Dortmund", "price": 4.8},
                            ],
                        }
                    ],
                }
            ],
        }
    ]
    odds = parse_odds_api_payload(payload, "D1", 2026)
    assert len(odds) == 1
    assert odds.loc[0, "home_team"] == "Bayern München"
    assert odds.loc[0, "away_team"] == "Borussia Dortmund"
    assert odds.loc[0, "snapshot_type"] == "captured"
    assert odds.loc[0, "draw_odds"] == pytest.approx(4.2)
