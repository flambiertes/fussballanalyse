"""
Holt aktuelle Laenderspiel-Ergebnisse via ESPN API und aktualisiert:
  - data/results.csv          (fuer strength_model.py)
  - data/wm2026_matches_group.csv  (aktuelle WM-Ergebnisse fuer Simulation)

Taeglich oder nach jedem Spieltag ausfuehren:
  python update_results.py
  python update_results.py --from 2026-04-01  (ab bestimmtem Datum)
  python update_results.py --lookback-days 2  (letzte Tage erneut pruefen)
  python update_results.py --today            (nur heute)
"""

import argparse
import json
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests
from tqdm.auto import tqdm

DATA_DIR = Path(__file__).resolve().parent / "data"
RESULTS_CSV = DATA_DIR / "results.csv"
WM_GROUP_CSV  = DATA_DIR / "wm2026_matches_group.csv"
WM_KO_CSV     = DATA_DIR / "wm2026_matches_knockout.csv"
THIRD_COMBOS  = DATA_DIR / "third_place_combinations.csv"

TEAM_NAME_MAP = {
    "Bosnia-Herzegovina": "Bosnia and Herzegovina",
    "China": "China PR",
    "Congo DR": "DR Congo",
    "Curaçao": "Curacao",
    "Czechia": "Czech Republic",
    "Kyrgyz Republic": "Kyrgyzstan",
    "Türkiye": "Turkey",
    "Turkiye": "Turkey",
}

ESPN_BASE = "http://site.api.espn.com/apis/site/v2/sports/soccer"

# ESPN-Liga-Codes fuer internationale Spiele
ESPN_LEAGUES = [
    "fifa.friendly",        # Internationale Testspiele
    "fifa.world",           # WM 2026 (ab 11.06.2026) - bestaetigt funktionierend
    "fifa.worldq.conmebol", # CONMEBOL-Qualifikation
    "fifa.worldq.uefa",     # UEFA-Qualifikation
    "fifa.worldq.afc",      # AFC-Qualifikation
    "fifa.worldq.concacaf", # CONCACAF-Qualifikation
    "fifa.worldq.caf",      # CAF-Qualifikation
]

WM_START = date(2026, 6, 11)  # Vor diesem Datum gibt es keine ESPN-WM-Daten
DEFAULT_LOOKBACK_DAYS = 1      # Gestern erneut pruefen, falls Spiele spaet fertig wurden
LEAGUE_START_DATES = {
    "fifa.world": WM_START,
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

# Turniernamens-Mapping ESPN -> Kaggle-Format (fuer Konsistenz mit results.csv)
TOURNAMENT_MAP = {
    "FIFA World Cup": "FIFA World Cup",
    "FIFA World Cup Qualifying - CONMEBOL": "FIFA World Cup qualification",
    "FIFA World Cup Qualifying - UEFA": "FIFA World Cup qualification",
    "FIFA World Cup Qualifying - AFC": "FIFA World Cup qualification",
    "FIFA World Cup Qualifying - CONCACAF": "FIFA World Cup qualification",
    "FIFA World Cup Qualifying - CAF": "FIFA World Cup qualification",
    "FIFA World Cup Qualifying - OFC": "FIFA World Cup qualification",
    "FIFA World Cup Qualifying - Intercontinental": "FIFA World Cup qualification",
    "International Friendly": "Friendly",
    "Friendly": "Friendly",
    "fifa.friendly": "Friendly",
}


def fetch_espn_day(league: str, day: date) -> list[dict]:
    """Holt alle Spiele eines Tages fuer eine ESPN-Liga."""
    url = f"{ESPN_BASE}/{league}/scoreboard"
    params = {"dates": day.strftime("%Y%m%d"), "limit": 100}
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return []
        data = r.json()
    except Exception:
        return []

    results = []
    for event in data.get("events", []):
        comps = event.get("competitions", [])
        if not comps:
            continue
        comp = comps[0]
        status = comp.get("status", {}).get("type", {})
        # Spiel als abgeschlossen werten wenn completed=True ODER state="post"
        is_done = status.get("completed", False) or status.get("state", "") == "post"
        if not is_done:
            continue

        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            continue

        # Heim/Auswärts bestimmen
        home = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
        away = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])

        try:
            home_score = int(float(home.get("score", "NaN")))
            away_score = int(float(away.get("score", "NaN")))
        except ValueError:
            continue

        neutral = comp.get("neutralSite", False)
        tournament_raw = event.get("season", {}).get("slug", "")
        tournament_name = event.get("name", "")

        # Turniername aus notes oder season
        notes = comp.get("notes", [])
        for note in notes:
            if note.get("type") == "event":
                tournament_raw = note.get("headline", tournament_raw)

        # Auf Kaggle-Format normalisieren
        tournament = TOURNAMENT_MAP.get(tournament_raw, tournament_raw or "Friendly")
        if league == "fifa.world":
            tournament = "FIFA World Cup"
        elif league == "fifa.friendly":
            tournament = "Friendly"

        # Spielende-Typ aus ESPN shortDetail (z.B. 'FT', 'AET', 'FT-Pens')
        status_detail = (status.get("shortDetail", "") or "").lower()
        if "pen" in status_detail:
            extra = "n.E."
        elif "aet" in status_detail or "et" in status_detail:
            extra = "n.V."
        else:
            extra = ""

        # Sieger: ESPN setzt winner=True beim Sieger-Competitor (auch bei Elfmeter)
        home_is_winner = home.get("winner", None)
        away_is_winner = away.get("winner", None)
        if home_is_winner is True:
            winner_team = home["team"]["displayName"]
        elif away_is_winner is True:
            winner_team = away["team"]["displayName"]
        else:
            winner_team = None  # Gruppe (Unentschieden) oder noch laufend

        results.append({
            "date": day.isoformat(),
            "home_team": home["team"]["displayName"],
            "away_team": away["team"]["displayName"],
            "home_score": home_score,
            "away_score": away_score,
            "tournament": tournament,
            "city": comp.get("venue", {}).get("address", {}).get("city", ""),
            "country": comp.get("venue", {}).get("address", {}).get("country", ""),
            "neutral": str(neutral).upper(),
            "winner_team": winner_team,
            "extra": extra,
        })
    return results


def fetch_date_range(start: date, end: date) -> pd.DataFrame:
    """Holt alle Ergebnisse zwischen start und end (inklusiv)."""
    all_rows = []
    days = (end - start).days + 1
    for i in tqdm(range(days), desc="Tage abrufen"):
        day = start + timedelta(days=i)
        for league in ESPN_LEAGUES:
            if day < LEAGUE_START_DATES.get(league, date.min):
                continue
            rows = fetch_espn_day(league, day)
            all_rows.extend(rows)
        time.sleep(0.3)  # Rate-Limiting

    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    df["date"] = pd.to_datetime(df["date"])
    df["home_team"] = df["home_team"].replace(TEAM_NAME_MAP)
    df["away_team"] = df["away_team"].replace(TEAM_NAME_MAP)
    # Duplikate entfernen (gleiches Spiel in mehreren Ligen)
    df = df.drop_duplicates(subset=["date", "home_team", "away_team"])
    return df


def update_results_csv(new_df: pd.DataFrame) -> int:
    """Fuegt neue Ergebnisse zu results.csv hinzu. Gibt Anzahl neuer Zeilen zurueck."""
    if new_df.empty:
        return 0

    existing = pd.read_csv(RESULTS_CSV, parse_dates=["date"])
    existing["home_team"] = existing["home_team"].replace(TEAM_NAME_MAP)
    existing["away_team"] = existing["away_team"].replace(TEAM_NAME_MAP)
    existing_keys = set(
        zip(existing["date"].dt.date, existing["home_team"], existing["away_team"])
    )

    new_df["date"] = pd.to_datetime(new_df["date"])
    new_df["home_team"] = new_df["home_team"].replace(TEAM_NAME_MAP)
    new_df["away_team"] = new_df["away_team"].replace(TEAM_NAME_MAP)
    mask = new_df.apply(
        lambda r: (r["date"].date(), r["home_team"], r["away_team"]) not in existing_keys,
        axis=1,
    )
    truly_new = new_df[mask]

    if truly_new.empty:
        return 0

    # Nur Standardspalten in results.csv schreiben (winner_team/extra sind KO-intern)
    results_cols = ["date", "home_team", "away_team", "home_score", "away_score",
                    "tournament", "city", "country", "neutral"]
    truly_new_clean = truly_new[[c for c in results_cols if c in truly_new.columns]]
    combined = pd.concat([existing, truly_new_clean], ignore_index=True)
    combined = combined.sort_values("date").reset_index(drop=True)
    combined.to_csv(RESULTS_CSV, index=False)
    return len(truly_new)


def update_wm_group_matches(new_df: pd.DataFrame) -> int:
    """
    Schreibt tatsaechliche WM-Ergebnisse in wm2026_matches_group.csv.
    Fuegt Spalten goals_home und goals_away hinzu falls noch nicht vorhanden.
    """
    if new_df.empty:
        return 0

    wm = pd.read_csv(WM_GROUP_CSV, parse_dates=["date"])
    if "goals_home" not in wm.columns:
        wm["goals_home"] = pd.NA
        wm["goals_away"] = pd.NA

    wm_games = new_df[new_df["tournament"] == "FIFA World Cup"].copy()
    updated = 0

    for _, row in wm_games.iterrows():
        mask = (
            (wm["team_home"] == row["home_team"]) &
            (wm["team_away"] == row["away_team"])
        ) | (
            (wm["team_home"] == row["away_team"]) &
            (wm["team_away"] == row["home_team"])
        )
        if mask.any():
            idx = wm.index[mask][0]
            if pd.isna(wm.at[idx, "goals_home"]):
                if wm.at[idx, "team_home"] == row["home_team"]:
                    wm.at[idx, "goals_home"] = row["home_score"]
                    wm.at[idx, "goals_away"] = row["away_score"]
                else:
                    wm.at[idx, "goals_home"] = row["away_score"]
                    wm.at[idx, "goals_away"] = row["home_score"]
                updated += 1

    if updated > 0:
        wm.to_csv(WM_GROUP_CSV, index=False)
    return updated


def build_group_standings() -> dict:
    """
    Berechnet tatsaechliche Gruppentabellen aus wm2026_matches_group.csv.
    Gibt {group: [(team, pts, gd, gf), ...]} zurueck (absteigend sortiert).
    """
    if not WM_GROUP_CSV.exists():
        return {}
    df = pd.read_csv(WM_GROUP_CSV)
    if "goals_home" not in df.columns:
        return {}
    played = df.dropna(subset=["goals_home", "goals_away"])
    standings = {}
    for group, gdf in played.groupby("group"):
        records = {}
        for _, row in gdf.iterrows():
            h, a = row["team_home"], row["team_away"]
            gh, ga = int(row["goals_home"]), int(row["goals_away"])
            for team in [h, a]:
                records.setdefault(team, {"pts": 0, "gf": 0, "ga": 0})
            records[h]["gf"] += gh; records[h]["ga"] += ga
            records[a]["gf"] += ga; records[a]["ga"] += gh
            if gh > ga:   records[h]["pts"] += 3
            elif gh == ga: records[h]["pts"] += 1; records[a]["pts"] += 1
            else:          records[a]["pts"] += 3
        sorted_teams = sorted(
            records.items(),
            key=lambda x: (-x[1]["pts"], -(x[1]["gf"] - x[1]["ga"]), -x[1]["gf"], x[0])
        )
        standings[group] = sorted_teams
    return standings


def build_actual_qualifiers(standings: dict) -> dict:
    """
    Bestimmt {slot_beschreibung: team} aus den tatsaechlichen Gruppentabellen.
    Benoetigt third_place_combinations.csv fuer die Drittplazierten-Zuteilung.
    Gibt None zurueck wenn Gruppenphase noch nicht komplett.
    """
    if not standings:
        return {}
    qualifiers = {}
    thirds = []
    for group, table in standings.items():
        if len(table) < 3:
            continue
        qualifiers[f"Winner Group {group}"]    = table[0][0]
        qualifiers[f"Runner-up Group {group}"] = table[1][0]
        t, rec = table[2]
        thirds.append((t, rec["pts"], rec["gf"] - rec["ga"], rec["gf"], group))

    if not THIRD_COMBOS.exists() or len(thirds) < 8:
        return qualifiers

    thirds.sort(key=lambda x: (-x[1], -x[2], -x[3]))
    best8 = thirds[:8]
    qualified_groups = "".join(sorted(t[4] for t in best8))

    combos = pd.read_csv(THIRD_COMBOS)
    THIRD_SLOTS_BY_WINNER = {
        "1A": "3rd Group C/E/F/H/I",
        "1B": "3rd Group E/F/G/I/J",
        "1D": "3rd Group B/E/F/I/J",
        "1E": "3rd Group A/B/C/D/F",
        "1G": "3rd Group A/E/H/I/J",
        "1I": "3rd Group C/D/F/G/H",
        "1K": "3rd Group D/E/I/J/L",
        "1L": "3rd Group E/H/I/J/K",
    }
    teams_by_group = {t[4]: t[0] for t in best8}
    matches = combos[combos["qualified_groups"] == qualified_groups]
    if len(matches) == 1:
        row = matches.iloc[0]
        for winner_slot, desc in THIRD_SLOTS_BY_WINNER.items():
            group_char = row[f"third_for_{winner_slot}"]
            if group_char in teams_by_group:
                qualifiers[desc] = teams_by_group[group_char]

    return qualifiers


def update_wm_ko_matches(new_df: pd.DataFrame) -> int:
    """
    Traegt tatsaechliche KO-Ergebnisse in wm2026_matches_knockout.csv ein.
    Loest Slot-Beschreibungen (z.B. 'Winner Group A') zu echten Teamnamen auf.
    Gibt Anzahl neu eingetragener Ergebnisse zurueck.
    """
    if not WM_KO_CSV.exists():
        return 0

    ko = pd.read_csv(WM_KO_CSV, parse_dates=["date"])

    # Neue Spalten anlegen falls nicht vorhanden
    for col in ["team_home", "team_away", "goals_home", "goals_away", "winner", "extra"]:
        if col not in ko.columns:
            ko[col] = pd.NA

    # Gruppen-Qualifikanten bestimmen (nur wenn Gruppenphase vollstaendig)
    standings = build_group_standings()
    qualifiers = build_actual_qualifiers(standings)

    # Teamnamen in KO-CSV eintragen wo moeglich
    for idx, row in ko.iterrows():
        if pd.isna(ko.at[idx, "team_home"]) and qualifiers:
            resolved_h = qualifiers.get(row["team_home_desc"])
            resolved_a = qualifiers.get(row["team_away_desc"])
            if resolved_h:
                ko.at[idx, "team_home"] = resolved_h
            if resolved_a:
                ko.at[idx, "team_away"] = resolved_a

    # Ergebnisse aus new_df matchen (FIFA World Cup Spiele)
    wc_games = new_df[new_df["tournament"] == "FIFA World Cup"].copy() if not new_df.empty else pd.DataFrame()
    updated = 0

    for idx, row in ko.iterrows():
        if pd.notna(ko.at[idx, "goals_home"]):
            continue  # bereits eingetragen
        th = ko.at[idx, "team_home"]
        ta = ko.at[idx, "team_away"]
        if pd.isna(th) or pd.isna(ta):
            continue  # Teamnamen noch nicht aufgeloest

        match_date = ko.at[idx, "date"]
        # Suche in new_df
        if not wc_games.empty:
            hit = wc_games[
                (wc_games["date"].dt.date == match_date.date()) &
                (
                    ((wc_games["home_team"] == th) & (wc_games["away_team"] == ta)) |
                    ((wc_games["home_team"] == ta) & (wc_games["away_team"] == th))
                )
            ]
        else:
            hit = pd.DataFrame()

        # Fallback: suche in gesamtem results.csv
        if hit.empty:
            all_results = pd.read_csv(RESULTS_CSV, parse_dates=["date"])
            all_results["home_team"] = all_results["home_team"].replace(TEAM_NAME_MAP)
            all_results["away_team"] = all_results["away_team"].replace(TEAM_NAME_MAP)
            hit = all_results[
                (all_results["date"].dt.date == match_date.date()) &
                (all_results["tournament"] == "FIFA World Cup") &
                (
                    ((all_results["home_team"] == th) & (all_results["away_team"] == ta)) |
                    ((all_results["home_team"] == ta) & (all_results["away_team"] == th))
                )
            ]

        if hit.empty:
            continue

        r = hit.iloc[0]
        if r["home_team"] == th:
            gh, ga = int(r["home_score"]), int(r["away_score"])
        else:
            gh, ga = int(r["away_score"]), int(r["home_score"])

        ko.at[idx, "goals_home"] = gh
        ko.at[idx, "goals_away"] = ga

        # Sieger bestimmen
        if gh > ga:
            ko.at[idx, "winner"] = th
            ko.at[idx, "extra"]  = ""
        elif ga > gh:
            ko.at[idx, "winner"] = ta
            ko.at[idx, "extra"]  = ""
        else:
            # Unentschieden → Verlaengerung/Elfmeter
            winner_from_api = r.get("winner_team") if "winner_team" in r.index else None
            if pd.notna(winner_from_api) and winner_from_api:
                ko.at[idx, "winner"] = winner_from_api
                went_pen = r.get("went_to_penalties", False) if "went_to_penalties" in r.index else False
                ko.at[idx, "extra"] = "n.E." if went_pen else "n.V."
            else:
                ko.at[idx, "extra"] = "n.E."  # Elfmeter angenommen, Sieger unbekannt
        updated += 1

    ko.to_csv(WM_KO_CSV, index=False)
    return updated


def get_last_date_in_results() -> date:
    """Gibt das letzte Datum in results.csv zurueck (mit tatsaechlichem Ergebnis)."""
    df = pd.read_csv(RESULTS_CSV, parse_dates=["date"])
    df = df.dropna(subset=["home_score", "away_score"])
    df = df[df["home_score"].astype(str).str.upper() != "NA"]
    return df["date"].max().date()


def main():
    parser = argparse.ArgumentParser(description="WM-Ergebnisse aktualisieren")
    parser.add_argument("--from", dest="start", help="Startdatum YYYY-MM-DD")
    parser.add_argument("--to", dest="end", help="Enddatum YYYY-MM-DD (Standard: heute)")
    parser.add_argument("--today", action="store_true", help="Nur heutigen Tag holen")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help="Im Automatikmodus mindestens so viele Tage rueckwirkend erneut pruefen",
    )
    args = parser.parse_args()

    today = date.today()

    if args.today:
        start = end = today
    elif args.start:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end) if args.end else today
    else:
        # Automatisch: verpasste Tage holen und die letzten Tage erneut pruefen.
        last = get_last_date_in_results()
        end = today
        lookback_start = today - timedelta(days=max(args.lookback_days, 0))
        start = min(last + timedelta(days=1), lookback_start)
        print(
            f"Letztes Ergebnis in CSV: {last} -> hole ab {start} "
            f"(Lookback: {args.lookback_days} Tag(e))"
        )

    if start > end:
        print("Keine neuen Daten zu holen.")
        return

    print(f"Hole Ergebnisse {start} bis {end} ...")
    new_data = fetch_date_range(start, end)

    if new_data.empty:
        print("Keine neuen Spiele gefunden.")
        return

    print(f"{len(new_data)} Spiele gefunden.")

    n_results = update_results_csv(new_data)
    print(f"results.csv: +{n_results} neue Zeilen")

    n_wm = update_wm_group_matches(new_data)
    print(f"wm2026_matches_group.csv: {n_wm} WM-Ergebnisse eingetragen")

    n_ko = update_wm_ko_matches(new_data)
    print(f"wm2026_matches_knockout.csv: {n_ko} KO-Ergebnisse eingetragen")

    if n_results > 0:
        # Inkrementeller Elo-Update (nur neue Spiele, < 1 Sek)
        from strength_model import update_elo_with_new_matches
        n_elo = update_elo_with_new_matches(new_data)
        if n_elo > 0:
            print(f"Elo-Checkpoint aktualisiert: {n_elo} neue Spiele eingerechnet.")
            print("team_strengths.csv wurde neu aufgebaut.")
        else:
            print("Kein Elo-Checkpoint vorhanden -> strength_model.py ausfuehren.")


if __name__ == "__main__":
    main()
