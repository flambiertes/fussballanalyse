"""
Holt aktuelle Laenderspiel-Ergebnisse via ESPN API und aktualisiert:
  - data/results.csv          (fuer strength_model.py)
  - data/wm2026_matches_group.csv  (aktuelle WM-Ergebnisse fuer Simulation)

Taeglich oder nach jedem Spieltag ausfuehren:
  python update_results.py
  python update_results.py --from 2026-04-01  (ab bestimmtem Datum)
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
WM_GROUP_CSV = DATA_DIR / "wm2026_matches_group.csv"

TEAM_NAME_MAP = {
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
        if league == "fifa.friendly":
            tournament = "Friendly"

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

    combined = pd.concat([existing, truly_new], ignore_index=True)
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
    args = parser.parse_args()

    today = date.today()

    if args.today:
        start = end = today
    elif args.start:
        start = date.fromisoformat(args.start)
        end = date.fromisoformat(args.end) if args.end else today
    else:
        # Automatisch: ab dem Tag nach dem letzten Ergebnis in der CSV
        last = get_last_date_in_results()
        start = last + timedelta(days=1)
        end = today
        print(f"Letztes Ergebnis in CSV: {last} -> hole ab {start}")

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
