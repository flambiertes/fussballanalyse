from __future__ import annotations

import argparse
import csv
import io
import os
import re
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .config import DB_PATH, LEAGUES, RAW_DIR, TEAM_ALIASES
from .database import (
    initialize,
    load_matches,
    upsert_bookmaker_odds,
    upsert_market_values,
    upsert_matches,
)


FOOTBALL_DATA_URL = "https://www.football-data.co.uk/mmz4281/{season_code}/{league}.csv"
OPENLIGADB_URL = "https://api.openligadb.de/getmatchdata/{league}/{season}/{matchday}"
ODDS_API_URL = "https://api.the-odds-api.com/v4/sports/{sport}/odds"
TRANSFERMARKT_MARKET_URL = (
    "https://www.transfermarkt.de/{slug}/marktwerteverein/"
    "wettbewerb/{competition}/stichtag/{snapshot}/plus/1"
)
USER_AGENT = "fussballanalyse/0.1 (private research project)"
TRANSFERMARKT_LEAGUES = {
    "D1": {"slug": "bundesliga", "competition": "L1"},
    "D2": {"slug": "2-bundesliga", "competition": "L2"},
}
ODDS_API_SPORTS = {
    "D1": "soccer_germany_bundesliga",
    "D2": "soccer_germany_bundesliga2",
}

BOOKMAKER_NAMES = {
    "Avg": "Average",
    "Max": "Maximum",
    "B365": "Bet365",
    "BW": "Bwin",
    "IW": "Interwetten",
    "PS": "Pinnacle",
    "WH": "William Hill",
    "VC": "VBet",
    "BFE": "Betfair Exchange",
}


def canonical_team(name: str) -> str:
    cleaned = " ".join(str(name).replace("\xa0", " ").split())
    return TEAM_ALIASES.get(cleaned, cleaned)


def season_code(season_start: int) -> str:
    return f"{season_start % 100:02d}{(season_start + 1) % 100:02d}"


def parse_market_value(text: str) -> float:
    cleaned = (
        str(text).replace("\xa0", " ").replace("€", "")
        .replace("EUR", "").strip()
    )
    match = re.search(r"([0-9.]+(?:,[0-9]+)?)\s*(Mrd\.|Mio\.|Tsd\.)?", cleaned)
    if not match:
        return float("nan")
    number = float(match.group(1).replace(".", "").replace(",", "."))
    multiplier = {
        "Mrd.": 1_000_000_000,
        "Mio.": 1_000_000,
        "Tsd.": 1_000,
        None: 1,
    }[match.group(2)]
    return number * multiplier


def parse_transfermarkt_market_page(
    html: str,
    snapshot: str | pd.Timestamp,
) -> pd.DataFrame:
    snapshot = pd.Timestamp(snapshot).normalize()
    soup = BeautifulSoup(html, "html.parser")
    table = next(
        (
            candidate
            for candidate in soup.select("table.items")
            if any(
                header.get_text(" ", strip=True).startswith("Wert ")
                for header in candidate.select("thead th")
            )
        ),
        None,
    )
    if table is None:
        raise ValueError("Keine Transfermarkt-Stichtagstabelle gefunden")
    headers = [header.get_text(" ", strip=True) for header in table.select("thead th")]
    value_index = next(i for i, header in enumerate(headers) if header.startswith("Wert "))
    team_index = headers.index("Verein")
    squad_index = next(
        (i for i, header in enumerate(headers) if header.startswith("Kadergröße ")),
        None,
    )
    rows = []
    for row in table.select("tbody > tr"):
        cells = row.find_all("td", recursive=False)
        if len(cells) <= max(team_index, value_index):
            continue
        team = canonical_team(cells[team_index].get_text(" ", strip=True))
        value = parse_market_value(cells[value_index].get_text(" ", strip=True))
        if not team or pd.isna(value):
            continue
        squad_size = None
        if squad_index is not None and len(cells) > squad_index:
            squad_text = cells[squad_index].get_text(" ", strip=True)
            squad_size = int(squad_text) if squad_text.isdigit() else None
        rows.append(
            {
                "team": team,
                "as_of": snapshot,
                "squad_value_eur": value,
                "squad_size": squad_size,
                "source": "Transfermarkt Stichtag",
            }
        )
    result = pd.DataFrame(rows).drop_duplicates("team")
    if result.empty:
        # Manche Off-Season-Stichtage besitzen historisch keine Liga-Zuordnung.
        return result
    if len(result) < 10:
        raise ValueError(f"Nur {len(result)} Vereinsmarktwerte erkannt")
    return result


def fetch_transfermarkt_market_snapshot(
    league: str,
    snapshot: str | pd.Timestamp,
    raw_dir: Path = RAW_DIR,
    force: bool = False,
    timeout: int = 30,
) -> pd.DataFrame:
    if league not in TRANSFERMARKT_LEAGUES:
        raise ValueError(f"Unbekannte Liga {league}")
    snapshot_text = pd.Timestamp(snapshot).strftime("%Y-%m-%d")
    target = raw_dir / "transfermarkt" / f"{league}_{snapshot_text}.html"
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and not force:
        html = target.read_text(encoding="utf-8")
    else:
        info = TRANSFERMARKT_LEAGUES[league]
        url = TRANSFERMARKT_MARKET_URL.format(
            slug=info["slug"], competition=info["competition"], snapshot=snapshot_text
        )
        response = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; private-football-analysis/0.1)",
                "Accept-Language": "de-DE,de;q=0.9",
            },
            timeout=timeout,
        )
        response.raise_for_status()
        html = response.text
        parsed = parse_transfermarkt_market_page(html, snapshot_text)
        target.write_text(html, encoding="utf-8")
        return parsed
    return parse_transfermarkt_market_page(html, snapshot_text)


def quarterly_snapshot_dates(start_year: int, end_date: str | pd.Timestamp) -> list[pd.Timestamp]:
    end = pd.Timestamp(end_date).normalize()
    dates = []
    for year in range(start_year, end.year + 1):
        # Januar/April sowie September/November liegen sicher innerhalb einer
        # laufenden Saison. Exakte Sommer-Stichtage hatten in alten Jahren
        # teilweise noch keine Liga-Zuordnung.
        for month in (1, 4, 9, 11):
            snapshot = pd.Timestamp(year=year, month=month, day=1)
            if snapshot <= end:
                dates.append(snapshot)
    if end not in dates:
        dates.append(end)
    return sorted(set(dates))


def parse_football_data_csv(raw: bytes, league: str, season_start: int) -> pd.DataFrame:
    last_error = None
    rows = None
    for encoding in ("utf-8-sig", "cp1252", "latin1"):
        try:
            text = raw.decode(encoding)
            reader = csv.reader(io.StringIO(text))
            header = next(reader)
            index = {name.strip(): position for position, name in enumerate(header)}
            required = {"Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG"}
            if not required.issubset(index):
                raise ValueError(f"Unerwartete Football-Data-Spalten: {header}")
            # Alte Dateien besitzen vereinzelt eine zusaetzliche Wettquoten-Spalte.
            # csv.reader ist hier robuster als der pandas-C-Parser; wir lesen nur
            # die stabilen Ergebnisfelder am Anfang jeder Zeile.
            rows = [row for row in reader if len(row) > max(index[name] for name in required)]
            break
        except UnicodeDecodeError as exc:
            last_error = exc
    if rows is None:
        raise ValueError(f"CSV konnte nicht gelesen werden: {last_error}")
    def column(name: str) -> list[str]:
        return [row[index[name]].strip() for row in rows]

    dates = pd.to_datetime(column("Date"), format="mixed", dayfirst=True, errors="coerce")
    result = pd.DataFrame(
        {
            "competition": league,
            "season": int(season_start),
            "match_date": dates,
            "matchday": pd.NA,
            "home_team": pd.Series(column("HomeTeam")).map(canonical_team),
            "away_team": pd.Series(column("AwayTeam")).map(canonical_team),
            "home_goals": pd.to_numeric(column("FTHG"), errors="coerce"),
            "away_goals": pd.to_numeric(column("FTAG"), errors="coerce"),
            "source": "football-data.co.uk",
        }
    )
    result = result.dropna(subset=["match_date", "home_team", "away_team"])
    result["status"] = result["home_goals"].notna().map({True: "FINISHED", False: "SCHEDULED"})
    return result.reset_index(drop=True)


def parse_football_data_odds(raw: bytes, league: str, season_start: int) -> pd.DataFrame:
    """Liest alle vollstaendigen 1X2-Opening- und Closing-Quotentripel."""
    text = None
    for encoding in ("utf-8-sig", "cp1252", "latin1"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise ValueError("Quoten-CSV konnte nicht dekodiert werden")
    reader = csv.reader(io.StringIO(text))
    header = [name.strip() for name in next(reader)]
    index = {name: position for position, name in enumerate(header)}
    required = {"HomeTeam", "AwayTeam"}
    if not required.issubset(index):
        raise ValueError(f"Unerwartete Football-Data-Spalten: {header}")

    triples: list[tuple[str, str, str, str, str]] = []
    # Closing zuerst erkennen, damit z.B. B365CH nicht als normales H-Feld gilt.
    for home_column in header:
        if home_column.endswith("CH"):
            prefix = home_column[:-2]
            draw_column, away_column = prefix + "CD", prefix + "CA"
            if draw_column in index and away_column in index:
                triples.append((prefix, "closing", home_column, draw_column, away_column))
    for home_column in header:
        if not home_column.endswith("H") or home_column.endswith("CH"):
            continue
        prefix = home_column[:-1]
        draw_column, away_column = prefix + "D", prefix + "A"
        if draw_column in index and away_column in index:
            triples.append((prefix, "opening", home_column, draw_column, away_column))

    rows = []
    for raw_row in reader:
        if len(raw_row) <= max(index["HomeTeam"], index["AwayTeam"]):
            continue
        home_team = canonical_team(raw_row[index["HomeTeam"]].strip())
        away_team = canonical_team(raw_row[index["AwayTeam"]].strip())
        for prefix, snapshot_type, home_column, draw_column, away_column in triples:
            try:
                values = [
                    float(raw_row[index[column]].strip())
                    for column in (home_column, draw_column, away_column)
                ]
            except (IndexError, TypeError, ValueError):
                continue
            if not all(value > 1.0 for value in values):
                continue
            rows.append(
                {
                    "competition": league,
                    "season": int(season_start),
                    "home_team": home_team,
                    "away_team": away_team,
                    "bookmaker": BOOKMAKER_NAMES.get(prefix, prefix),
                    "snapshot_type": snapshot_type,
                    "observed_at": pd.NaT,
                    "home_odds": values[0],
                    "draw_odds": values[1],
                    "away_odds": values[2],
                    "source": "football-data.co.uk",
                }
            )
    return pd.DataFrame(rows)


def fetch_football_data_season(
    league: str,
    season_start: int,
    raw_dir: Path = RAW_DIR,
    force: bool = False,
    timeout: int = 30,
) -> pd.DataFrame:
    if league not in LEAGUES:
        raise ValueError(f"Unbekannte Liga {league}; erlaubt: {sorted(LEAGUES)}")
    target = raw_dir / "football-data" / f"{league}_{season_start}.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() and not force:
        raw = target.read_bytes()
    else:
        url = FOOTBALL_DATA_URL.format(season_code=season_code(season_start), league=league)
        response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
        response.raise_for_status()
        raw = response.content
        if b"<html" in raw[:500].lower():
            raise ValueError(f"Statt CSV wurde HTML geliefert: {url}")
        target.write_bytes(raw)
    return parse_football_data_csv(raw, league, season_start)


def parse_openligadb_matches(payload: list[dict], league: str, season_start: int) -> pd.DataFrame:
    rows = []
    for match in payload:
        results = match.get("matchResults") or []
        final = next((r for r in results if r.get("resultTypeID") == 2), None)
        if final is None and results:
            final = max(results, key=lambda r: r.get("resultOrderID", 0))
        finished = bool(match.get("matchIsFinished")) and final is not None
        group = match.get("group") or {}
        match_date = pd.to_datetime(
            match.get("matchDateTimeUTC") or match.get("matchDateTime"), utc=True
        )
        if pd.notna(match_date):
            match_date = match_date.tz_localize(None)
        rows.append(
            {
                "competition": league,
                "season": int(season_start),
                "match_date": match_date,
                "matchday": group.get("groupOrderID"),
                "home_team": canonical_team((match.get("team1") or {}).get("teamName", "")),
                "away_team": canonical_team((match.get("team2") or {}).get("teamName", "")),
                "home_goals": final.get("pointsTeam1") if finished else None,
                "away_goals": final.get("pointsTeam2") if finished else None,
                "status": "FINISHED" if finished else "SCHEDULED",
                "source": "OpenLigaDB",
                "source_match_id": match.get("matchID"),
            }
        )
    return pd.DataFrame(rows)


def fetch_openligadb_season(
    league: str,
    season_start: int,
    timeout: int = 30,
    pause: float = 0.05,
) -> pd.DataFrame:
    if league not in LEAGUES:
        raise ValueError(f"Unbekannte Liga {league}; erlaubt: {sorted(LEAGUES)}")
    api_league = LEAGUES[league]["openligadb_code"]
    frames = []
    for matchday in range(1, 35):
        url = OPENLIGADB_URL.format(league=api_league, season=season_start, matchday=matchday)
        response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        if payload:
            frames.append(parse_openligadb_matches(payload, league, season_start))
        time.sleep(pause)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def parse_odds_api_payload(
    payload: list[dict], league: str, season_start: int
) -> pd.DataFrame:
    rows = []
    for event in payload:
        home_team = canonical_team(event.get("home_team", ""))
        away_team = canonical_team(event.get("away_team", ""))
        if not home_team or not away_team:
            continue
        for bookmaker in event.get("bookmakers") or []:
            market = next(
                (item for item in bookmaker.get("markets") or [] if item.get("key") == "h2h"),
                None,
            )
            if market is None:
                continue
            prices: dict[str, float] = {}
            for outcome in market.get("outcomes") or []:
                name = str(outcome.get("name", ""))
                if name.lower() in {"draw", "tie", "unentschieden"}:
                    prices["draw"] = outcome.get("price")
                elif canonical_team(name) == home_team:
                    prices["home"] = outcome.get("price")
                elif canonical_team(name) == away_team:
                    prices["away"] = outcome.get("price")
            if set(prices) != {"home", "draw", "away"}:
                continue
            rows.append(
                {
                    "competition": league,
                    "season": int(season_start),
                    "home_team": home_team,
                    "away_team": away_team,
                    "bookmaker": bookmaker.get("title") or bookmaker.get("key") or "Unknown",
                    "snapshot_type": "captured",
                    "observed_at": market.get("last_update") or bookmaker.get("last_update"),
                    "home_odds": prices["home"],
                    "draw_odds": prices["draw"],
                    "away_odds": prices["away"],
                    "source": "the-odds-api.com",
                }
            )
    return pd.DataFrame(rows)


def fetch_live_bookmaker_odds(
    league: str,
    season_start: int,
    api_key: str,
    regions: str = "eu",
    timeout: int = 30,
) -> pd.DataFrame:
    if league not in ODDS_API_SPORTS:
        raise ValueError(f"Keine Odds-API-Zuordnung fuer {league}")
    response = requests.get(
        ODDS_API_URL.format(sport=ODDS_API_SPORTS[league]),
        params={
            "apiKey": api_key,
            "regions": regions,
            "markets": "h2h",
            "oddsFormat": "decimal",
            "dateFormat": "iso",
        },
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
    )
    if not response.ok:
        raise RuntimeError(f"Odds-API antwortet mit HTTP {response.status_code}")
    return parse_odds_api_payload(response.json(), league, season_start)


def import_historical(
    leagues: list[str],
    start_season: int,
    end_season: int,
    db_path: Path = DB_PATH,
    force: bool = False,
) -> int:
    initialize(db_path)
    count = 0
    for league in leagues:
        for season in range(start_season, end_season + 1):
            try:
                matches = fetch_football_data_season(league, season, force=force)
                count += upsert_matches(matches, db_path)
                raw_path = RAW_DIR / "football-data" / f"{league}_{season}.csv"
                odds = parse_football_data_odds(raw_path.read_bytes(), league, season)
                odds_count = upsert_bookmaker_odds(odds, db_path)
                count += odds_count
                print(
                    f"{league} {season}/{str(season + 1)[-2:]}: "
                    f"{len(matches)} Spiele, {odds_count} Quotensaetze"
                )
            except requests.HTTPError as exc:
                print(f"{league} {season}: nicht verfuegbar ({exc.response.status_code})")
    return count


def import_live(league: str, season: int, db_path: Path = DB_PATH) -> int:
    matches = fetch_openligadb_season(league, season)
    return upsert_matches(matches, db_path)


def import_live_bookmaker_odds(
    league: str,
    season: int,
    api_key: str,
    regions: str = "eu",
    db_path: Path = DB_PATH,
) -> int:
    odds = fetch_live_bookmaker_odds(league, season, api_key, regions=regions)
    if odds.empty:
        return 0
    scheduled = load_matches(competition=league, seasons=[season], path=db_path)
    pair_columns = ["competition", "season", "home_team", "away_team"]
    known_pairs = pd.MultiIndex.from_frame(scheduled[pair_columns])
    odds_pairs = pd.MultiIndex.from_frame(odds[pair_columns])
    known = odds_pairs.isin(known_pairs)
    if not known.all():
        unresolved = odds.loc[~known, pair_columns].drop_duplicates()
        print(
            "Warnung: API-Paarungen ohne passenden Spielplan werden ausgelassen:\n"
            f"{unresolved.to_string(index=False)}",
            flush=True,
        )
        odds = odds.loc[known].copy()
    return upsert_bookmaker_odds(odds, db_path)


def import_market_values(csv_path: Path, db_path: Path = DB_PATH) -> int:
    values = pd.read_csv(csv_path)
    values["team"] = values["team"].map(canonical_team)
    values["as_of"] = pd.to_datetime(values["as_of"], errors="raise")
    return upsert_market_values(values, db_path)


def import_bookmaker_odds(csv_path: Path, db_path: Path = DB_PATH) -> int:
    """Importiert manuelle/API-Quoten, insbesondere zeitgestempelte Live-Captures."""
    odds = pd.read_csv(csv_path)
    for column in ("home_team", "away_team"):
        if column in odds:
            odds[column] = odds[column].map(canonical_team)
    if "observed_at" in odds:
        odds["observed_at"] = pd.to_datetime(odds["observed_at"], errors="coerce", utc=True)
    if "source" not in odds:
        odds["source"] = f"CSV:{csv_path.name}"
    if "snapshot_type" not in odds:
        odds["snapshot_type"] = "captured"
    return upsert_bookmaker_odds(odds, db_path)


def import_transfermarkt_snapshots(
    leagues: list[str],
    start_year: int,
    end_date: str | pd.Timestamp,
    db_path: Path = DB_PATH,
    force: bool = False,
    pause: float = 1.5,
) -> int:
    count = 0
    dates = quarterly_snapshot_dates(start_year, end_date)
    total = len(leagues) * len(dates)
    number = 0
    for league in leagues:
        for snapshot in dates:
            number += 1
            values = fetch_transfermarkt_market_snapshot(league, snapshot, force=force)
            if values.empty:
                print(
                    f"[{number:>3}/{total}] {league} {snapshot.date()}: "
                    "kein historischer Ligastand, uebersprungen",
                    flush=True,
                )
                continue
            count += upsert_market_values(values, db_path)
            print(
                f"[{number:>3}/{total}] {league} {snapshot.date()}: "
                f"{len(values)} Vereine",
                flush=True,
            )
            if number < total:
                time.sleep(max(pause, 0.0))
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Bundesliga-Ergebnisse in SQLite importieren")
    sub = parser.add_subparsers(dest="command", required=True)
    historical = sub.add_parser("historical", help="Football-Data-Saisons importieren")
    historical.add_argument("--leagues", nargs="+", choices=sorted(LEAGUES), default=["D1", "D2"])
    historical.add_argument("--start", type=int, default=1993)
    historical.add_argument("--end", type=int, default=2025)
    historical.add_argument("--force", action="store_true")
    live = sub.add_parser("live", help="Spielplan/Ergebnisse von OpenLigaDB importieren")
    live.add_argument("--league", choices=sorted(LEAGUES), default="D1")
    live.add_argument("--season", type=int, required=True)
    live_odds = sub.add_parser("live-odds", help="Aktuelle 1X2-Quoten mit Abrufzeit speichern")
    live_odds.add_argument("--league", choices=sorted(ODDS_API_SPORTS), default="D1")
    live_odds.add_argument("--season", type=int, required=True)
    live_odds.add_argument("--regions", default="eu")
    live_odds.add_argument("--api-key-env", default="THE_ODDS_API_KEY")
    market = sub.add_parser("market-values", help="Lizenzierte/manuelle Marktwert-CSV importieren")
    market.add_argument("--file", type=Path, required=True)
    odds = sub.add_parser("bookmaker-odds", help="Manuelle oder per API gelieferte 1X2-Quoten")
    odds.add_argument("--file", type=Path, required=True)
    transfermarkt = sub.add_parser(
        "transfermarkt", help="Vereinsmarktwerte von wenigen historischen Stichtagsseiten importieren"
    )
    transfermarkt.add_argument("--leagues", nargs="+", choices=sorted(TRANSFERMARKT_LEAGUES), default=["D1"])
    transfermarkt.add_argument("--start-year", type=int, default=2011)
    transfermarkt.add_argument("--end-date", default=date.today().isoformat())
    transfermarkt.add_argument("--pause", type=float, default=1.5)
    transfermarkt.add_argument("--force", action="store_true")
    args = parser.parse_args()
    if args.command == "historical":
        count = import_historical(args.leagues, args.start, args.end, force=args.force)
    elif args.command == "live":
        count = import_live(args.league, args.season)
    elif args.command == "live-odds":
        api_key = os.environ.get(args.api_key_env)
        if not api_key:
            raise ValueError(f"Umgebungsvariable {args.api_key_env} ist nicht gesetzt")
        count = import_live_bookmaker_odds(
            args.league, args.season, api_key, regions=args.regions
        )
    elif args.command == "market-values":
        count = import_market_values(args.file)
    elif args.command == "bookmaker-odds":
        count = import_bookmaker_odds(args.file)
    else:
        count = import_transfermarkt_snapshots(
            args.leagues, args.start_year, args.end_date,
            force=args.force, pause=args.pause,
        )
    print(f"Datenbank aktualisiert: {count} Datensaetze")


if __name__ == "__main__":
    main()
