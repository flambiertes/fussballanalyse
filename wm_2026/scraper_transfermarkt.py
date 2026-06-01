"""
Holt Kader-Marktwerte aller WM-2026-Nationen von Transfermarkt.
Ausgabe: data/squad_values.csv  (team, squad_value_eur)

Zwei Methoden:
  1. WM-2026-Uebersichtsseite auf Transfermarkt (alle Teams auf einmal)
  2. Fallback: Einzelne Team-Seiten mit bekannten Transfermarkt-IDs
"""

import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}

# Transfermarkt: Slug + ID fuer jede WM-2026-Nation
# Format: "Name in results.csv": ("tm-slug", tm-id)
WM_TEAMS = {
    # Gruppe A
    "Mexico":           ("mexiko",                3458),
    "South Africa":     ("suedafrika",             3727),
    "South Korea":      ("suedkorea",              3464),
    "Czech Republic":   ("tschechien",             3375),
    # Gruppe B
    "Canada":           ("kanada",                 3716),
    "Bosnia and Herzegovina": ("bosnien-herzegowina", 3384),
    "Qatar":            ("katar",                  7352),
    "Switzerland":      ("schweiz",                3379),
    # Gruppe C
    "Brazil":           ("brasilien",              3439),
    "Morocco":          ("marokko",                3717),
    "Haiti":            ("haiti",                  3712),
    "Scotland":         ("schottland",             3388),
    # Gruppe D
    "United States":    ("vereinigte-staaten",     3438),
    "Paraguay":         ("paraguay",               3447),
    "Australia":        ("australien",             3460),
    "Turkey":           ("tuerkei",                3381),
    # Gruppe E
    "Germany":          ("deutschland",            3262),
    "Curacao":          ("curacao",                9753),
    "Ivory Coast":      ("elfenbeinkueste",        3726),
    "Ecuador":          ("ecuador",                3456),
    # Gruppe F
    "Netherlands":      ("niederlande",            3378),
    "Japan":            ("japan",                  3468),
    "Sweden":           ("schweden",               3387),
    "Tunisia":          ("tunesien",               3720),
    # Gruppe G
    "Belgium":          ("belgien",                3376),
    "Egypt":            ("aegypten",               3718),
    "Iran":             ("iran",                   3466),
    "New Zealand":      ("neuseeland",             3461),
    # Gruppe H
    "Spain":            ("spanien",                3374),
    "Cape Verde":       ("kap-verde",              7399),
    "Saudi Arabia":     ("saudi-arabien",          3469),
    "Uruguay":          ("uruguay",                3443),
    # Gruppe I
    "France":           ("frankreich",             3377),
    "Senegal":          ("senegal",                3700),
    "Iraq":             ("irak",                   3473),
    "Norway":           ("norwegen",               3389),
    # Gruppe J
    "Argentina":        ("argentinien",            3437),
    "Algeria":          ("algerien",               3715),
    "Austria":          ("oesterreich",            3383),
    "Jordan":           ("jordanien",              3476),
    # Gruppe K
    "Portugal":         ("portugal",               3373),
    "DR Congo":         ("demokratische-republik-kongo", 3732),
    "Uzbekistan":       ("usbekistan",             3483),
    "Colombia":         ("kolumbien",              3457),
    # Gruppe L
    "England":          ("england",                3066),
    "Croatia":          ("kroatien",               3382),
    "Ghana":            ("ghana",                  3722),
    "Panama":           ("panama",                 3713),
}


def parse_market_value(text: str) -> float:
    """Konvertiert '1,20 Mrd. €' oder '540 Mio. €' oder '12,50 Mio. €' in float (EUR)."""
    if not text:
        return float("nan")
    text = text.replace("\xa0", " ").replace("€", "").strip()
    multiplier = 1.0
    if "Mrd" in text or "bn" in text.lower():
        multiplier = 1_000_000_000
        text = re.sub(r"(Mrd\.?|bn)", "", text, flags=re.I)
    elif "Mio" in text or "m" in text.lower():
        multiplier = 1_000_000
        text = re.sub(r"(Mio\.?|m\b)", "", text, flags=re.I)
    elif "Tsd" in text or "k" in text.lower():
        multiplier = 1_000
        text = re.sub(r"(Tsd\.?|k\b)", "", text, flags=re.I)
    text = text.replace(",", ".").strip()
    try:
        return float(text) * multiplier
    except ValueError:
        return float("nan")


def get_via_overview_page() -> dict[str, float]:
    """
    Versucht alle Marktwerte ueber die Transfermarkt-WM-2026-Seite zu holen.
    Gibt {team_name: marktwert} zurueck.
    """
    url = "https://www.transfermarkt.de/weltmeisterschaft-2026/teilnehmer/pokalwettbewerb/WM26"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print(f"  Uebersichtsseite nicht erreichbar (Status {r.status_code})")
            return {}
        soup = BeautifulSoup(r.text, "html.parser")
        rows = soup.select("table.items tbody tr")
        if not rows:
            print("  Keine Tabellenzeilen gefunden auf Uebersichtsseite.")
            return {}

        result = {}
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue
            team_tag = row.select_one("td.hauptlink a")
            if not team_tag:
                continue
            team_name = team_tag.get_text(strip=True)
            # Letzter <td> enthaelt Gesamtmarktwert
            value_text = cells[-1].get_text(strip=True)
            value = parse_market_value(value_text)
            if team_name and value == value:  # nicht NaN
                result[team_name] = value
        return result
    except Exception as e:
        print(f"  Fehler bei Uebersichtsseite: {e}")
        return {}


def get_via_team_page(slug: str, tm_id: int) -> float:
    """Holt Marktwert einer einzelnen Nationalmannschaft."""
    url = f"https://www.transfermarkt.de/{slug}/kader/verein/{tm_id}/saison_id/2025"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return float("nan")
        soup = BeautifulSoup(r.text, "html.parser")

        # Methode 1: data-header__market-value-wrapper
        tag = soup.select_one("div.data-header__market-value-wrapper a")
        if tag:
            return parse_market_value(tag.get_text())

        # Methode 2: Suche nach "Gesamtmarktwert"
        for span in soup.find_all("span", class_="data-header__label"):
            if "marktwert" in span.get_text().lower():
                val_span = span.find_next_sibling("a") or span.find_next("a")
                if val_span:
                    return parse_market_value(val_span.get_text())
    except Exception as e:
        print(f"    Fehler: {e}")
    return float("nan")


def main():
    print("=== Transfermarkt Marktwerte WM 2026 ===\n")

    # Schritt 1: Versuche Uebersichtsseite
    print("Versuche WM-Uebersichtsseite ...")
    overview = get_via_overview_page()
    if overview:
        print(f"  {len(overview)} Teams via Uebersichtsseite gefunden.")
    else:
        print("  Fallback: Einzelne Team-Seiten werden abgerufen.")

    records = []
    for team, (slug, tm_id) in WM_TEAMS.items():
        value = float("nan")

        # Uebersichtsseite: Teamname-Matching (Transfermarkt-Name != results.csv-Name)
        if overview:
            for tm_name, v in overview.items():
                if slug.replace("-", " ").lower() in tm_name.lower() or \
                   team.lower() in tm_name.lower():
                    value = v
                    break

        # Fallback: Einzelseite
        if value != value:  # NaN
            print(f"  Einzelseite: {team} ...")
            value = get_via_team_page(slug, tm_id)
            time.sleep(1.5)

        status = f"{value/1_000_000:>8.1f} Mio. EUR" if value == value else "     n/a"
        print(f"  {team:<35} {status}")
        records.append({"team": team, "squad_value_eur": value})

    df = pd.DataFrame(records)
    n_ok = df["squad_value_eur"].notna().sum()
    out = DATA_DIR / "squad_values.csv"
    df.to_csv(out, index=False)

    print(f"\nGespeichert: {out}")
    print(f"Marktwerte gefunden: {n_ok}/{len(df)} Teams")
    print("\nTop 10 nach Marktwert:")
    print(
        df.dropna()
        .sort_values("squad_value_eur", ascending=False)
        .head(10)
        .assign(mio=lambda x: (x["squad_value_eur"] / 1e6).round(1))
        [["team", "mio"]]
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
