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

MONEY_RE = re.compile(
    r"\d+(?:[,.]\d+)?\s*(?:Mrd\.?|Mio\.?|Tsd\.?|bn|m\b|k\b)",
    re.I,
)

TM_NAME_TO_TEAM = {
    "Frankreich": "France",
    "England": "England",
    "Spanien": "Spain",
    "Portugal": "Portugal",
    "Deutschland": "Germany",
    "Brasilien": "Brazil",
    "Niederlande": "Netherlands",
    "Argentinien": "Argentina",
    "Norwegen": "Norway",
    "Belgien": "Belgium",
    "Elfenbeinküste": "Ivory Coast",
    "Marokko": "Morocco",
    "Senegal": "Senegal",
    "Türkei": "Turkey",
    "Schweden": "Sweden",
    "Uruguay": "Uruguay",
    "Kroatien": "Croatia",
    "Vereinigte Staaten": "United States",
    "Ecuador": "Ecuador",
    "Schweiz": "Switzerland",
    "Kolumbien": "Colombia",
    "Japan": "Japan",
    "Algerien": "Algeria",
    "Österreich": "Austria",
    "Ghana": "Ghana",
    "Kanada": "Canada",
    "Mexiko": "Mexico",
    "Tschechien": "Czech Republic",
    "Schottland": "Scotland",
    "Paraguay": "Paraguay",
    "Bosnien-Herzegowina": "Bosnia and Herzegovina",
    "Demokratische Republik Kongo": "DR Congo",
    "Südkorea": "South Korea",
    "Ägypten": "Egypt",
    "Australien": "Australia",
    "Tunesien": "Tunisia",
    "Usbekistan": "Uzbekistan",
    "Kap Verde": "Cape Verde",
    "Haiti": "Haiti",
    "Südafrika": "South Africa",
    "Saudi-Arabien": "Saudi Arabia",
    "Neuseeland": "New Zealand",
    "Panama": "Panama",
    "Iran": "Iran",
    "Curaçao": "Curacao",
    "Irak": "Iraq",
    "Katar": "Qatar",
    "Jordanien": "Jordan",
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
    "United States":    ("vereinigte-staaten",     3505),
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
    text = (
        text.replace("\xa0", " ")
        .replace("€", "")
        .replace("â‚¬", "")
        .replace("EUR", "")
        .strip()
    )
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


def get_via_overview_page() -> dict[str, dict[str, object]]:
    """
    Versucht alle Marktwerte ueber die Transfermarkt-WM-2026-Seite zu holen.
    Gibt {team_name: marktwert} zurueck.
    """
    url = "https://www.transfermarkt.de/weltmeisterschaft/teilnehmer/pokalwettbewerb/FIWC"
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
            team_name = TM_NAME_TO_TEAM.get(team_tag.get_text(strip=True), team_tag.get_text(strip=True))
            href = team_tag.get("href", "")
            link_match = re.search(r"/([^/]+)/(?:startseite|kader)/verein/(\d+)", href)
            slug = link_match.group(1) if link_match else ""
            tm_id = int(link_match.group(2)) if link_match else None
            money_values = MONEY_RE.findall(row.get_text(" ", strip=True))
            parsed_values = [parse_market_value(v) for v in money_values]
            value = max([v for v in parsed_values if v == v], default=float("nan"))
            if team_name and value == value:  # nicht NaN
                result[team_name] = {"value": value, "slug": slug, "tm_id": tm_id}
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


def get_position_values(slug: str, tm_id: int) -> dict[str, float]:
    """Liest die Kaderdetails nach Positionen von der Teamseite."""
    url = f"https://www.transfermarkt.de/{slug}/kader/verein/{tm_id}"
    empty = {
        "market_value_gk_eur": float("nan"),
        "market_value_def_eur": float("nan"),
        "market_value_mid_eur": float("nan"),
        "market_value_att_eur": float("nan"),
    }
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return empty
        soup = BeautifulSoup(r.text, "html.parser")
        mapping = {
            "Torwart": "market_value_gk_eur",
            "Abwehr": "market_value_def_eur",
            "Mittelfeld": "market_value_mid_eur",
            "Sturm": "market_value_att_eur",
        }
        result = empty.copy()
        position_table = None
        for box in soup.select("div.box"):
            headline = box.select_one(".content-box-headline")
            if headline and "Kaderdetails nach Positionen" in headline.get_text(" ", strip=True):
                position_table = box.select_one("table")
                break
        if position_table is None:
            return result

        for cell in position_table.select("td.hauptlink"):
            pos = cell.get_text(" ", strip=True)
            if pos not in mapping:
                continue
            sibling_texts = []
            sibling = cell
            for _ in range(4):
                sibling = sibling.find_next_sibling("td")
                if sibling is None:
                    break
                sibling_texts.append(sibling.get_text(" ", strip=True))
            money_values = MONEY_RE.findall(" ".join(sibling_texts))
            values = [parse_market_value(v) for v in money_values]
            result[mapping[pos]] = max([v for v in values if v == v], default=float("nan"))
        return result
    except Exception as e:
        print(f"    Positionswerte Fehler: {e}")
        return empty


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
        page_slug, page_tm_id = slug, tm_id

        # Uebersichtsseite: Teamname-Matching (Transfermarkt-Name != results.csv-Name)
        if overview:
            for tm_name, info in overview.items():
                if slug.replace("-", " ").lower() in tm_name.lower() or \
                   team.lower() in tm_name.lower():
                    value = info["value"]
                    page_slug = info.get("slug") or page_slug
                    page_tm_id = info.get("tm_id") or page_tm_id
                    break

        # Fallback: Einzelseite
        if value != value:  # NaN
            print(f"  Einzelseite: {team} ...")
            value = get_via_team_page(page_slug, page_tm_id)
            time.sleep(1.5)

        pos_values = get_position_values(page_slug, page_tm_id)
        if all(v == v for v in pos_values.values()):
            value = sum(v for v in pos_values.values() if v == v)
        time.sleep(0.5)

        status = f"{value/1_000_000:>8.1f} Mio. EUR" if value == value else "     n/a"
        print(f"  {team:<35} {status}")
        records.append({"team": team, "squad_value_eur": value, **pos_values})

    df = pd.DataFrame(records)
    n_ok = df["squad_value_eur"].notna().sum()
    out = DATA_DIR / "squad_values.csv"
    if n_ok == 0 and out.exists():
        print("\nKeine Marktwerte gefunden; bestehende squad_values.csv bleibt erhalten.")
        return
    df.to_csv(out, index=False)

    print(f"\nGespeichert: {out}")
    print(f"Marktwerte gefunden: {n_ok}/{len(df)} Teams")
    print("\nTop 10 nach Marktwert:")
    print(
        df.dropna(subset=["squad_value_eur"])
        .sort_values("squad_value_eur", ascending=False)
        .head(10)
        .assign(mio=lambda x: (x["squad_value_eur"] / 1e6).round(1))
        [["team", "mio"]]
        .to_string(index=False)
    )


if __name__ == "__main__":
    main()
