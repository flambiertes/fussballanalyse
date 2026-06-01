"""
Importiert manuell eingegebene Laenderspiel-Ergebnisse in results.csv.
Erwartet das Format:
  DD/MM/YYYY
  Heimteam
  Heimtore
  Auswärtstore
  Auswärtsteam
  (naechstes Spiel...)
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

DATA_DIR = Path(__file__).resolve().parent / "data"

# Uebersetzung Deutsch -> Englisch (Kaggle-Format)
DE_TO_EN = {
    "Afghanistan": "Afghanistan",
    "Ägypten": "Egypt",
    "Albanien": "Albania",
    "Algerien": "Algeria",
    "Andorra": "Andorra",
    "Angola": "Angola",
    "Argentinien": "Argentina",
    "Armenien": "Armenia",
    "Äquatorialguinea": "Equatorial Guinea",
    "Aserbaidschan": "Azerbaijan",
    "Äthiopien": "Ethiopia",
    "Australien": "Australia",
    "Bahrain": "Bahrain",
    "Belgien": "Belgium",
    "Benin": "Benin",
    "Bolivien": "Bolivia",
    "Bosnien und Herzegowina": "Bosnia and Herzegovina",
    "Botsuana": "Botswana",
    "Brasilien": "Brazil",
    "Bulgarien": "Bulgaria",
    "Burkina Faso": "Burkina Faso",
    "Burundi": "Burundi",
    "Chile": "Chile",
    "China": "China PR",
    "Costa Rica": "Costa Rica",
    "Curaçao": "Curacao",
    "Dänemark": "Denmark",
    "Deutschland": "Germany",
    "Ecuador": "Ecuador",
    "Elfenbeinküste": "Ivory Coast",
    "England": "England",
    "Finnland": "Finland",
    "Frankreich": "France",
    "Gambia": "Gambia",
    "Ghana": "Ghana",
    "Grenada": "Grenada",
    "Griechenland": "Greece",
    "Guinea": "Guinea",
    "Haiti": "Haiti",
    "Honduras": "Honduras",
    "Indien": "India",
    "Iran": "Iran",
    "Irak": "Iraq",
    "Irland": "Republic of Ireland",
    "Island": "Iceland",
    "Israel": "Israel",
    "Italien": "Italy",
    "Jamaika": "Jamaica",
    "Japan": "Japan",
    "Jordanien": "Jordan",
    "Kamerun": "Cameroon",
    "Kanada": "Canada",
    "Kap Verde": "Cape Verde",
    "Kasachstan": "Kazakhstan",
    "Katar": "Qatar",
    "Kirgisistan": "Kyrgyzstan",
    "Kolumbien": "Colombia",
    "Komoren": "Comoros",
    "Kosovo": "Kosovo",
    "Kroatien": "Croatia",
    "Kuba": "Cuba",
    "Lebanon": "Lebanon",
    "Libanon": "Lebanon",
    "Liberia": "Liberia",
    "Libyen": "Libya",
    "Madagaskar": "Madagascar",
    "Malawi": "Malawi",
    "Mali": "Mali",
    "Marokko": "Morocco",
    "Mauretanien": "Mauritania",
    "Mexiko": "Mexico",
    "Moldova": "Moldova",
    "Mongolei": "Mongolia",
    "Montenegro": "Montenegro",
    "Mosambik": "Mozambique",
    "Neuseeland": "New Zealand",
    "Nicaragua": "Nicaragua",
    "Niederlande": "Netherlands",
    "Niger": "Niger",
    "Nigeria": "Nigeria",
    "Nordirland": "Northern Ireland",
    "Nordmazedonien": "North Macedonia",
    "Norwegen": "Norway",
    "Österreich": "Austria",
    "Palästina": "Palestine",
    "Panama": "Panama",
    "Paraguay": "Paraguay",
    "Peru": "Peru",
    "Polen": "Poland",
    "Portugal": "Portugal",
    "Republik Korea": "South Korea",
    "Rumänien": "Romania",
    "Russland": "Russia",
    "Sambia": "Zambia",
    "San Marino": "San Marino",
    "Saudi-Arabien": "Saudi Arabia",
    "Schottland": "Scotland",
    "Schweden": "Sweden",
    "Schweiz": "Switzerland",
    "Senegal": "Senegal",
    "Serbien": "Serbia",
    "Simbabwe": "Zimbabwe",
    "Singapur": "Singapore",
    "Slowakei": "Slovakia",
    "Slowenien": "Slovenia",
    "Spanien": "Spain",
    "Sudan": "Sudan",
    "Südafrika": "South Africa",
    "Südkorea": "South Korea",
    "Syrien": "Syria",
    "Togo": "Togo",
    "Trinidad & Tobago": "Trinidad and Tobago",
    "Tschechien": "Czech Republic",
    "Tunesien": "Tunisia",
    "Türkei": "Turkey",
    "Turkmenistan": "Turkmenistan",
    "Tschad": "Chad",
    "Uganda": "Uganda",
    "Ukraine": "Ukraine",
    "Ungarn": "Hungary",
    "Uruguay": "Uruguay",
    "USA": "United States",
    "Usbekistan": "Uzbekistan",
    "Venezuela": "Venezuela",
    "Wales": "Wales",
    "Weißrussland": "Belarus",
}

RAW_DATA = """
31/03/2026
Haiti
1
1
Island
China
0
2
Kamerun
Äquatorialguinea
1
1
Madagaskar
Australien
5
1
Curaçao
Kasachstan
1
0
Komoren
Botsuana
1
0
Malawi
Iran
5
0
Costa Rica
Liberia
2
2
Libyen
Simbabwe
1
0
Sambia
Serbien
2
1
Saudi-Arabien
San Marino
0
0
Andorra
Niger
0
1
Togo
Norwegen
0
0
Schweiz
Montenegro
2
3
Slowenien
Ungarn
0
0
Griechenland
Russland
0
0
Mali
Jordanien
2
2
Nigeria
Südafrika
1
2
Panama
Guinea
0
1
Benin
Peru
2
2
Honduras
Marokko
2
1
Paraguay
Algerien
0
0
Uruguay
Schottland
0
1
Elfenbeinküste
Niederlande
1
1
Ecuador
Irland
0
0
Nordmazedonien
England
0
1
Japan
Ukraine
1
0
Albanien
Österreich
1
0
Republik Korea
Wales
1
1
Nordirland
Slowakei
2
0
Rumänien
Senegal
3
1
Gambia
Spanien
0
0
Ägypten
USA
0
2
Portugal
Argentinien
5
0
Sambia
Kanada
0
0
Tunesien
01/04/2026
Brasilien
3
1
Kroatien
Mexiko
1
1
Belgien
16/05/2026
Irland
5
0
Grenada
23/05/2026
Mexiko
2
0
Ghana
26/05/2026
Marokko
5
0
Burundi
Nigeria
2
0
Simbabwe
27/05/2026
Jamaika
2
0
Indien
28/05/2026
Ägypten
1
0
Russland
Irland
1
0
Katar
29/05/2026
Iran
3
1
Gambia
Südafrika
0
0
Nicaragua
Andorra
0
1
Irak
Bosnien und Herzegowina
0
0
Nordmazedonien
30/05/2026
Schottland
4
1
Curaçao
Indien
0
1
Simbabwe
Jamaika
0
3
Nigeria
31/05/2026
Ecuador
2
1
Saudi-Arabien
Republik Korea
5
0
Trinidad & Tobago
Mexiko
1
0
Australien
Japan
1
0
Island
Singapur
4
0
Mongolei
Schweiz
4
1
Jordanien
Tschechien
2
1
Kosovo
Kap Verde
3
0
Serbien
Polen
0
2
Ukraine
Deutschland
4
0
Finnland
USA
3
2
Senegal
Brasilien
6
2
Panama
"""


def translate(name: str) -> str:
    name = name.strip()
    en = DE_TO_EN.get(name)
    if en is None:
        print(f"  WARNUNG: Kein Mapping fuer '{name}' - wird unveraendert uebernommen")
        return name
    return en


def parse_raw(text: str) -> list[dict]:
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    skip_tokens = {"verschoben", "verschieben", "abgesagt", "kommentar", "-"}

    records = []
    current_date = None
    i = 0

    while i < len(lines):
        line = lines[i]

        # Datum erkennen
        try:
            current_date = datetime.strptime(line, "%d/%m/%Y").date()
            i += 1
            continue
        except ValueError:
            pass

        # Ueberspringe Sonderzeilen
        if line.lower() in skip_tokens:
            i += 1
            continue

        # Versuche Spiel zu lesen: home, score1, score2, away
        if current_date and i + 3 < len(lines):
            home_raw = line
            score1_raw = lines[i + 1]
            score2_raw = lines[i + 2]
            away_raw = lines[i + 3]

            # Pruefe ob score1 und score2 Zahlen sind
            if score1_raw.strip() in skip_tokens or score2_raw.strip() in skip_tokens:
                i += 4
                continue

            try:
                home_score = int(score1_raw)
                away_score = int(score2_raw)
            except ValueError:
                i += 1
                continue

            # Prüfe ob away_raw nicht wie ein Datum aussieht
            try:
                datetime.strptime(away_raw, "%d/%m/%Y")
                i += 1
                continue
            except ValueError:
                pass

            home_en = translate(home_raw)
            away_en = translate(away_raw)

            records.append({
                "date": current_date.isoformat(),
                "home_team": home_en,
                "away_team": away_en,
                "home_score": home_score,
                "away_score": away_score,
                "tournament": "Friendly",
                "city": "",
                "country": "",
                "neutral": "FALSE",
            })
            i += 4
        else:
            i += 1

    return records


def main():
    print("Parse manuelle Ergebnisse ...")
    records = parse_raw(RAW_DATA)
    new_df = pd.DataFrame(records)
    new_df["date"] = pd.to_datetime(new_df["date"])
    print(f"  {len(new_df)} Spiele geparst.")

    # Gegen bestehende CSV deduplizieren
    results_path = DATA_DIR / "results.csv"
    existing = pd.read_csv(results_path, parse_dates=["date"])
    existing_keys = set(
        zip(existing["date"].dt.date, existing["home_team"], existing["away_team"])
    )

    mask = new_df.apply(
        lambda r: (r["date"].date(), r["home_team"], r["away_team"]) not in existing_keys,
        axis=1,
    )
    truly_new = new_df[mask]
    dupes = len(new_df) - len(truly_new)

    if dupes > 0:
        print(f"  {dupes} Spiele bereits in CSV vorhanden (uebersprungen).")

    if truly_new.empty:
        print("Keine neuen Spiele zum Hinzufuegen.")
        return

    combined = pd.concat([existing, truly_new], ignore_index=True)
    combined = combined.sort_values("date").reset_index(drop=True)
    combined.to_csv(results_path, index=False)

    print(f"\nresults.csv: +{len(truly_new)} neue Spiele hinzugefuegt.")
    print("\nNeue Spiele:")
    for _, r in truly_new.sort_values("date").iterrows():
        print(f"  {r['date'].date()}  {r['home_team']:<30} {int(r['home_score'])}-{int(r['away_score'])}  {r['away_team']}")


if __name__ == "__main__":
    main()
