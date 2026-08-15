# Bundesliga-Tippsystem

Zeitlich saubere Prognose-Pipeline fuer Bundesliga und 2. Bundesliga. Das
Projekt uebernimmt die belastbaren Teile des WM-2026-Modells, trennt aber
Turniersimulation und Ligaprognose konsequent.

## Aktueller Stand

- SQLite-Datenbank fuer Spiele, Marktwert-Stichtage, Buchmacherquoten und jede Prognose
- historische Ergebnisse fuer D1 und D2 ab 1993/94 via Football-Data.co.uk
- aktueller Spielplan und laufende Ergebnisse via OpenLigaDB
- zeitgewichtetes Angriffs-/Abwehrmodell mit echtem Dixon-Coles-Likelihood-Fit
- automatisch geschaetzter Heimvorteil und Remisparameter `rho`
- optionaler Form-, Heim-/Auswaertsform-, H2H- und Marktwerteffekt
- historische Transfermarkt-Vereinswerte mit exaktem Stichtag
- historische 1X2-Opening-/Closing-Quoten sowie zeitgestempelte Live-Quoten
- gleitender Zweitliga-Prior fuer Aufsteiger, der nach zehn D1-Spielen auslaeuft
- Walk-forward-Backtest ohne Zukunftsdaten
- Tippauswahl nach maximalen erwarteten Punkten: 4 exakt, 3 Tordifferenz,
  2 Tendenz, 0 falsch

Die generierten Daten unter `data/` werden nicht versioniert. Sie lassen sich
mit den folgenden Befehlen reproduzieren.

## Installation

Vom Repository-Wurzelverzeichnis:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r bundesliga/requirements.txt
```

## Daten aufbauen

Historie beider Ligen herunterladen. Dabei werden Ergebnisse und alle vorhandenen
1X2-Buchmacherquoten gemeinsam importiert:

```bash
.venv/bin/python -m bundesliga.data_sources historical \
  --leagues D1 D2 --start 1993 --end 2025
```

Aktuellen Spielplan und Ergebnisse aktualisieren:

```bash
.venv/bin/python -m bundesliga.data_sources live --league D1 --season 2026
```

Die Daten landen in `bundesliga/data/bundesliga.sqlite`. Wiederholte Importe
aktualisieren vorhandene Spiele, statt Duplikate anzulegen.

## Naechsten Spieltag tippen

```bash
.venv/bin/python -m bundesliga.predict \
  --league D1 --season 2026 \
  --output bundesliga/data/tipps_spieltag_1_2026.xlsx
```

Der Live-Standard verwendet die auf 2018/19 bis 2021/22 ausgewaehlten kleinen
Form-, H2H- und Marktwertgewichte, den gleitenden Aufsteiger-Prior und – sofern
vorhanden – den Buchmacher-Konsens fuer die 1X2-Wahrscheinlichkeiten. Vor jeder neuen
Tipperstellung zuerst den OpenLigaDB- und Quotenimport ausfuehren. Jeder Lauf wird samt
Lambdas, Wahrscheinlichkeiten, Tipp, Zeitstempel und Konfiguration in der
Datenbank gespeichert.

Mehrere Spieltage koennen vorlaeufig gemeinsam ausgegeben werden:

```bash
.venv/bin/python -m bundesliga.predict \
  --league D1 --season 2026 --matchdays 3 \
  --output bundesliga/data/tipps_spieltage_1_bis_3_2026.xlsx
```

Spaetere Spieltage verwenden den aktuellen Informationsstand und werden vor
ihrem jeweiligen Tipptermin mit neuen Ergebnissen und Quoten ueberschrieben.

## Buchmacherquoten

Historische Football-Data-Dateien enthalten je nach Saison Quoten mehrerer
Anbieter sowie Marktmittelwerte. Der Import speichert sie in `bookmaker_odds`
getrennt als `opening` und `closing`. Normale Backtests verwenden ausschliesslich
`opening`; `closing` muss explizit angefordert werden.

Aktuelle Quoten lassen sich optional ueber The Odds API abrufen. Dafuer ist ein
eigener API-Schluessel erforderlich:

```bash
export THE_ODDS_API_KEY="dein-schluessel"
.venv/bin/python -m bundesliga.data_sources live-odds \
  --league D1 --season 2026
```

Jeder Abruf wird als `captured` mit dem vom Anbieter gelieferten Zeitpunkt
gespeichert. Die Live-Prognose verwendet nur Captures, die zum `as_of`-Zeitpunkt
bereits vorlagen. Fehlen Quoten, faellt sie automatisch auf das interne Modell
zurueck.

Alternativ koennen Quoten aus einer eigenen CSV importiert werden:

```bash
.venv/bin/python -m bundesliga.data_sources bookmaker-odds --file quoten.csv
```

Pflichtspalten sind `bookmaker`, `home_odds`, `draw_odds`, `away_odds` und
entweder `match_id` oder `competition,season,home_team,away_team`. Fuer
`captured` ist ausserdem `observed_at` erforderlich. `snapshot_type` darf
`opening`, `closing` oder `captured` sein.

## Walk-forward-Backtest

Ein Basismodell fuer vier Saisons blind testen:

```bash
.venv/bin/python -m bundesliga.backtest \
  --league D1 --seasons 2022 2023 2024 2025 \
  --output bundesliga/data/backtest.csv
```

Die aktuell ausgewaehlte erweiterte Variante:

```bash
.venv/bin/python -m bundesliga.backtest \
  --league D1 --seasons 2022 2023 2024 2025 \
  --form-weight 0.10 --h2h-weight 0.05 \
  --market-weight 0.05 --bookmaker-weight 1.0 \
  --lower-league-priors --promotion-penalty 0.20 \
  --output bundesliga/data/backtest_final.csv
```

Ein Prognose-Spieltag reicht im Backtest von Dienstag bis Montag. Das Modell
wird am Dienstag nur mit davor beendeten Spielen neu trainiert. Damit kann kein
spaeteres Wochenendergebnis in eine fruehere Prognose gelangen.

## Feature-Experimente

Form- und H2H-Gewichte werden auf einer Entwicklungsperiode verglichen, ohne
den teuren Basisfit mehrfach auszufuehren:

```bash
.venv/bin/python -m bundesliga.experiments \
  --league D1 --seasons 2018 2019 2020 2021 \
  --baseline-csv bundesliga/data/validation.csv \
  --output bundesliga/data/feature_vergleich.csv
```

Die Varianten werden zuerst nach Brier-Score, dann Log-Loss und erst danach
nach Tippspielpunkten sortiert. So wird kein zufaellig gluecklicher Tippgeber
mit schlecht kalibrierten Wahrscheinlichkeiten bevorzugt.

## Marktwerte

Der Stichtagsimport ruft nur die Vereinsuebersicht einer Liga ab, arbeitet
seriell mit Pause und legt jede HTML-Antwort in einem lokalen Cache ab. Vier
In-Season-Stichtage pro Jahr reichen fuer das zeitlich saubere Backtesting:

```bash
.venv/bin/python -m bundesliga.data_sources transfermarkt \
  --leagues D1 --start-year 2011 --end-date 2026-08-15 --pause 1.5
```

Ein wiederholter Lauf liest den Cache und erzeugt keine erneuten Requests.
Fehlende historische Liga-Zuordnungen werden protokolliert und ausgelassen.
Der Marktwert wirkt nur als Saisonstart-Prior und laeuft ueber die ersten zehn
Ligaspiele aus. Ein dauerhaftes Marktwertgewicht hat im Backtest schlechter
kalibriert, weil es die bereits aus Ergebnissen erkannte Teamstaerke doppelt
zaehlt.

Alternativ koennen lizenzierte oder manuell erstellte Stichtagsdaten importiert
werden:

```bash
.venv/bin/python -m bundesliga.data_sources market-values \
  --file pfad/marktwerte.csv
```

Pflichtspalten:

```text
team,as_of,squad_value_eur,source
```

Optionale Spalten:

```text
goalkeeper_value_eur,defense_value_eur,midfield_value_eur,attack_value_eur
```

Entscheidend ist, dass `as_of` der damalige Informationsstand ist. Spaeter
bekannt gewordene Marktwerte duerfen nicht rueckwirkend in einen Backtest.

## Bisherige Ergebnisse

Der erste unangetastete D1-Test umfasst 1.224 Spiele der Saisons 2022/23 bis
2025/26:

| Variante | Punkte | Punkte/Spiel | Brier | Scoreline-Log-Loss |
|---|---:|---:|---:|---:|
| Basismodell | 1.608 | 1,314 | 0,5945 | 3,1072 |
| Form 0,10 + H2H 0,05 | 1.612 | 1,317 | 0,5940 | 3,1080 |
| plus Marktwert-Prior 0,05 | 1.618 | 1,322 | 0,5941 | 3,1081 |
| plus gleitender Aufsteiger-Prior | 1.604 | 1,310 | 0,5928 | 3,0966 |
| Marktwert + Aufsteiger-Prior | 1.607 | 1,313 | 0,5928 | 3,0966 |
| Buchmacher-1X2 + Poisson-Ergebnisverteilung | **1.680** | **1,373** | **0,5772** | **3,0798** |
| kompletter Live-Feature-Satz + Buchmacher-1X2 | 1.674 | 1,368 | 0,5772 | 3,0730 |

Form, H2H und Marktwert sind damit bisher kleine Signale. Das Buchmacher-Signal
liefert den klar groessten Sprung: Gegenueber der bisherigen Bestmarke von 1.618
Punkten sind es 62 Punkte mehr. Bei Gewicht 1,0 stammen die 1X2-Wahrscheinlichkeiten
aus dem margenbereinigten Marktmittel; das Poisson-Modell bestimmt weiterhin die
Verteilung der exakten Ergebnisse innerhalb von Heimsieg, Remis und Auswaertssieg.
Diese Ergebnisse bleiben eine Startlinie und keine Garantie fuer kommende Saisons.

## Tests

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest bundesliga/tests -q
```

`PYTEST_DISABLE_PLUGIN_AUTOLOAD` verhindert in restriktiven Umgebungen, dass
ein global installiertes Rerun-Plugin einen lokalen Socket oeffnen moechte.

## Naechste Ausbaustufen

1. Form auf gegnerbereinigte Torresiduen statt Punkte/Tordifferenz umstellen.
2. Halbwertszeit, Ridge und Aufsteiger-Abbildung per Rolling Validation tunen.
3. Den Live-Quotenabruf vor jeder Tippabgabe automatisieren und dessen Abdeckung ueberwachen.
4. Verletzungen, Startaufstellungen und Trainerwechsel als spaetere, ebenfalls
   einzeln messbare Features aufnehmen.
5. Nach jedem Spieltag Kalibrierungsdiagramme und Drift-Warnungen erzeugen.
