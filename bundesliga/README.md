# Bundesliga-Tippsystem

Zeitlich saubere Prognose-Pipeline fuer Bundesliga und 2. Bundesliga. Das
Projekt uebernimmt die belastbaren Teile des WM-2026-Modells, trennt aber
Turniersimulation und Ligaprognose konsequent.

## Aktueller Stand

**Beschlossen am 09.09.2026 und am 11.09. nach der Champions League bestätigt:** Für Spieltag 3 verwenden wir den
[dokumentierten 270-Tage-Tippzettel](Review/Spieltag%203/EMPFEHLUNG.md).
Die einzige Abweichung zur bisherigen Methode ist Hoffenheim–Stuttgart **1:2**.
Die [Entscheidungsübersicht](Review/ENTSCHEIDUNGEN.md) hält Ziel, geprüfte
Varianten, Grenzen und den nächsten Auswertungsschritt zusammen.

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
- optionale CHECK24-Turnierstrategie, die einen kompletten Neun-Spiele-Tippzettel
  auf die Chance eines Zielwerts statt auf den Saisonmittelwert optimiert
- experimenteller optionaler Modus `contest`: Auswahl fuer den Spieltagspreis gegen
  automatisch erzeugte Konkurrenzszenarien, ohne manuelle Tippanteile

Die SQLite-Datenbank, Rohdaten und umfangreichen Modell-Caches unter `data/`
werden nicht versioniert. Ausgewählte Ergebnisdateien, Prüfscripte und die
eingefrorenen Tippzettel sind gezielt versioniert; siehe
[Ergebnisarchiv](Review/ARCHIV.md). Neue Modellläufe lassen sich mit den folgenden
Befehlen erstellen. Historische Archive bleiben als damaliger Stand erhalten.

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

Football-Data enthaelt keine offiziellen Spieltagsnummern. Fuer vollstaendige
D1-Spieltagstests diese Metadaten separat zuordnen (eine Anfrage pro Saison,
danach Cache; Ergebnisse und Match-IDs bleiben erhalten):

```bash
.venv/bin/python -m bundesliga.rounds \
  --seasons 2018 2019 2020 2021 2022 2023 2024 2025 \
  --cache-dir bundesliga/data/raw/draw_matchday_review
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

Der Live-Standard ist wieder `expected-points`: Fuer den Konkurrenzmodus ist
bisher kein realer Vorteil belegt. Der abgeschlossene Vergleich mit hoechstens
zwei modellbegruendeten Abweichungen steht im
[7+2-Ergebnisbericht](Review/SIEBEN_PLUS_ZWEI_ERGEBNIS.md).
Der allgemeine `LIVE_CONFIG`-Standard bleibt 420 Tage mit Quoten; der ausdrücklich
angenommene Tippzettel für Spieltag 3 verwendet separat 270 Tage ohne Quoten.

Der optionale experimentelle Modus `contest` bewertet den gesamten Neun-Spiele-Zettel gegen
neun feste, hypothetische Konkurrenzszenarien. Es werden keine Teilnehmerdaten
oder woechentlich abgetippten Prozente benoetigt:

```bash
.venv/bin/python -m bundesliga.predict \
  --league D1 --season 2026 --tip-strategy contest \
  --output bundesliga/data/tipps_contest.xlsx
```

Die Szenarien kombinieren 100, 1.000 und 10.000 angenommene Gegner mit drei
Staerken der Konzentration auf Tipps mit hohen erwarteten Punkten. Die genauen
Tippanteile werden aus den vorhandenen Ergebniswahrscheinlichkeiten abgeleitet;
sie sind **Annahmen, keine gemessenen CHECK24-Anteile**. Die Parameter stehen
in `ContestConfig` in `contest.py` und wurden nicht an den Top-3-Screenshots
angepasst.

Optimiert wird das geometrische Mittel der simulierten Gewinnanteile ueber die
Szenarien. Schlechte Durchschnittspunkte werden dabei bewusst akzeptiert.
Gleichstaende werden als gleichmaessige Preisteilung beziehungsweise Auslosung
modelliert; das ist keine verifizierte CHECK24-Regel. Alle Teilnehmer werden
gegen dieselben simulierten Ergebnisse gewertet. Die angenommene Konkurrenz
waehlt ihre Tipps pro Spiel unabhaengig; Fanmuster und weitere Zusammenhaenge
innerhalb eines fremden Tippzettels sind nicht abgebildet.

Die Auswahl verwendet 8.192 simulierte Spieltage; weitere 32.768 unabhaengige
Simulationen dienen nur zur Bewertung. Es gibt keine feste Anzahl erzwungener
Aussenseitertipps. Die Suche ist naeherungsweise, kein garantiertes globales
Optimum. Auch die unabhaengige Bewertung prueft nur die angenommenen Szenarien,
nicht die reale Konkurrenz. Seltene Ereignisse bleiben simulationsunsicher.

Der Excel-/CSV-Export erhaelt eine `.metadata.json` mit allen Annahmen,
Szenariovergleichen, Simulationsfehlern der Differenzen und erwarteten Punkten.
Die Angaben sind keine echten Gewinnwahrscheinlichkeiten. Der Modus ist
experimentell. `--tip-strategy expected-points` liefert als Standard die Auswahl
fuer maximale Durchschnittspunkte.

Die fruehere Variante `target-score` optimiert auf mindestens 24 Punkte.
Der Zielwert folgt aus dem beobachteten ersten
Spieltag (26 Punkte fuer Platz 1, 24 Punkte fuer die naechste sichtbare Gruppe):

```bash
.venv/bin/python -m bundesliga.predict \
  --league D1 --season 2026 \
  --tip-strategy target-score --target-points 24 \
  --output bundesliga/data/tipps_spieltag_2_2026_risk.xlsx
```

Das ist keine zufaellige Upset-Auswahl. Die Software sucht per lokaler Optimierung
mit mehreren Startloesungen einen Tippzettel mit hoher
`P(Spieltagspunkte >= 24)`; ein globales Optimum ist nicht garantiert. Schlechte
Spieltage duerfen als Nebenwirkung auftreten, sind aber nicht selbst das Ziel.

**Review nach Spieltag 2:** Der gespeicherte Zielwert-Zettel erzielte 13 Punkte,
die drei vorgelegten Top-Zettel 24/22/22. Gegenueber der Auswahl nach erwarteten
Punkten hatte die Strategie keine Tendenz geaendert. Eine feste Punkteschwelle
optimiert nicht die Chance auf Platz 1 gegen die Konkurrenz. Fuer das nun
priorisierte Ziel Spieltagspreis muss die Auswahl auch Teilnehmerzahl,
Tippverteilung und Gleichstandsregeln beruecksichtigen. Weil diese Daten nicht
verfuegbar sind, verwendet `contest` die oben beschriebenen Szenarien.
Details, Ergebnisvergleich und die konkrete Richtung
stehen in [der Auswertung zu Spieltag 2](Review/Spieltag%202/AUSWERTUNG.md).

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

Ein Basismodell fuer vier Saisons testen (die unten verwendeten Perioden sind
in diesem Projekt inzwischen bekannt und deshalb kein neuer Blindtest):

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

Bei vorhandenen Spieltagsnummern wird jeder offizielle Spieltag getrennt
berechnet. Alle neun Spiele erhalten denselben Informationsstand: Dienstag
der Kalenderwoche der ersten Partie, ausschliesslich mit vorherigen Ergebnissen.
Nachholspiele bleiben beim urspruenglichen Spieltag. Nicht zeitgestempelte
Quoten fuer Spiele in spaeteren Kalenderwochen werden dabei nicht verwendet;
das interne Modell uebernimmt. Die Verfuegbarkeit historischer Opening-Quoten
innerhalb der ersten Kalenderwoche bleibt mangels Zeitstempel eine Annahme.

Ohne offizielle Nummern nutzt nur `expected-points` weiterhin Kalenderfenster;
die Zusammenfassung kennzeichnet das als `calendar_windows`. Die Turniermodi
verlangen offizielle Spieltagsnummern. Mit `--matchdays 17` laesst sich ein
bestimmter Spieltag der ausgewaehlten Saisons pruefen.

Die Turnierstrategie laesst sich mit demselben Walk-forward-Lauf testen:

```bash
.venv/bin/python -m bundesliga.backtest \
  --league D1 --seasons 2022 2023 2024 2025 \
  --form-weight 0.10 --h2h-weight 0.05 \
  --market-weight 0.05 --bookmaker-weight 1.0 \
  --lower-league-priors --promotion-penalty 0.20 \
  --tip-strategy target-score --target-points 24 \
  --output bundesliga/data/backtest_contest_2022_2025.csv
```

Die Zusammenfassung enthaelt nun Mittelwert, Standardabweichung und Bestwert
pro vollstaendigem offiziellen Spieltag sowie Trefferzahlen fuer mindestens 18,
20, 22 und 24 Punkte. Zwei Spieltage in derselben Woche werden getrennt gezaehlt.
Unvollstaendige Gruppen werden separat ausgewiesen. Fuer eine ehrliche
Auswahl sollte die Strategie auf 2018/19 bis 2021/22 entwickelt und nur einmal
auf 2022/23 bis 2025/26 getestet werden. Ein bereits bekannter Spieltag darf
danach als Realitaetscheck dienen, aber nicht mehr zum Tuning desselben Laufs.

## Feature-Experimente

Die neue Turnierauswahl kann ohne erneute Modellfits auf bereits gespeicherten
Walk-forward-Wahrscheinlichkeiten nachgerechnet werden:

```bash
.venv/bin/python -m bundesliga.contest_backtest \
  --input bundesliga/data/backtest_d1_2018_2021_official_frozen.csv \
  --output bundesliga/data/contest_replay_2018_2021.csv
```

Der Replay benoetigt insbesondere Lambdas, `rho`, 1X2-Wahrscheinlichkeiten,
`as_of` und die tatsaechlichen Ergebnisse. Ergebnisse werden erst nach der
Tippauswahl zur Bewertung verwendet. Die Zeitreinheit der Eingabe wird vom
urspruenglichen Walk-forward-Lauf uebernommen. Nur vollstaendige offizielle
Spieltage mit gemeinsamem Prognosezeitpunkt werden verglichen. Alte Dateien mit
mehreren Prognosezeitpunkten innerhalb eines Spieltags werden abgewiesen.
Reale Platzierungen lassen sich ohne Konkurrenzdaten
nicht rueckwirkend messen. Auch der regulaere Backtest unterstuetzt
`--tip-strategy contest`; unvollstaendige Fenster behalten die normale Auswahl
und werden in der Ausgabe entsprechend gekennzeichnet.

Der erste, inzwischen als eingeschraenkt erkannte Replay auf 2018/19 bis 2021/22
umfasste nur 106 Neun-Spiele-Kalenderfenster und liess 23 andere Fenster aus.
Das waren **nicht alle 136 offiziellen Spieltage** dieser vier Saisons:

| Auswahl | Punkte/Spieltag | Standardabweichung | Bestwert | Spieltage ab 18 Punkten |
|---|---:|---:|---:|---:|
| Erwartete Punkte | 11,48 | 4,03 | 20 | 9 |
| `contest` | 10,15 | 4,04 | 20 | 4 |

Damit sind bessere Spitzenergebnisse bisher **nicht belegt**. Die neue Auswahl
aendert im Mittel 6,92 Tendenzen pro Zettel. Ihr Vorteil in den simulierten
Konkurrenzszenarien ist ein modellabhaengiges Ergebnis, kein Nachweis hoeherer
realer Gewinnchancen. Ergebnisse und Grenzen stehen auch im
[Strategiereview](Review/STRATEGIE_OHNE_KONKURRENZDATEN.md).

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

Der urspruengliche, damals noch unangetastete D1-Test umfasst 1.224 Spiele der
Saisons 2022/23 bis 2025/26. Inzwischen wurde dieser Zeitraum mehrfach untersucht
und ist kein neuer Blindtest. Die folgende Tabelle dokumentiert den urspruenglichen Wochenfit;
die korrigierten Ergebnisse nach einheitlichen offiziellen Spieltags-Stichtagen
stehen im [Modellaudit](Review/MODELL_UND_SPIELTAGSAUDIT.md):

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

Der neue Vergleich mit einer einfachen Quotenheuristik zeigt einen sehr kleinen
Zusatznutzen: 2022/23 bis 2025/26 erzielt die aktuelle Kombination nach Korrektur
1.677 Punkte, Quotenfavorit mit festem 2:1/1:2/1:1 und gleichem Modellfallback
1.674. Der hoehere Aufwand ist damit fuer das Ziel Spieltagspreis bislang nicht
durch einen belastbaren Mehrwert gerechtfertigt. Das interne Modell und 50 %
Quotengewicht erreichen jeweils einmal 25 Spieltagspunkte bei weniger Gesamtpunkten;
ein einzelner Spitzenwert belegt keine bessere Gewinnstrategie.

## Halbwertszeit und gegnerbereinigte Form über 15 Saisons

Die bisher verwendeten **420 Tage waren ein gesetzter Standardwert**, keine
dokumentierte Optimierung. Der Wert stammt bereits aus dem ersten Pipeline-
Commit; alle 24 bis zum 08.09.2026 gespeicherten Prognoseläufe verwenden ihn.

Der reproduzierbare Vergleich in `half_life_study.py` untersucht 2011/12 bis
2025/26: 4.590 Spiele auf 510 offiziellen Spieltagen, plus 2010/11 als Formvorlauf.
Sieben feste und zwei saisonabhängige Halbwertszeiten werden mit bisheriger,
gegnerbereinigter und ausgeschalteter Form verglichen. Die neue Form verwendet
Abweichungen von damals vor dem Spiel berechneten Torerwartungen. Für jede
Testsaison erfolgt die Parameterauswahl nur anhand der fünf vorigen Saisons.

Das [vorab gespeicherte Protokoll](Review/HALBWERTSZEIT_PROTOKOLL.md) beschreibt
Kandidaten, Auswahlregel und Grenzen. Der abgeschlossene
[Ergebnisbericht](Review/HALBWERTSZEIT_UND_FORM.md) zeigt einen kleinen Vorteil
bei der Prognosegüte für längere Halbwertszeiten, aber keinen zusätzlichen
24-Punkte-Spieltag durch die zeitlich getrennte Parameterauswahl. Kürzere
Halbwertszeiten liefern einzelne zusätzliche Spitzen bei schlechterer
Prognosegüte; ein Tagespreis-Vorteil ist damit nicht gesichert.
Aufruf aus dem Repository-Hauptordner:

```bash
.venv/bin/python -m bundesliga.rounds \
  --seasons 2010 2011 2012 2013 2014 2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025 \
  --cache-dir bundesliga/data/raw/draw_matchday_review
OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 OMP_NUM_THREADS=1 \
  .venv/bin/python -m bundesliga.half_life_study --workers 3
```

Zwischenergebnisse werden pro Halbwertszeit und Saison gespeichert; ein erneuter
vollständiger Aufruf prüft den Fingerabdruck von Code und Eingangsdaten und setzt
einen unveränderten Lauf fort. Für geänderte Varianten ein neues `--output`
verwenden. CSVs und Excel-Auswertung liegen in `data/half_life_15_seasons/`.
Live-Parameter und Live-Tippregel werden durch den Versuch nicht verändert.

## Höchstens zwei begründete Risiken

`limited_risk.py` und `limited_risk_study.py` implementieren und prüfen die
7+2-Idee mit einem gemeinsamen Budget von höchstens einem erwarteten Punkt
Verzicht und höchstens einem zusätzlichen Remistipp. Die jährliche Auswahl
zielt ausdrücklich auf Spitzenspieltage ab 22/24 Punkten und verwendet nur
die fünf vorherigen Saisons. Konkurrenzdaten werden nicht benötigt.

Der [abgeschlossene Vergleich](Review/SIEBEN_PLUS_ZWEI_ERGEBNIS.md) rechtfertigt
keine Live-Umstellung: Auf 340 Testspieltagen ergibt die zeitlich getrennte
Auswahl drei statt zwei Spieltage ab 22 Punkten, weiterhin keinen ab 24 und
niedrigere Durchschnittspunkte. Das komplette interne 270-Tage-Modell bleibt
im bereits bekannten historischen Vergleich auffälliger für das Spitzenziel.

```bash
OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 OMP_NUM_THREADS=1 \
  .venv/bin/python -m bundesliga.limited_risk_study
```

Ausgaben: `data/limited_risk_15_seasons/`, einschließlich `sieben_plus_zwei.xlsx`.
Regeln und Grenzen stehen im [Versuchsprotokoll](Review/SIEBEN_PLUS_ZWEI_PROTOKOLL.md).

## Tests

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 .venv/bin/python -m pytest bundesliga/tests -q
```

`PYTEST_DISABLE_PLUGIN_AUTOLOAD` verhindert in restriktiven Umgebungen, dass
ein global installiertes Rerun-Plugin einen lokalen Socket oeffnen moechte.

## Naechste Ausbaustufen

1. Die im 15-Saison-Vergleich untersuchte Residuenform vor einer Live-Übernahme
   mit dem Ziel Tagespreis bewerten; bislang kein nachgewiesener Vorteil dafür.
2. Ridge und Aufsteiger-Abbildung separat per Rolling Validation untersuchen;
   der Halbwertszeit-Vergleich ist abgeschlossen.
3. Den Live-Quotenabruf vor jeder Tippabgabe automatisieren und dessen Abdeckung ueberwachen.
4. Verletzungen, Startaufstellungen und Trainerwechsel als spaetere, ebenfalls
   einzeln messbare Features aufnehmen.
5. Nach jedem Spieltag Kalibrierungsdiagramme und Drift-Warnungen erzeugen.
