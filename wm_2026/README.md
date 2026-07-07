# WM-2026-Tippspiel-Pipeline

Monte-Carlo-Simulation eines Fußballturniers mit Tipp-Optimierung für ein
Tippspiel. Zuletzt genutzt: WM 2026. Ausgelegt auf Wiederverwendung für
zukünftige EMs/WMs — siehe [Neues Turnier aufsetzen](#neues-turnier-aufsetzen).

## Täglicher Workflow während des Turniers

```
python3 update_results.py     # Ergebnisse holen (ESPN) + Elo + Auto-Kalibrierung
python3 wm_simulation.py      # 5000 Turniere simulieren -> Excel + Tipps
```

Das ist alles. `update_results.py` erledigt automatisch mit:
- neue Ergebnisse in `results.csv`, Gruppen- und KO-CSV (inkl. Elfmeterschießen)
- KO-Progression ("Winner Match 74" → echter Teamname)
- inkrementellen Elo-Update
- Auto-Kalibrierung von `mu_wm_correction` (siehe unten)

`wm_simulation.py` schreibt `wm2026_simulation.xlsx` (Tipps, Turnierbaum,
Wahrscheinlichkeiten, Erklärung der Rechenlogik) und friert Tipps für bereits
gespielte Spiele in `data/tips_history.csv` ein.

Ausführlicher Tipp-Report (Punkte pro Batch, Bias-Analyse):
`python3 calibrate.py` — nur zum Anschauen, die Kalibrierung selbst läuft
automatisch mit.

## Die Skripte

| Skript | Zweck | Wann ausführen |
|---|---|---|
| `update_results.py` | ESPN-Ergebnisse → CSVs, Elo, Kalibrierung | täglich im Turnier |
| `wm_simulation.py` | Simulation → Excel + Tipps | nach jedem Update |
| `strength_model.py` | Poisson-Fit + Elo komplett neu (inkl. Kalibrierung) | vor dem Turnier; im Turnier nur bei Bedarf |
| `scraper_transfermarkt.py` | Kader-Marktwerte → `squad_values.csv` | einmal vor dem Turnier |
| `calibrate.py` | Tipp-Report; Kalibrierung standalone | optional |
| `import_manual_results.py` | manuelle Ergebnis-Eingabe (Fallback) | wenn ESPN nichts liefert |

## Modell in Kurzform

Erwartete Tore pro Team: `exp(att + def_Gegner + Heimvorteil + Marktwert-Korrektur
+ Elo-Bonus + MU_INTERCEPT)`. Poisson-Parameter aus Länderspielen ab 2015
(zeitgewichtet, Turnier-gewichtet), Elo kumulativ seit 1872, Marktwerte von
Transfermarkt (positionsgetrennt Angriff/Abwehr). Details im Excel-Blatt
"Erklaerung".

`MU_INTERCEPT = mu (aus Poisson-Fit) + mu_wm_correction (auto-kalibriert)` —
beides in `data/model_metadata.json`.

## Erkenntnisse aus der WM 2026 — nicht wieder rausbauen!

1. **Dixon-Coles-Korrektur** (`DC_RHO = -0.15` in `wm_simulation.py`):
   Unabhängige Poisson-Ziehungen unterschätzen niedrige Remis massiv
   (WM 2026: real 28% Remis, unkorrigiert 18%; das 1:1 war das häufigste
   Ergebnis). Backtest: +6% Tipp-Punkte.

2. **`SCORE_TIP_MODE = "expected_points"`** maximiert den Punkte-Erwartungswert
   — aber **nur zusammen mit DC_RHO**. Ohne Remis-Korrektur tippt der
   EV-Modus fast nie Unentschieden und ist schlechter als "average".
   Backtest WM 2026 (88 Spiele): average 165 Punkte, EV ohne DC 158,
   EV mit DC 175.

3. **Elfmetertore zählen zum Endergebnis** (Tippspiel-Regel: 1:1 n.V. mit
   3:4 i.E. = Endergebnis 4:5, kein Remis in der KO-Runde). Deshalb:
   - Simulation addiert simulierte Elfmetertore aufs Ergebnis
     (`SHOOTOUT_SCORES`, empirisch aus allen WM-Shootouts 1982–2022:
     1 Tor Abstand ≈ 54%, 2 Tore ≈ 37%, 3 Tore ≈ 9%)
   - `update_results.py` speichert `pens_home`/`pens_away` in der KO-CSV
   - dadurch bewertet der Tipp-Optimierer 1-Tor-Differenz-Tipps korrekt
   - Tipp-Punkte: 4 exakt / 3 Tordifferenz / 2 Tendenz / 0 falsch

4. **Gastgeber-Heimvorteil**: Gastgeber sind NICHT neutral (Mexiko 2026:
   6:0 Tore in der Gruppenphase). `HOST_TEAMS` + `VENUE_HOST_COUNTRY` in
   `wm_simulation.py` pflegen. Gruppenphase: Gastgeber immer heim; KO-Phase:
   nur wenn das Venue im eigenen Land liegt.

5. **Kalibrierung = Fixpunkt-Verfahren gegen Modell-Lambdas**
   (`calibrate.py`): Ziel-Torschnitt = gewichtetes Mittel aus echten
   Turnier-Toren (n Spiele) und historischem WM-Schnitt 2.65 (Gewicht
   N0=50). Konvergiert bei Mehrfach-Ausführung, kappt Einzelschritte auf
   ±0.10. **Niemals** wieder auf gerundeten Tipps kalibrieren (alter Bug:
   addierte bei jedem Lauf dasselbe Delta erneut → Modell pendelte zwischen
   Unter- und Überschätzung). Kalibriert wird auf 120-Minuten-Toren, die
   Tipp-Wertung nutzt das offizielle Ergebnis inkl. Elfmetertoren.

6. **KO-Progression in `update_results.py`**: löst "Winner Match N" /
   "Loser Match N" aus früheren KO-Ergebnissen auf. War bis Juli 2026
   kaputt — R16+-Ergebnisse wurden nie automatisch eingetragen.

## Neues Turnier aufsetzen (z.B. EM 2028)

Am besten den Ordner kopieren (`em_2028/`) und anpassen:

1. **Daten-CSVs neu anlegen** (Schema aus den 2026er Dateien übernehmen):
   - `data/wm2026_groups.csv` — Gruppen und Teams
   - `data/wm2026_matches_group.csv` — Spielplan Gruppenphase
     (`goals_home/away` leer lassen)
   - `data/wm2026_matches_knockout.csv` — KO-Spielplan mit
     `team_home_desc`/`team_away_desc`-Slots und `venue`
   - Dateinamen ggf. umbenennen → Pfade in den Skripten anpassen
2. **Drittplatzierten-Logik prüfen**: `third_place_combinations.csv` +
   `THIRD_SLOTS_BY_WINNER` sind WM-2026-spezifisch (48 Teams, 12 Gruppen).
   EM: eigene Regeln (Annex im UEFA-Reglement), Kombinationstabelle neu bauen.
   Bei Formaten ohne Drittplatzierte den Teil deaktivieren.
3. **`wm_simulation.py` Konstanten**:
   - `HOST_TEAMS` + `VENUE_HOST_COUNTRY` → neue Gastgeber/Stadien
   - `BRACKET` + `STAGE_OF_MATCH` → neue Match-Nummern des KO-Baums
   - `DC_RHO = -0.15` und `SCORE_TIP_MODE = "expected_points"` beibehalten
4. **`update_results.py`**: `ESPN_LEAGUES` (z.B. `uefa.euro`),
   `WM_START`, `TOURNAMENT_MAP` prüfen; `MATCH_WEIGHTS` in
   `strength_model.py` hat für EM/Kontinentalturniere schon Gewichte
5. **`calibrate.py`**: `WM_HISTORICAL_GOALS_PER_GAME` an den historischen
   Schnitt des Turniertyps anpassen (EM 2021: 2.78, EM 2024: 2.29 —
   evtl. 2.5 als Prior)
6. **Vor dem Turnier einmal**:
   `scraper_transfermarkt.py` (Team-Liste/IDs aktualisieren!) →
   `strength_model.py` → `wm_simulation.py`
7. **Tippspiel-Regeln prüfen**: Zählt das Elfmeterschießen wieder zum
   Endergebnis? Falls nicht (z.B. Wertung nach 120 Min), muss die
   Shootout-Logik in `sim_ko_match` und `calibrate.load_actual_results`
   angepasst werden.

## TODOs / Ideen für die Weiterentwicklung

### Historische Turniere nachrechnen (Backtesting-Framework)

Die wichtigste offene Baustelle: Alle Modell-Parameter wurden bisher nur an
der WM 2026 validiert (88–104 Spiele = viel Rauschen). Besser: alte Turniere
"blind" durchtippen und gegen die echten Ergebnisse werten.

**Idee:** Modellstand auf den Tag vor Turnierstart zurücksetzen (z.B. EM 2016,
WM 2018, EM 2021, WM 2022, EM 2024), Tipps für alle Spiele generieren, mit
den damaligen Ergebnissen nach der Tipp-Punktelogik auswerten. Über 5+
Turniere ≈ 300 Spiele — genug, um Abhängigkeiten zu erkennen, die in einem
einzelnen Turnier untergehen.

**Was dafür nötig ist:**
1. **Zeitreise im Modell**: `strength_model.py` braucht einen
   `--as-of YYYY-MM-DD`-Parameter, der Poisson-Fit und Elo nur auf Spielen
   VOR dem Stichtag rechnet (`results.csv` reicht bis 1872 zurück, die Daten
   sind also da). Achtung Leakage: auch `CUTOFF_YEAR` und die Zeitgewichtung
   relativ zum Stichtag rechnen, nicht relativ zu heute.
2. **Historische Spielpläne** als CSVs im heutigen Schema (Gruppen + KO mit
   Slots und Venues) — die Ergebnisse stehen schon in `results.csv`, müssen
   nur den match_nrs zugeordnet werden.
3. **Marktwerte**: Transfermarkt hat historische Kaderwerte (Stichtags-Archiv),
   der Scraper müsste das Datum mitgeben. Falls zu aufwendig: das Modell hat
   einen Fallback ohne Marktwerte (`strength_combined` nur aus
   Poisson + Elo) — dann aber die Backtest-Ergebnisse getrennt bewerten,
   weil das Live-Modell mit Marktwerten läuft.
4. **Turnier-Regeln je Ausgabe**: Gastgeber (`HOST_TEAMS`), Bracket,
   Drittplatzierten-Regeln (EM: beste Vier von Sechs; WM 2018/2022: keine
   Dritten) — pro Turnier eine kleine Config statt Konstanten im Code wäre
   dafür der saubere Umbau.

**Was man damit tunen/prüfen kann:**
- `DC_RHO`: ist -0.15 über viele Turniere stabil oder war 2026 ein Ausreißer?
- `SCORE_TIP_MODE`: schlägt expected_points den average-Modus konsistent?
- Kalibrierungs-Prior (`N0_SHRINKAGE`, historischer Torschnitt) und wie
  schnell die Auto-Kalibrierung im Turnierverlauf nachziehen sollte
- `MATCH_WEIGHTS`, `DECAY_HALFLIFE_YEARS`, `ELO_MAX_GOAL_BOOST`,
  Marktwert-Gewichte — bisher alles Bauchgefühl, nie systematisch getestet
- Elfmeter-Modell: reicht `strength_combined` mit Clip 0.40–0.60 oder gibt
  es Teams mit systematischem Shootout-Skill?
- Gastgeber-Heimvorteil: ist der volle `home_adv` (~+27% Tore) bei
  Turnieren richtig oder zu viel? (2026 sah eher nach "eher mehr" aus)

**Wichtig fürs Vorgehen:** Parameter an einem Teil der Turniere tunen und am
Rest validieren (nicht alles auf allen optimieren), sonst overfittet man die
Vergangenheit genauso wie vorher die WM 2026.

### Kleinere Ideen
- Elo-Boost symmetrisch machen (aktuell bekommt nur das stärkere Team einen
  Bonus, dem schwächeren wird nichts abgezogen — bläht die Torsumme minimal
  auf, die Kalibrierung fängt es ab)
- `tips_history.csv` um die Modell-Lambdas zum Tipp-Zeitpunkt erweitern —
  dann kann man nachträglich exakt analysieren, welcher Modellstand welchen
  Tipp erzeugt hat
- Turnier-Configs (Teams, Bracket, Gastgeber, Regeln) aus dem Code in eine
  Config-Datei je Turnier ziehen — macht Punkt 4 oben und die
  Wiederverwendung deutlich einfacher

## Dateien in `data/`

| Datei | Inhalt | Quelle |
|---|---|---|
| `results.csv` | alle Länderspiele seit 1872 (120-Min-Ergebnisse) | Kaggle + ESPN-Updates |
| `squad_values.csv` | Kader-Marktwerte (gesamt + je Position) | Transfermarkt-Scraper |
| `team_strengths.csv` | kombinierte Teamstärken | strength_model.py |
| `poisson_params.csv`, `elo_checkpoint.csv` | Modell-Checkpoints | strength_model.py |
| `model_metadata.json` | `mu`, `home_adv`, `mu_wm_correction` | strength_model.py + calibrate.py |
| `wm2026_groups.csv`, `wm2026_matches_*.csv` | Spielplan + Ergebnisse | manuell + update_results.py |
| `third_place_combinations.csv` | FIFA-Annex-C-Zuteilung der Dritten | FIFA-Reglement |
| `tips_history.csv` | abgegebene Tipps (gespielte Spiele eingefroren) | wm_simulation.py |
| `calibration_report.txt` | letzter Kalibrierungsbericht | calibrate.py |
