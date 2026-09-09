# Audit der Spieltagsauswertung und des Modellbeitrags

Stand: 8. September 2026. Anlass sind die niedrigen Spitzenergebnisse und die
Frage, welchen Zusatznutzen das eigene Modell bei voller Quotenübernahme hat.

Nachtrag 09.09.2026: Der vollständige Neulauf im
[15-Saison-Vergleich](HALBWERTSZEIT_UND_FORM.md) reproduziert die Modellschätzungen,
zeigt aber eine ältere abweichende Quotenaggregation für 2018/19. Mit dem
aktuellen Marktdurchschnitt statt des früheren Mittels einschließlich
Maximalquoten ändert sich die 420-Tage-Basis 2018–2021 von 1.572 auf 1.569 Punkte.
Die folgenden Zahlen dokumentieren den damaligen Audit; im neuen Versuch
werden alle Varianten mit einheitlichen aktuellen Quoten verglichen.

## Warum zuvor 106 statt 136 Spieltage?

Die Eingabedatei für 2018/19 bis 2021/22 enthält alle 1.224 Spiele aus vier
Saisons, also 136 offizielle Spieltage. Der vorherige Replay gruppierte nach
Prognosezeitpunkt beziehungsweise Dienstag–Montag-Kalenderwoche und nahm nur
Gruppen mit genau neun Spielen auf. Das ergab 106 Gruppen; 23 andere Wochen
mit 1, 8, 10, 16 oder 18 Spielen blieben unberücksichtigt. Beispielsweise
verschwanden dadurch ganze englische Wochen aus der Spieltagsauswertung.

Es fehlten somit 30 offizielle Spieltage in diesem Vergleich. Die Bezeichnung
als Spieltagsvergleich war zu ungenau. Bei der Prüfung wurden keine Mischungen
mehrerer offizieller Spieltage innerhalb der 106 aufgenommenen Gruppen gefunden;
das Problem ist die unvollständige Auswahl, nicht eine falsche Punkteaddition.

Die offiziellen Nummern wurden aus den bereits heruntergeladenen OpenLigaDB-
Saisons zugeordnet. Alle Ergebnisse wurden mit den vorhandenen Football-Data-
Datensätzen abgeglichen. 3.060 Spiele von 2016/17 bis 2025/26 besitzen jetzt diese
Zuordnung in der Datenbank. Spiel-IDs, Ergebnisse, Quoten und alte Prognosen
wurden dabei nicht überschrieben. Ein fehlender Alias für DSC Arminia Bielefeld
wurde ergänzt.

## Gemeinsamer Informationsstand

Nur die alten Einzelspieltipps nach offiziellen Runden zusammenzuzählen genügt
für einen feststehenden Neun-Spiele-Zettel nicht: Nachholspiele waren teilweise
mit späteren Informationen getippt worden. Acht offizielle Runden in 2018–2021
und vier in 2022–2025 enthielten mehrere Prognosezeitpunkte.

Diese zwölf Runden wurden mit einem gemeinsamen frühen Stichtag neu gerechnet:
Dienstag der Woche ihrer ersten Partie, Training nur mit davor liegenden
Ergebnissen. Für Nachholspiele in späteren Wochen wurden undatierte historische
Quoten ausgeschlossen. Bei acht beziehungsweise fünf Spielen greift deshalb
das interne Modell ohne Quoten. Zeitgestempelte Captures wären nur zulässig,
wenn sie schon zum Stichtag vorlagen.

Die übrigen, bereits einheitlichen Runden wurden aus dem vorhandenen Lauf
übernommen. Die neuen Dateien enthalten jeweils 1.224 Spiele in 136 offiziellen
Runden mit jeweils genau einem Prognosezeitpunkt. `original_forecast_reused`
kennzeichnet übernommene Prognosen. Das ist eine Korrektur bekannter Daten,
kein neuer Blindtest. Für Opening-Quoten der ersten Kalenderwoche fehlt weiterhin
ein genauer historischer Erfassungszeitpunkt; ihre Verfügbarkeit wird angenommen.

Die zuvor beobachteten 26 Punkte des internen Modells am 17. Spieltag 2025/26
enthielten eine spätere Prognose für Hamburg–Leverkusen. Mit dem gemeinsamen
Stichtag 13. Januar 2026 bleiben **25 Punkte**. Die am 4. März nachgeholte Partie
wird nun ebenfalls mit dem Januar-Modell und ohne spätere Quoten getippt.

## Was bringt Poisson gegenüber einer einfachen Quotenregel?

Bei Quotengewicht 1,0 bestimmt der Markt die Gesamtwahrscheinlichkeiten für
Heimsieg, Remis und Auswärtssieg. Das interne Dixon-Coles-Modell verteilt diese
Masse auf genaue Ergebnisse und Tordifferenzen. Daraus wird der Tipp mit den
höchsten erwarteten Punkten gewählt. Das ist mehr als bloß den Favoriten zu
tippen: Auch die gewählte Tendenz kann durch die Punktelogik beeinflusst werden.
Es ist aber kein zusätzlicher eigener Vorsprung bei den 1X2-Wahrscheinlichkeiten.

Zum Vergleich wurden feste 2:1/1:2/1:1- und 1:0/0:1/1:1-Tipps auf die jeweils
wahrscheinlichste Markttendenz ausgewertet. Ohne zulässige Quoten übernehmen
beide Heuristiken denselben internen Tipp wie die aktuelle Kombination. Der
Vergleich bevorteilt somit keine Methode durch spätere Quoten bei Nachholspielen.

Alle übrigen Modellmerkmale bleiben bei den folgenden Varianten gleich.
"Intern" bezeichnet das Dixon-Coles-Modell inklusive der vorhandenen Form-,
H2H-, Marktwert- und Aufsteigermerkmale vor der Quotenangleichung. Die Mischung
bei 50 % ist die vorhandene geometrische Mischung der 1X2-Wahrscheinlichkeiten.
Es wurden keine Gewichte an diesen Ergebnissen nachoptimiert.

### 2022/23 bis 2025/26: 136 offizielle Spieltage

| Methode | Gesamtpunkte | Punkte/Spieltag | Bestwert | Ab 22 Punkten | Ab 24 Punkten |
|---|---:|---:|---:|---:|---:|
| Quotenfavorit, fest 2:1/1:2/1:1 | 1.674 | 12,31 | 23 | 2 | 0 |
| Quotenfavorit, fest 1:0/0:1/1:1 | 1.649 | 12,13 | 22 | 2 | 0 |
| Bisherige Kombination, 100 % Quotengewicht | 1.677 | 12,33 | 23 | 2 | 0 |
| Internes Modell, ohne Quoten | 1.606 | 11,81 | 25 | 4 | 1 |
| Mischung, 50 % Quotengewicht | 1.653 | 12,15 | 25 | 3 | 1 |

Die bisherige Kombination gewinnt über vier Saisons lediglich drei Punkte
gegenüber der einfachen 2:1-Regel. Das ist kein belastbarer Mehrwert für die
aufwendigere Methode. Der Vergleich ist zudem nachträglich auf bereits bekannten
Daten erfolgt. Ein einzelner 25er bei anderer Gewichtung genügt ebenso wenig,
um eine bessere reale Gewinnchance zu belegen.

### 2018/19 bis 2021/22: 136 offizielle Spieltage

| Methode | Gesamtpunkte | Punkte/Spieltag | Bestwert | Ab 22 Punkten | Ab 24 Punkten |
|---|---:|---:|---:|---:|---:|
| Quotenfavorit, fest 2:1/1:2/1:1 | 1.604 | 11,79 | 22 | 1 | 0 |
| Quotenfavorit, fest 1:0/0:1/1:1 | 1.590 | 11,69 | 21 | 0 | 0 |
| Bisherige Kombination, 100 % Quotengewicht | 1.572 | 11,56 | 20 | 0 | 0 |
| Internes Modell, ohne Quoten | 1.581 | 11,63 | 22 | 1 | 0 |
| Mischung, 50 % Quotengewicht | 1.573 | 11,57 | 20 | 0 | 0 |

Hier lag die einfache 2:1-Regel sogar vorne. Aus den Ergebnissen lässt sich
weder eine überzeugende Preisstrategie noch ein stabiler Zusatznutzen der
vollständigen Modellkomplexität ableiten. Die frühere Bevorzugung der Quoten
bezog sich vor allem auf Wahrscheinlichkeitsgüte und Durchschnittspunkte;
das ist nicht dasselbe Ziel wie ein seltener Spieltagspreis.

## Umsetzung und Artefakte

- `rounds.py`: geprüfte Zuordnung offizieller Nummern, Cacheimport und
  ausdrückliche Unterscheidung zwischen offiziellen Runden und Kalenderfenstern.
- `backtest.py`: gemeinsamer Stichtag je offiziellem Spieltag, Behandlung späterer
  Quoten, vollständige Rundenzählung und optionaler Filter `--matchdays`.
- `contest_backtest.py`: Replay nach offiziellen Runden; Ablehnung von Zetteln
  mit mehreren Prognosezeitpunkten oder einem Stichtag nach Spieltagsbeginn.
- `data/backtest_d1_2018_2021_official_frozen.csv` und
  `data/backtest_d1_2022_2025_official_frozen.csv`: korrigierte Prognosen.
- `data/frozen_round_model_audit.json`: Kennzahlen des Variantenvergleichs.
- `data/frozen_predictions_2018_2021.csv` und
  `data/frozen_predictions_2022_2025.csv`: Einzelspieltipps der Vergleichsregeln.
- `data/repair_official_backtests.py`, `data/audit_frozen_models.py`:
  reproduzierbare Korrektur und Vergleich aus dem Paketordner mit `PYTHONPATH=..`.

Die Gruppierungs- und Zeitregeln wurden mit Tests für zwei Spieltage in derselben
Woche und für Nachholspiele geprüft. Der korrigierte Replay wurde außerdem an
dem betroffenen 17. Spieltag 2025/26 ausgeführt. Ein Vergleich mit der realen
Konkurrenz bleibt mangels deren Tipps nicht möglich.
