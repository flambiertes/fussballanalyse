# Strategie ohne manuelle Konkurrenzdaten

Nachtrag: Die 106 unten ausgewerteten Neun-Spiele-Kalenderfenster waren keine
vollständige Auswertung der 136 offiziellen Spieltage. Die damaligen Ergebnisse
bleiben als eingeschränkter Versuch dokumentiert. Die korrigierte Gruppierung
und der Vergleich mit einfachen Quotenregeln stehen im
[Modellaudit](MODELL_UND_SPIELTAGSAUDIT.md).

Stand: 8. September 2026. Gewünscht ist die Chance auf einen einzelnen
Spieltagspreis; schlechte Durchschnittspunkte werden akzeptiert. Wöchentliche
Eingaben von Tippanteilen oder fremden Zetteln sind ausdrücklich nicht vorgesehen.

## Umsetzung

`contest` ist ein optionaler experimenteller Modus der Live-Prognose. Nach dem
Vergleich wurde `expected-points` wieder als Standard eingesetzt, weil ein
praktischer Vorteil des Konkurrenzmodells nicht belegt ist. Die vorhandenen
Spielwahrscheinlichkeiten bleiben die Grundlage. Es werden neun feste
Konkurrenzszenarien verwendet: 100, 1.000 oder 10.000 angenommene Gegner,
kombiniert mit drei Stärken der Konzentration auf konventionelle Punktetipps.
Diese Szenarien wurden nicht anhand der Gewinner von Spieltag 2 angepasst.

Die angenommenen genauen Tippanteile folgen
`Q(Tipp) proportional zu sqrt(P(exaktes Ergebnis)) * exp(beta * erwartete Punkte)`
mit `beta = 1,0 / 2,5 / 5,0`. Das ist eine transparente Modellannahme, keine
aus der App ermittelte oder empirisch kalibrierte Verteilung. Die Konkurrenz
wählt pro Spiel unabhängig; Fanmuster über mehrere Spiele fehlen.

Die Auswahl optimiert näherungsweise das geometrische Mittel der erwarteten
Gewinnanteile über die neun Szenarien. Gleichstände zählen als gleichmäßige
Teilung oder gleichmäßige Auslosung. Die tatsächliche CHECK24-Regel ist dadurch
nicht verifiziert. Es gibt keine feste Zahl vorgeschriebener Außenseiter.

Alle Teilnehmer werden gegen dieselben Ergebnisse gewertet. Die Punkteverteilung
eines angenommenen Gegners wird bedingt auf diese Ergebnisse exakt berechnet;
seine Punktzahl wird nicht unabhängig von unserer gezogen. Eine lokale Suche
mit mehreren Startzetteln verwendet 8.192 simulierte Spieltage. Weitere 32.768
unabhängige Simulationen bewerten den ausgewählten Zettel, ohne die Auswahl
nachträglich zu verändern. Jede exportierte Vorschau erhält eine JSON-Datei
mit Annahmen, Vergleichswerten und Simulationsfehlern der Differenzen.

## Historischer Vergleich

Eingabe: `data/backtest_d1_2018_2021_live_config_with_odds.csv`.
Die bereits gespeicherten Wahrscheinlichkeiten wurden rekonstruiert; tatsächliche
Ergebnisse gingen erst nach der Auswahl in die Bewertung ein. Der Test umfasst
106 vollständige Neun-Spiele-Kalenderfenster der Saisons 2018/19 bis 2021/22.
23 andere Kalenderfenster sind ausgeschlossen. Das ist die vorhandene
Entwicklungsperiode, kein neuer unangetasteter Test.

| Kennzahl | Erwartete Punkte | Neuer Modus |
|---|---:|---:|
| Durchschnittliche Punkte je Spieltag | 11,48 | 10,15 |
| Standardabweichung | 4,03 | 4,04 |
| Bester Spieltag | 20 | 20 |
| Mindestens 18 Punkte | 9 | 4 |
| Mindestens 20 Punkte | 1 | 1 |
| Mindestens 22 oder 24 Punkte | 0 | 0 |

Im Mittel änderte der neue Modus 6,92 Tendenzen. Eine Verbesserung hoher
Punktzahlen oder eine deutlich höhere Streuung ist hier **nicht nachgewiesen**.
Die unabhängige Kontrollsimulation bewertete den neuen Zettel in allen 106
Fenstern beim kombinierten Szenarioziel höher als die normale Auswahl. Das
bestätigt die Optimierung unter ihren Annahmen, nicht die Richtigkeit dieser
Annahmen über reale Teilnehmer.

Zusätzlich wurde für jedes historische Endergebnis die erwartete Preisbeteiligung
gegen das jeweils angenommene Feld berechnet: Punktverteilung der Gegner bedingt
auf die tatsächlichen neun Ergebnisse, anschließend dieselbe Gleichstandsformel
wie im Optimierer. Im Mittel lag der neue Zettel in allen neun angenommenen
Feldern vor dem normalen Zettel. Bei 10.000 angenommenen Gegnern waren die
absoluten Werte für beide Methoden sehr klein. Auch dieser Vergleich bleibt
von den unbeobachteten Tippanteilen abhängig und liefert keine realen Siegquoten.

Artefakte:

- `data/contest_replay_2018_2021.csv`: Zettel und Punkte pro Kalenderfenster.
- `data/contest_replay_2018_2021.json`: aggregierte Kennzahlen und Konfiguration.
- `data/contest_replay_2018_2021_assumed_prizes.csv`: Preisbeteiligung bedingt auf
  historische Ergebnisse, getrennt nach angenommenem Konkurrenzszenario.

## Vorschau und technische Prüfung

Eine Vorschau für Spieltag 3 liegt unter
`data/tipps_spieltag_3_2026_contest_vorschau.xlsx`, mit zugehöriger `.metadata.json`.
Sie weicht bei sechs Spielen in der Tendenz ab. Erwartete Punkte: 9,88 statt
13,15. In acht von neun unabhängigen Kontrollszenarien ist der geschätzte
Gewinnanteil höher; im kleinsten Feld mit geringer Favoritenkonzentration ist
kein klarer Vorteil zu erkennen.

Diese Vorschau verwendet die vorhandenen Quoten vom 31. August 2026. Vor einer
tatsächlichen Tippabgabe müssen die Quoten und Ergebnisse regulär aktualisiert
werden. Für den neuen Modus sind dabei keine Konkurrenzdaten einzugeben.

29 Tests bestanden, darunter eine vollständige Auszählung eines kleinen Feldes
mit Gleichständen, die Abhängigkeit der Teilnehmerpunkte von denselben
Ergebnissen, numerische Extremfälle und die Trennung von Auswahl und Bewertung.
Live-Prognose mit Excel, JSON und Datenbank sowie der reguläre Backtest wurden
zusätzlich ausgeführt und geprüft.
