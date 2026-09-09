# Spieltag 2 – Auswertung und neues Strategieziel

Stand: 8. September 2026. Alle 306 Saisonspiele wurden über den vorhandenen
OpenLigaDB-Import aktualisiert. Die neun Endergebnisse des zweiten Spieltags
stimmen mit den drei CHECK24-Screenshots in diesem Ordner überein.

## Ergebnisvergleich

Bewertet wurde der tatsächlich gespeicherte Zettel
`data/tipps_spieltag_2_2026_risk.xlsx`, Lauf `live-d1-99625d5682df`,
Informationsstand 31. August 2026, 12:40:47. Die damaligen Tipps und
Wahrscheinlichkeiten wurden nicht überschrieben.

| Spiel | Ergebnis | Unser Tipp | Unsere Punkte |
|---|---:|---:|---:|
| Stuttgart – Köln | 4:1 | 2:1 | 2 |
| Hoffenheim – Dortmund | 2:3 | 1:2 | 3 |
| Leverkusen – Union | 4:0 | 2:0 | 2 |
| Gladbach – Elversberg | 3:4 | 1:0 | 0 |
| Bremen – Leipzig | 3:1 | 1:2 | 0 |
| Paderborn – Freiburg | 0:1 | 0:1 | 4 |
| Schalke – Bayern | 0:0 | 0:2 | 0 |
| Hamburg – Mainz | 0:5 | 0:1 | 2 |
| Frankfurt – Augsburg | 1:4 | 2:1 | 0 |
| **Summe** | | | **13** |

Die drei vom Nutzer als Top 3 eingeordneten Zettel ergeben nachgerechnet
**24 Punkte (mypl), 22 (Mareike Leidl), 22 (CsZoli78)**.
Die Screenshots zeigen die Zettel, aber keine vollständige Rangliste oder
Teilnehmerzahl. Einzelne Tipps und Quellen stehen in [vergleich.csv](vergleich.csv).

Alle drei tippten Bremen, das Schalke-Remis und Augsburg. Zwei tippten Elversberg;
CsZoli78 wählte dort ein Remis. Auch diese Zettel enthalten viele 1:0-, 0:1-
und 1:2-Tipps. Das entscheidende Risiko lag in den gewählten Spielausgängen.

## Warum die 24-Punkte-Strategie zu wenig verändert

Der Optimierer sucht mittels mehrerer Startlösungen und lokaler Verbesserungen
einen Zettel mit hoher `P(Punkte >= 24)`. Er kennt weder Teilnehmerzahl noch
Konkurrenztipps, Rangfolge oder Preisvergabe. Eine globale Optimalität ist
durch dieses Suchverfahren nicht garantiert.

Für diesen Spieltag änderte er gegenüber der Auswahl nach erwarteten Punkten
**keine einzige Tendenz**. Nur Leverkusen wurde von 2:1 auf 2:0 und Bayern
von 0:1 auf 0:2 geändert.

| Auswahl | Erwartete Punkte | Modellchance auf mindestens 24 | Tatsächliche Punkte |
|---|---:|---:|---:|
| Erwartete Punkte | 12,9256 | 0,8144 % | 13 |
| Zielwert 24 | 12,9220 | 0,8265 % | 13 |

Die Verbesserung beträgt nur rund **0,012 Prozentpunkte**. Diese Modellchance
ist keine Gewinnchance gegen die CHECK24-Konkurrenz. Ein höherer fixer Zielwert
würde dieses grundsätzliche Problem ebenfalls nicht lösen.

Für diesen Vergleich wurden die gespeicherten Lambdas und 1X2-Wahrscheinlichkeiten
verwendet. Der nicht gespeicherte Dixon-Coles-Parameter wurde ausschließlich
mit Spielen vor dem damaligen Prognosezeitpunkt erneut geschätzt
(`rho = -0.08241218168070989`). Die rekonstruierte Zielwahrscheinlichkeit stimmt
mit dem gespeicherten Wert überein. Dies ist eine Diagnose des vorhandenen
Laufs, kein neuer Blindtest.

## Welche Risiken schon vorher plausibel waren

| Alternativer Ausgang | Gespeicherte Wahrscheinlichkeit vor Spieltag 2 |
|---|---:|
| Augsburg gewinnt in Frankfurt | 27,2 % |
| Bremen schlägt Leipzig | 26,3 % |
| Elversberg gewinnt in Gladbach | 22,9 % |
| Schalke spielt gegen Bayern remis | 12,0 % |

Augsburg, Bremen und Elversberg waren nach unseren damaligen Zahlen durchaus
mögliche Außenseiter. Schalke-Remis war das deutlich größere Risiko. Dass die
vier Ausgänge tatsächlich eintraten, beweist weder eine systematische
Fehleinschätzung noch, dass man künftig immer vier Außenseiter wählen sollte.
Die drei Gewinnerzettel sind eine nach Erfolg ausgewählte Stichprobe; die
erfolglosen Risikozettel fehlen.

## Zweite Baustelle: Welche Information darf die Prognose verändern?

`bookmaker_weight=1.0` übernimmt die 1X2-Wahrscheinlichkeiten vollständig aus
dem margenbereinigten Quotenmittel. Form, H2H und Aufsteiger-Prior beeinflussen
weiterhin die genaue Ergebnisverteilung innerhalb von Sieg, Remis und Niederlage,
können deren Gesamtwahrscheinlichkeit aber nicht mehr verschieben.

Das interne Modell vor dieser Angleichung gab Elversberg rund **34,7 %**
Siegchance und Schalke rund **19,1 %** Remischance; nach der Angleichung waren
es **22,9 %** und **12,0 %**. Bei Augsburg war es umgekehrt: intern **22,3 %**,
mit Quoten **27,2 %**. Das sind Modellunterschiede, keine Belege dafür, dass
das interne Modell grundsätzlich besser ist. Historisch waren die Quoten
die stärkste Verbesserung.

Schalkes Zweitliga-Abwehr fließt bereits über den Angriffs-/Abwehr-Prior ein.
Trainerwirkung, Mentalität, Ausfälle und Aufstellungen sind derzeit keine
eigenständigen Merkmale. Eine zusätzliche Bewertung muss vor Anpfiff mit
Quelle, Zeitpunkt und begrenztem Einfluss festgehalten werden. Die genannten
Trainer-/Mentalitätserklärungen wurden in dieser Auswertung nicht unabhängig
verifiziert. Augsburgs 3:0 gegen Schalke am ersten Spieltag ist in den
aktualisierten historischen Ergebnissen enthalten und war schon vor dem
zweiten Spieltag bekannt.

## Festgelegte Richtung: Chance auf den Spieltagspreis

Das gewünschte Ziel ist **eine möglichst hohe Chance auf einen Spieltagssieg**;
niedrige Durchschnittspunkte und viele schlechte Spieltage werden akzeptiert.
Eine Mindestpunktzahl oder eine feste Anzahl Außenseitertipps bildet das nicht ab.

Die dafür passende Auswahl bewertet einen kompletten Zettel gegen ein Modell
der Konkurrenz:

1. Spielausgänge anhand der vor Anpfiff verfügbaren Wahrscheinlichkeiten
   simulieren. Alle Teilnehmer werden gegen dieselben simulierten Ergebnisse
   gewertet; ihre Punktzahlen sind deshalb voneinander abhängig.
2. Konkurrenzzettel anhand von Teilnehmerzahl und Tippverteilungen modellieren.
   Möglichst auch die Verteilung genauer Ergebnisse und typische Muster ganzer
   Zettel berücksichtigen. Buchmacherwahrscheinlichkeit und Tippanteil sind
   verschiedene Größen.
3. Den eigenen Zettel auf die Wahrscheinlichkeit des ersten Platzes optimieren.
   Gleichstände nach den tatsächlichen Preisregeln behandeln. Bei gleichmäßiger
   Teilung oder Auslosung wäre der erwartete Gewinnanteil ein mögliches Ziel;
   die CHECK24-Regel ist hier noch nicht verifiziert.
4. Gezielte Abweichungen dort wählen, wo ein plausibler Ausgang von der
   Konkurrenz besonders selten getippt wird. Beispiel, ausdrücklich hypothetisch:
   27 % Siegchance bei nur 8 % Sieg-Tipps kann Augsburg interessant machen.
   Der Quotient allein ersetzt nicht die Optimierung des ganzen Zettels.
5. Ergebnisse über verschiedene plausible Konkurrenzmodelle und unabhängige
   Simulationsstichproben prüfen. Prognoseverbesserungen getrennt von der
   Turnierstrategie testen; diesen bereits bekannten Spieltag nicht zum
   nachträglichen Erfolgsnachweis verwenden.

Nachtrag: Der Nutzer hat klargestellt, dass keine Konkurrenzdaten verfügbar sind
und keine wöchentliche manuelle Erfassung gewünscht ist. Der neue experimentelle
Modus `contest` erzeugt deshalb neun feste Konkurrenzszenarien automatisch aus
den vorhandenen Wahrscheinlichkeiten. Teilnehmerzahl, Favoritenkonzentration
und Gleichstandsbehandlung sind explizite Annahmen. Die Top-3-Screenshots dienen
nicht zur Schätzung der Tippverteilung. Simulierte Gewinnanteile sind keine
gemessenen realen Gewinnchancen. `target-score` bleibt als Vergleich verfügbar.
Bedienung und Grenzen stehen in der README; wöchentliche Eingaben entfallen.

## Quellen und Prüfung

- [OpenLigaDB, Bundesliga 2026, Spieltag 2](https://api.openligadb.de/getmatchdata/bl1/2026/2),
  abgerufen am 8. September 2026 über den Projektimport.
- `IMG_9798.jpeg`, `IMG_9799.jpeg`, `IMG_9800.jpeg` in diesem Ordner.
- Gespeicherter Prognoselauf und ursprüngliche Excel-Datei vom 31. August 2026.
- Alle neun Ergebnisse und alle vier Punktesummen wurden programmgesteuert
  gegengeprüft; die Rekonstruktion der gespeicherten Zielwahrscheinlichkeit ebenfalls.
