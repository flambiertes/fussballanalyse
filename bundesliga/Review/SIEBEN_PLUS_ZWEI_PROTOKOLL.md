# Vorab festgelegter Versuch: sieben Basistipps, höchstens zwei Risiken

Festgelegt am 09.09.2026 vor Auswertung dieser neuen Kombinationen.
Die Ergebnisse der einzelnen Halbwertszeiten aus dem 15-Saison-Vergleich sind
bereits bekannt. Dies ist eine darauf aufbauende, retrospektive Untersuchung,
kein unangetasteter Blindtest.

## Regel

1. Basis ist die bisherige Auswahl nach erwarteten Punkten: 420 Tage,
   bisherige Form 0,10, 100 % zulässige Opening-Quoten; ohne Quoten Modellfallback.
2. Risikosignal ist das interne Modell mit 90, 120 oder 270 Tagen, ebenfalls
   bisherige Form 0,10, ohne Quoten. 420 Tage ohne Quoten dient als Kontrollvariante:
   So lässt sich erkennen, ob kürzere Halbwertszeit überhaupt nötig ist.
3. Eine andere Tendenz kommt infrage, wenn sie in der Basis mindestens 20 %
   Wahrscheinlichkeit besitzt und das Risikomodell sie mindestens fünf
   Prozentpunkte höher bewertet. Sie muss nicht dessen wahrscheinlichste
   Tendenz sein; auch ein ausreichend unterstützter Außenseiter ist möglich.
4. Innerhalb dieser Tendenz bleibt der nach der Basisverteilung beste genaue
   Ergebnistipp maßgeblich. Sortierung nach größter positiver Abweichung,
   dann geringsten erwarteten Punktekosten, dann Spiel-ID und Ergebnistipp.
5. Greifbare Risikobegrenzung: höchstens zwei geänderte Begegnungen und insgesamt
   höchstens **ein erwarteter Punkt Verlust** gegenüber dem alten Neunerzettel,
   gemessen unter der Basisverteilung. Höchstens ein zusätzlich getipptes Remis.
   Null oder eine Änderung sind zulässig; keine erzwungenen Außenseitertipps.

20 %, fünf Prozentpunkte und ein erwarteter Punkt sind bewusst gesetzte
Grenzen für diesen Versuch. Sie werden nicht an den Ergebnissen optimiert.
Ein Modell-Quoten-Unterschied ist kein nachgewiesener Informationsvorsprung.

## Auswertung und Auswahl

- Dieselben 4.590 Spiele auf 510 offiziellen Spieltagen 2011/12–2025/26 wie
  zuvor; Wiederverwendung der damals vor jedem Spieltag berechneten Modelle.
  Stichtage, Form, Aufsteiger-Priors und Behandlung späterer Quoten identisch.
- Alle Risiken und die unveränderte Basis vollständig auswerten. Die schon
  bekannten kompletten Umstellungen auf 90/120/270 Tage ohne Quoten werden
  als zusätzliche Vergleichszeilen gezeigt, nicht zur Auswahl zugelassen.
- Primäres Auswahlziel jetzt **Häufigkeit von Spieltagen mit mindestens 22
  Punkten**, sekundär mindestens 24 Punkte. Das nähert das gewünschte
  Spitzenziel an; es ist keine Schätzung realer Gewinne gegen die Konkurrenz.
- Bei Gleichstand zuerst weniger tatsächliche Tippänderungen bevorzugen,
  danach mehr Gesamtpunkte, zuletzt deterministisch nach Kandidatenname.
  Die Basis mit null Änderungen ist ein zulässiger Kandidat.
- Für jede Testsaison 2016/17–2025/26 anhand der fünf vorherigen vollständigen
  Saisons wählen, diese Wahl dann für die ganze folgende Saison festhalten.
  Basistipps und konkrete Risikoauswahl werden vor jedem Spieltag aktualisiert.
- Pro Saison und insgesamt: ≥18/20/22/24 Punkte, Durchschnitt, Maximum,
  Zahl der Änderungen, erwarteter Punkteverzicht und Anzahl schlechterer,
  gleicher oder besserer Spieltage als die Basis.
- Unsicherheit: gepaarte Bootstrap-Stichproben ganzer Testsaisons, 10.000
  Wiederholungen, Seed 20260909. Bei nur zehn Blöcken und wenigen Spitzentagen
  bleiben die Aussagen begrenzt.
- Auswahl für 2026/27 aus 2021/22–2025/26 als dokumentiertes Ergebnis ausgeben.
  Kein Umschalten der Live-Strategie allein wegen eines einzelnen Rekordtags.

Benötigt keine Konkurrenzdaten und keine manuell abgeschriebenen App-Prozente.
