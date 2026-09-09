# Form und höchstens zwei Abweichungen

Nachtrag: Die unten ausgewerteten 106 Kalenderfenster sind eine eingeschränkte
Auswahl aus vier Saisons mit 136 offiziellen Spieltagen. Die Spieltagslogik
wurde inzwischen korrigiert; siehe [Modellaudit](MODELL_UND_SPIELTAGSAUDIT.md).
Die hier dokumentierten Zahlen werden als ursprünglicher Versuch aufbewahrt.

Stand: 8. September 2026. Empfehlung: Die alte Auswahl nach erwarteten Punkten
bleibt die Grundlage und ist wieder der Live-Standard. Der unbeschränkte
Konkurrenzmodus wurde zu früh als Standard eingeführt; er bleibt optional.

## Wie viel Momentum ist enthalten?

- Grundstärke: sechs Jahre Ergebnisgeschichte, zeitlich gewichtet mit 420 Tagen
  Halbwertszeit. Das reagiert eher langsam auf kurzfristige Veränderungen.
- Zusätzliche Form: letzte acht verfügbare Spiele in der Trainingsliga, jüngere
  stärker gewichtet, Halbwertszeit 3,5 Spiele. Punkte dominieren; eine auf
  +/-3 begrenzte Tordifferenz ergänzt sie. Eine Dämpfung in Richtung neutral
  begrenzt den Einfluss kleiner Stichproben. Es gibt keine Gegnerbereinigung.
- `form_weight=0.10` wirkt auf den Logarithmus der erwarteten Tore, nicht als
  pauschaler Zuschlag von zehn Prozentpunkten auf eine Siegchance.
- Beispiel vor Spieltag 2: Der Formterm erhöhte Augsburgs erwartete Tore um
  4,10 % und senkte Frankfurts um 3,69 %. Andere Merkmale wirken zusätzlich.
- Separate Heim-/Auswärtsform hat derzeit Gewicht 0. Trainerwechsel,
  Mentalität, Verletzungen, Aufstellungen und xG sind keine eigenen Merkmale.
- Zweitliga-Angriffs-/Abwehrstärken werden für Aufsteiger separat übertragen
  und über zehn Erstligaspiele ausgeblendet. Ihre jüngsten Zweitligaergebnisse
  werden nicht zusätzlich als kurzfristige D1-Form eingerechnet.
- Bei vorhandenen Quoten und `bookmaker_weight=1.0` kommen die endgültigen
  1X2-Wahrscheinlichkeiten vollständig aus dem Quotenmittel. Die interne Form
  verändert dann noch die genaue Ergebnisverteilung innerhalb jedes Ausgangs.
  Ohne Quoten wirkt die interne Form auch auf die endgültigen 1X2-Chancen.

## Explorative Auswahl: mindestens sieben Originaltipps behalten

Eine einfache Regel wurde vor dem Auszählen festgelegt, ohne Parametersuche:

1. Ausgangspunkt ist der alte Tippzettel. Mindestens sieben genaue Tipps bleiben.
2. Eine andere Tendenz wird nur erwogen, wenn sie im internen Dixon-Coles-Modell
   der wahrscheinlichste 1X2-Ausgang ist und vom bisherigen Tipp abweicht.
3. Der Ausgang muss in der ursprünglichen Prognose mindestens 20 % haben und
   vom internen Modell höher eingeschätzt werden als in dieser Prognose.
4. Es werden höchstens zwei Spiele gewählt, sortiert nach dieser Differenz.
   Innerhalb der alternativen Tendenz wird das Ergebnis mit den höchsten
   erwarteten Tippspielpunkten unter der ursprünglichen Prognose gewählt.

20 % ist hier eine heuristische Grenze für diesen Versuch, kein bewiesenes
Optimum. Ein Unterschied zwischen internem Modell und Quoten ist ein
Prüfkandidat, kein belegter Wissensvorsprung. Es können auch null oder eine
Abweichung entstehen. Es werden keine Ergebnisse genutzt, bevor die Auswahl
feststeht. Keine neue produktive `7+2`-Strategie wurde damit automatisch aktiviert.

## Vergleich auf der bekannten Entwicklungsperiode

106 vollständige Neun-Spiele-Kalenderfenster aus 2018/19 bis 2021/22; dieselbe
Eingabe wie im bisherigen Strategiereview. Im Mittel wurden 0,84 Spiele geändert.

| Kennzahl | Alte Auswahl | Höchstens zwei Abweichungen |
|---|---:|---:|
| Durchschnittliche Punkte | 11,48 | 11,58 |
| Bestwert | 20 | 22 |
| Spieltage ab 18 Punkten | 9 | 5 |
| Spieltage ab 20 Punkten | 1 | 1 |
| Spieltage ab 22 Punkten | 0 | 1 |
| Spieltage ab 24 Punkten | 0 | 0 |

Das ist gemischt und kein Nachweis einer besseren Preisstrategie. Ein einzelner
höherer Bestwert genügt nicht; zudem ist die Entwicklungsperiode bereits bekannt.
Ein Vergleich über einen vorher festgelegten zukünftigen Zeitraum ist sinnvoller
als weitere Anpassungen an dieselben zwei aktuellen Spieltage.

Die nachträgliche Anwendung auf Spieltag 1 ergab 12 statt 12 Punkte: Union–Frankfurt
wurde von 2:1 auf 1:2 geändert, das tatsächliche 3:3 brachte beiden null Punkte.
Spieltag 2 ergab 12 statt 13: Elversberg 0:1 statt Gladbach 1:0 brachte drei Punkte
hinzu; Paderborn–Freiburg 0:0 statt 0:1 verlor vier. Das ist ebenfalls kein Blindtest.

Artefakte: `data/check_seven_plus_two.py`, `data/seven_plus_two_development.csv`,
`data/seven_plus_two_development.json`, `data/seven_plus_two_matchdays_1_2.csv`.
Reproduktion aus dem Paketordner: `PYTHONPATH=.. ../.venv/bin/python data/check_seven_plus_two.py`.

## Geduld bedeutet keine Erfolgsgarantie

Zwei mäßige Spieltage widerlegen die alte Strategie nicht. Ausbleibender Erfolg
macht einen guten nächsten Spieltag aber auch nicht wahrscheinlicher. Beispiel:
Die gespeicherte Modellchance für mindestens 24 Punkte an Spieltag 2 lag bei
0,8265 %. Bei hypothetisch gleicher Chance und unabhängigen Spieltagen ergäben
sich über 34 Spieltage nur rund 24,6 % für mindestens einen solchen Spieltag.
Das ist eine Veranschaulichung, keine Saisonprognose und keine Preisgewinnchance.
