# Empfehlung für Spieltag 5, 2026/27

Stand: **09.10.2026, 08:57 Uhr MESZ** (Stichtag 2026-10-09T06:57:33 UTC).
Erstes Spiel: heute, Fr. 20:30. Status: **vom Nutzer am 09.10.2026 angenommen**, einschließlich
Union–Elversberg 1:2 („ok I go for elversberg“). Eine Abgabe in der Tipp-App erfolgt nicht durch den Assistenten.

Regel wie an den Spieltagen 3 und 4: vollständiges internes Modell mit
270 Tagen Halbwertszeit, Form 0,10, H2H 0,05, Marktwert 0,05, Aufsteiger-Prior,
**Quotengewicht null**, Auswahl nach maximalen erwarteten 4/3/2-Punkten.
Keine manuellen Einzeländerungen.

## Tippzettel

| Anpfiff MESZ | Partie | Empfehlung | Vorläufig 18.09. | 420 Tage + Quoten | Modell H/U/A | Markt H/U/A |
|---|---|---:|---:|---:|---|---|
| Fr. 09.10., 20:30 | Dortmund – Bremen | **2:0** | 2:0 | 2:0 | 78/15/7 | 72/17/11 |
| Sa. 10.10., 15:30 | Augsburg – Bayern | **1:3** | 1:3 | 1:3 | 8/13/80 | 9/11/80 |
| Sa. 10.10., 15:30 | Paderborn – Stuttgart | **1:2** | 0:1 | 1:2 | 25/26/49 | 20/21/60 |
| Sa. 10.10., 15:30 | Hoffenheim – Hamburg | **2:1** | 2:1 | 2:1 | 63/20/16 | 67/19/14 |
| Sa. 10.10., 15:30 | Mainz – Leverkusen | **1:2** | 1:2 | 1:2 | 28/25/47 | 32/25/43 |
| Sa. 10.10., 15:30 | Union Berlin – Elversberg | **1:2** | 1:0 | 2:1 | 26/24/51 | 41/25/34 |
| Sa. 10.10., 18:30 | Leipzig – Frankfurt | **2:1** | 2:1 | 2:1 | 60/20/20 | 61/20/19 |
| So. 11.10., 15:30 | Köln – Mönchengladbach | **2:1** | 2:1 | 2:1 | 42/25/33 | 47/24/29 |
| So. 11.10., 17:30 | Freiburg – Schalke | **1:0** | 1:0 | 1:0 | 54/26/20 | 58/23/19 |

Markt = margenbereinigter Konsens aus 23 Anbietern (The Odds API,
abgerufen 09.10.2026, 08:50 MESZ).

## Änderungen gegenüber dem Vorabstand

- **Paderborn–Stuttgart 1:2 statt 0:1.** Gleiche Tendenz; die 420-Tage-
  Quotenvariante liefert ebenfalls 1:2.
- **Union–Elversberg 1:2 statt 1:0: einziger Tendenzwechsel und einzige
  Abweichung von der 420-Tage-Quotenvariante (2:1).** Union hat nach vier
  Spielen 1 Punkt und 4:17 Tore (3:3, 0:4, 1:3, 0:7), Aufsteiger Elversberg
  7 Punkte und 8:6 Tore. Das 270-Tage-Modell gewichtet diesen Verlauf stark;
  der Markt sieht Union weiterhin vorne. Die Abweichung (51 % gegenüber 34 %
  Auswärtssieg) ist deutlich größer als bei Stuttgart an Spieltag 3 und damit
  der unsicherste Tipp des Zettels.

Verletzungen, Sperren und Trainerwechsel sind keine Modellmerkmale. Eine
kurze Websuche am 09.10. lieferte keine verwertbaren Personalmeldungen; ein
gezielter Abgleich vor Anpfiff wurde nicht vorgenommen.

## Rückblick Spieltag 3

Der angenommene 270-Tage-Zettel erzielte 8 Punkte, die parallel eingefrorene
420-Tage-Methode 12. Der Unterschied ist genau die Gegenposition
Hoffenheim–Stuttgart (1:2 getippt, 2:1 Endstand). Ein einzelner Spieltag
entscheidet die Strategiefrage nicht. Der Zettel für Spieltag 4 ist im
Repository nicht archiviert.

## Datenstand und Dateien

- OpenLigaDB am 09.10. aktualisiert: D1 36, D2 54 abgeschlossene Partien
  2026/27; zwischen 21.09. und 08.10. lagen in beiden Ligen keine Spiele.
- Run-IDs: `live-d1-4a988d3c3987` (270 Tage), `live-d1-d6298c07c597` (420 Tage).
- `data/spieltag_5_2026_empfehlung_20261009T065733Z/`: beide Läufe mit
  Konfiguration, `vergleich.csv` und `tipps_spieltag_5.xlsx`.
