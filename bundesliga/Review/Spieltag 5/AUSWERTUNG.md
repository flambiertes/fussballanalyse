# Spieltag 5 – Auswertung

Stand: **12.10.2026**. Alle neun Partien sind abgeschlossen. Der am 09.10.
angenommene und laut Nutzer unverändert abgegebene 270-Tage-Zettel erzielt
**14 Punkte**, die parallel eingefrorene 420-Tage-Quotenvariante **17 Punkte**.
Der gesamte Unterschied entsteht bei **Union–Elversberg: Endstand 1:0**.
Unser Tipp 1:2 bringt null Punkte, die Alternative 2:1 drei.

## Einzelvergleich

| Partie | Endstand | Angenommen, 270 Tage | Punkte | 420 Tage + Quoten | Punkte |
|---|---:|---:|---:|---:|---:|
| Dortmund – Bremen | 2:2 | 2:0 | 0 | 2:0 | 0 |
| Augsburg – Bayern | 2:2 | 1:3 | 0 | 1:3 | 0 |
| Paderborn – Stuttgart | 0:2 | 1:2 | 2 | 1:2 | 2 |
| Hoffenheim – Hamburg | 2:3 | 2:1 | 0 | 2:1 | 0 |
| Mainz – Leverkusen | 3:4 | 1:2 | 3 | 1:2 | 3 |
| Union Berlin – Elversberg | 1:0 | 1:2 | 0 | 2:1 | 3 |
| Leipzig – Frankfurt | 6:0 | 2:1 | 2 | 2:1 | 2 |
| Köln – Mönchengladbach | 2:1 | 2:1 | 4 | 2:1 | 4 |
| Freiburg – Schalke | 2:1 | 1:0 | 3 | 1:0 | 3 |
| **Summe** | | | **14** | | **17** |

Bewertung: 4 exakt, 3 richtige Tordifferenz, 2 richtige Tendenz, 0 falsch.
Der angenommene Zettel hat einen exakten Treffer, zwei Differenztreffer,
zwei weitere richtige Tendenzen und vier Fehlertipps.

Der [Vorabzettel vom 18.09.](../Spieltage%205%20und%206/VORLAEUFIG.md)
hätte **18 Punkte** erzielt. Er enthält Union–Elversberg 1:0 und
Paderborn–Stuttgart 0:1. Der Union-Tipp wäre exakt gewesen (+4 gegenüber
dem abgegebenen Zettel); der andere Stuttgart-Tipp erzielt ebenfalls zwei
Punkte. Das ist ein nachträglicher Vergleich eines vorhandenen Zettels,
kein nachträglich verbesserter Tipp.

## Einordnung der Elversberg-Abweichung

Vor Anpfiff gab das 270-Tage-Modell Elversberg rund 51 % Siegchance, der
damalige Markt rund 34 %. Der Markt bevorzugte Union mit rund 41 %,
das interne Modell gab Union rund 26 %. Die Gegenposition war damit
vorab dokumentiert und modellbegründet; diesmal ging sie verloren.
Die [ursprüngliche Empfehlung](EMPFEHLUNG.md) bleibt unverändert erhalten.

Auch an Spieltag 3 verlor die einzige Gegenposition gegen die eingefrorene
Vergleichsmethode vier Punkte (8 statt 12). Zusammengenommen ergeben die
beiden dokumentierten Vergleiche **22 statt 29 Punkte**. Für Spieltag 4
liegt kein archivierter Zettel vor; er wird nicht rekonstruiert oder
mitgezählt. Zwei Spieltage reichen nicht, um eine Strategieüberlegenheit
zu belegen oder das Modell gezielt umzustellen. Es liegen keine
Konkurrenzzettel und keine verifizierte Platzierung für Spieltag 5 vor.

## Daten und Prüfung

- [OpenLigaDB, Bundesliga 2026/27, Spieltag 5](https://api.openligadb.de/getmatchdata/bl1/2026/5),
  abgerufen am 12.10.2026. Alle neun Spiele sind als abgeschlossen markiert;
  verwendet wurden ausdrücklich die Endergebnisse (`resultTypeID=2`).
- Die gesamte D1-Saison wurde über den Projektimport aktualisiert:
  306 Spiele, davon 45 abgeschlossen. Der separate Spieltagsabruf stimmt
  bei Mannschaften, Spieltagsnummern, Status und Toren mit der Datenbank überein.
- Beide eingefrorenen CSVs wurden über Match-ID und Mannschaften eindeutig
  zugeordnet. Ihre Tipps stimmen mit den ursprünglichen Datenbankläufen
  `live-d1-4a988d3c3987` und `live-d1-d6298c07c597` überein.
- Alle Einzelpunkte und Summen wurden mit `tip_points` und einer unabhängigen
  Umsetzung der 4/3/2-Regel gegengeprüft. Ursprüngliche Prognosen,
  Wahrscheinlichkeiten und Modellparameter wurden nicht verändert.
- [Vergleich als CSV](../../data/spieltag_5_2026_auswertung_20261012/vergleich.csv),
  [Excel](../../data/spieltag_5_2026_auswertung_20261012/auswertung.xlsx) und
  [Prüfmetadaten mit Eingabeprüfsummen](../../data/spieltag_5_2026_auswertung_20261012/auswertung.metadata.json).

Vor Spieltag 6 bleibt der Zettel vom 18.09. vorläufig. Vor der Abgabe
müssen Ergebnisse beider Ligen, aktuelle Quoten und der Informationsstand
erneuert werden; Änderungen sind gegenüber dem eingefrorenen Vorabzettel
auszuweisen. Diese Auswertung erstellt noch keine neue Empfehlung für Spieltag 6.
