# Empfehlung für Spieltag 7, 2026/27

Stand: **23.10.2026, 15:55:16 Uhr MESZ** (Stichtag 2026-10-23T13:55:16 UTC).
Erstes Spiel: heute, Stuttgart–Mönchengladbach, **20:30 Uhr Berliner Zeit**.
Status: **vom Nutzer am 23.10.2026 angenommen und laut Nutzer in der Tipp-App
eingetragen**. Der Nutzer hat alle Tipps für die Spieltage 7, 8 und 9 bestätigt
und das Committen und Pushen beauftragt. Die externe App wurde vom Assistenten
nicht bedient oder geprüft. Die Erstellungsmetadaten bleiben als ursprünglicher
Stand vor dieser Bestätigung erhalten.

Die vereinbarte Regel bleibt: vollständiges internes Modell mit 270 Tagen
Halbwertszeit, Form 0,10, H2H 0,05, Marktwert 0,05, Aufsteiger-Prior,
Quotengewicht null und Auswahl nach maximalen erwarteten 4/3/2-Punkten.
Keine manuellen Einzeländerungen.

## Tippzettel

| Anpfiff Berliner Zeit | Partie | Empfehlung | 420 Tage + Quoten | Modell H/U/A | Markt H/U/A |
|---|---|---:|---:|---|---|
| Fr. 23.10., 20:30 | Stuttgart – Mönchengladbach | **2:1** | 2:1 | 71/17/12 | 71/16/13 |
| Sa. 24.10., 15:30 | Augsburg – Union Berlin | **2:1** | 1:0 | 54/24/22 | 52/24/25 |
| Sa. 24.10., 15:30 | Paderborn – Hamburg | **1:0** | 1:0 | 39/29/31 | 42/26/32 |
| Sa. 24.10., 15:30 | Leipzig – Elversberg | **2:1** | 2:1 | 60/19/21 | 67/18/15 |
| Sa. 24.10., 15:30 | Köln – Schalke | **1:0** | 1:0 | 44/30/26 | 46/25/29 |
| Sa. 24.10., 15:30 | Mainz – Bremen | **2:1** | 2:1 | 55/23/22 | 55/23/22 |
| Sa. 24.10., 18:30 | Dortmund – Frankfurt | **2:1** | 2:1 | 73/16/11 | 65/19/16 |
| So. 25.10., 15:30 | Hoffenheim – Leverkusen | **1:2** | 1:2 | 26/21/53 | 34/24/42 |
| So. 25.10., 17:30 | Freiburg – Bayern | **1:3** | 1:2 | 13/15/72 | 14/16/70 |

Wahrscheinlichkeiten in Prozent, einzeln gerundet. Am Sonntag gilt nach der
Zeitumstellung MEZ; Freitag und Samstag MESZ. Markt = margenbereinigter
Anbieterkonsens. Für Mainz–Bremen und Hoffenheim–Leverkusen liegen jeweils
22 Anbieter vor, sonst 23. Die 205 verwendeten aktuellen Anbieter-Snapshots
stammen vom 23.10., 15:51:17 bis 15:53:31 Uhr MESZ.

## Unterschiede und knappe Entscheidungen

Beide Methoden wählen dieselben neun Tendenzen. Unterschiede bestehen nur
bei **Augsburg–Union 2:1 statt 1:0** und **Freiburg–Bayern 1:3 statt 1:2**.
Es gibt keine erzwungenen Außenseiter- oder Remistipps.

**Paderborn–Hamburg** ist die knappste Partie: intern 39,3 % Heimsieg,
29,2 % Remis und 31,4 % Auswärtssieg. **Köln–Schalke** ist mit 44,1 %
Heimsieg ebenfalls kein klarer Favoritentipp. Bei **Hoffenheim–Leverkusen**
ist das interne Modell mit 53,1 % Auswärtssieg deutlich zuversichtlicher
als der Markt mit 41,6 %. Dortmund erhält intern 72,9 %, am Markt 64,8 %.

## Personallage

Die [Bundesliga-Personalübersicht vom 23.10.](https://www.bundesliga.com/en/bundesliga/news/team-news-line-ups-2026-27-bayern-dortmund-fantasy-20707)
meldet bei Gladbach unter anderem Kleindienst, Honorat, Itakura und Kühn
als Ausfälle; Stuttgart fehlen unter anderem Vagnoman und Tomás.
Bremen muss weiter auf Stage und Njinmah verzichten. Bei Bayern fehlen
Musiala und Díaz. Das sind vorläufige Meldungen, keine bestätigten
Startaufstellungen. Verletzungen, Aufstellungen und Europapokalbelastung
sind keine expliziten Modellmerkmale; keine manuelle Tippanpassung daraus.
Eine vollständige Analyse der Zusatzbelastung wurde nicht durchgeführt.

## Datenstand und Prüfung

- Frischer vollständiger OpenLigaDB-Import: D1 306 Spiele, davon 54
  abgeschlossen; D2 306 Spiele, davon 72 abgeschlossen.
- Die neun ungespielten Partien gehören zu Spieltag 7 und liegen nach dem
  gemeinsamen Stichtag. Mannschaften und Anpfiffzeiten stimmen mit dem
  [DFB-Spielplan](https://datencenter.dfb.de/de/competitions/bundesliga/seasons/2026-2027/matchday/7)
  überein.
- The Odds API: 349 Anbieter-Snapshots importiert; alle neun Zielpartien
  besitzen frische Vergleichsquoten. Jeder Lauf trainiert mit 1.853 Spielen
  im konfigurierten Zeitfenster.
- Run-IDs: `live-d1-9be49a85fbaa` (270 Tage),
  `live-d1-ff1609d48f47` (420 Tage). Alle 18 Ergebnisverteilungen aus
  archivierten Parametern rekonstruiert und alle Tipps mit einer unabhängigen
  Erwartungswertberechnung der 4/3/2-Regel geprüft.
- [Excel](../../data/spieltag_7_2026_empfehlung_20261023T135516Z/tipps_spieltag_7.xlsx),
  [CSV-Vergleich](../../data/spieltag_7_2026_empfehlung_20261023T135516Z/vergleich.csv),
  [Quotenalter](../../data/spieltag_7_2026_empfehlung_20261023T135516Z/quoten_alter.csv),
  [Herkunft und Prüfungen](../../data/spieltag_7_2026_empfehlung_20261023T135516Z/provenance.json).

Spieltag 6 brachte [20 statt 17 Punkte](../Spieltag%206/AUSWERTUNG.md).
Die drei dokumentierten Vergleiche (3, 5 und 6) ergeben 42 statt 46 Punkte;
eine Strategieüberlegenheit oder reale Tagespreis-Chance ist damit nicht belegt.
