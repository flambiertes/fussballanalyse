# Empfehlung für Spieltag 3, 2026/27

Stand: **09.09.2026, 07:17 Uhr MESZ**. Nächster Spieltag: 11.–13. September.
Die Termine wurden mit dem [offiziellen Spielplan](https://www.bundesliga.com/de/bundesliga/spieltag/de/3)
abgeglichen. Der Nutzer hat diesen Zettel am 09.09.2026 angenommen
(„ok lassen wir uns überraschen“). Damit ist dies unser beschlossener Stand
für den Spieltag. Eine Abgabe in der Tipp-App wurde durch den Assistenten
nicht vorgenommen oder überprüft.

## Der vorgeschlagene Zettel

| Anpfiff MESZ | Partie | Empfehlung | Bisherige Methode |
|---|---|---:|---:|
| Fr. 11.09., 20:30 | Union Berlin – Schalke | **1:0** | 1:0 |
| Sa. 12.09., 15:30 | Dortmund – Paderborn | **2:0** | 2:0 |
| Sa. 12.09., 15:30 | Hoffenheim – Stuttgart | **1:2** | 2:1 |
| Sa. 12.09., 15:30 | Freiburg – Mönchengladbach | **2:1** | 2:1 |
| Sa. 12.09., 15:30 | Augsburg – Leverkusen | **1:2** | 1:2 |
| Sa. 12.09., 15:30 | Mainz – Frankfurt | **2:1** | 2:1 |
| Sa. 12.09., 18:30 | Köln – Bremen | **2:1** | 2:1 |
| So. 13.09., 15:30 | Leipzig – Hamburg | **2:0** | 2:0 |
| So. 13.09., 17:30 | Elversberg – Bayern | **0:2** | 0:2 |

Dies ist der unveränderte Output des vollständigen internen 270-Tage-Modells
mit bisheriger Form 0,10, H2H/Marktwert/Priors wie zuvor und Quotengewicht null.
Die genaue Ergebniswahl maximiert weiterhin erwartete 4/3/2-Tipppunkte.
Es wurden keine einzelnen Tipps nachträglich aus anderen Varianten übernommen.
Der 420-Tage-Live-Standard wurde zum Vergleich mit aktuellen Quoten gerechnet.

## Begründung der wichtigsten Entscheidungen

**Stuttgart ist die bewusste Gegenposition.** Das interne Modell ergibt
31,97 % Hoffenheim / 21,62 % Remis / 46,41 % Stuttgart. Der aktuelle
margenbereinigte Quotenvergleich liegt bei 41,11 % / 24,08 % / 34,81 %.
Das ist der einzige Tendenzwechsel gegenüber dem bisherigen Zettel. Der VfB
kommt aus einem 4:1 gegen Köln; Hoffenheim verlor die ersten beiden Partien
jeweils 2:3. Quellen: [VfB-Spielberichte](https://www.vfb.de/de/1893/profis/saison/2026-2027/bundesliga/spielberichte/)
und [TSG-Spielplan](https://www.bundesliga.com/en/bundesliga/matchday/2026-2027/tsg-hoffenheim).

Der Modellwert ist keine um aktuelle Ausfälle bereinigte echte Siegchance:
Tiago Tomás fällt laut VfB mit einer Muskelverletzung aus, und Stuttgart spielt
am 9. September noch Champions League gegen Viking Stavanger. Verletzungen,
Rotation und zusätzliche Belastung sind keine expliziten Modellmerkmale.
Das begrenzt die Sicherheit dieses Tipps; die konkrete Empfehlung bleibt
nach heutigem Stand 1:2. Quellen: [VfB-Personalmeldung vom 3. September](https://www.vfb.de/de/vfb/aktuell/neues/profis/2627/vorschau-vfb-stuttgart-gegen-1--fc-koeln/)
und [VfB-Vorschau vom 8. September](https://www.vfb.de/en/vfb/latest/news/professionals/2627/ucl-vorschau-vfb-stuttgart-gegen-viking-stavanger/).

**Augsburgs Start rechtfertigt für mich keinen zusätzlichen Siegertipp gegen
Leverkusen.** Die Heim-Siegchance liegt im Modell bei 23,53 %, im Quotenvergleich
bei 23,47 %. Der Vorsprung ist also nicht vorhanden. Die offizielle Vorschau
weist zudem darauf hin, dass Augsburgs Gegner trotz Chancen für ungefähr vier
Tore erst einmal trafen; Torhüter Dahmen hat stark dazu beigetragen. Das schmälert
den guten Start nicht, begrenzt aber die Aussage der bloßen 7:1-Torbilanz.
[Bundesliga-Vorschau Augsburg–Leverkusen](https://www.bundesliga.com/de/bundesliga/spieltag/2026-2027/3/fc-augsburg-vs-bayer-04-leverkusen/liveticker).

**Auch bei Elversberg bleibe ich bei Bayern.** Beide Prognosen sehen die Gäste
deutlich vorne. Elversberg erzielte laut offizieller Vorschau sieben Treffer
aus 3,9 xG. Aus zwei erfolgreichen Spielen lässt sich deshalb kein entsprechend
großer dauerhafter Stärkegewinn ableiten. Diese Einordnung ist eine qualitative
Plausibilitätsprüfung; xG wurde nicht heimlich als zusätzliches Modellfeature
eingerechnet. [Bundesliga-Vorschau Elversberg–Bayern](https://www.bundesliga.com/de/bundesliga/spieltag/2026-2027/3/sv-elversberg-vs-fc-bayern-muenchen/liveticker).

Dass der Zettel diesmal keinen Remistipp enthält und acht Tipps der bisherigen
Methode entsprechen, ergibt sich aus der vorab gewählten Regel. Es bedeutet
weder eine Prognose, dass tatsächlich kein Spiel unentschieden endet, noch
eine Verpflichtung, an jedem Spieltag mehrere Außenseiter einzubauen.

## Datenstand und Dateien

- D1 aktualisiert: 18 abgeschlossene Partien 2026/27; D2: 36 abgeschlossene
  Partien 2026/27. Gesamte Saisonspielpläne jeweils 306 Spiele.
- Aktuelle Quoten von The Odds API eingelesen; für alle neun Begegnungen
  verfügbar. Jeweils letzter Anbieterstand zwischen 07:11:40 und 07:16:15 MESZ
  am 9. September. Keine alten August-Captures in diesem Vergleich verwendet.
- Prognosestichtag für beide Varianten: 2026-09-09T05:17:15 UTC.
- Empfehlungen, Vergleich, Modellparameter und Run-IDs sind unveränderlich
  datiert gespeichert in
  `data/spieltag_3_2026_empfehlung_20260909T051715Z/`.
- [Excel-Tippzettel](../../data/spieltag_3_2026_empfehlung_20260909T051715Z/tipps_spieltag_3.xlsx),
  [Einzelspielvergleich](../../data/spieltag_3_2026_empfehlung_20260909T051715Z/vergleich.csv).

Die 270-Tage-Variante wird für diesen Zettel bewusst als Kandidat für mehr
Spitzenspieltage verwendet. Die historischen Ergebnisse belegen keine sichere
höhere Gewinnchance gegen die tatsächliche Konkurrenz. Eine spätere Revision
vor Abgabe sollte als neuer datierter Stand gespeichert werden, damit dieser
erste Vorschlag nach dem Spieltag noch überprüfbar bleibt.
