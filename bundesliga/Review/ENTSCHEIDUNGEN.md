# Entscheidungen und Stand am 23.10.2026

## Ziel

Wir wollen gelegentlich einen Spieltagspreis gewinnen. Niedrigere
Durchschnittspunkte und viele schlechte Spieltage sind dafür akzeptabel.
Mehr durchschnittliche Punkte allein sind deshalb kein ausreichender
Erfolgsnachweis. Ohne reale Konkurrenzzettel können wir tatsächliche
Gewinnchancen nicht zuverlässig ausrechnen.

Es gibt keinen Zugriff auf die Tippverteilung der Konkurrenz. Wöchentliches
Abschreiben von App-Prozenten ist ausdrücklich nicht Teil des Arbeitsablaufs.
Die drei vorliegenden Spitzenzettel von Spieltag 2 sind Beispiele erfolgreicher
Tipps, keine repräsentative Konkurrenzstichprobe.

## Was geprüft und entschieden wurde

| Frage | Befund | Konsequenz |
|---|---|---|
| Was geschah an Spieltag 2? | Unser gespeicherter Zettel: 13 Punkte; die drei gezeigten Spitzenzettel: 24/22/22. | Ergebnisse, Screenshots und Einzelpunkte im [Review](Spieltag%202/AUSWERTUNG.md) erhalten. |
| Warum verändert das Ziel „24 Punkte“ so wenig? | Am zweiten Spieltag nur zwei genaue Ergebnisse geändert, keine Tendenz; weiterhin 13 Punkte. | Zielwertoptimierung allein ist kein nachgewiesener Weg zum Tagespreis. |
| Können wir Konkurrenz ohne manuelle Eingaben simulieren? | Optionaler `contest`-Modus mit neun ausdrücklich hypothetischen Szenarien; viele Remistipps und kein überzeugender historischer Vorteil. | Experiment bleibt optional, `expected-points` ist wieder Standard. |
| Warum zuvor 106 Spieltage? | Es waren vollständige Kalenderfenster, keine vollständige Auswahl der 136 offiziellen Spieltage aus vier Saisons. | Offizielle Runden und gemeinsamer früher Stichtag, auch für Nachholspiele; [Audit](MODELL_UND_SPIELTAGSAUDIT.md). |
| Sind viele Remis normal? | Über 340 Spieltage 2016/17–2025/26 hatten 58 mindestens vier Remis (17,1 %); nur 13 mindestens fünf (3,8 %). | Kein künstlich remislastiger Standardzettel. |
| Woher kommen 420 Tage Halbwertszeit? | Bereits im initialen Pipeline-Commit gesetzt; alle 24 bis dahin gespeicherten Läufe mit 420 Tagen. Kein dokumentierter Optimierungslauf. | Vergleich über 15 vollständige Saisons durchgeführt. |
| Welche Halbwertszeit ist sinnvoll? | Im untersuchten Raster 630–840 Tage etwas bessere Prognosegüte; saisonale Verläufe 630→180 und 180→630 wurden zeitlich getrennt nie ausgewählt. | Kein belastbarer zusätzlicher Tagespreis-Vorteil durch die Auswahl auf Prognosegüte. |
| Hilft gegnerbereinigte Form? | Kleine Verbesserung bei langem Gedächtnis, bei kurzem Gedächtnis eher Verschlechterung. | Als untersuchtes Merkmal vorhanden; keine pauschale Live-Übernahme. |
| Funktioniert sieben plus höchstens zwei Risiken? | Zeitlich getrennte Auswahl: auf 340 Spieltagen dreimal ≥22 statt zweimal, weiterhin kein ≥24; geringerer Durchschnitt. | Die getestete Regel wird nicht zum Live-Standard. |
| Welcher Kandidat bleibt für Spitzen interessant? | Komplettes 270-Tage-Modell ohne Quoten: auf denselben 340 Spieltagen achtmal ≥22 und zweimal ≥24; 11,67 statt 11,96 Durchschnittspunkte. | Bewusster Risikokandidat, keine gesicherte Überlegenheit; historische Daten bereits bekannt. |

Die ausführlichen Protokolle und Ergebnisse stehen im
[Halbwertszeit-Bericht](HALBWERTSZEIT_UND_FORM.md) und im
[7+2-Bericht](SIEBEN_PLUS_ZWEI_ERGEBNIS.md).

## Beschlossener Zettel für Spieltag 3

Der am 09.09.2026 mit frischen Ergebnissen und Vergleichsquoten berechnete
[Tippzettel](Spieltag%203/EMPFEHLUNG.md) wurde vom Nutzer angenommen.
Er verwendet das interne 270-Tage-Modell ohne Quotenübernahme und ohne
nachträgliche Einzeländerungen. Gegenüber der bisherigen Methode ändert sich
nur **Hoffenheim–Stuttgart: 1:2 statt 2:1**. Die anderen acht genauen Tipps
bleiben gleich. Es werden keine zusätzlichen Außenseiter oder Remis erzwungen.

Der allgemeine Code-Standard `LIVE_CONFIG` bleibt davon getrennt bei 420 Tagen
und Quotengewicht 1,0. Die für diesen Spieltag gewählte Variante sowie beide
Prognoseläufe sind mit Zeitstempel und vollständiger Konfiguration archiviert.
Es wurde keine Tippabgabe in einer externen App vorgenommen.

## Auswertung nach Spieltag 5

Die [Auswertung zu Spieltag 5](Spieltag%205/AUSWERTUNG.md) ist abgeschlossen:
**14 Punkte** für den angenommenen 270-Tage-Zettel, **17 Punkte** für die
parallel archivierte 420-Tage-Quotenvariante. Union–Elversberg endete 1:0;
unser 1:2-Tipp verlor gegenüber dem Vergleichstipp 2:1 drei Punkte.
Die ursprünglichen Tipps und Konfigurationen bleiben unverändert.

Spieltag 3 brachte 8 statt 12 Punkte. Beide dokumentierten Vergleiche ergeben
damit 22 statt 29; der nicht archivierte Spieltag 4 bleibt unbewertet.
Das ist bisher kein Vorteil für die gewählte Variante, aber auch keine
ausreichende Grundlage für gezieltes Nachjustieren. Reale Platzierungen
und Tagespreis-Chancen sind ohne Konkurrenzdaten weiterhin nicht messbar.

Die 270-Tage-Regel und der allgemeine Code-Standard bleiben unverändert.

## Empfehlung vor Spieltag 6

Der [neu berechnete Zettel vom 16.10., 11:06 MESZ](Spieltag%206/EMPFEHLUNG.md)
enthält aktuelle Ergebnisse beider Ligen und frische Vergleichsquoten.
Die einzige Änderung zum Vorabzettel vom 18.09. ist
**Elversberg–Augsburg 2:1 statt 0:1**. Die parallel eingefrorene
420-Tage-Quotenvariante tippt dort 1:2; die übrigen acht Tipps sind gleich.
Der Markt sieht diese Partie fast ausgeglichen, während das interne Modell
Elversberg stärker bevorzugt. Der Nutzer hat den neuen Zettel am 16.10.
unverändert angenommen und das Archivieren, Committen und Pushen beauftragt.
Eine Abgabe in der externen Tipp-App wurde vom Assistenten nicht vorgenommen
oder geprüft. Beide Läufe und ihre vollständigen Konfigurationen sind separat
archiviert. Nach dem Spieltag werden diese eingefrorenen Zettel nach der
gleichen 4/3/2-Regel ausgewertet.

## Auswertung Spieltag 6 und Empfehlung für Spieltag 7

Der angenommene Zettel erzielt [20 Punkte](Spieltag%206/AUSWERTUNG.md),
die eingefrorene Vergleichsmethode 17. Elversberg–Augsburg endet 4:3 und
bringt unserem 2:1-Tipp drei zusätzliche Punkte. Drei exakte Treffer,
zwei weitere Differenztreffer, ein weiterer Tendenztreffer. Der Nutzer
berichtet von einem knapp verpassten Tagespreis; der Abstand ist ohne
Gewinnerpunktzahl oder Platzierung nicht bezifferbar.

Die dokumentierten Spieltage 3, 5 und 6 ergeben zusammen **42 statt 46 Punkte**.
Spieltag 4 bleibt unbewertet. Die Regel und der allgemeine Code-Standard
bleiben unverändert.

Die [neue Empfehlung vom 23.10., 15:55 MESZ](Spieltag%207/EMPFEHLUNG.md)
enthält frische Ergebnisse beider Ligen und aktuelle Vergleichsquoten.
Beide Methoden wählen dieselben Tendenzen; abweichende genaue Tipps:
Augsburg–Union 2:1 statt 1:0 und Freiburg–Bayern 1:3 statt 1:2.
Erste Partie heute Stuttgart–Mönchengladbach, 20:30 Uhr Berliner Zeit, 2:1.
Der Nutzer hat den neuen Zettel am 23.10. angenommen und in der App eingetragen.

Auf Wunsch des Nutzers wurden zusätzlich die [Spieltage 8 und 9 vorläufig
berechnet](Spieltage%208%20und%209/VORLAEUFIG.md), damit bereits Tipps in der
App hinterlegt werden können, falls eine Aktualisierung ausbleibt. Gleiche
270-Tage-Regel und gleicher Informationsstand wie Spieltag 7; noch unbekannte
Ergebnisse der dazwischenliegenden Runden werden nicht vorweggenommen.
Die Vorabzettel bleiben eingefroren; vor den jeweiligen Spieltagen neu rechnen
und Änderungen ausweisen. Die externe App wurde vom Assistenten nicht bedient.

Der Nutzer hat am 23.10. bestätigt, dass **alle Tipps für die Spieltage 7, 8
und 9 in der App eingetragen sind**, und das Committen und Pushen beauftragt.
Spieltag 8 und 9 bleiben vorläufige Rückfallzettel. Beim nächsten Treffen nach
Spieltag 7 zuerst dessen Ergebnisse auswerten und anschließend Spieltag 8 mit
neuen Ergebnissen und Vergleichsquoten rechnen; Änderungen gegenüber dem
eingetragenen Vorabzettel ausdrücklich zeigen. Vor Spieltag 9 entsprechend
vorgehen. Die ursprünglichen Prognosen und Erstellungsmetadaten bleiben erhalten.

## Ablauf nach jedem abgeschlossenen Spieltag

1. Tatsächliche Endergebnisse abrufen und die neun Begegnungen abgleichen.
2. Den angenommenen 270-Tage-Zettel und die parallel eingefrorene bisherige
   Methode mit denselben 4/3/2-Regeln auswerten. Abweichende Tipps
   ausdrücklich zeigen.
3. Falls der Nutzer seine tatsächlich abgegebenen Tipps verändert hat, diese
   getrennt vom ursprünglichen Vorschlag festhalten.
4. Keine Erwartungen oder Tipps nachträglich überschreiben. Ein einzelner
   guter oder schlechter Spieltag entscheidet die Strategiefrage nicht.

## Methodische Grenzen, die erhalten bleiben

- ≥22/24 Punkte sind beobachtbare Ersatzgrößen, keine realen Tagesgewinne.
- Die historischen Untersuchungen sind retrospektiv; nach wiederholter
  Sichtung dürfen frühere Testjahre nicht erneut als unangetastet bezeichnet werden.
- Opening-Quoten der ersten Spielwoche sind nicht exakt zeitgestempelt;
  spätere undatierte Quoten für Nachholspiele wurden ausgeschlossen.
- Die alte Quotenaggregation für 2018/19 enthielt Maximalquoten; der aktuelle
  einheitliche Marktmittelwert erklärt 1.569 statt 1.572 Basispunkte über
  2018/19–2021/22. Dies ist kein Modellverbesserungseffekt.
- Historische Marktwerte sind unvollständig. Verletzungen, Aufstellungen,
  zusätzliche Belastung und Trainerwechsel sind keine expliziten Modellmerkmale.

Code, Tests, unveränderte Ergebnisarchive und der angenommene Tippzettel werden
gemeinsam versioniert. [Archivübersicht](ARCHIV.md).
