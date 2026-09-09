# Entscheidungen und Stand am 09.09.2026

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

## Nach Spieltag 3

1. Tatsächliche Endergebnisse abrufen und die neun Begegnungen abgleichen.
2. Den angenommenen 270-Tage-Zettel und die parallel eingefrorene bisherige
   Methode mit denselben 4/3/2-Regeln auswerten. Den Stuttgart-Unterschied
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
