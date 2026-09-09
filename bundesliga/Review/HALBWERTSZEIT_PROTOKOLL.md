# Vorab festgelegter Vergleich: Halbwertszeit und Form

Festgelegt am 08.09.2026 vor Berechnung der neuen Varianten.

## Herkunft

`half_life_days = 420.0` steht bereits im initialen Pipeline-Commit `540ee25`.
Alle 24 in der Datenbank vorhandenen Prognoseläufe verwenden 420 Tage.
Eine Optimierung dieses Parameters ist nicht dokumentiert. Der Wert ist ein
gesetzter Ausgangspunkt, kein empirisch nachgewiesenes Optimum.

## Daten und zeitliche Trennung

- Auswertung: 2011/12–2025/26, 15 vollständige Saisons, 510 offizielle Spieltage,
  4.590 Spiele. 2010/11 liefert ausschließlich Vorlauf für die Form.
- Vor jedem offiziellen Spieltag neuer Fit; gemeinsamer Stichtag ist der
  Dienstag vor dessen erster Begegnung. Nachholspiele behalten diesen Stichtag.
- Im Fit nur Ergebnisse mit Datum vor dem Stichtag; unverändert sechs Jahre
  Trainingsfenster. 15 Jahre Evaluation sind etwas anderes als 15 Jahre Training.
- Erste fünf Saisons als initiale Auswahlhistorie. Für jede Saison 2016/17–2025/26
  wird ausschließlich anhand der fünf vorherigen vollständigen Saisons gewählt.
  Die Auswahl bleibt für die gesamte folgende Saison fest.
- Diese historische Untersuchung ist retrospektiv. Teile der jüngeren Saisons
  wurden schon in anderen Experimenten betrachtet; kein unangetasteter Blindtest.

## Kandidaten und Auswahlregel

- Feste Halbwertszeiten: 90, 120, 180, 270, 420, 630, 840 Tage.
- Saisonabhängig: linear 630 → 180 Tage von Spieltag 1 bis 34;
  Gegenprobe 180 → 630. Der offizielle Spieltag ist vor dem Spiel bekannt.
- Gleiche übrige Parameter wie bisher: Ridge, Sechsjahresfenster,
  Aufsteiger-Priors, H2H und Marktwerte werden nicht zusätzlich optimiert.
- Formvarianten je Halbwertszeit: keine Form; bisherige Form mit Gewicht 0,10;
  gegnerbereinigte Form mit Gewichten 0,05 / 0,10 / 0,20.
- Neue Form: standardisiertes Tordifferenz-Residuum gegenüber der eigenen,
  vor dem damaligen Spiel gespeicherten Basiserwartung ohne Form und Quoten;
  begrenzt auf ±3, letzte acht D1-Spiele, Halbwertszeit 3,5 Spiele,
  Schrumpfung um drei gewichtete Beobachtungen. Nur tatsächlich vor dem
  aktuellen Stichtag beendete Partien liefern Residuen. Erwartungswerte werden
  nicht nachträglich mit dem heutigen Fit für alte Spiele neu berechnet.
- Primäres Auswahlkriterium: mittlerer Log-Loss des exakten Ergebnisses,
  sekundär 1X2-Brier-Score, deterministischer Kandidatenname bei Gleichstand.
  Niedriger ist besser. Keine Wahl nach einem einzelnen Punkterekord.
- Getrennte Auswahl von (a) festen Halbwertszeiten bei bisheriger Form,
  (b) festen plus saisonalen Halbwertszeiten bei bisheriger Form,
  (c) Halbwertszeit plus Form. Dadurch lässt sich Zusatznutzen zuordnen.
- Zunächst eigene Wahrscheinlichkeiten ohne Quoten. Zusätzlich dieselben
  Kandidaten mit 50 % und 100 % Quotengewicht auswerten, jeweils mit einer
  innerhalb dieser Gewichtung nur auf früheren Saisons beruhenden Auswahl.
  Das Quotengewicht selbst wird in diesem Versuch nicht optimiert.

## Ausgaben und Grenzen

Pro Kandidat und Saison: Ergebnis-Log-Loss, 1X2-Log-Loss, Brier-Score,
Tipppunkte, Spieltagmaximum und Häufigkeiten ≥18 / ≥20 / ≥22 / ≥24.
Saisonphasen 1–8, 9–25, 26–34 separat beschreiben; nicht nachträglich daraus
eine weitere Strategie zusammenstellen. Unsicherheit der gepaarten
Unterschiede durch 10.000 Bootstrap-Stichproben ganzer Testsaisons (Seed
20260908); zehn Saisons sind wenig unabhängige Blöcke.

Quoten: historische Opening-Konsensquoten; für nachgeholte Spiele außerhalb
der ursprünglichen Kalenderwoche keine untimestamped Quoten verwenden.
Für gewöhnliche Spiele ist rechtzeitige Verfügbarkeit mangels genauer
Zeitstempel eine Annahme. Fehlende Quoten führen zum eigenen Modell zurück.
Marktwerte nur mit Datenbank-Stichtag vor bzw. am Prognosetermin.

Die Punktauswertung verwendet durchgehend die bisherige Maximum-Erwartungswert-
Tippregel (4/3/2 Punkte). ≥24 ist eine deskriptive Schwelle und ohne
Konkurrenzdaten kein Beleg für einen gewonnenen Tagespreis. Es erfolgt keine
automatische Änderung der Live-Konfiguration aufgrund dieses Experiments.
