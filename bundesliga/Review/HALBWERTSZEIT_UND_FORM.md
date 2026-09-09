# Halbwertszeit und Form: Ergebnis des 15-Saison-Vergleichs

Abgeschlossen am 09.09.2026. [Excel mit allen Ergebnissen](../data/half_life_15_seasons/halbwertszeit_15_saisons.xlsx)
und [vorab festgelegtes Protokoll](HALBWERTSZEIT_PROTOKOLL.md).

**Die 420 Tage waren ein gesetzter Startwert, keine dokumentierte Optimierung.**
Der neue Vergleich findet etwas bessere Ergebniswahrscheinlichkeiten mit längerer
Halbwertszeit und kleiner gegnerbereinigter Formkorrektur. Eine bessere Strategie
für den Tagespreis ist damit noch nicht nachgewiesen. Die Live-Konfiguration
bleibt deshalb bei 420 Tagen und der bisherigen Tippregel.

## Herkunft und Bedeutung der 420 Tage

Der initiale Pipeline-Commit `540ee25` enthält bereits `half_life_days = 420.0`.
Alle 24 bisher in SQLite gespeicherten Prognoseläufe verwenden diesen Wert.
Die README führte seine Optimierung bisher als zukünftige Arbeit auf.

Ein Ergebnis erhält im Fit das relative Gewicht `0.5 ** (Alter_in_Tagen / H)`.
Bei H = 420 zählt ein ein Jahr altes Spiel noch rund 55 % eines neuen Spiels.
Die Gewichte werden anschließend gemeinsam normalisiert; ihre relativen
Verhältnisse bleiben erhalten. Das ist das Gedächtnis der Mannschaftsstärken.
Davon getrennt existiert der kurzfristige Formaufschlag: letzte acht Ligaspiele,
Halbwertszeit 3,5 Spiele, bisher Gewicht 0,10. Kürzere Modell-Halbwertszeit und
stärkere Zusatzform können ähnliche Informationen mehrfach verstärken.

## Umfang und Verfahren

- 2011/12–2025/26: **4.590 Spiele, 510 vollständige offizielle Spieltage**.
- 2010/11 zusätzlich als Vorlauf für historische Formresiduen.
- 4.896 D1-Fits, jeweils mit ergänzendem D2-Fit für Aufsteiger-Priors.
- Neun Halbwertszeit-Verläufe × fünf Formvarianten; jede Kombination ohne
  Quoten sowie mit 50 % und 100 % Quotengewicht ausgewertet.
- Vor jedem Spieltag nur damals verfügbare Ergebnisse. Einheitlicher Stichtag
  je offiziellem Spieltag, einschließlich Nachholspielen.
- Unverändert sechs Jahre Training je Fit. Die 15 Jahre bezeichnen die Länge
  der **Auswertung**, nicht ein Trainingsfenster mit späteren Ergebnissen.
- Für die zehn Testsaisons 2016/17–2025/26 erfolgt die Auswahl jeweils anhand
  der fünf vorherigen vollständigen Saisons. Sie bleibt während der nächsten
  Saison fest. Primäres Auswahlziel ist Ergebnis-Log-Loss, nicht der Punkterekord.

Diese Untersuchung ist retrospektiv. Teile der jüngeren Saisons waren schon
Gegenstand früherer Versuche; sie ist kein neuer unangetasteter Blindtest.

## Feste und saisonabhängige Halbwertszeiten

Alle 15 Saisons, interne Wahrscheinlichkeiten ohne Quoten, bisherige Form 0,10.
Log-Loss und Brier: niedriger ist besser. Tipppunkte folgen unverändert der
Regel 4 exakt / 3 Differenz / 2 Tendenz und der Maximum-Erwartungswert-Tippwahl.

| Halbwertszeit | Ergebnis-Log-Loss | 1X2-Brier | Punkte/Spieltag | ≥22 Punkte | ≥24 Punkte |
|---|---:|---:|---:|---:|---:|
| 90 Tage | 3,1129 | 0,6085 | 11,60 | 7 | 3 |
| 120 Tage | 3,0842 | 0,6031 | 11,66 | 7 | 3 |
| 180 Tage | 3,0610 | 0,5977 | 11,63 | 4 | 2 |
| 270 Tage | 3,0486 | 0,5950 | 11,68 | 11 | 2 |
| **420 Tage, bisher** | **3,0409** | **0,5937** | **11,68** | **6** | **1** |
| 630 Tage | 3,0379 | 0,5932 | 11,71 | 5 | 1 |
| 840 Tage | 3,0376 | 0,5936 | 11,69 | 6 | 1 |
| Saisonanfang 630 → Saisonende 180 | 3,0421 | 0,5935 | 11,69 | 4 | 1 |
| Saisonanfang 180 → Saisonende 630 | 3,0467 | 0,5951 | 11,62 | 7 | 2 |

Die Pfeile sind lineare Verläufe zwischen Spieltag 1 und 34. Im untersuchten
Raster liegen 630–840 Tage bei der Prognosegüte vorne. 840 ist zugleich die
obere Rastergrenze; daraus folgt kein exakt bestimmtes universelles Optimum.

**Für das Preisziel ist die rechte Tabellenhälfte relevant:** Die schlechter
kalibrierten 90-/120-Tage-Varianten erreichen jeweils drei 24er-Spieltage,
die bisherige 420-Tage-Variante einen. Auch 270 Tage ist mit elf statt sechs
Spieltagen ab 22 Punkten ein auffälliger Kandidat. Diese Unterschiede dürfen
nicht unterschlagen werden. Es sind jedoch sehr wenige Ereignisse unter vielen
verglichenen Varianten, keine belastbar gemessenen realen Tagesgewinne.

Auch in den letzten zehn Saisons erzielen die fest betrachteten 90-, 120- und
270-Tage-Varianten jeweils zweimal mindestens 24 Punkte gegenüber einmal bei
420 Tagen. Diese Kandidaten wurden hier nicht durch ein vorher festgelegtes
Tagespreis-Auswahlverfahren gewählt. Die Stichprobe begründet eine Risikohypothese,
noch keinen gesicherten Vorsprung.

## Ändert sich die sinnvolle Halbwertszeit im Saisonverlauf?

Deskriptiver Ergebnis-Log-Loss, gleiche 15 Saisons und bisherige Form:

| Halbwertszeit | Spieltage 1–8 | Spieltage 9–25 | Spieltage 26–34 |
|---|---:|---:|---:|
| 420 | 3,0580 | 3,0101 | 3,0839 |
| 630 | 3,0487 | 3,0091 | 3,0828 |
| 840 | 3,0455 | 3,0097 | 3,0833 |
| 630 → 180 | 3,0507 | 3,0099 | 3,0955 |

Deine Vermutung hat einen kleinen deskriptiven Anhaltspunkt: Zu Saisonbeginn
liegt unter den festen Kandidaten 840 vorne, später 630. Die Unterschiede sind
klein. Ein starkes Absenken bis auf 180 Tage schadet besonders am Saisonende.
**Keine der beiden vorab definierten saisonalen Varianten wird in einer der
zehn folgenden Testsaisons ausgewählt**, auch nicht mit Quoten oder zusätzlicher
Formauswahl. Deshalb sind die Ergebnisse der Auswahl „nur feste Halbwertszeiten“
und „feste plus saisonale Halbwertszeiten“ identisch.

Damit sind diese beiden Verläufe geprüft. Ein sanfterer Verlauf wie 840 → 630
wäre eine neue, aus den Ergebnissen entstandene Hypothese und ist hier nicht
als erfolgreiche Strategie getestet worden.

## Was bringt gegnerbereinigte Form?

Für ein vergangenes Spiel wird aus Sicht der Mannschaft berechnet:

`Residuum = [(eigene Tore − Gegentore) − (erwartete eigene Tore − erwartete Gegentore)] / sqrt(erwartete eigene Tore + erwartete Gegentore)`

Die Erwartungen stammen aus dem vor diesem Spiel berechneten Modell ohne
Form und ohne Quoten, einschließlich der sonstigen bestehenden Merkmale.
Ein Remis gegen einen stärkeren Gegner kann damit positiv, dasselbe Resultat
gegen einen schwächeren Gegner negativ eingehen. Die Residuen werden auf ±3
begrenzt, über die letzten acht D1-Spiele gewichtet und zu null geschrumpft.
Die Formhistorie respektiert ebenfalls das Sechsjahresfenster. Zukünftige
Ergebnisse und noch ungespielte Nachholpartien gehen nicht ein.

Bei kurzen Halbwertszeiten verschlechtert der zusätzliche Formaufschlag im
Gesamtvergleich die Prognosegüte. Bei langen Halbwertszeiten hilft eine kleine
Residuenkorrektur etwas. Beispiel 630 Tage: Ergebnis-Log-Loss 3,0379 mit bisheriger
Form gegenüber 3,0372 mit Residuenform 0,10. Das ist ein kleiner Effekt.

## Auswahl nur anhand älterer Saisons: 340 Spieltage

Eigene Wahrscheinlichkeiten, ohne Quoten. „Auswahl Halbwertszeit“ verwendet
weiterhin die bisherige Form; „plus Form“ wählt auch die Formvariante aus.

| Verfahren | Ergebnis-Log-Loss | 1X2-Brier | Gesamtpunkte | Punkte/Spieltag | ≥22 | ≥24 |
|---|---:|---:|---:|---:|---:|---:|
| Immer 420 Tage + bisherige Form | 3,0612 | 0,5958 | 3.964 | 11,66 | 5 | 1 |
| Auswahl Halbwertszeit | 3,0580 | 0,5953 | 3.950 | 11,62 | 4 | 1 |
| Auswahl Halbwertszeit plus Form | 3,0575 | 0,5951 | 3.962 | 11,65 | 4 | 1 |

Ohne Quoten wählt die Halbwertszeit-Regel für 2016/17–2020/21 jeweils 840 Tage,
danach jeweils 630. Bei gemeinsamer Auswahl wird ab 2017/18 gegnerbereinigte
Form gewählt, meistens mit Gewicht 0,10. **Die etwas bessere Prognosegüte
übersetzt sich nicht in mehr Spitzenspieltage oder Gesamtpunkte.**

Gepaarter Bootstrap mit ganzen Saisons, 10.000 Stichproben:

- Nur Halbwertszeit: Log-Loss-Differenz −0,00327;
  95-%-Intervall **[−0,00663; +0,00067]**, schließt null ein.
- Halbwertszeit plus Form: −0,00370;
  Intervall **[−0,00678; −0,000019]**, liegt äußerst knapp unter null.
- Beim Brier-Score und den Durchschnittspunkten schließen die Intervalle
  für beide Verfahren null ein. Für „plus Form“ beträgt das Punkteintervall
  rund **−0,176 bis +0,156 Punkte pro Spieltag**.

Zehn Saisons sind nur zehn Unsicherheitsblöcke. Das knappe Log-Loss-Ergebnis
ist ein schwacher Hinweis, kein Anlass für eine starke Wirksamkeitsbehauptung.

### Derselbe Test mit Quoten

| Quotengewicht | Verfahren | Ergebnis-Log-Loss | Gesamtpunkte | Punkte/Spieltag | ≥24 |
|---|---|---:|---:|---:|---:|
| 50 % | Immer 420 + bisherige Form | 3,0477 | 4.025 | 11,84 | 1 |
| 50 % | Auswahl Halbwertszeit | 3,0446 | 4.018 | 11,82 | 1 |
| 50 % | Auswahl Halbwertszeit plus Form | 3,0449 | 4.037 | 11,87 | 1 |
| 100 % | Immer 420 + bisherige Form | 3,0406 | 4.066 | 11,96 | 0 |
| 100 % | Auswahl Halbwertszeit | 3,0380 | 4.061 | 11,94 | 0 |
| 100 % | Auswahl Halbwertszeit plus Form | 3,0383 | 4.052 | 11,92 | 0 |

Auch hier kein zusätzlicher Spieltag ab 24 Punkten durch die zeitlich getrennte
Auswahl. Bei 100 % Quotengewicht kann das Modell bei vorhandenen Quoten nur
noch die Ergebnisverteilung innerhalb der drei Tendenzen verbessern. Kleine
Brier-Unterschiede stammen von den Partien ohne zulässige Quoten.

## Konsequenz

Die 420 Tage sind jetzt eine geprüfte Vergleichsbasis. Sie waren nicht optimiert,
sind aber auch kein Hinweis darauf, dass unser Hauptproblem zu wenig Momentum
ist. Für bessere Wahrscheinlichkeiten sind längeres Gedächtnis plus kleine
gegnerbereinigte Korrektur interessante Kandidaten. Eine nach derselben
Auswahlregel für 2026/27 ausschließlich auf 2021/22–2025/26 getroffene Wahl wäre
ohne Quoten 840 Tage plus Residuenform 0,10; der Abstand zur Form 0,05 beträgt
nur rund 0,000006 Log-Loss und ist praktisch kein präziser Gewichtsnachweis.

Für Tagespreise bleiben die kürzeren Halbwertszeiten beziehungsweise 270 Tage
eine gesonderte Risikohypothese. Diese Untersuchung liefert dafür beobachtete
Spitzenergebnisse, aber keinen Nachweis gegen die reale Konkurrenz. Deshalb
werden weder Live-Halbwertszeit noch Quotengewicht oder Tippstrategie durch
diesen Versuch automatisch umgestellt. Das Ergebnis ist eine abgeschlossene
Modelluntersuchung, keine fertig nachgewiesene Preisstrategie.

## Datenprüfung und Grenzen

- Ergebnisse und offizielle Spieltagsnummern aus Football-Data und OpenLigaDB
  abgeglichen. Zusätzliche offizielle Nummern 2010–2015 importiert; keine
  Spielresultate oder alten Prognosen überschrieben.
- Opening-Quoten für 4.575 von 4.590 Spielen. Bei 15 später ausgetragenen
  Begegnungen werden keine undatierten späteren Quoten verwendet. Für normale
  Spielwochen ist rechtzeitige Verfügbarkeit mangels genauer Zeitstempel eine
  Annahme. Quelle der Spieltagszuordnung beispielsweise
  [OpenLigaDB 2011/12](https://api.openligadb.de/getmatchdata/bl1/2011).
- Die bestehende Marktwert-Historie umfasst nur 18 verschiedene Vereine.
  Je Saison sind für 156–210 Begegnungen beide Vereine darin enthalten.
  Fehlende Werte wirken neutral. Alle Kandidaten verwenden dieselben Daten;
  der Befund gilt für diese Pipeline, deren Marktwertabdeckung historisch
  unvollständig ist. Auch Ridge und Aufsteiger-Abbildung wurden konstant gehalten.
- Der neue 420-Tage-Lauf reproduziert die Torerwartungen und Dixon-Coles-
  Parameter aller 2.448 früher geprüften Spiele 2018/19–2025/26 bis auf
  Rundung. Bei gleichen Quoten werden auch die alten Tipps reproduziert.
- **Abweichung zum älteren Bericht für 2018/19:** Dessen gespeicherte Quoten
  entsprechen dem Mittel aller Opening-Einträge einschließlich `BbMx`
  (Maximalquoten). Der aktuelle Helfer bevorzugt `BbAv`, den Marktdurchschnitt.
  Das betrifft 306 Quotenvektoren und fünf Tipps. Die aktuelle 420-Tage-Basis
  erreicht über 2018/19–2021/22 deshalb 1.569 statt der früher dokumentierten
  1.572 Punkte. Das ist kein Halbwertszeit-Effekt. Hier verwenden sämtliche
  Kandidaten die einheitliche aktuelle Quotenaggregation; die älteren Dateien
  bleiben als historische Artefakte erhalten. 2022/23–2025/26 bleiben 1.677 Punkte.
- 41 Tests bestanden. Zusätzlich unabhängiger Abgleich der alten Prognosen,
  Kontrolle aller 4.896 erfolgreichen D1-Fits und vollständiger Spieltagsgruppen.
  Vor der ersten Formauswertung wurde ihre Historie ausdrücklich auf das
  unveränderte Sechsjahresfenster begrenzt; im Manifest dokumentiert, ohne
  Änderung der bereits berechneten Basismodellfits.

## Reproduzierbarkeit

Aus dem Repository-Hauptordner:

```bash
OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 OMP_NUM_THREADS=1 \
  .venv/bin/python -m bundesliga.half_life_study --workers 3
PYTHONPATH=. .venv/bin/python bundesliga/data/audit_half_life_outputs.py
```

In `data/half_life_15_seasons/` liegen Manifest, vorbereitete Eingaben,
Basisschätzungen pro Saison, Einzelspielmetriken aller Kandidaten, jährliche
Auswahlentscheidungen, saisonale und gesamte Kennzahlen, Unsicherheitsintervalle,
Datenabdeckung und unabhängiger Prüfbericht. Das Excel bündelt die Ergebnistabellen.
