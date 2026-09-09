# Ergebnis: sieben Basistipps und höchstens zwei Risiken

09.09.2026. [Vorab festgelegte Regel](SIEBEN_PLUS_ZWEI_PROTOKOLL.md),
[Excel-Auswertung](../data/limited_risk_15_seasons/sieben_plus_zwei.xlsx).

**Die getestete 7+2-Regel rechtfertigt keine Umstellung der Live-Strategie.**
Sie kostet im zeitlich getrennten Vergleich im Mittel rund 0,27 Punkte pro
Spieltag, bringt einen zusätzlichen Spieltag ab 22 Punkten und keinen ab 24.
Die komplette 270-Tage-Variante bleibt für das Spitzenziel auffälliger als
diese begrenzte Auswahl einzelner Gegenpositionen. Das ist ein Vergleich
bereits bekannter historischer Daten, kein Nachweis künftiger Tagesgewinne.

## Was wurde umgesetzt?

Grundlage ist der bisherige Neunerzettel: 420 Tage, Form 0,10 und zulässige
Quoten zu 100 %. Für höchstens zwei Spiele kann das interne Modell mit 90,
120 oder 270 Tagen eine andere Tendenz vorschlagen. Als Kontrollvariante wird
auch das interne 420-Tage-Modell verwendet.

Eine Abweichung benötigt mindestens 20 % Basiswahrscheinlichkeit und eine
mindestens fünf Prozentpunkte höhere Bewertung durch das Risikomodell. Das
alternative Ergebnis wird innerhalb dieser Tendenz weiterhin nach den
erwarteten Punkten der Basis gewählt. Insgesamt darf der Zettel höchstens
einen erwarteten Punkt verlieren und höchstens einen zusätzlichen Remistipp
enthalten. Mindestens sieben genaue Tipps bleiben erhalten; null oder eine
Abweichung sind zulässig.

Das sind vorab gesetzte Grenzen, keine empirisch optimierten Schwellen. Der
erwartete Punkteverzicht ist unter der Basisverteilung berechnet. Er begrenzt
nicht den tatsächlich möglichen Verlust an einem einzelnen Spieltag.

Die Auswahlfunktion erhält ausschließlich Prognosematrizen und Spiel-IDs,
keine echten Ergebnisse. Die Ergebnisse werden erst danach zum Auszählen
verwendet. Alle 510 offiziellen Spieltage 2011/12–2025/26 sind enthalten,
mit denselben damaligen Modellständen wie im Halbwertszeit-Vergleich.
Konkurrenzdaten oder manuelle App-Prozente sind nicht erforderlich.

## Auswahl anhand vorheriger Saisons: 340 Spieltage

Für jede Saison 2016/17–2025/26 wird anhand der fünf vorherigen vollständigen
Saisons gewählt. Das Auswahlziel ist diesmal die Zahl der Spieltage ab 22
Punkten, danach ab 24 Punkten. Bei Gleichstand werden weniger Änderungen
bevorzugt, danach mehr Gesamtpunkte. Die unveränderte Basis ist ein Kandidat.

| Verfahren | Punkte/Spieltag | Maximum | ≥22 Punkte | ≥24 Punkte | Änderungen/Spieltag |
|---|---:|---:|---:|---:|---:|
| Bisherige Basis | 11,96 | 23 | 2 | 0 | 0,00 |
| 7+2 mit 90 Tagen | 11,59 | 24 | 4 | 1 | 1,41 |
| 7+2 mit 120 Tagen | 11,46 | 22 | 2 | 0 | 1,29 |
| 7+2 mit 270 Tagen | 11,41 | 23 | 2 | 0 | 0,96 |
| 7+2 mit 420 Tagen, Kontrolle | 11,41 | 23 | 2 | 0 | 0,91 |
| **Auswahl anhand älterer Saisons** | **11,69** | **23** | **3** | **0** | **0,94** |

Die letzte Zeile erzielt 3.974 statt 4.066 Gesamtpunkte. Gegenüber der Basis
werden 73 Spieltage besser, 173 gleich und 94 schlechter. Im Mittel werden
0,31 erwartete Punkte für die Risiken aufgegeben.

Die jährliche Regel wählt für 2016–2018 die Basis, für 2019–2021 die
120-Tage-Risikovariante und ab 2022 die 90-Tage-Risikovariante. Den einzigen
24er der 90-Tage-Variante am dritten Spieltag 2021/22 verpasst sie deshalb:
Dieser Erfolg war aus der vorangegangenen Auswahlperiode nicht ableitbar.

Bootstrap mit 10.000 gepaarten Stichproben ganzer Testsaisons:

- Durchschnitt: −0,271 Punkte pro Spieltag, 95-%-Intervall
  **[−0,471; −0,062]**.
- Spieltage ab 22: +0,10 pro Saison, Intervall **[−0,20; +0,40]**.
- Spieltage ab 24: beide Verfahren haben null beobachtete Treffer. Ein dadurch
  rechnerisch degeneriertes Bootstrap-Intervall von null bedeutet nicht, dass
  ihre wahren seltenen Trefferchancen erwiesenermaßen gleich wären.

Ein geringerer Durchschnitt wäre für dein Ziel akzeptabel, wenn ein Vorteil
bei Spitzenspieltagen erkennbar wäre. Diesen liefert die zeitlich getrennte
Auswahl hier nicht überzeugend. Die negative Bewertung beruht deshalb nicht
allein auf den verlorenen Durchschnittspunkten.

## Alle 15 Saisons: 510 Spieltage

| Verfahren | Gesamtpunkte | Punkte/Spieltag | ≥22 | ≥24 |
|---|---:|---:|---:|---:|
| Bisherige Basis | 6.091 | 11,94 | 3 | 0 |
| 7+2, 90 Tage | 5.827 | 11,43 | 4 | 1 |
| 7+2, 120 Tage | 5.812 | 11,40 | 2 | 0 |
| 7+2, 270 Tage | 5.839 | 11,45 | 2 | 0 |
| 7+2, 420 Tage | 5.865 | 11,50 | 3 | 0 |

Die 90-Tage-Variante erzeugt tatsächlich einen zusätzlichen Spitzen-Spieltag.
Am dritten Spieltag 2021/22 wechselt sie Bielefeld–Frankfurt von 0:1 auf 1:1
(tatsächlich 1:1, +4 Punkte) und Wolfsburg–Leipzig von 1:2 auf 2:1
(tatsächlich 1:0, +3 Punkte). Der Zettel steigt von 17 auf 24 Punkte.
Ein einzelner solcher Treffer nach Sichtung vieler Varianten genügt nicht
als Beleg für eine bessere zukünftige Auswahlregel.

## Warum die komplette 270-Tage-Variante weiterhin auffällt

Die folgenden Vergleichsvarianten verwenden das interne Modell ohne Quoten
für alle neun Partien. Sie gehören ausdrücklich **nicht** zum Kandidatenpool
der obigen jährlichen 7+2-Auswahl. Sie wurden bereits im vorherigen Versuch
betrachtet; die Tabelle ist ein deskriptiver Vergleich, kein neuer Blindtest.

2016/17–2025/26, dieselben 340 Spieltage:

| Verfahren | Punkte/Spieltag | ≥22 | ≥24 | Geänderte genaue Tipps/Spieltag |
|---|---:|---:|---:|---:|
| Bisherige Basis | 11,96 | 2 | 0 | 0,00 |
| Komplett 90 Tage, ohne Quoten | 11,66 | 5 | 2 | 3,92 |
| Komplett 120 Tage, ohne Quoten | 11,75 | 5 | 2 | 3,40 |
| **Komplett 270 Tage, ohne Quoten** | **11,67** | **8** | **2** | **1,78** |

Komplett auf ein anderes Modell umzuschalten bedeutet also nicht, dass neun
andere Tipps entstehen. Bei 270 Tagen ändern sich im Schnitt 1,78 genaue
Tipps; auf 251 von 340 Spieltagen sind es höchstens zwei. Auf 89 Spieltagen
sind es drei bis sechs. Von insgesamt 605 Änderungen betreffen 433 eine
andere Tendenz und 172 nur das genaue Ergebnis innerhalb derselben Tendenz.

Die beiden 24+-Spieltage sind 2016/17, Spieltag 5 (24 statt 14 Punkte bei drei
Änderungen) und 2025/26, Spieltag 17 (25 statt 21 bei einer Änderung).
Eine starre Grenze von zwei Änderungen hätte zumindest den ersten Zettel
nicht vollständig nachbilden können. Daraus folgt noch nicht, dass jede
Aufhebung einer Risikogrenze vorteilhaft wäre.

Der deskriptive Unterschied bei ≥22 beträgt +0,6 Spieltage pro Saison;
das Saison-Bootstrap-Intervall liegt bei [+0,2; +1,0]. Es korrigiert nicht
für die bereits erfolgte Sichtung vieler Kandidaten. Bei ≥24 sind es nur
zwei zusätzliche Ereignisse, das Intervall reicht von null bis +0,5 pro
Saison. Reale Preisgewinne bleiben ohne Konkurrenzdaten unbekannt.

## Entscheidung

Die hier getestete 7+2-Regel wird nicht als Live-Standard übernommen. Auch
ihre nach demselben Verfahren aus 2021/22–2025/26 abgeleitete Auswahl für
2026/27, `hybrid_90`, ist im Excel dokumentiert und wird nicht mit einem
nachgewiesenen Vorteil verwechselt.

Für einen künftig vorab festgelegten Vergleich zum Ziel Spitzenspieltag ist
das komplette interne 270-Tage-Modell der interessantere Kandidat. Seine
schlechtere Prognosegüte und etwas niedrigeren Durchschnittspunkte sind
bekannt; der mögliche Nutzen liegt gerade in den beobachteten zusätzlichen
Spitzen. Ein solcher zukünftiger Vergleich sollte mit eingefrorenen Tipps
und ohne laufende nachträgliche Regeländerungen erfolgen. Dieser Versuch
hat keine Live-Tipps veröffentlicht und keine Live-Parameter geändert.

## Umsetzung und Prüfung

- `limited_risk.py`: wiederverwendbare Auswahlfunktion mit festen Grenzen.
- `limited_risk_study.py`: vollständiger Vergleich, Auswahl anhand älterer
  Saisons, Unsicherheitsintervalle und Excel-Ausgabe.
- 47 Tests bestanden, darunter Grenzen für Änderungshäufigkeit, gemeinsames
  Budget, zusätzliche Remis, Außenseiter ohne Modellfavoritenstatus und
  Ausschluss aktueller beziehungsweise zukünftiger Saisons aus der Auswahl.
- Alle 4.590 Basistipps und ihre Punkte stimmen exakt mit dem abgeschlossenen
  Halbwertszeit-Vergleich überein. Alle 2.550 Zettel der Basis und vier
  Risikoregeln bestehen aus neun Spielen; sämtliche Risikogrenzen werden geprüft.
- Die Daten- und Quotenbeschränkungen des
  [Halbwertszeit-Vergleichs](HALBWERTSZEIT_UND_FORM.md) gelten weiter.

Reproduktion aus dem Repository-Hauptordner:

```bash
OPENBLAS_NUM_THREADS=1 VECLIB_MAXIMUM_THREADS=1 OMP_NUM_THREADS=1 \
  .venv/bin/python -m bundesliga.limited_risk_study
```

Ergebnisse und Fingerabdrücke liegen in `data/limited_risk_15_seasons/`:
Einzelspieltipps mit Begründungswerten, Rundenergebnisse, Gesamt- und
Saisontabellen, Auswahlentscheidungen, Bootstrap-Intervalle und Excel.
