# Versioniertes Ergebnisarchiv

Die in diesem Review besprochenen Ergebnisse und der angenommene Tippzettel
sollen nach einem Checkout verfügbar sein. `data/.gitignore` enthält deshalb
eine ausdrückliche Dateiliste für dieses Archiv. Die
[Entscheidungsübersicht](ENTSCHEIDUNGEN.md) beschreibt den verbindlichen Stand.

## Enthaltene Belege

| Bereich | Dateien unter `data/` | Einordnung |
|---|---|---|
| Spieltag 1 und 2 | `tipps_spieltag_1_2026.xlsx`, `tipps_spieltag_2_2026_risk.xlsx`, `contest_rueckblick_spieltage_1_2_2026.*` | Ursprüngliche Zettel und nachträglicher Strategievergleich. |
| Frühere Konkurrenzversuche | `contest_input_*`, `contest_replay_*`, `seven_plus_two_development.*` | Historische Zwischenstände; die 106 Kalenderfenster sind keine vollständige Spieltagsauswertung. |
| Offizielle Spieltage | `backtest_d1_*_official_frozen.*`, `frozen_predictions_*`, `frozen_round_*`, `modellaudit_offizielle_spieltage.xlsx` | Korrigierte gemeinsame Stichtage; die später entdeckte Quotenabweichung 2018/19 bleibt im Bericht vermerkt. |
| Audit-Ausgangsdaten | `backtest_d1_*_live_config_with_odds.csv`, `official_round_*` | Frühere Prognosen und Diagnose der Rundengruppierung; keine neuen Live-Empfehlungen. |
| Remishäufigkeit | `draw_matchday_frequency_2016_2025.*` | Offizielle Runden 2016/17–2025/26. |
| Halbwertszeit und Form | `half_life_15_seasons/` | Alle Kandidaten- und Saisontabellen, jährliche Auswahl, deren Einzelspielprognosen, Intervalle, Prüfbericht, Manifest und Excel. |
| Begrenztes Risiko | `limited_risk_15_seasons/` | Einzelspieltipps, Rundentabellen, Auswahl, Unsicherheit und Excel des vollständigen 7+2-Versuchs. |
| Angenommener Spieltag 3 | `spieltag_3_2026_empfehlung_20260909T051715Z/` | 270-Tage-Empfehlung, parallel eingefrorene bisherige Methode, Konfigurationen, Run-IDs, Quotenalter und Excel. |
| Angenommener Spieltag 5 | `spieltag_5_2026_empfehlung_20261009T065733Z/` | Unverändert abgegebene 270-Tage-Empfehlung und eingefrorene 420-Tage-Quotenvariante. |
| Auswertung Spieltag 5 | `spieltag_5_2026_auswertung_20261012/` | Endergebnisse, Einzelpunkte aller drei archivierten Varianten, CSV, Excel und Prüfmetadaten; [Bericht](Spieltag%205/AUSWERTUNG.md). |
| Angenommener Spieltag 6 | `spieltag_6_2026_empfehlung_20261016T090611Z/` | Am 16.10. unverändert angenommene 270-Tage-Empfehlung, 420-Tage-Vergleich, Änderungen zum Vorabzettel, Quotenalter und Herkunftsmetadaten. |
| Auswertung Spieltag 6 | `spieltag_6_2026_auswertung_20261023/` | 20 statt 17 Punkte, Endergebnisse, Einzelpunkte, Eingabeprüfsummen und Excel; [Bericht](Spieltag%206/AUSWERTUNG.md). |
| Angenommener Spieltag 7 | `spieltag_7_2026_empfehlung_20261023T135516Z/` | Am 23.10. angenommen und laut Nutzer in der App eingetragen; 270-Tage-Empfehlung, 420-Tage-Vergleich, Konfigurationen, Quotenalter und Herkunft. |
| Auswertung Spieltag 7 | `spieltag_7_2026_auswertung_20261030/` | 13 statt 11 Punkte, Endergebnisse, Einzelpunkte, Eingabeprüfsummen und Excel; [Bericht](Spieltag%207/AUSWERTUNG.md). |
| Check vor Spieltag 8 | `spieltag_8_2026_empfehlung_20261030T112635Z/` | Alle neun Vorabtipps unverändert bestätigt; neue 270- und 420-Tage-Läufe, Vergleich, unabhängige Erwartungswertprüfung, Quotenalter und Excel; [Bericht](Spieltag%208/EMPFEHLUNG.md). |
| Auswertung Spieltag 8 | `spieltag_8_2026_auswertung_20261106/` | 8 statt 8 Punkte, Endergebnisse, Einzelpunkte, Eingabeprüfsummen und Excel; [Bericht](Spieltag%208/AUSWERTUNG.md). |
| Angenommener Spieltag 9 | `spieltag_9_2026_empfehlung_20261106T115233Z/` | Am 06.11. angenommen und laut Nutzer in der App gespeichert. Eine Änderung zum Vorabzettel: Paderborn–Frankfurt 0:1 statt 1:2; neue 270- und 420-Tage-Läufe, Vergleich, Erwartungswertprüfung, Quotenalter und Excel; [Bericht](Spieltag%209/EMPFEHLUNG.md). |
| Vorläufige Spieltage 8 und 9 | `spieltage_8_9_2026_vorlaeufig_20261023T140335Z/` | 18 Rückfalltipps, laut Nutzer am 23.10. bereits in der App eingetragen, zur späteren Aktualisierung; Excel, CSV und vollständige Metadaten; [Zettel](Spieltage%208%20und%209/VORLAEUFIG.md). |
| Auswertung Spieltag 9 | `spieltag_9_2026_auswertung_20261113/` | 22 statt 22 Punkte, Endergebnisse, unabhängig geprüfte Einzelpunkte, Eingabeprüfsummen und Excel; [Bericht](Spieltag%209/AUSWERTUNG.md). |
| Vorläufiger Spieltag 10, eingetragen | `spieltag_10_2026_vorlaeufig_20261113T092950Z/` | Laut Nutzer am 13.11. in der App eingetragen. Nach frischen Importen und Quoten berechnet; 270- und 420-Tage-Läufe, Konfigurationen, unabhängige Erwartungswertprüfung, Quotenalter und Excel. Vor dem Auftakt am 20.11. erneut prüfen; [Zettel](Spieltag%2010/EMPFEHLUNG.md). |
| Freitagscheck Spieltag 10 | `spieltag_10_2026_freitagscheck_20261120T190321Z/` | Am 20.11. alle neun Originaltipps bestätigt; neue 270- und 420-Tage-Läufe, Originalvergleich, unabhängige Prüfung, Quotenalter, Excel und Herkunft. Vom Nutzer am 20.11. angenommen und zur Versionierung freigegeben; [Bericht](Spieltag%2010/FREITAGSCHECK.md). |
| Vorläufige Spieltage 11 und 12, eingetragen | `spieltage_11_12_2026_vorlaeufig_20261113T095253Z/` | 18 Tipps, laut Nutzer am 13.11. in der App eingetragen; gleicher Informationsstand und gleiche 270-Tage-Regel wie Spieltag 10. Zwei Excel-Blätter, CSV, Konfiguration und unabhängige Erwartungswertprüfung. Originale als Referenz für spätere Änderungen erhalten; [Zettel](Spieltage%2011%20und%2012/VORLAEUFIG.md). |
| Audit-Helfer | Die fünf ausgewählten `data/*.py` | Prüf- und Rekonstruktionsscripte der vorherigen Reviews. |

Die [vollständige Dateiliste mit SHA-256-Prüfsummen](ARCHIV_MANIFEST.json)
kennzeichnet die exakt archivierten Stände. Screenshots und die Auswertung
des Nutzers liegen zusätzlich in `Review/Spieltag 1/` und `Review/Spieltag 2/`.

## Was weiterhin lokal bleibt

Die SQLite-Datenbank, Zugangsdaten, Rohantworten der Datenanbieter,
Pickle-Eingabecaches sowie die großen `base_*`- und `metrics_*`-Zwischendateien
der Halbwertszeitstudie werden nicht hinzugefügt. Sie sind zum Lesen der
archivierten Tabellen nicht erforderlich.

Für einen vollständigen neuen Modelllauf müssen die benötigten historischen
Ergebnisse, Spieltagsnummern, Marktwerte und Quoten zuerst aufgebaut werden;
die Befehle stehen in der README. Für einen geänderten Daten- oder Codestand
ein **neues `--output`-Verzeichnis** wählen. Die Fingerabdrücke der eingefrorenen
Versuche dürfen nicht einfach an neue Daten angepasst werden.

Die Audit-Helfer benötigen teilweise die lokalen Rohdaten und Modell-Caches.
Beispielsweise erfordert `data/audit_half_life_outputs.py` die vorher berechneten
Basisschätzungen und Kandidatenmetriken. Seine bereits abgeschlossenen
Prüfergebnisse sind in `half_life_15_seasons/independent_audit.json` enthalten.
Die Scripte `audit_official_rounds.py`, `repair_official_backtests.py`,
`audit_frozen_models.py` und `check_seven_plus_two.py` werden aus dem
Paketordner `bundesliga/` mit `PYTHONPATH=..` aufgerufen. Neue Ergebnisse nicht
unbemerkt über das versionierte historische Archiv schreiben.

Das Archiv garantiert, welche Tipps und Kennzahlen damals vorlagen. Ein neuer
Abruf externer Daten kann wegen Korrekturen und veränderter Verfügbarkeit einen
anderen Fingerabdruck erzeugen; das ist kein Grund, alte Ergebnisse zu ersetzen.
