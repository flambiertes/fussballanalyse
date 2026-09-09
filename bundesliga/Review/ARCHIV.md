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
