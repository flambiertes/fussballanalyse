# -*- coding: utf-8 -*-
"""
Created on Mon Oct 27 13:48:05 2025

@author: Meinert
"""

import pandas as pd

# === Einstellungen ===
excel_datei = "daten.xlsx"      # Name deiner Excel-Datei
blattname = 0                   # 0 = erstes Tabellenblatt, oder z. B. "Tabelle1"
ausgabe_datei = "tabelle.txt"   # HTML-Ausgabe als Textdatei

# === Excel laden ===
df = pd.read_excel(excel_datei, sheet_name=blattname, header=None)

# === HTML generieren ===
html = "<table> <tbody>\n"
for _, zeile in df.iterrows():
    html += " <tr>"
    for wert in zeile:
        # leere Zellen -> leere <td>
        inhalt = "" if pd.isna(wert) else str(wert)
        html += f" <td>{inhalt}</td>"
    html += " </tr>\n"
html += " </tbody> </table>"

# === In Datei schreiben ===
with open(ausgabe_datei, "w", encoding="utf-8") as f:
    f.write(html)

print(f"✅ HTML-Tabelle wurde als '{ausgabe_datei}' gespeichert.")