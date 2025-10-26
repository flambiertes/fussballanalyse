# -*- coding: utf-8 -*-
"""
Created on Tue Oct 21 10:26:20 2025

@author: Meinert
"""

import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import numpy as np
import time 

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

# -----------------------------
# 1️⃣ Scraper
# -----------------------------
def get_matchday_data(season, matchday):
    """
    Scrapet Spieldaten für einen bestimmten Spieltag einer Saison.
    Implementiert eine Verzögerung, um Timeouts zu vermeiden.
    """
    season_str = f"{season}-{season+1}"
    url = f"https://www.worldfootball.net/schedule/2-bundesliga-{season_str}-spieltag/{matchday}/"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                      " AppleWebKit/537.36 (KHTML, like Gecko)"
                      " Chrome/118.0 Safari/537.36"
    }
    
    # Timeout auf 10 Sekunden gesetzt
    try:
        r = requests.get(url, headers=headers, timeout=10) 
    except requests.exceptions.ReadTimeout as e:
        print(f"❌ Read Timeout beim Abrufen von {url}. Überspringe diesen Spieltag. ({e})")
        # Bei Timeout eine längere Pause machen, bevor die Schleife fortgesetzt wird
        time.sleep(10)
        return pd.DataFrame()
    except requests.exceptions.RequestException as e:
        print(f"❌ Ein allgemeiner Fehler ist beim Abrufen von {url} aufgetreten. ({e})")
        time.sleep(10)
        return pd.DataFrame()

    if r.status_code != 200:
        print(f"⚠️ Fehler beim Abrufen {url} ({r.status_code})")
        return pd.DataFrame()

    soup = BeautifulSoup(r.text, "html.parser")
    # Suche die Tabelle, die die Spieldaten enthält
    table = soup.find("table", class_="standard_tabelle")
    if not table:
        print(f"⚠️ Keine Tabelle gefunden in {url}")
        return pd.DataFrame()

    # !--- KORREKTUR: Alle Zeilen durchgehen, nicht nur ab [1:] ---!
    rows = table.find_all("tr")
    matches = []
    
    for row in rows:
        cols = [c.get_text(strip=True) for c in row.find_all("td")]
        
        # Prüfung auf die minimale Anzahl von Spalten für ein gültiges Spiel
        if len(cols) < 6: 
            continue

        home = cols[2]
        away = cols[4]
        full_result = cols[5] # z.B. "3:1 (1:1)"
        
        # Ergebnis extrahieren (nur der erste Teil vor dem Klammerausdruck)
        result_parts = full_result.split(" ")
        result = result_parts[0] # z.B. "3:1"

        # Prüfung, ob es ein gültiges Ergebnis ist
        if ":" not in result or result.strip() == "":
            continue # Ignoriere Header-Zeilen oder unvollständige Spiele

        try:
            # Tore parsen
            gh, ga = map(int, result.split(":"))
        except ValueError:
            # Falls das Format nicht passt (z.B. Abbruch), überspringen
            continue

        matches.append({
            "season": season,
            "matchday": matchday,
            "home": home,
            "away": away,
            "goals_home": gh,
            "goals_away": ga
        })

    if not matches:
        print(f"⚠️ Keine Spiele gefunden in {url}")
        return pd.DataFrame()

    # Nach jedem Spieltag eine Pause von 1 Sekunde einlegen
    time.sleep(1) 
    
    return pd.DataFrame(matches)


def get_season_data(season):
    frames = []
    # 34 Spieltage in der 2. Bundesliga
    for matchday in range(1, 35): 
        df = get_matchday_data(season, matchday)
        if not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# -----------------------------
# 2️⃣ Tabelle berechnen
# -----------------------------
def compute_table(matches):
    """Erstellt Tabellenstände nach jedem Spieltag."""
    # Verwenden Sie np.sort für alphabetische Sortierung der Teams
    teams = np.sort(list(set(matches["home"]) | set(matches["away"])))
    history = []

    points = {t: 0 for t in teams}
    goals_for = {t: 0 for t in teams}
    goals_against = {t: 0 for t in teams}
    # Neu: Spieleanzahl
    matches_played = {t: 0 for t in teams} 

    for md in range(1, matches["matchday"].max() + 1):
        md_matches = matches[matches["matchday"] == md]
        for _, row in md_matches.iterrows():
            h, a = row["home"], row["away"]
            gh, ga = row["goals_home"], row["goals_away"]
            
            # Update Goals
            goals_for[h] += gh
            goals_against[h] += ga
            goals_for[a] += ga
            goals_against[a] += gh
            
            # Update Matches Played
            matches_played[h] += 1
            matches_played[a] += 1
            
            # Update Points
            if gh > ga:
                points[h] += 3
            elif gh < ga:
                points[a] += 3
            else:
                points[h] += 1
                points[a] += 1

        table = pd.DataFrame({
            "team": teams,
            "points": [points[t] for t in teams],
            "goals_for": [goals_for[t] for t in teams],
            "goals_against": [goals_against[t] for t in teams],
            "matches_played": [matches_played[t] for t in teams] # Hinzugefügt
        })
        table["gd"] = table["goals_for"] - table["goals_against"]
        
        # Filtern Sie nur Teams, die bereits gespielt haben, um eine saubere Tabelle zu erhalten.
        table = table[table["matches_played"] > 0] 

        # Sortieren: 1. Punkte, 2. Tordifferenz, 3. Tore für (absteigend)
        table = table.sort_values(
            by=["points", "gd", "goals_for"], 
            ascending=False
        ).reset_index(drop=True)
        
        table["rank"] = table.index + 1
        table["matchday"] = md
        history.append(table)
        
    return pd.concat(history, ignore_index=True)


# -----------------------------
# 3️⃣ Auswertung
# -----------------------------
import os
import pandas as pd
from tqdm import tqdm

DATA_DIR = "data"  # ggf. anpassen, falls dein Pfad anders heißt

def analyze_progress(all_tables, analyze_md=9):
    """
    Analysiert, wie viele Punkte Teams am Saisonende haben, wenn sie am Spieltag X auf Platz Y standen.
    """
    records = []
    seasons = all_tables["season"].unique()

    for season in seasons:
        t_season = all_tables[all_tables["season"] == season]
        if t_season["matchday"].max() < 34 or t_season["matchday"].max() < analyze_md:
            continue

        md_table = t_season[t_season["matchday"] == analyze_md]
        end_table = t_season[t_season["matchday"] == t_season["matchday"].max()]

        for _, row in md_table.iterrows():
            team = row["team"]
            place_md = row["place"]
            pts_md = row["points"]
            pts_end_data = end_table[end_table["team"] == team]["points"].values
            if len(pts_end_data) == 0:
                continue
            pts_end = pts_end_data[0]

            records.append({
                "season": season,
                "team": team,
                "place_md": place_md,
                "pts_md": pts_md,
                "pts_end": pts_end,
                "delta": pts_end - pts_md
            })

    df = pd.DataFrame(records)

    # Zusammenfassung nach Platz
    summary = df.groupby("place_md")["delta"].agg(
        avg_delta="mean",
        stddev="std",
        median_delta="median",
        best_case="max",
        worst_case="min",
        count="count"
    ).reset_index()

    avg_pts_md = df.groupby("place_md")["pts_md"].mean().reset_index()
    avg_pts_md.rename(columns={"pts_md": "avg_points_md"}, inplace=True)
    summary = summary.merge(avg_pts_md, on="place_md")

    return df, summary


def analyze_progress_by_points(all_tables, analyze_md=9):
    """
    Analysiert, wie viele Punkte Teams am Saisonende haben, wenn sie am Spieltag X eine bestimmte Punktzahl hatten.
    """
    records = []
    seasons = all_tables["season"].unique()

    for season in seasons:
        t_season = all_tables[all_tables["season"] == season]
        if t_season["matchday"].max() < 34 or t_season["matchday"].max() < analyze_md:
            continue

        md_table = t_season[t_season["matchday"] == analyze_md]
        end_table = t_season[t_season["matchday"] == t_season["matchday"].max()]

        for _, row in md_table.iterrows():
            team = row["team"]
            pts_md = row["points"]
            pts_end_data = end_table[end_table["team"] == team]["points"].values
            if len(pts_end_data) == 0:
                continue
            pts_end = pts_end_data[0]

            records.append({
                "season": season,
                "team": team,
                "pts_md": pts_md,
                "pts_end": pts_end,
                "delta": pts_end - pts_md
            })

    df = pd.DataFrame(records)

    # Gruppierung nach Punktzahl am Stichtag
    summary = df.groupby("pts_md")["delta"].agg(
        avg_delta="mean",
        stddev="std",
        median_delta="median",
        best_case="max",
        worst_case="min",
        count="count"
    ).reset_index()

    # Punktzahl selbst als Referenz
    summary["avg_points_md"] = summary["pts_md"]

    return df, summary

def run_full_analysis(all_tables):
    all_matchday_summaries = []
    all_matchday_summaries_points = []

    # Platzbasierte Analyse
    for md in tqdm(range(1, 34), desc="Analysiere Spieltage (platzbasiert)"):
        _, summary = analyze_progress(all_tables, analyze_md=md)
        summary["matchday"] = md
        all_matchday_summaries.append(summary)

    # Punktbasierte Analyse
    for md in tqdm(range(1, 34), desc="Analysiere Spieltage (punktbasiert)"):
        _, summary_points = analyze_progress_by_points(all_tables, analyze_md=md)
        summary_points["matchday"] = md
        all_matchday_summaries_points.append(summary_points)

    # Exportieren der Ergebnisse
    if all_matchday_summaries:
        all_df = pd.concat(all_matchday_summaries, ignore_index=True)
        excel_path_places = os.path.join(DATA_DIR, "2_bundesliga_analyse.xlsx")
        try:
            all_df.to_excel(excel_path_places, sheet_name="Platzentwicklung", float_format="%.2f", index=False)
            print(f"💾 Platzbasierte Analyse erfolgreich in '{excel_path_places}' gespeichert.")
        except Exception as e:
            print(f"❌ Fehler beim Speichern der platzbasierten Excel-Datei: {e}")

    if all_matchday_summaries_points:
        all_points_df = pd.concat(all_matchday_summaries_points, ignore_index=True)
        excel_path_points = os.path.join(DATA_DIR, "2_bundesliga_analyse_punktebasiert.xlsx")
        try:
            all_points_df.to_excel(excel_path_points, sheet_name="Punkteentwicklung", float_format="%.2f", index=False)
            print(f"💾 Punktebasierte Analyse erfolgreich in '{excel_path_points}' gespeichert.")
        except Exception as e:
            print(f"❌ Fehler beim Speichern der punktbasierten Excel-Datei: {e}")


# -------------------------------------------------------
# Hauptanalyse – alle Spieltage und beide Methoden
# -------------------------------------------------------





# -----------------------------
# 4️⃣ Hauptprogramm
# -----------------------------
def load_or_scrape_season(season):
    """Lädt CSV, falls vorhanden – prüft Vollständigkeit – sonst scrapet und speichert sie."""
    path = os.path.join(DATA_DIR, f"2bundesliga_{season}.csv")
    
    # Standard: 34 Spieltage (matchdays) * 9 Spiele/Spieltag (rows) = 306 Zeilen
    REQUIRED_MATCHDAYS = 34
    REQUIRED_ROWS = 306
    
    # 1. Vollständigkeitsprüfung und Laden
    if os.path.exists(path):
        df_loaded = pd.read_csv(path)
        
        is_complete = (df_loaded["matchday"].nunique() == REQUIRED_MATCHDAYS) and \
                      (len(df_loaded) == REQUIRED_ROWS)
        
        if is_complete:
            # print(f"✅ Lade Saison {season} aus Datei...") 
            return df_loaded
        else:
            # Datei ist unvollständig (z.B. falsche Anzahl Spieltage oder Zeilen), lösche sie und erzwinge erneutes Scraping
            print(f"⚠️ Saison {season} (aus Datei) ist UNVOLLSTÄNDIG!")
            print(f"   -> Gefundene Spieltage: {df_loaded['matchday'].nunique()}/{REQUIRED_MATCHDAYS}")
            print(f"   -> Gefundene Spiele (Zeilen): {len(df_loaded)}/{REQUIRED_ROWS}")
            print(f"   -> Lösche unvollständige Datei und scrape erneut...")
            os.remove(path) 
            # Fahren Sie mit dem Scraping-Teil fort
    
    # 2. Scraping Logik
    print(f"⏳ Scraping Saison {season} (Timeout: 10s, Pause: 1s pro Spieltag)...")
    df = get_season_data(season)
    
    # 3. Speichern nur, wenn vollständig
    if not df.empty:
        is_complete_scraped = (df["matchday"].nunique() == REQUIRED_MATCHDAYS) and \
                              (len(df) == REQUIRED_ROWS)
        
        if is_complete_scraped:
            df.to_csv(path, index=False)
            print(f"💾 Saison {season} gespeichert.")
        else:
            # Wird nur gedruckt, wenn Daten da sind, aber unvollständig
            print(f"⚠️ Saison {season} konnte nicht vollständig gescrapt werden ({df['matchday'].nunique()}/{REQUIRED_MATCHDAYS} Spieltage, {len(df)}/{REQUIRED_ROWS} Spiele). Nicht gespeichert.")
            
    return df

# Haupt-Scraping-Loop
all_data = []
# range(1998, 2024) -> Saisons 1998/99 bis 2023/24
for season in tqdm(range(1994, 2025), desc="Loading/scraping seasons"):
    df = load_or_scrape_season(season)
    if not df.empty:
        all_data.append(df)
        
print(f"✅ Erfolgreich geladene Saisons: {len(all_data)}")
if not all_data:
    print("❌ Keine Daten zum Verarbeiten gefunden.")
    # Füge exit() nicht in Canvas Code ein, um die Ausführung nicht zu beenden, falls nötig
    # exit() 

# Spiele zusammenfassen
matches = pd.concat(all_data, ignore_index=True)
print(f"Gesamtspiele: {len(matches)}")

print("📊 Berechne Tabellenstände für jede Saison...")

all_tables = []
for season in tqdm(matches["season"].unique(), desc="Berechne Tabellen"):
    season_matches = matches[matches["season"] == season]
    table_history = compute_table(season_matches)
    table_history["season"] = season
    all_tables.append(table_history)

all_tables = pd.concat(all_tables, ignore_index=True)

# Für spätere Analysen wird "rank" in "place" umbenannt (wie im Analysecode erwartet)
all_tables.rename(columns={"rank": "place"}, inplace=True)

# -------------------------------------------------------
# Analysen durchführen
# -------------------------------------------------------
print("📈 Starte Gesamtanalyse...")
run_full_analysis(all_tables)

