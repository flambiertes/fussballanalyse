"""
WM-2026-Simulation v2
Zwei Modi:
  1. Gruppenphase: 1000 Simulationen je Gruppe -> Platzierungswahrscheinlichkeiten
  2. KO-Phase: 100 Simulationen je Runde -> Weiterkommen-Wahrscheinlichkeiten

Excel-Ausgabe:
  - Blatt "Uebersicht"   : Gesamtwahrscheinlichkeiten aller Teams
  - Blatt "Gruppe A-L"   : Tabelle + Spielergebnisse je Gruppe
  - Blatt "Turnierbaum"  : Visueller Bracket (links R32, rechts Finale)

Bereits gespielte Partien (fett) werden aus den CSV-Dateien gelesen.
Simulierte Partien erscheinen kursiv.

Ausfuehren:
  python wm_simulation.py
"""

import numpy as np
import pandas as pd
from scipy.stats import poisson
from pathlib import Path
from collections import defaultdict
from tqdm.auto import tqdm
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side, GradientFill
from openpyxl.utils import get_column_letter

DATA_DIR = Path(__file__).resolve().parent / "data"

N_GROUP_SIM = 1000
N_KO_SIM    = 200   # pro Runde

MATCH_WEIGHTS = {
    "FIFA World Cup": 4.0,
    "UEFA Euro": 3.0,
    "Copa America": 3.0,
    "Africa Cup of Nations": 2.5,
    "AFC Asian Cup": 2.5,
    "CONCACAF Gold Cup": 2.5,
    "UEFA Nations League": 2.0,
    "FIFA World Cup qualification": 2.0,
    "Friendly": 0.5,
}

# ---------------------------------------------------------------------------
# Turnierbaum-Layout: match_nr -> (row_team1, row_team2, col_team, col_score)
# Spalten: R32=1, R16=4, QF=7, SF=10, Final=13 (1-basiert)
# ---------------------------------------------------------------------------
BRACKET = {
    # R32 (col 1/2)
    73: (2,  3,  1, 2),
    75: (6,  7,  1, 2),
    74: (10, 11, 1, 2),
    77: (14, 15, 1, 2),
    83: (18, 19, 1, 2),
    84: (22, 23, 1, 2),
    81: (26, 27, 1, 2),
    82: (30, 31, 1, 2),
    76: (34, 35, 1, 2),
    78: (38, 39, 1, 2),
    79: (42, 43, 1, 2),
    80: (46, 47, 1, 2),
    86: (50, 51, 1, 2),
    88: (54, 55, 1, 2),
    85: (58, 59, 1, 2),
    87: (62, 63, 1, 2),
    # R16 (col 4/5)
    89: (4,  5,  4, 5),
    90: (12, 13, 4, 5),
    93: (20, 21, 4, 5),
    94: (28, 29, 4, 5),
    91: (36, 37, 4, 5),
    92: (44, 45, 4, 5),
    95: (52, 53, 4, 5),
    96: (60, 61, 4, 5),
    # QF (col 7/8)
    97:  (8,  9,  7, 8),
    98:  (24, 25, 7, 8),
    99:  (40, 41, 7, 8),
    100: (56, 57, 7, 8),
    # SF (col 10/11)
    101: (16, 17, 10, 11),
    102: (48, 49, 10, 11),
    # Final (col 13/14)
    104: (32, 33, 13, 14),
    103: (37, 38, 13, 14),  # Spiel um Platz 3
}

# Welche R32-Matches in welche R16-Matches muenden
R32_TO_R16 = {
    73: 89, 75: 89,
    74: 90, 77: 90,
    83: 93, 84: 93,
    81: 94, 82: 94,
    76: 91, 78: 91,
    79: 92, 80: 92,
    86: 95, 88: 95,
    85: 96, 87: 96,
}
R16_TO_QF  = {89: 97, 90: 97, 93: 98, 94: 98, 91: 99, 92: 99, 95: 100, 96: 100}
QF_TO_SF   = {97: 101, 98: 101, 99: 102, 100: 102}
SF_TO_FINAL= {101: 104, 102: 104}


# ---------------------------------------------------------------------------
# Datenladen
# ---------------------------------------------------------------------------
def load_inputs():
    strengths_path = DATA_DIR / "team_strengths.csv"
    groups_path    = DATA_DIR / "wm2026_groups.csv"
    if not strengths_path.exists():
        raise FileNotFoundError("team_strengths.csv fehlt -> strength_model.py ausfuehren")
    if not groups_path.exists():
        raise FileNotFoundError("wm2026_groups.csv fehlt")
    strengths = pd.read_csv(strengths_path).set_index("team")
    groups_df = pd.read_csv(groups_path)
    groups    = {g: list(v["team"]) for g, v in groups_df.groupby("group")}
    return strengths, groups


def load_actual_group_results():
    """Liest bereits gespielte Gruppenspiele (goals_home/goals_away nicht NaN)."""
    path = DATA_DIR / "wm2026_matches_group.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["date"])
    if "goals_home" not in df.columns:
        return pd.DataFrame()
    played = df.dropna(subset=["goals_home", "goals_away"]).copy()
    played["goals_home"] = played["goals_home"].astype(int)
    played["goals_away"] = played["goals_away"].astype(int)
    return played


def load_actual_ko_results():
    """Liest bereits gespielte KO-Spiele."""
    path = DATA_DIR / "wm2026_matches_knockout.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, parse_dates=["date"])
    if "goals_home" not in df.columns:
        return pd.DataFrame()
    played = df.dropna(subset=["goals_home", "goals_away"]).copy()
    return played


# ---------------------------------------------------------------------------
# Spielsimulation
# ---------------------------------------------------------------------------
def expected_goals(team_a, team_b, strengths, neutral=True):
    def get(t, col, default):
        return strengths.at[t, col] if (t in strengths.index and col in strengths.columns) else default

    if "att" in strengths.columns and "def" in strengths.columns:
        h_adv = 0.0 if neutral else get(team_a, "home_adv", 0.25)
        lam_a = np.exp(np.clip(get(team_a,"att",0) + get(team_b,"def",0) + h_adv, -6, 6))
        lam_b = np.exp(np.clip(get(team_b,"att",0) + get(team_a,"def",0),          -6, 6))
    else:
        s_a = get(team_a, "strength_combined", 0.5)
        s_b = get(team_b, "strength_combined", 0.5)
        share = s_a / (s_a + s_b + 1e-9)
        lam_a = 2.75 * share
        lam_b = 2.75 * (1 - share)
    return max(lam_a, 0.05), max(lam_b, 0.05)


def sim_match(lam_a, lam_b):
    return int(poisson.rvs(lam_a)), int(poisson.rvs(lam_b))


def sim_ko_match(team_a, team_b, strengths):
    """90 Min + ggf. Verlaengerung + Elfmeter."""
    lam_a, lam_b = expected_goals(team_a, team_b, strengths)
    g_a, g_b = sim_match(lam_a, lam_b)
    if g_a != g_b:
        return team_a if g_a > g_b else team_b, g_a, g_b, ""
    # Verlaengerung
    et_a, et_b = sim_match(lam_a / 3, lam_b / 3)
    g_a += et_a; g_b += et_b
    if g_a != g_b:
        return team_a if g_a > g_b else team_b, g_a, g_b, "n.V."
    # Elfmeter
    def get(t, col, d):
        return strengths.at[t, col] if t in strengths.index and col in strengths.columns else d
    s_a = get(team_a, "strength_combined", 0.5)
    s_b = get(team_b, "strength_combined", 0.5)
    p_a = np.clip(s_a / (s_a + s_b + 1e-9), 0.40, 0.60)
    winner = team_a if np.random.random() < p_a else team_b
    return winner, g_a, g_b, "n.E."


# ---------------------------------------------------------------------------
# Gruppenphase
# ---------------------------------------------------------------------------
def simulate_group_once(teams, strengths, fixed_matches):
    """Simuliert eine Gruppe einmal. fixed_matches: dict (home,away)->(gh,ga)."""
    records = {t: {"pts": 0, "gf": 0, "ga": 0} for t in teams}
    match_scores = {}

    for i, t_a in enumerate(teams):
        for t_b in teams[i+1:]:
            key = (t_a, t_b)
            key_rev = (t_b, t_a)
            if key in fixed_matches:
                g_a, g_b = fixed_matches[key]
            elif key_rev in fixed_matches:
                g_b, g_a = fixed_matches[key_rev]
            else:
                lam_a, lam_b = expected_goals(t_a, t_b, strengths)
                g_a, g_b = sim_match(lam_a, lam_b)

            match_scores[key] = (g_a, g_b)
            records[t_a]["gf"] += g_a; records[t_a]["ga"] += g_b
            records[t_b]["gf"] += g_b; records[t_b]["ga"] += g_a
            if g_a > g_b:   records[t_a]["pts"] += 3
            elif g_a == g_b: records[t_a]["pts"] += 1; records[t_b]["pts"] += 1
            else:            records[t_b]["pts"] += 3

    table = pd.DataFrame.from_dict(records, orient="index")
    table["gd"] = table["gf"] - table["ga"]
    table = table.sort_values(["pts","gd","gf"], ascending=False)
    return list(table.index), match_scores, records  # records fuer Tabellen-Stats


def simulate_group_n(group_name, teams, strengths, actual_played, n=N_GROUP_SIM):
    """n Simulationen einer Gruppe. Gibt Positions-Wahrscheinlichkeiten + Match-Stats."""
    fixed = {}
    for _, row in actual_played.iterrows():
        if row["team_home"] in teams and row["team_away"] in teams:
            fixed[(row["team_home"], row["team_away"])] = (int(row["goals_home"]), int(row["goals_away"]))

    pos_counts  = {t: [0,0,0,0] for t in teams}
    table_stats = {t: {"pts": [], "gf": [], "ga": []} for t in teams}
    match_stats = defaultdict(lambda: {"goals_h": [], "goals_a": [], "wins_h": 0, "draws": 0, "wins_a": 0, "score_counts": defaultdict(int)})

    for _ in range(n):
        ranking, scores, records = simulate_group_once(teams, strengths, fixed)
        for t in teams:
            table_stats[t]["pts"].append(records[t]["pts"])
            table_stats[t]["gf"].append(records[t]["gf"])
            table_stats[t]["ga"].append(records[t]["ga"])
        for pos, team in enumerate(ranking):
            pos_counts[team][pos] += 1
        for (h, a), (gh, ga) in scores.items():
            match_stats[(h,a)]["goals_h"].append(gh)
            match_stats[(h,a)]["goals_a"].append(ga)
            match_stats[(h,a)]["score_counts"][(gh, ga)] += 1
            if gh > ga:   match_stats[(h,a)]["wins_h"] += 1
            elif gh == ga: match_stats[(h,a)]["draws"] += 1
            else:          match_stats[(h,a)]["wins_a"] += 1

    pos_probs = {t: [c/n for c in pos_counts[t]] for t in teams}
    avg_scores = {}
    for (h,a), s in match_stats.items():
        avg_scores[(h,a)] = {
            "avg_h":   round(np.mean(s["goals_h"]), 1),
            "avg_a":   round(np.mean(s["goals_a"]), 1),
            "p_win_h":     round(s["wins_h"] / n, 3),
            "p_draw":      round(s["draws"]  / n, 3),
            "p_win_a":     round(s["wins_a"] / n, 3),
            "likely_score": max(s["score_counts"], key=s["score_counts"].get) if s["score_counts"] else (1, 1),
            "fixed":       (h, a) in fixed or (a, h) in fixed,
            "fixed_gh": fixed.get((h,a), fixed.get((a,h), (None,None)))[0] if (h,a) in fixed or (a,h) in fixed else None,
            "fixed_ga": fixed.get((h,a), fixed.get((a,h), (None,None)))[1] if (h,a) in fixed or (a,h) in fixed else None,
        }
        if (a, h) in fixed:
            avg_scores[(h,a)]["fixed_gh"], avg_scores[(h,a)]["fixed_ga"] = fixed[(a,h)][1], fixed[(a,h)][0]

    # Durchschnitts-Tabelle: sortiert nach avg_pts, avg_gd, avg_gf
    avg_table = []
    for t in teams:
        avg_pts = np.mean(table_stats[t]["pts"])
        avg_gf  = np.mean(table_stats[t]["gf"])
        avg_ga  = np.mean(table_stats[t]["ga"])
        avg_table.append({
            "team": t, "avg_pts": round(avg_pts, 1),
            "avg_gf": round(avg_gf, 1), "avg_ga": round(avg_ga, 1),
            "avg_gd": round(avg_gf - avg_ga, 1),
        })
    avg_table.sort(key=lambda x: (-x["avg_pts"], -x["avg_gd"], -x["avg_gf"]))

    return pos_probs, avg_scores, fixed, avg_table


# ---------------------------------------------------------------------------
# KO-Phase
# ---------------------------------------------------------------------------
def get_qualifiers_from_groups(group_pos_probs, groups):
    """
    Zieht Qualifier per Wahrscheinlichkeit fuer eine KO-Simulation.
    Gibt dict {Beschreibung -> team} zurueck.
    """
    qualifiers = {}
    thirds = []

    for grp, teams in groups.items():
        probs = group_pos_probs[grp]
        for pos, label in enumerate(["Winner", "Runner-up", "3rd"]):
            candidates = list(teams)
            weights = [probs[t][pos] for t in candidates]
            total = sum(weights)
            if total < 1e-9:
                chosen = candidates[0]
            else:
                chosen = np.random.choice(candidates, p=[w/total for w in weights])
            qualifiers[f"{label} Group {grp}"] = chosen
            if label == "3rd":
                pts_probs = {t: probs[t][0]*3 + probs[t][1]*2 + probs[t][2]*1 for t in candidates}
                thirds.append((chosen, pts_probs[chosen], grp))

    # 8 beste Dritte (vereinfacht: nach erwarteten Punkten)
    thirds.sort(key=lambda x: x[1], reverse=True)
    best8 = [t[0] for t in thirds[:8]]
    # Zufaellige Zuweisung zu den 8 "3rd"-Slots im Bracket
    slots = [
        "3rd Group A/B/C/D/F", "3rd Group C/D/F/G/H", "3rd Group C/E/F/H/I",
        "3rd Group E/H/I/J/K", "3rd Group B/E/F/I/J", "3rd Group A/E/H/I/J",
        "3rd Group E/F/G/I/J", "3rd Group D/E/I/J/L",
    ]
    np.random.shuffle(best8)
    for slot, team in zip(slots, best8):
        qualifiers[slot] = team

    return qualifiers


def resolve_team(desc, qualifiers):
    """Loest 'Winner Group A' etc. in Teamnamen auf."""
    return qualifiers.get(desc, desc)


def simulate_ko_bracket_once(ko_matches_df, qualifiers, strengths, actual_ko):
    """Simuliert ein komplettes KO-Bracket. Gibt {match_nr: (winner, ga, gb, extra)} zurueck."""
    results  = {}  # match_nr -> winner
    scores   = {}  # match_nr -> (team_a, team_b, goals_a, goals_b, extra)

    ko_df = ko_matches_df.copy()

    def get_team(desc, match_nr):
        # Aufgeloest aus vorherigen Runden?
        if desc.startswith("Winner Match "):
            prev = int(desc.split()[-1])
            return results.get(prev, desc)
        return resolve_team(desc, qualifiers)

    for _, row in ko_df.sort_values("match_nr").iterrows():
        mnr  = int(row["match_nr"])
        desc_a = row["team_home_desc"]
        desc_b = row["team_away_desc"]
        team_a = get_team(desc_a, mnr)
        team_b = get_team(desc_b, mnr)

        # Bereits gespielt?
        if mnr in actual_ko:
            ar = actual_ko[mnr]
            winner = ar["winner"]
            results[mnr] = winner
            scores[mnr] = (ar["team_a"], ar["team_b"], ar["goals_a"], ar["goals_b"], ar["extra"])
            continue

        if "Group" in team_a or "Group" in team_b or "Match" in team_a or "Match" in team_b:
            # Noch nicht aufloesbar (Gruppe nicht fertig simuliert)
            results[mnr] = team_a
            scores[mnr] = (team_a, team_b, 0, 0, "?")
            continue

        winner, ga, gb, extra = sim_ko_match(team_a, team_b, strengths)
        results[mnr] = winner
        scores[mnr]  = (team_a, team_b, ga, gb, extra)

    return results, scores


def simulate_ko_n(ko_matches_df, group_pos_probs, groups, strengths, actual_ko, n=N_KO_SIM):
    """
    N Simulationen des KO-Brackets.
    Gibt (ko_results, team_stage_counts) zurueck.
    team_stage_counts: {team: {stage: count}} fuer direkte Wahrscheinlichkeitsberechnung.
    """
    match_stats = defaultdict(lambda: {
        "team_a_counts": defaultdict(int),
        "team_b_counts": defaultdict(int),
        "winner_counts": defaultdict(int),
        "goals_a": [], "goals_b": [],
        "score_counts":        defaultdict(int),  # alle Ergebnisse
        "score_counts_win_a":  defaultdict(int),  # nur wenn team_a gewinnt
        "score_counts_win_b":  defaultdict(int),  # nur wenn team_b gewinnt
    })

    # Stages: Sieger eines Matches erreicht die naechste Runde
    stage_of_match = {}
    for mnr in range(73, 89):  stage_of_match[mnr] = "p_R16"
    for mnr in range(89, 97):  stage_of_match[mnr] = "p_QF"
    for mnr in range(97, 101): stage_of_match[mnr] = "p_SF"
    for mnr in [101, 102]:     stage_of_match[mnr] = "p_F"
    stage_of_match[104] = "p_Winner"

    team_stage_counts = defaultdict(lambda: defaultdict(int))

    for _ in range(n):
        qualifiers = get_qualifiers_from_groups(group_pos_probs, groups)
        results, scores = simulate_ko_bracket_once(ko_matches_df, qualifiers, strengths, actual_ko)

        for mnr, winner in results.items():
            stage = stage_of_match.get(mnr)
            if stage and winner and "Group" not in winner and "Match" not in winner:
                team_stage_counts[winner][stage] += 1

        for mnr, (ta, tb, ga, gb, extra) in scores.items():
            match_stats[mnr]["team_a_counts"][ta] += 1
            match_stats[mnr]["team_b_counts"][tb] += 1
            if results.get(mnr):
                match_stats[mnr]["winner_counts"][results[mnr]] += 1
            match_stats[mnr]["goals_a"].append(ga)
            match_stats[mnr]["goals_b"].append(gb)
            match_stats[mnr]["score_counts"][(ga, gb)] += 1
            winner = results.get(mnr)
            if winner == ta:
                match_stats[mnr]["score_counts_win_a"][(ga, gb)] += 1
            elif winner == tb:
                match_stats[mnr]["score_counts_win_b"][(ga, gb)] += 1

    # Komprimieren zu ko_results
    ko_results = {}
    for mnr, s in match_stats.items():
        if not s["team_a_counts"]:
            continue
        best_a = max(s["team_a_counts"], key=s["team_a_counts"].get)
        best_b = max(s["team_b_counts"], key=s["team_b_counts"].get) if s["team_b_counts"] else best_a
        total  = sum(s["winner_counts"].values())
        p_a    = s["winner_counts"].get(best_a, 0) / total if total else 0.5
        ko_results[mnr] = {
            "team_a":   best_a,
            "team_b":   best_b,
            "p_a_wins": round(p_a, 3),
            "avg_ga":   round(np.mean(s["goals_a"]), 1) if s["goals_a"] else 0,
            "avg_gb":   round(np.mean(s["goals_b"]), 1) if s["goals_b"] else 0,
            "likely_score": (
                max(s["score_counts_win_a"], key=s["score_counts_win_a"].get)
                if p_a >= 0.5 and s["score_counts_win_a"]
                else max(s["score_counts_win_b"], key=s["score_counts_win_b"].get)
                if p_a < 0.5 and s["score_counts_win_b"]
                else max(s["score_counts"], key=s["score_counts"].get)
                if s["score_counts"] else (1, 0)
            ),
            "is_fixed": mnr in actual_ko,
        }
        if mnr in actual_ko:
            ar = actual_ko[mnr]
            ko_results[mnr].update({
                "team_a":      ar["team_a"],
                "team_b":      ar["team_b"],
                "fixed_ga":    ar["goals_a"],
                "fixed_gb":    ar["goals_b"],
                "fixed_extra": ar["extra"],
            })

    return ko_results, team_stage_counts


# ---------------------------------------------------------------------------
# Gesamtwahrscheinlichkeiten aller Teams
# ---------------------------------------------------------------------------
def compute_overall_probs(group_pos_probs, team_stage_counts, groups, n_ko):
    """
    Direkte Zaehlung: wie oft hat jedes Team jede Runde erreicht?
    Korrekt und einfach — keine Wahrscheinlichkeitsmultiplikation noetig.
    """
    all_teams = [t for g in groups.values() for t in g]
    rows = []

    for t in all_teams:
        # R32-Qualifikation aus Gruppenphase
        p_r32 = 0.0
        for grp, teams in groups.items():
            if t in teams:
                pp = group_pos_probs[grp]
                p_r32 = pp[t][0] + pp[t][1] + pp[t][2] * 0.67
                break

        sc = team_stage_counts[t]
        rows.append({
            "team":     t,
            "p_R32":    round(p_r32, 3),
            "p_R16":    round(sc["p_R16"]    / n_ko, 3),
            "p_QF":     round(sc["p_QF"]     / n_ko, 3),
            "p_SF":     round(sc["p_SF"]     / n_ko, 3),
            "p_F":      round(sc["p_F"]      / n_ko, 3),
            "p_Winner": round(sc["p_Winner"] / n_ko, 3),
        })

    df = pd.DataFrame(rows).sort_values("p_Winner", ascending=False).reset_index(drop=True)
    df.insert(0, "rank", df.index + 1)
    return df


# ---------------------------------------------------------------------------
# Excel-Ausgabe
# ---------------------------------------------------------------------------
FONT_BOLD   = Font(bold=True)
FONT_ITALIC = Font(italic=True)
FONT_FIXED  = Font(bold=True, color="1F4E79")
FILL_FIXED  = PatternFill("solid", fgColor="D6E4F0")
FILL_HEADER = PatternFill("solid", fgColor="2E75B6")
FONT_HEADER = Font(bold=True, color="FFFFFF")
FILL_GROUP  = PatternFill("solid", fgColor="EBF3FB")
FILL_WINNER = PatternFill("solid", fgColor="C6EFCE")
FILL_LOSER  = PatternFill("solid", fgColor="FFCCCC")
FILL_DRAW   = PatternFill("solid", fgColor="FFEB9C")
ALIGN_CENTER= Alignment(horizontal="center", vertical="center")
ALIGN_LEFT  = Alignment(horizontal="left",   vertical="center")

def col_width(ws, col, width):
    ws.column_dimensions[get_column_letter(col)].width = width

def hdr_cell(ws, row, col, value, fill=None):
    c = ws.cell(row=row, column=col, value=value)
    c.font = FONT_HEADER
    c.fill = fill or FILL_HEADER
    c.alignment = ALIGN_CENTER
    return c

def pct(v):
    return f"{v*100:.1f}%"


def write_overview_sheet(wb, overview_df):
    ws = wb.create_sheet("Uebersicht")
    ws.freeze_panes = "A2"
    headers = ["#", "Team", "R32", "R16", "VF", "HF", "Finale", "Weltmeister"]
    cols    = ["rank", "team", "p_R32", "p_R16", "p_QF", "p_SF", "p_F", "p_Winner"]
    for ci, h in enumerate(headers, 1):
        hdr_cell(ws, 1, ci, h)

    for ri, row in overview_df.iterrows():
        r = ri + 2
        ws.cell(r, 1, int(row["rank"])).alignment = ALIGN_CENTER
        ws.cell(r, 2, row["team"]).alignment = ALIGN_LEFT
        for ci, col in enumerate(cols[2:], 3):
            c = ws.cell(r, ci, pct(row[col]))
            c.alignment = ALIGN_CENTER
            if col == "p_Winner" and row[col] >= 0.10:
                c.fill = FILL_WINNER
            elif col == "p_Winner" and row[col] >= 0.03:
                c.fill = FILL_GROUP

    widths = [5, 22, 8, 8, 8, 8, 8, 12]
    for i, w in enumerate(widths, 1):
        col_width(ws, i, w)


def score_from_avg(avg_h, avg_a, p_win_h, p_win_a):
    """
    Rundet Ø-Tore zu ganzen Zahlen.
    Stellt sicher, dass das Ergebnis den wahrscheinlichsten Ausgang widerspiegelt:
    - Klarer Favorit (>55%): Mindestsieg sicherstellen
    - Sonst: einfach runden (Unentschieden moeglich)
    """
    gh = round(avg_h)
    ga = round(avg_a)
    if p_win_h > 0.55 and gh <= ga:
        gh = ga + 1
    elif p_win_a > 0.55 and ga <= gh:
        ga = gh + 1
    return gh, ga


def build_expected_table(teams, avg_scores, fixed_matches):
    """Baut eine konkrete Tabelle aus gerundeten Ø-Ergebnissen (W/U/N/Pts)."""
    records = {t: {"sp": 0, "w": 0, "d": 0, "l": 0, "gf": 0, "ga": 0, "pts": 0} for t in teams}

    for (h, a), s in avg_scores.items():
        if h not in teams or a not in teams:
            continue
        if s.get("fixed"):
            gh = s.get("fixed_gh")
            ga = s.get("fixed_ga")
            if gh is None: gh = round(s["avg_h"])
            if ga is None: ga = round(s["avg_a"])
        else:
            gh, ga = score_from_avg(s["avg_h"], s["avg_a"], s["p_win_h"], s["p_win_a"])

        records[h]["sp"] += 1; records[a]["sp"] += 1
        records[h]["gf"] += gh; records[h]["ga"] += ga
        records[a]["gf"] += ga; records[a]["ga"] += gh
        if gh > ga:
            records[h]["w"] += 1; records[h]["pts"] += 3; records[a]["l"] += 1
        elif gh == ga:
            records[h]["d"] += 1; records[h]["pts"] += 1
            records[a]["d"] += 1; records[a]["pts"] += 1
        else:
            records[a]["w"] += 1; records[a]["pts"] += 3; records[h]["l"] += 1

    table = []
    for t, r in records.items():
        table.append({**r, "team": t, "gd": r["gf"] - r["ga"]})
    table.sort(key=lambda x: (-x["pts"], -x["gd"], -x["gf"]))
    return table


def write_group_sheet(wb, grp, teams, pos_probs, avg_scores, fixed_matches, avg_table):
    ws = wb.create_sheet(f"Gruppe {grp}")
    ws.cell(1, 1, f"Gruppe {grp}").font = Font(bold=True, size=12)

    # --- Tabelle 1: Erwartete Tabelle (aus gerundeten Ø-Ergebnissen) ---
    ws.cell(2, 1, "Erwartete Tabelle").font = Font(bold=True, size=11)
    t1h = ["Pl.", "Team", "Sp", "S", "U", "N", "Tore", "TD", "Pkt"]
    for ci, h in enumerate(t1h, 1):
        hdr_cell(ws, 3, ci, h)

    exp_table = build_expected_table(teams, avg_scores, fixed_matches)
    fills_t1 = [FILL_WINNER, FILL_GROUP, None, None]
    for ri, row in enumerate(exp_table, 4):
        tore_str = f"{row['gf']} : {row['ga']}"
        gd_str   = f"+{row['gd']}" if row["gd"] > 0 else str(row["gd"])
        vals = [ri - 3, row["team"], row["sp"], row["w"], row["d"], row["l"], tore_str, gd_str, row["pts"]]
        for ci, val in enumerate(vals, 1):
            c = ws.cell(ri, ci, val)
            c.alignment = ALIGN_LEFT if ci == 2 else ALIGN_CENTER
        fill = fills_t1[ri - 4]
        if fill:
            for ci in range(1, 10):
                ws.cell(ri, ci).fill = fill

    # --- Tabelle 2: Platzierungswahrscheinlichkeiten ---
    ws.cell(10, 1, "Platzierungswahrscheinlichkeiten").font = Font(bold=True, size=11)
    headers_t = ["Team", "1. Platz", "2. Platz", "3. Platz", "4. Platz"]
    for ci, h in enumerate(headers_t, 1):
        hdr_cell(ws, 11, ci, h)

    sorted_teams = sorted(teams, key=lambda t: -pos_probs[t][0])
    for ri, team in enumerate(sorted_teams, 12):
        ws.cell(ri, 1, team).alignment = ALIGN_LEFT
        for ci, pos in enumerate(range(4), 2):
            c = ws.cell(ri, ci, pct(pos_probs[team][pos]))
            c.alignment = ALIGN_CENTER
            if pos == 0 and pos_probs[team][0] >= 0.5:
                c.fill = FILL_WINNER

    # --- Spiele ---
    ws.cell(18, 1, "Spiele").font = Font(bold=True, size=11)
    headers_m = ["Heim", "Ergebnis / Prognose", "Auswärts", "Heim-Sieg%", "Unentsch.%", "Ausw-Sieg%"]
    for ci, h in enumerate(headers_m, 1):
        hdr_cell(ws, 19, ci, h)

    row = 20
    for (h, a), s in sorted(avg_scores.items()):
        if s["fixed"]:
            gh = s.get("fixed_gh"); ga = s.get("fixed_ga")
            if gh is None: gh = round(s["avg_h"])
            if ga is None: ga = round(s["avg_a"])
            score_str = f"{int(gh)} : {int(ga)}"
            font = FONT_FIXED
            fill = FILL_FIXED
        else:
            gh, ga = score_from_avg(s["avg_h"], s["avg_a"], s["p_win_h"], s["p_win_a"])
            score_str = f"{gh} : {ga}"
            font = FONT_ITALIC
            fill = None

        for ci, val in enumerate([h, score_str, a,
                                   pct(s["p_win_h"]), pct(s["p_draw"]), pct(s["p_win_a"])], 1):
            c = ws.cell(row, ci, val)
            c.font = font
            c.alignment = ALIGN_CENTER if ci != 1 and ci != 3 else ALIGN_LEFT
            if fill: c.fill = fill
        row += 1

    for i, w in enumerate([22, 16, 22, 8, 8, 8], 1):
        col_width(ws, i, w)


def write_bracket_sheet(wb, ko_results, actual_ko):
    ws = wb.create_sheet("Turnierbaum")

    # Spalten-Header
    round_labels = {1: "Sechzehntelfinale (32 Teams)", 4: "Achtelfinale (16 Teams)",
                    7: "Viertelfinale (8 Teams)", 10: "Halbfinale (4 Teams)", 13: "Finale"}
    for col, label in round_labels.items():
        c = ws.cell(1, col, label)
        c.font = FONT_HEADER
        c.fill = FILL_HEADER
        c.alignment = ALIGN_CENTER
        ws.merge_cells(start_row=1, start_column=col, end_row=1, end_column=col+1)

    for mnr, (r1, r2, ct, cs) in BRACKET.items():
        if mnr not in ko_results:
            continue
        r = ko_results[mnr]
        is_fixed = r.get("is_fixed", False)

        team_a = r["team_a"]
        team_b = r["team_b"]

        if is_fixed:
            score_str = f"{r['fixed_ga']} : {r['fixed_gb']}"
            if r.get("fixed_extra"):
                score_str += f" {r['fixed_extra']}"
            font_a = FONT_FIXED
            font_b = FONT_FIXED
            fill_a = FILL_WINNER if r["fixed_ga"] > r["fixed_gb"] else FILL_LOSER
            fill_b = FILL_LOSER  if r["fixed_ga"] > r["fixed_gb"] else FILL_WINNER
        else:
            p_a = r["p_a_wins"]
            # Gerundetes Ø-Ergebnis, konsistent mit Favorit
            ga, gb = score_from_avg(r["avg_ga"], r["avg_gb"], p_a, 1 - p_a)
            score_str = f"{ga} : {gb}"
            a_is_fav = p_a >= 0.50
            font_a = Font(italic=True, bold=a_is_fav,     color="1F4E79" if a_is_fav else "000000")
            font_b = Font(italic=True, bold=not a_is_fav, color="1F4E79" if not a_is_fav else "000000")
            fill_a = FILL_WINNER if p_a > 0.55 else (FILL_DRAW if p_a >= 0.45 else None)
            fill_b = FILL_WINNER if p_a < 0.45 else (FILL_DRAW if p_a <= 0.55 else None)
            # Gewinnwahrscheinlichkeit als Tooltip in Kommentarspalte
            team_a = f"{team_a} ({pct(p_a)})"
            team_b = f"{team_b} ({pct(1-p_a)})"

        c1 = ws.cell(r1 + 1, ct, team_a)
        c2 = ws.cell(r2 + 1, ct, team_b)
        cs_cell = ws.cell(r1 + 1, cs, score_str)

        c1.font = font_a; c1.alignment = ALIGN_LEFT
        c2.font = font_b; c2.alignment = ALIGN_LEFT
        cs_cell.font = FONT_FIXED if is_fixed else FONT_ITALIC
        cs_cell.alignment = ALIGN_CENTER
        if fill_a: c1.fill = fill_a
        if fill_b: c2.fill = fill_b
        if is_fixed: cs_cell.fill = FILL_FIXED

    # Spaltenbreiten
    for col in [1, 4, 7, 10, 13]:
        col_width(ws, col,   20)
        col_width(ws, col+1, 14)
    for col in [3, 6, 9, 12]:
        col_width(ws, col, 2)

    ws.row_dimensions[1].height = 20
    ws.freeze_panes = "A2"


# ---------------------------------------------------------------------------
# Drittplatzierte-Blatt
# ---------------------------------------------------------------------------
def write_thirds_sheet(wb, group_pos_probs, groups):
    """
    Zeigt fuer jede Gruppe den wahrscheinlichsten Drittplatzierten
    und die Gesamtrangliste der 12 Dritten (8 kommen weiter).
    """
    ws = wb.create_sheet("Drittplatzierte")

    # Header
    headers = ["Gruppe", "Team", "P(3. Platz)", "P(Qualifikation als Bester Dritter)"]
    for ci, h in enumerate(headers, 1):
        hdr_cell(ws, 1, ci, h)

    # Fuer jede Gruppe: Wahrscheinlichkeit Dritter zu werden
    thirds_data = []
    for grp in sorted(groups.keys()):
        teams = groups[grp]
        pp = group_pos_probs[grp]
        for t in teams:
            p3 = pp[t][2]
            if p3 > 0.01:
                thirds_data.append((grp, t, p3))

    # Sortiere nach P(3. Platz) absteigend
    thirds_data.sort(key=lambda x: -x[2])

    # Schreibe Gruppen-Dritte
    row = 2
    current_grp = None
    grp_best = {}  # grp -> (team, p3)

    for grp in sorted(groups.keys()):
        teams = groups[grp]
        pp = group_pos_probs[grp]
        # Bester Dritter je Gruppe
        best = max(teams, key=lambda t: pp[t][2])
        grp_best[grp] = (best, pp[best][2])

        for t in sorted(teams, key=lambda t: -pp[t][2]):
            p3 = pp[t][2]
            if p3 < 0.01:
                continue
            c_grp   = ws.cell(row, 1, grp)
            c_team  = ws.cell(row, 2, t)
            c_p3    = ws.cell(row, 3, pct(p3))
            c_grp.alignment = ALIGN_CENTER
            c_team.alignment = ALIGN_LEFT
            c_p3.alignment   = ALIGN_CENTER
            if t == best:
                for c in [c_grp, c_team, c_p3]:
                    c.font = Font(bold=True)
                    c.fill = FILL_GROUP
            row += 1
        row += 1  # Leerzeile zwischen Gruppen

    # Separator
    row += 1
    ws.cell(row, 1, "Rangliste der Dritten (top 8 qualifizieren sich)").font = Font(bold=True, size=11)
    row += 1

    headers2 = ["Rang", "Gruppe", "Wahrscheinlichster Dritter", "P(3. Platz)", "Qualifikation (8/12)"]
    for ci, h in enumerate(headers2, 1):
        hdr_cell(ws, row, ci, h)
    row += 1

    # Rangliste nach P(3. Platz) des besten Dritten je Gruppe
    ranked = sorted(grp_best.items(), key=lambda x: -x[1][1])
    for rank, (grp, (team, p3)) in enumerate(ranked, 1):
        p_qual = min(p3 * (8/12), p3)  # Vereinfachte Qualifikationswahrscheinlichkeit
        ws.cell(row, 1, rank).alignment    = ALIGN_CENTER
        ws.cell(row, 2, grp).alignment     = ALIGN_CENTER
        ws.cell(row, 3, team).alignment    = ALIGN_LEFT
        ws.cell(row, 4, pct(p3)).alignment = ALIGN_CENTER
        c_q = ws.cell(row, 5, pct(p_qual))
        c_q.alignment = ALIGN_CENTER
        if rank <= 8:
            for col in range(1, 6):
                ws.cell(row, col).fill = FILL_WINNER
        row += 1

    for i, w in enumerate([10, 22, 28, 14, 22], 1):
        col_width(ws, i, w)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("Lade Eingaben ...")
    strengths, groups = load_inputs()
    actual_group = load_actual_group_results()
    actual_ko_df = load_actual_ko_results()

    # Actual KO results als Dict
    actual_ko = {}
    if not actual_ko_df.empty:
        for _, row in actual_ko_df.iterrows():
            mnr = int(row["match_nr"])
            ga, gb = int(row["goals_home"]), int(row["goals_away"])
            winner = row["team_home"] if ga > gb else (row["team_away"] if gb > ga else row["team_home"])
            actual_ko[mnr] = {
                "team_a": row["team_home"], "team_b": row["team_away"],
                "goals_a": ga, "goals_b": gb, "winner": winner, "extra": "",
            }

    # --- Gruppenphase simulieren ---
    print(f"\nSimuliere Gruppenphase ({N_GROUP_SIM}x je Gruppe) ...")
    group_pos_probs  = {}
    group_avg_scores = {}
    group_fixed      = {}
    group_avg_tables = {}

    for grp in sorted(groups.keys()):
        teams = groups[grp]
        played = actual_group[actual_group["group"] == grp] if not actual_group.empty else pd.DataFrame()
        pos_probs, avg_scores, fixed, avg_table = simulate_group_n(grp, teams, strengths, played)
        group_pos_probs[grp]   = pos_probs
        group_avg_scores[grp]  = avg_scores
        group_fixed[grp]       = fixed
        group_avg_tables[grp]  = avg_table
        print(f"  Gruppe {grp}: {', '.join(sorted(pos_probs, key=lambda t: -pos_probs[t][0])[:2])} vorne")

    # --- KO-Phase simulieren ---
    ko_path = DATA_DIR / "wm2026_matches_knockout.csv"
    ko_df = pd.read_csv(ko_path)

    print(f"\nSimuliere KO-Phase ({N_KO_SIM}x) ...")
    ko_results, team_stage_counts = simulate_ko_n(ko_df, group_pos_probs, groups, strengths, actual_ko)

    # --- Gesamtwahrscheinlichkeiten ---
    overview_df = compute_overall_probs(group_pos_probs, team_stage_counts, groups, N_KO_SIM)

    # --- Excel schreiben ---
    print("\nSchreibe Excel ...")
    wb = openpyxl.Workbook()
    wb.remove(wb.active)  # Standard-Sheet entfernen

    write_overview_sheet(wb, overview_df)

    for grp in sorted(groups.keys()):
        write_group_sheet(
            wb, grp, groups[grp],
            group_pos_probs[grp],
            group_avg_scores[grp],
            group_fixed[grp],
            group_avg_tables[grp],
        )

    write_bracket_sheet(wb, ko_results, actual_ko)
    write_thirds_sheet(wb, group_pos_probs, groups)

    out = DATA_DIR / "wm2026_simulation.xlsx"
    wb.save(out)
    print(f"Gespeichert: {out}")

    print("\nTop 10 Weltmeister-Wahrscheinlichkeit:")
    print(overview_df[["rank","team","p_R32","p_QF","p_F","p_Winner"]].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
