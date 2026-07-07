"""
Kalibrierungsmodul fuer die WM-2026-Simulation.

Zwei Aufgaben:

1. AUTO-KALIBRIERUNG von mu_wm_correction (laeuft automatisch bei
   strength_model.py und update_results.py mit, kann aber auch standalone
   ausgefuehrt werden): vergleicht die echten Tore aller gespielten WM-Spiele
   (120-Minuten-Ergebnisse, ohne Elfmetertore) mit den Modell-Lambdas unter
   den AKTUELLEN Parametern und passt mu_wm_correction in model_metadata.json
   an.

   Fixpunkt-Verfahren mit Shrinkage: Ziel-Torschnitt ist das gewichtete
   Mittel aus den echten Turnier-Toren (Gewicht n Spiele) und dem
   historischen WM-Schnitt (Gewicht N0_SHRINKAGE) - bei wenig Daten dominiert
   der Prior, mit jedem Spieltag die echten Daten. Der Standardfehler des
   Log-Deltas liegt bei 16 Spielen um +-0.15, ungebremst jagt man Rauschen.
   delta = ln(Ziel / Modell-Erwartung); da gegen die aktuellen Lambdas
   verglichen wird, konvergiert mehrfaches Ausfuehren ohne neue Ergebnisse
   (delta -> 0), statt die Korrektur immer weiter aufzuaddieren. Das ersetzt
   die alte tipp-basierte Korrektur, die (a) bei jedem Lauf dasselbe Delta
   erneut addierte und (b) auf gerundeten Tipps aus veralteten
   Modell-Staenden kalibrierte. MAX_STEP_PER_RUN kappt den Einzelschritt.

2. TIPP-REPORT: vergleicht gespeicherte Tipps (data/tips_history.csv) mit den
   offiziellen Ergebnissen. Elfmetertore zaehlen dabei ZUM Ergebnis
   (1:1 n.E. mit 4:3 i.E. = 5:4), wie im echten Tippspiel - dafuer muessen
   pens_home/pens_away in wm2026_matches_knockout.csv gefuellt sein
   (macht update_results.py automatisch).

Aufruf: python3 calibrate.py
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent / "data"

# Historischer WM-Schnitt als Shrinkage-Prior (WM-2018: 2.64, WM-2022: 2.72)
WM_HISTORICAL_GOALS_PER_GAME = 2.65

N0_SHRINKAGE = 50        # Spiele-Aequivalent des Priors: Ziel = (n*real + N0*hist) / (n+N0)
MAX_STEP_PER_RUN = 0.10  # Kappung der mu-Aenderung pro Lauf


# ---------------------------------------------------------------------------
# Datenladen
# ---------------------------------------------------------------------------

def load_tip_history() -> pd.DataFrame:
    path = DATA_DIR / "tips_history.csv"
    if not path.exists():
        print("Keine tips_history.csv gefunden. Bitte erst wm_simulation.py ausfuehren.")
        return pd.DataFrame()
    df = pd.read_csv(path, dtype=str)
    df["match_nr"] = df["match_nr"].astype(int)
    df["tip_home"] = pd.to_numeric(df["tip_home"], errors="coerce")
    df["tip_away"] = pd.to_numeric(df["tip_away"], errors="coerce")
    return df


def load_actual_results() -> pd.DataFrame:
    """
    Laedt gespielte Ergebnisse aus Gruppen- und KO-Phase.
    Spalten:
      goals_home/goals_away         - offizielles Ergebnis inkl. Elfmetertoren (Tipp-Wertung)
      goals_home_120/goals_away_120 - reine Spieltore nach 120 Min (Lambda-Kalibrierung)
    """
    frames = []

    group_path = DATA_DIR / "wm2026_matches_group.csv"
    if group_path.exists():
        df = pd.read_csv(group_path)
        if "goals_home" in df.columns and "goals_away" in df.columns:
            played = df.dropna(subset=["goals_home", "goals_away"]).copy()
            played["goals_home"] = played["goals_home"].astype(int)
            played["goals_away"] = played["goals_away"].astype(int)
            played["goals_home_120"] = played["goals_home"]
            played["goals_away_120"] = played["goals_away"]
            frames.append(played[["match_nr", "team_home", "team_away",
                                  "goals_home", "goals_away",
                                  "goals_home_120", "goals_away_120"]])

    ko_path = DATA_DIR / "wm2026_matches_knockout.csv"
    if ko_path.exists():
        ko = pd.read_csv(ko_path)
        if "goals_home" in ko.columns and "team_home" in ko.columns:
            played_ko = ko.dropna(subset=["goals_home", "goals_away", "team_home", "team_away"]).copy()
            played_ko["goals_home_120"] = played_ko["goals_home"].astype(int)
            played_ko["goals_away_120"] = played_ko["goals_away"].astype(int)
            # Elfmetertore zaehlen zum offiziellen Ergebnis
            for col, pens in [("goals_home", "pens_home"), ("goals_away", "pens_away")]:
                if pens in played_ko.columns:
                    played_ko[col] = (
                        played_ko[f"{col}_120"] +
                        pd.to_numeric(played_ko[pens], errors="coerce").fillna(0).astype(int)
                    )
                else:
                    played_ko[col] = played_ko[f"{col}_120"]
            frames.append(played_ko[["match_nr", "team_home", "team_away",
                                     "goals_home", "goals_away",
                                     "goals_home_120", "goals_away_120"]])

    if not frames:
        print("Keine Ergebnisse gefunden.")
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_model_config() -> dict:
    """Liest aktuelle Modell-Parameter aus model_metadata.json."""
    path = DATA_DIR / "model_metadata.json"
    cfg = {"mu": 0.0, "home_adv": 0.25}
    if path.exists():
        with open(path) as f:
            cfg.update(json.load(f))
        cfg["config_exists"] = True
    else:
        cfg["config_exists"] = False
    return cfg


# ---------------------------------------------------------------------------
# Auto-Kalibrierung (modellbasiert, Fixpunkt)
# ---------------------------------------------------------------------------

def compute_lambda_gap(results: pd.DataFrame) -> dict | None:
    """
    Vergleicht die Summe der echten Tore (120 Min) mit der Summe der
    Modell-Lambdas unter den aktuellen Parametern (inkl. mu_wm_correction
    und Gastgeber-Heimvorteil).
    """
    if results.empty:
        return None

    # Lazy-Import + Reload: wm_simulation liest model_metadata.json beim
    # Import - so rechnen wir garantiert mit dem aktuellen Parameterstand.
    import importlib
    import wm_simulation
    wm_simulation = importlib.reload(wm_simulation)

    strengths, _, _ = wm_simulation.load_inputs()

    # Venue-Zuordnung fuer den KO-Heimvorteil
    ko_venues = {}
    ko_path = DATA_DIR / "wm2026_matches_knockout.csv"
    if ko_path.exists():
        ko = pd.read_csv(ko_path)
        if "venue" in ko.columns:
            ko_venues = dict(zip(ko["match_nr"].astype(int), ko["venue"]))

    expected, actual = 0.0, 0.0
    for r in results.itertuples():
        mnr = int(r.match_nr)
        if mnr in ko_venues:
            home = wm_simulation.host_home_team(r.team_home, r.team_away, venue=ko_venues[mnr])
        else:
            home = wm_simulation.host_home_team(r.team_home, r.team_away)
        lam_h, lam_a = wm_simulation.match_lambdas(r.team_home, r.team_away, strengths, home_team=home)
        expected += lam_h + lam_a
        actual += r.goals_home_120 + r.goals_away_120

    n = len(results)
    actual_pg   = actual / n
    expected_pg = expected / n
    # Shrinkage-Ziel: gewichtetes Mittel aus Turnier-Toren und historischem
    # WM-Schnitt. Das ist der Fixpunkt der Kalibrierung - bei wenig Spielen
    # dominiert der Prior, mit jedem Spieltag die echten Daten.
    target_pg = (n * actual_pg + N0_SHRINKAGE * WM_HISTORICAL_GOALS_PER_GAME) / (n + N0_SHRINKAGE)
    return {
        "n": n,
        "actual_per_game": actual_pg,
        "expected_per_game": expected_pg,
        "target_per_game": target_pg,
        "delta_raw": float(np.log(target_pg / expected_pg)) if expected_pg > 0 else 0.0,
    }


def auto_calibrate_mu(gap: dict, model_cfg: dict) -> dict:
    """
    Wendet das gekappte Delta auf mu_wm_correction an und schreibt
    model_metadata.json (bestehende Keys bleiben erhalten).
    """
    old_correction = model_cfg.get("mu_wm_correction", 0.302)
    weight = gap["n"] / (gap["n"] + N0_SHRINKAGE)
    step = float(np.clip(gap["delta_raw"], -MAX_STEP_PER_RUN, MAX_STEP_PER_RUN))
    new_correction = round(old_correction + step, 4)

    updated = dict(model_cfg)
    updated.pop("config_exists", None)
    updated["mu_wm_correction"] = new_correction
    with open(DATA_DIR / "model_metadata.json", "w") as f:
        json.dump(updated, f, indent=2)

    mu_effective = model_cfg.get("mu", 0.0) + new_correction
    return {
        **gap,
        "old_correction": old_correction,
        "weight": round(weight, 3),
        "step": round(step, 4),
        "new_correction": new_correction,
        "mu_effective": round(mu_effective, 4),
        "base_lambda": round(float(np.exp(mu_effective)), 3),
    }


def run_calibration(verbose: bool = True) -> dict | None:
    """
    Einstiegspunkt fuer strength_model.py und update_results.py.
    Gibt das Kalibrierungs-Info-Dict zurueck (oder None ohne Daten).
    """
    results = load_actual_results()
    if results.empty:
        if verbose:
            print("Keine gespielten WM-Spiele - Kalibrierung uebersprungen.")
        return None
    model_cfg = load_model_config()
    gap = compute_lambda_gap(results)
    if gap is None:
        return None
    info = auto_calibrate_mu(gap, model_cfg)
    print(
        f"  Auto-Kalibrierung: {info['n']} Spiele | "
        f"Tore/Spiel real {info['actual_per_game']:.2f} | Ziel (geshrinkt) {info['target_per_game']:.2f} | "
        f"Modell {info['expected_per_game']:.2f} | "
        f"mu_wm_correction {info['old_correction']:.4f} -> {info['new_correction']:.4f}"
    )
    return info


# ---------------------------------------------------------------------------
# Tipp-Punkte-System (identisch mit wm_simulation.py)
# ---------------------------------------------------------------------------

def tip_points(tip_h, tip_a, actual_h, actual_a) -> int:
    if tip_h == actual_h and tip_a == actual_a:
        return 4
    tip_diff    = tip_h - tip_a
    actual_diff = actual_h - actual_a
    if tip_diff == actual_diff:
        return 3
    tip_tendency    = (tip_diff > 0) - (tip_diff < 0)
    actual_tendency = (actual_diff > 0) - (actual_diff < 0)
    if tip_tendency == actual_tendency:
        return 2
    return 0


# ---------------------------------------------------------------------------
# Auswertung
# ---------------------------------------------------------------------------

def score_tips(tips: pd.DataFrame, results: pd.DataFrame) -> pd.DataFrame:
    """Berechnet Tipp-Punkte fuer alle gespielten Spiele mit gespeichertem Tipp."""
    merged = results.merge(
        tips[["match_nr", "tip_home", "tip_away"]],
        on="match_nr",
        how="inner",
    )
    merged = merged.dropna(subset=["tip_home", "tip_away"])
    if merged.empty:
        return merged

    merged["tip_home"] = merged["tip_home"].astype(int)
    merged["tip_away"] = merged["tip_away"].astype(int)
    merged["points"] = merged.apply(
        lambda r: tip_points(r["tip_home"], r["tip_away"], r["goals_home"], r["goals_away"]),
        axis=1,
    )
    merged["exact"]    = (merged["tip_home"] == merged["goals_home"]) & (merged["tip_away"] == merged["goals_away"])
    merged["tip_diff"] = merged["tip_home"] - merged["tip_away"]
    merged["act_diff"] = merged["goals_home"] - merged["goals_away"]
    merged["correct_tendency"] = np.sign(merged["tip_diff"]) == np.sign(merged["act_diff"])
    return merged


def analyze_bias(scored: pd.DataFrame) -> dict:
    """Berechnet Bias-Metriken (auf offiziellen Ergebnissen inkl. Elfmetertoren)."""
    tipped_total = (scored["tip_home"] + scored["tip_away"]).mean()
    actual_total = (scored["goals_home"] + scored["goals_away"]).mean()

    return {
        "tipped_goals_per_game": round(tipped_total, 2),
        "actual_goals_per_game": round(actual_total, 2),
        "goal_underestimate":    round(actual_total - tipped_total, 2),
        "home_tip_avg":          round(scored["tip_home"].mean(), 2),
        "home_real_avg":         round(scored["goals_home"].mean(), 2),
        "away_tip_avg":          round(scored["tip_away"].mean(), 2),
        "away_real_avg":         round(scored["goals_away"].mean(), 2),
    }


def analyze_by_generation(tips: pd.DataFrame, results: pd.DataFrame) -> list[dict]:
    """Teilt Tipps nach run_timestamp auf und berechnet Leistung je Batch."""
    if "run_timestamp" not in tips.columns:
        return []

    timestamps = sorted(tips["run_timestamp"].dropna().unique())
    batches = []

    for i, ts in enumerate(timestamps):
        batch_tips = tips[tips["run_timestamp"] == ts]
        batch_scored = score_tips(batch_tips, results)
        if batch_scored.empty:
            continue

        n = len(batch_scored)
        total_pts = int(batch_scored["points"].sum())
        wrong     = int((batch_scored["points"] == 0).sum())
        batches.append({
            "timestamp": ts,
            "label":     f"Batch {i + 1}",
            "n":         n,
            "total_pts": total_pts,
            "max_pts":   n * 4,
            "pct":       round(total_pts / (n * 4) * 100, 1) if n > 0 else 0,
            "wrong":     wrong,
            "wrong_pct": round(wrong / n * 100, 1) if n > 0 else 0,
            "avg_tipped_goals": round((batch_scored["tip_home"] + batch_scored["tip_away"]).mean(), 2),
            "avg_actual_goals": round((batch_scored["goals_home"] + batch_scored["goals_away"]).mean(), 2),
        })

    return batches


def suggest_calibration(bias: dict, scored: pd.DataFrame, calib: dict | None) -> list[str]:
    """Gibt konkrete Anpassungsempfehlungen als Strings zurueck."""
    suggestions = []

    if calib is not None:
        suggestions.append(
            f"mu_wm_correction automatisch kalibriert: {calib['new_correction']:.4f} "
            f"(effektives MU = {calib['mu_effective']:.4f}, Basis-Lambda {calib['base_lambda']:.3f}). "
            f"Naechste Simulation nutzt den neuen Wert."
        )

    home_gap = bias["home_real_avg"] - bias["home_tip_avg"]
    away_gap = bias["away_real_avg"] - bias["away_tip_avg"]
    if abs(home_gap - away_gap) > 0.3:
        side = "Heimteams" if home_gap > away_gap else "Auswaertsteams"
        gap = max(home_gap, away_gap)
        suggestions.append(
            f"{side} der Ansetzung unterschaetzt (+{gap:.2f} real vs. Tipp). "
            f"Moegliche Ursachen: Gastgeber-Heimvorteil (HOST_TEAMS pruefen) oder Setzlisten-Effekt."
        )

    wrong_pct = (scored["points"] == 0).sum() / max(len(scored), 1)
    if wrong_pct > 0.40:
        suggestions.append(
            f"Zu viele falsche Tendenz-Tipps ({wrong_pct:.0%}). "
            f"Empfehlung: strength_model.py neu ausfuehren mit aktuellen WM-Ergebnissen."
        )

    return suggestions


# ---------------------------------------------------------------------------
# Bericht
# ---------------------------------------------------------------------------

def print_report(
    scored: pd.DataFrame,
    bias: dict,
    suggestions: list[str],
    calib: dict | None,
    batches: list[dict],
):
    n = len(scored)
    total_pts = scored["points"].sum()
    max_pts   = n * 4
    exact     = scored["exact"].sum()
    correct   = (scored["points"] >= 2).sum()
    wrong     = (scored["points"] == 0).sum()

    sep = "-" * 65
    print(sep)
    print("  WM 2026 – Tipp-Kalibrierungsbericht")
    print(sep)

    # --- Modell-Status ---
    if calib:
        print()
        print("  MODELL-STATUS (model_metadata.json)")
        print(f"  {'mu (Poisson-Modell)':<30}: {load_model_config().get('mu', 0.0):.4f}")
        print(f"  {'mu_wm_correction (auto)':<30}: {calib['old_correction']:.4f} -> {calib['new_correction']:.4f}")
        print(f"  {'mu_effective':<30}: {calib['mu_effective']:.4f}  "
              f"(Basis-Lambda pro Team: {calib['base_lambda']:.3f})")
        print()
        print(f"  Tore/Spiel (120 Min) real         : {calib['actual_per_game']:.2f}  ({calib['n']} Spiele)")
        print(f"  Kalibrierungs-Ziel (geshrinkt)    : {calib['target_per_game']:.2f}  "
              f"(Prior: hist. WM-Schnitt {WM_HISTORICAL_GOALS_PER_GAME:.2f}, N0={N0_SHRINKAGE})")
        print(f"  Tore/Spiel Modell-Erwartung       : {calib['expected_per_game']:.2f}")
        print(f"  Delta / Schritt (gekappt)         : {calib['delta_raw']:+.4f} / {calib['step']:+.4f}")

    print()
    print(sep)

    # --- Gesamt-Ergebnis ---
    print(f"  Gespielte Spiele mit gespeichertem Tipp : {n}")
    print(f"  Punkte gesamt  : {total_pts} / {max_pts} moeglich ({total_pts/max_pts*100:.1f}%)")
    print(f"  Exakte Tipps   : {exact} ({exact/max(n,1)*100:.1f}%)")
    print(f"  Richtige Tendenz (2-4 Pkt): {correct} ({correct/max(n,1)*100:.1f}%)")
    print(f"  Falsche Tendenz (0 Pkt)   : {wrong}  ({wrong/max(n,1)*100:.1f}%)")
    print()
    print("  Bias-Analyse (offizielle Ergebnisse inkl. Elfmetertoren):")
    print(f"    Echt. Tore/Spiel: {bias['actual_goals_per_game']:.2f}  |  Getippt: {bias['tipped_goals_per_game']:.2f}  |  Delta: {bias['goal_underestimate']:+.2f}")
    print(f"    Heim  (echt/tipp): {bias['home_real_avg']:.2f} / {bias['home_tip_avg']:.2f}")
    print(f"    Ausw. (echt/tipp): {bias['away_real_avg']:.2f} / {bias['away_tip_avg']:.2f}")

    # --- Aufschluesselung nach Tipp-Generation ---
    if len(batches) > 1:
        print()
        print(sep)
        print("  AUFSCHLUESSELUNG NACH TIPP-GENERATION")
        print()
        for b in batches:
            print(f"  {b['label']}  ({b['timestamp']})")
            print(f"    Spiele: {b['n']} | Punkte: {b['total_pts']}/{b['max_pts']} ({b['pct']:.1f}%) | "
                  f"Falsch: {b['wrong']} ({b['wrong_pct']:.1f}%) | "
                  f"Tore getippt/real: {b['avg_tipped_goals']:.2f}/{b['avg_actual_goals']:.2f}")

    print()
    print(sep)
    print()
    print("  Empfehlungen:")
    for s in suggestions:
        print(f"  → {s}")
    print(sep)

    # --- Detailtabelle ---
    print()
    print("  Einzelspiele (Ergebnis inkl. Elfmetertoren):")
    print(f"  {'Heim':<22} {'Tipp':>6}  {'Ergebnis':>9}  {'Pkt':>4}")
    for _, r in scored.sort_values("match_nr").iterrows():
        tip_str = f"{int(r['tip_home'])}:{int(r['tip_away'])}"
        res_str = f"{int(r['goals_home'])}:{int(r['goals_away'])}"
        mark = "✓" if r["exact"] else ("~" if r["points"] >= 2 else "✗")
        print(f"  {r['team_home']:<22} {tip_str:>6}  {res_str:>9}  {int(r['points']):>3} {mark}")


def save_report(
    scored: pd.DataFrame,
    bias: dict,
    suggestions: list[str],
    calib: dict | None,
    batches: list[dict],
):
    path = DATA_DIR / "calibration_report.txt"
    n = len(scored)
    total_pts = scored["points"].sum()
    lines = [
        "WM 2026 – Kalibrierungsbericht",
        "",
        "MODELL-STATUS",
    ]
    if calib:
        lines += [
            f"  mu_wm_correction = {calib['old_correction']:.4f} -> {calib['new_correction']:.4f}",
            f"  mu_effective     = {calib['mu_effective']:.4f}  (Basis-Lambda {calib['base_lambda']:.3f})",
            f"  Tore/Spiel (120 Min): real={calib['actual_per_game']:.2f} Ziel={calib['target_per_game']:.2f} Modell={calib['expected_per_game']:.2f} ({calib['n']} Spiele)",
            f"  Delta {calib['delta_raw']:+.4f} | Schritt (gekappt) {calib['step']:+.4f}",
        ]
    lines += [
        "",
        "GESAMT (Tipp-Wertung inkl. Elfmetertoren)",
        f"  Gespielte Spiele: {n}",
        f"  Punkte: {total_pts} / {n*4} ({total_pts/max(n*4,1)*100:.1f}%)",
        f"  Tore/Spiel: echt={bias['actual_goals_per_game']:.2f} getippt={bias['tipped_goals_per_game']:.2f}",
        "",
        "NACH GENERATION",
    ]
    for b in batches:
        lines.append(f"  {b['label']} ({b['timestamp']}): {b['total_pts']}/{b['max_pts']} ({b['pct']:.1f}%) — "
                     f"Tore getippt/echt: {b['avg_tipped_goals']:.2f}/{b['avg_actual_goals']:.2f}")
    lines += ["", "EMPFEHLUNGEN"] + [f"  → {s}" for s in suggestions]
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n  Bericht gespeichert: {path.name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    results = load_actual_results()
    if results.empty:
        print("Noch keine echten Ergebnisse vorhanden.")
        print("Bitte zuerst: python3 update_results.py")
        return

    calib = run_calibration(verbose=True)

    tips = load_tip_history()
    if tips.empty:
        return

    scored = score_tips(tips, results)
    if scored.empty:
        print("Keine Spiele konnten verglichen werden (Match-Nummern stimmen moeglicherweise nicht ueberein).")
        return

    bias        = analyze_bias(scored)
    batches     = analyze_by_generation(tips, results)
    suggestions = suggest_calibration(bias, scored, calib)

    print_report(scored, bias, suggestions, calib, batches)
    save_report(scored, bias, suggestions, calib, batches)


if __name__ == "__main__":
    main()
