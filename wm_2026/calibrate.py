"""
Kalibrierungsscript fuer die WM-2026-Simulation.

Vergleicht gespeicherte Tipps (data/tips_history.csv) mit echten Ergebnissen
(data/wm2026_matches_group.csv) und berechnet Tipp-Qualitaet sowie
Bias-Metriken. Gibt Empfehlungen zur Modell-Anpassung.

Aufruf: python3 calibrate.py
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent / "data"

# Historischer WM-Schnitt fuer Einordnung (WM-2018: 2.64, WM-2022: 2.72)
WM_HISTORICAL_GOALS_PER_GAME = 2.65


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
    path = DATA_DIR / "wm2026_matches_group.csv"
    if not path.exists():
        print("wm2026_matches_group.csv nicht gefunden.")
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "goals_home" not in df.columns or "goals_away" not in df.columns:
        return pd.DataFrame()
    played = df.dropna(subset=["goals_home", "goals_away"]).copy()
    played["goals_home"] = played["goals_home"].astype(int)
    played["goals_away"] = played["goals_away"].astype(int)
    return played


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
    """Berechnet Bias-Metriken."""
    tipped_total = (scored["tip_home"] + scored["tip_away"]).mean()
    actual_total = (scored["goals_home"] + scored["goals_away"]).mean()
    tipped_diff  = (scored["tip_home"] - scored["tip_away"]).mean()
    actual_diff  = (scored["goals_home"] - scored["goals_away"]).mean()

    home_tip_avg  = scored["tip_home"].mean()
    away_tip_avg  = scored["tip_away"].mean()
    home_real_avg = scored["goals_home"].mean()
    away_real_avg = scored["goals_away"].mean()

    return {
        "tipped_goals_per_game": round(tipped_total, 2),
        "actual_goals_per_game": round(actual_total, 2),
        "goal_underestimate":    round(actual_total - tipped_total, 2),
        "tipped_avg_diff":       round(tipped_diff, 2),
        "actual_avg_diff":       round(actual_diff, 2),
        "home_tip_avg":          round(home_tip_avg, 2),
        "home_real_avg":         round(home_real_avg, 2),
        "away_tip_avg":          round(away_tip_avg, 2),
        "away_real_avg":         round(away_real_avg, 2),
    }


def compute_recent_bias(tips: pd.DataFrame, results: pd.DataFrame) -> dict | None:
    """
    Berechnet Tipp-Bias ausschliesslich fuer Batch 2+ (neues Modell).
    Batch 1 (altes Modell, mu=0) wird ignoriert, da es den Gesamtbias verzerrt.
    Gibt None zurueck wenn keine auswertbaren Daten vorhanden.
    """
    if "run_timestamp" not in tips.columns:
        return None
    timestamps = sorted(tips["run_timestamp"].dropna().unique())
    if len(timestamps) < 2:
        return None
    recent_tips = tips[tips["run_timestamp"] != timestamps[0]]
    scored = score_tips(recent_tips, results)
    if scored.empty:
        return None
    return analyze_bias(scored)


def auto_apply_mu_correction(bias_recent: dict | None, model_cfg: dict) -> float:
    """
    Berechnet mu_wm_correction automatisch aus den aktuellen WM-Daten (nur Batch 2+)
    und schreibt sie in model_metadata.json.

    Logik: delta_mu = ln(actual / tipped_recent)
    Die Korrektur wird auf die bestehende mu_wm_correction aufaddiert.
    Gibt den neuen mu_wm_correction-Wert zurueck.
    """
    if bias_recent is None:
        return model_cfg.get("mu_wm_correction", 0.302)

    tipped = bias_recent["tipped_goals_per_game"]
    actual = bias_recent["actual_goals_per_game"]
    if tipped <= 0 or actual <= 0:
        return model_cfg.get("mu_wm_correction", 0.302)

    old_correction = model_cfg.get("mu_wm_correction", 0.302)
    delta = float(np.log(actual / tipped))
    new_correction = round(old_correction + delta, 4)

    path = DATA_DIR / "model_metadata.json"
    updated = dict(model_cfg)
    updated.pop("config_exists", None)
    updated["mu_wm_correction"] = new_correction
    with open(path, "w") as f:
        json.dump(updated, f, indent=2)

    return new_correction


def check_model_params(bias: dict, model_cfg: dict, bias_recent: dict | None = None) -> dict:
    """
    Vergleicht aktuelle Modell-Parameter mit den Anforderungen aus echten Daten.

    mu wirkt additiv auf log_lam_home und log_lam_away. Eine Erhoehung um delta
    skaliert die Gesamt-Tore/Spiel um exp(delta) — kein Divisor 2.
    mu_wm_correction ist die WM-spezifische Zusatzkorrektur (automatisch berechnet).
    """
    mu_current     = model_cfg.get("mu", 0.0)
    mu_correction  = model_cfg.get("mu_wm_correction", 0.302)
    mu_effective   = mu_current + mu_correction
    home_adv       = model_cfg.get("home_adv", 0.25)
    base_lambda    = np.exp(mu_effective)

    # Basis fuer Gap-Berechnung: Bias aus aktuellen Batches (Batch 2+) bevorzugt
    ref_bias = bias_recent if bias_recent is not None else bias
    tipped = ref_bias["tipped_goals_per_game"]
    actual = ref_bias["actual_goals_per_game"]

    mu_target_actual = (mu_effective + np.log(actual / tipped)) if tipped > 0 else None
    mu_target_hist   = (mu_effective + np.log(WM_HISTORICAL_GOALS_PER_GAME / tipped)) if tipped > 0 else None

    gap_to_actual = round(mu_target_actual - mu_effective, 3) if mu_target_actual else None
    gap_to_hist   = round(mu_target_hist - mu_effective, 3) if mu_target_hist else None

    is_calibrated = abs(gap_to_actual) < 0.05 if gap_to_actual is not None else False

    return {
        "mu_current":           round(mu_current, 4),
        "mu_correction":        round(mu_correction, 4),
        "mu_effective":         round(mu_effective, 4),
        "base_lambda":          round(base_lambda, 3),
        "home_adv":             round(home_adv, 4),
        "mu_target_actual_wm":  round(mu_target_actual, 3) if mu_target_actual else None,
        "mu_target_hist_wm":    round(mu_target_hist, 3) if mu_target_hist else None,
        "gap_to_actual":        gap_to_actual,
        "gap_to_hist":          gap_to_hist,
        "is_calibrated":        is_calibrated,
        "config_exists":        model_cfg.get("config_exists", False),
    }


def analyze_by_generation(tips: pd.DataFrame, results: pd.DataFrame) -> list[dict]:
    """
    Teilt Tipps nach run_timestamp auf und berechnet Leistung je Batch.
    Erster Batch = alte Modell-Tipps, spaetere Batches = neue Modell-Tipps.
    """
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
        correct   = int((batch_scored["points"] >= 2).sum())
        wrong     = int((batch_scored["points"] == 0).sum())
        avg_tipped = round((batch_scored["tip_home"] + batch_scored["tip_away"]).mean(), 2)
        avg_actual = round((batch_scored["goals_home"] + batch_scored["goals_away"]).mean(), 2)

        label = f"Batch {i + 1} ({'altes Modell – manuell importiert' if i == 0 else 'neues Modell – average-Modus'})"
        batches.append({
            "timestamp": ts,
            "label":     label,
            "n":         n,
            "total_pts": total_pts,
            "max_pts":   n * 4,
            "pct":       round(total_pts / (n * 4) * 100, 1) if n > 0 else 0,
            "correct":   correct,
            "wrong":     wrong,
            "wrong_pct": round(wrong / n * 100, 1) if n > 0 else 0,
            "avg_tipped_goals": avg_tipped,
            "avg_actual_goals": avg_actual,
        })

    return batches


def suggest_calibration(bias: dict, scored: pd.DataFrame, model_check: dict, new_correction: float | None = None) -> list[str]:
    """Gibt konkrete Anpassungsempfehlungen als Strings zurueck."""
    suggestions = []

    if new_correction is not None:
        suggestions.append(
            f"mu_wm_correction wurde automatisch aktualisiert: {new_correction:.4f} "
            f"(effektives MU = {model_check['mu_effective']:.4f}, Basis-Lambda {model_check['base_lambda']:.3f}). "
            f"Keine manuelle Anpassung noetig — naechste Simulation nutzt neuen Wert."
        )
    else:
        gap_to_actual = model_check.get("gap_to_actual")
        if gap_to_actual is not None and abs(gap_to_actual) < 0.05:
            suggestions.append(
                f"mu_wm_correction kalibriert: effektives MU = {model_check['mu_effective']:.4f}, "
                f"Basis-Lambda {model_check['base_lambda']:.3f}. Keine Anpassung noetig."
            )
        else:
            suggestions.append(
                f"mu_wm_correction konnte nicht automatisch berechnet werden (zu wenige Batches). "
                f"Empfehlung: calibrate.py nach dem naechsten Spieltag erneut ausfuehren."
            )

    home_gap = bias["home_real_avg"] - bias["home_tip_avg"]
    away_gap = bias["away_real_avg"] - bias["away_tip_avg"]
    if abs(home_gap - away_gap) > 0.3 and model_check.get("is_calibrated", True):
        if home_gap > away_gap:
            suggestions.append(
                f"Heimteams unterschaetzt (+{home_gap:.2f} real vs. Tipp). "
                f"Empfehlung: MARKET_ATTACK_LOG_ADJ leicht erhoehen (aktuell 0.35)."
            )
        else:
            suggestions.append(
                f"Auswaertsteams unterschaetzt (+{away_gap:.2f} real vs. Tipp). "
                f"Empfehlung: ELO_MAX_GOAL_BOOST pruefen (aktuell 0.08)."
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
    model_check: dict,
    batches: list[dict],
    bias_recent: dict | None = None,
    new_correction: float | None = None,
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
    print()
    print("  MODELL-STATUS (model_metadata.json)")
    print(f"  {'mu (Poisson-Modell)':<30}: {model_check['mu_current']:.4f}")
    print(f"  {'mu_wm_correction (auto)':<30}: {model_check['mu_correction']:.4f}")
    print(f"  {'mu_effective (= mu + corr.)':<30}: {model_check['mu_effective']:.4f}  "
          f"(Basis-Lambda pro Team: {model_check['base_lambda']:.3f})")
    print(f"  {'home_adv':<30}: {model_check['home_adv']:.4f}")
    print()
    print(f"  Tore/Spiel Ziel hist. WM-Schnitt  : {WM_HISTORICAL_GOALS_PER_GAME:.2f}")
    print(f"  Tore/Spiel alle Tipps (Gesamt)    : {bias['tipped_goals_per_game']:.2f}")
    print(f"  Tore/Spiel aktuell real (WM 2026) : {bias['actual_goals_per_game']:.2f}")
    if bias_recent:
        print(f"  Tore/Spiel Batch 2+ getippt       : {bias_recent['tipped_goals_per_game']:.2f}  (Referenz fuer Auto-Kalibrierung)")
        print(f"  Tore/Spiel Batch 2+ real          : {bias_recent['actual_goals_per_game']:.2f}")
    print()
    if new_correction is not None:
        print(f"  AUTO-KALIBRIERUNG: mu_wm_correction -> {new_correction:.4f} (gespeichert)")
    elif model_check["is_calibrated"]:
        print("  STATUS: Effektives MU kalibriert (Luecke < 0.05).")
    else:
        gap = model_check.get("gap_to_actual", 0)
        print(f"  STATUS: Nicht kalibriert — Luecke: {gap:+.3f}")

    print()
    print(sep)

    # --- Gesamt-Ergebnis ---
    print(f"  Gespielte Spiele mit gespeichertem Tipp : {n}")
    print(f"  Punkte gesamt  : {total_pts} / {max_pts} moeglich ({total_pts/max_pts*100:.1f}%)")
    print(f"  Exakte Tipps   : {exact} ({exact/max(n,1)*100:.1f}%)")
    print(f"  Richtige Tendenz (2-4 Pkt): {correct} ({correct/max(n,1)*100:.1f}%)")
    print(f"  Falsche Tendenz (0 Pkt)   : {wrong}  ({wrong/max(n,1)*100:.1f}%)")
    print()
    print("  Bias-Analyse:")
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
            print(f"  {b['label']}")
            print(f"    Timestamp   : {b['timestamp']}")
            print(f"    Ausgewertete Spiele: {b['n']}")
            if b['n'] > 0:
                print(f"    Punkte      : {b['total_pts']} / {b['max_pts']} ({b['pct']:.1f}%)")
                print(f"    Falsch (0 Pkt): {b['wrong']} ({b['wrong_pct']:.1f}%)")
                print(f"    Tore/Spiel  : getippt {b['avg_tipped_goals']:.2f} | real {b['avg_actual_goals']:.2f}")
            else:
                print("    (Keine gespielten Spiele in diesem Batch)")
            print()
        if len(batches) >= 2 and batches[0]["n"] > 0 and batches[1]["n"] > 0:
            print(f"  Hinweis: Batch 1 (altes Modell) hatte mu=0 und 'expected_points'-Modus.")
            print(f"  Batch 2+ verwendet mu={model_check['mu_current']:.4f} und 'average'-Modus.")
            print(f"  Direkte Vergleichbarkeit ist eingeschraenkt — Batch 1 verzerrt den Gesamt-Bias.")

    print()
    print(sep)
    print()
    print("  Empfehlungen:")
    for s in suggestions:
        print(f"  → {s}")
    print(sep)

    # --- Detailtabelle ---
    print()
    print("  Einzelspiele:")
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
    model_check: dict,
    batches: list[dict],
):
    path = DATA_DIR / "calibration_report.txt"
    n = len(scored)
    total_pts = scored["points"].sum()
    lines = [
        "WM 2026 – Kalibrierungsbericht",
        "",
        "MODELL-STATUS",
        f"  mu_current       = {model_check['mu_current']:.4f}  (Basis-Lambda {model_check['base_lambda']:.3f})",
        f"  home_adv         = {model_check['home_adv']:.4f}",
        f"  Kalibriert       = {'Ja' if model_check['is_calibrated'] else 'Nein'}",
    ]
    if not model_check["is_calibrated"] and model_check.get("gap_to_hist"):
        lines += [
            f"  Luecke (hist.)   = +{model_check['gap_to_hist']:.3f}",
            f"  Ziel mu (hist.)  = {model_check['mu_target_hist_wm']:.3f}",
            f"  Schnell-Fix      : MU_INTERCEPT += {model_check['gap_to_hist']:.3f} in wm_simulation.py",
        ]
    lines += [
        "",
        "GESAMT",
        f"  Gespielte Spiele: {n}",
        f"  Punkte: {total_pts} / {n*4} ({total_pts/max(n*4,1)*100:.1f}%)",
        f"  Tore/Spiel: echt={bias['actual_goals_per_game']:.2f} getippt={bias['tipped_goals_per_game']:.2f}",
        "",
        "NACH GENERATION",
    ]
    for b in batches:
        lines.append(f"  {b['label']}: {b['total_pts']}/{b['max_pts']} ({b['pct']:.1f}%) — "
                     f"Tore getippt/echt: {b['avg_tipped_goals']:.2f}/{b['avg_actual_goals']:.2f}")
    lines += ["", "EMPFEHLUNGEN"] + [f"  → {s}" for s in suggestions]
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n  Bericht gespeichert: {path.name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    tips    = load_tip_history()
    results = load_actual_results()

    if tips.empty:
        return

    if results.empty:
        print("Noch keine echten Ergebnisse vorhanden.")
        print("Bitte zuerst: python3 update_results.py")
        return

    model_cfg    = load_model_config()
    scored       = score_tips(tips, results)
    if scored.empty:
        print("Keine Spiele konnten verglichen werden (Match-Nummern stimmen moeglicherweise nicht ueberein).")
        return

    bias         = analyze_bias(scored)
    bias_recent  = compute_recent_bias(tips, results)
    new_correction = auto_apply_mu_correction(bias_recent, model_cfg)
    # Metadaten neu laden (enthaelt jetzt mu_wm_correction)
    model_cfg    = load_model_config()
    model_check  = check_model_params(bias, model_cfg, bias_recent)
    batches      = analyze_by_generation(tips, results)
    suggestions  = suggest_calibration(bias, scored, model_check, new_correction)

    print_report(scored, bias, suggestions, model_check, batches, bias_recent, new_correction)
    save_report(scored, bias, suggestions, model_check, batches)


if __name__ == "__main__":
    main()

"""
Kalibrierungsscript fuer die WM-2026-Simulation.

Vergleicht gespeicherte Tipps (data/tips_history.csv) mit echten Ergebnissen
(data/wm2026_matches_group.csv) und berechnet Tipp-Qualitaet sowie
Bias-Metriken. Gibt Empfehlungen zur Modell-Anpassung.

Aufruf: python3 calibrate.py
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path(__file__).resolve().parent / "data"

# Historischer WM-Schnitt fuer Einordnung (WM-2018: 2.64, WM-2022: 2.72)
WM_HISTORICAL_GOALS_PER_GAME = 2.65


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
    path = DATA_DIR / "wm2026_matches_group.csv"
    if not path.exists():
        print("wm2026_matches_group.csv nicht gefunden.")
        return pd.DataFrame()
    df = pd.read_csv(path)
    if "goals_home" not in df.columns or "goals_away" not in df.columns:
        return pd.DataFrame()
    played = df.dropna(subset=["goals_home", "goals_away"]).copy()
    played["goals_home"] = played["goals_home"].astype(int)
    played["goals_away"] = played["goals_away"].astype(int)
    return played


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
    """Berechnet Bias-Metriken."""
    tipped_total = (scored["tip_home"] + scored["tip_away"]).mean()
    actual_total = (scored["goals_home"] + scored["goals_away"]).mean()
    tipped_diff  = (scored["tip_home"] - scored["tip_away"]).mean()
    actual_diff  = (scored["goals_home"] - scored["goals_away"]).mean()

    home_tip_avg  = scored["tip_home"].mean()
    away_tip_avg  = scored["tip_away"].mean()
    home_real_avg = scored["goals_home"].mean()
    away_real_avg = scored["goals_away"].mean()

    return {
        "tipped_goals_per_game": round(tipped_total, 2),
        "actual_goals_per_game": round(actual_total, 2),
        "goal_underestimate":    round(actual_total - tipped_total, 2),
        "tipped_avg_diff":       round(tipped_diff, 2),
        "actual_avg_diff":       round(actual_diff, 2),
        "home_tip_avg":          round(home_tip_avg, 2),
        "home_real_avg":         round(home_real_avg, 2),
        "away_tip_avg":          round(away_tip_avg, 2),
        "away_real_avg":         round(away_real_avg, 2),
    }


def check_model_params(bias: dict, model_cfg: dict) -> dict:
    """
    Vergleicht aktuelle Modell-Parameter mit den Anforderungen aus echten Daten.

    mu wirkt additiv auf log_lam_home und log_lam_away. Eine Erhoehung um delta
    skaliert die Gesamt-Tore/Spiel um exp(delta) — kein Divisor 2.
    """
    mu_current = model_cfg.get("mu", 0.0)
    home_adv   = model_cfg.get("home_adv", 0.25)
    base_lambda = np.exp(mu_current)  # Basis-Lambda pro Team ohne alle Korrekturen

    tipped = bias["tipped_goals_per_game"]
    actual = bias["actual_goals_per_game"]

    # Ziel 1: aktuelle WM-Daten angleichen
    mu_target_actual = (mu_current + np.log(actual / tipped)) if tipped > 0 else None
    # Ziel 2: historischen WM-Schnitt (robuster bei kleiner Stichprobe)
    mu_target_hist   = (mu_current + np.log(WM_HISTORICAL_GOALS_PER_GAME / tipped)) if tipped > 0 else None

    gap_to_actual = round(mu_target_actual - mu_current, 3) if mu_target_actual else None
    gap_to_hist   = round(mu_target_hist - mu_current, 3) if mu_target_hist else None

    # Einschaetzung: Kalibriert wenn Luecke zum hist. Schnitt < 0.1
    is_calibrated = abs(gap_to_hist) < 0.10 if gap_to_hist is not None else False

    return {
        "mu_current":           round(mu_current, 4),
        "base_lambda":          round(base_lambda, 3),
        "home_adv":             round(home_adv, 4),
        "mu_target_actual_wm":  round(mu_target_actual, 3) if mu_target_actual else None,
        "mu_target_hist_wm":    round(mu_target_hist, 3) if mu_target_hist else None,
        "gap_to_actual":        gap_to_actual,
        "gap_to_hist":          gap_to_hist,
        "is_calibrated":        is_calibrated,
        "config_exists":        model_cfg.get("config_exists", False),
    }


def analyze_by_generation(tips: pd.DataFrame, results: pd.DataFrame) -> list[dict]:
    """
    Teilt Tipps nach run_timestamp auf und berechnet Leistung je Batch.
    Erster Batch = alte Modell-Tipps, spaetere Batches = neue Modell-Tipps.
    """
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
        correct   = int((batch_scored["points"] >= 2).sum())
        wrong     = int((batch_scored["points"] == 0).sum())
        avg_tipped = round((batch_scored["tip_home"] + batch_scored["tip_away"]).mean(), 2)
        avg_actual = round((batch_scored["goals_home"] + batch_scored["goals_away"]).mean(), 2)

        label = f"Batch {i + 1} ({'altes Modell – manuell importiert' if i == 0 else 'neues Modell – average-Modus'})"
        batches.append({
            "timestamp": ts,
            "label":     label,
            "n":         n,
            "total_pts": total_pts,
            "max_pts":   n * 4,
            "pct":       round(total_pts / (n * 4) * 100, 1) if n > 0 else 0,
            "correct":   correct,
            "wrong":     wrong,
            "wrong_pct": round(wrong / n * 100, 1) if n > 0 else 0,
            "avg_tipped_goals": avg_tipped,
            "avg_actual_goals": avg_actual,
        })

    return batches


def suggest_calibration(bias: dict, scored: pd.DataFrame, model_check: dict) -> list[str]:
    """Gibt konkrete Anpassungsempfehlungen als Strings zurueck."""
    suggestions = []

    gap_to_hist = model_check.get("gap_to_hist")
    gap_to_actual = model_check.get("gap_to_actual")

    if gap_to_hist is not None and gap_to_hist > 0.1:
        mu_new_hist   = model_check["mu_target_hist_wm"]
        mu_new_actual = model_check["mu_target_actual_wm"]
        suggestions.append(
            f"mu zu niedrig: aktuell {model_check['mu_current']:.4f} "
            f"(Basis-Lambda {model_check['base_lambda']:.3f}). "
            f"Ziel hist. WM-Schnitt ({WM_HISTORICAL_GOALS_PER_GAME:.2f} Tore/Spiel): mu -> {mu_new_hist:.3f} (+{gap_to_hist:.3f}). "
            f"Ziel aktuelle WM-Daten ({bias['actual_goals_per_game']:.2f} Tore/Spiel): mu -> {mu_new_actual:.3f} (+{gap_to_actual:.3f}). "
            f"ACHTUNG: {len(scored)} Spiele sind eine kleine Stichprobe — Ziel hist. Schnitt ist robuster. "
            f"Empfehlung: strength_model.py neu ausfuehren (mu wird automatisch angepasst). "
            f"Schnell-Fix ohne Neu-Fitting: In wm_simulation.py MU_INTERCEPT += {gap_to_hist:.3f}."
        )
    elif gap_to_hist is not None and gap_to_hist < -0.1:
        suggestions.append(
            f"mu zu hoch: Tore werden ueberschaetzt ({bias['tipped_goals_per_game']:.2f} vs. real {bias['actual_goals_per_game']:.2f}). "
            f"Empfehlung: strength_model.py neu ausfuehren."
        )
    else:
        suggestions.append(
            f"mu kalibriert: aktuell {model_check['mu_current']:.4f}, "
            f"Basis-Lambda {model_check['base_lambda']:.3f}. Keine mu-Anpassung noetig."
        )

    home_gap = bias["home_real_avg"] - bias["home_tip_avg"]
    away_gap = bias["away_real_avg"] - bias["away_tip_avg"]
    if abs(home_gap - away_gap) > 0.3 and model_check.get("is_calibrated", True):
        if home_gap > away_gap:
            suggestions.append(
                f"Heimteams unterschaetzt (+{home_gap:.2f} real vs. Tipp). "
                f"Empfehlung: MARKET_ATTACK_LOG_ADJ leicht erhoehen (aktuell 0.35)."
            )
        else:
            suggestions.append(
                f"Auswaertsteams unterschaetzt (+{away_gap:.2f} real vs. Tipp). "
                f"Empfehlung: ELO_MAX_GOAL_BOOST pruefen (aktuell 0.08)."
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
    model_check: dict,
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
    print()
    print("  MODELL-STATUS (model_metadata.json)")
    print(f"  {'mu (Intercept)':<30}: {model_check['mu_current']:.4f}  "
          f"(Basis-Lambda pro Team: {model_check['base_lambda']:.3f})")
    print(f"  {'home_adv':<30}: {model_check['home_adv']:.4f}")
    print()
    print(f"  Tore/Spiel Ziel hist. WM-Schnitt  : {WM_HISTORICAL_GOALS_PER_GAME:.2f}")
    print(f"  Tore/Spiel aktuell getippt        : {bias['tipped_goals_per_game']:.2f}")
    print(f"  Tore/Spiel aktuell real (WM 2026) : {bias['actual_goals_per_game']:.2f}")
    print()
    if model_check["is_calibrated"]:
        print("  STATUS: mu ist ausreichend kalibriert (Luecke < 0.10).")
    else:
        gap = model_check["gap_to_hist"]
        mu_t = model_check["mu_target_hist_wm"]
        print(f"  STATUS: mu zu niedrig — Luecke zum hist. Schnitt: +{gap:.3f}")
        print(f"          Benoetigt mu ~{mu_t:.3f} (aktuell {model_check['mu_current']:.4f})")
        print(f"          Schnell-Fix (ohne Neu-Fitting):  MU_INTERCEPT += {gap:.3f}  in wm_simulation.py")
        print(f"          Bessere Loesung:  strength_model.py neu ausfuehren")

    print()
    print(sep)

    # --- Gesamt-Ergebnis ---
    print(f"  Gespielte Spiele mit gespeichertem Tipp : {n}")
    print(f"  Punkte gesamt  : {total_pts} / {max_pts} moeglich ({total_pts/max_pts*100:.1f}%)")
    print(f"  Exakte Tipps   : {exact} ({exact/max(n,1)*100:.1f}%)")
    print(f"  Richtige Tendenz (2-4 Pkt): {correct} ({correct/max(n,1)*100:.1f}%)")
    print(f"  Falsche Tendenz (0 Pkt)   : {wrong}  ({wrong/max(n,1)*100:.1f}%)")
    print()
    print("  Bias-Analyse:")
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
            print(f"  {b['label']}")
            print(f"    Timestamp   : {b['timestamp']}")
            print(f"    Ausgewertete Spiele: {b['n']}")
            if b['n'] > 0:
                print(f"    Punkte      : {b['total_pts']} / {b['max_pts']} ({b['pct']:.1f}%)")
                print(f"    Falsch (0 Pkt): {b['wrong']} ({b['wrong_pct']:.1f}%)")
                print(f"    Tore/Spiel  : getippt {b['avg_tipped_goals']:.2f} | real {b['avg_actual_goals']:.2f}")
            else:
                print("    (Keine gespielten Spiele in diesem Batch)")
            print()
        if len(batches) >= 2 and batches[0]["n"] > 0 and batches[1]["n"] > 0:
            print(f"  Hinweis: Batch 1 (altes Modell) hatte mu=0 und 'expected_points'-Modus.")
            print(f"  Batch 2+ verwendet mu={model_check['mu_current']:.4f} und 'average'-Modus.")
            print(f"  Direkte Vergleichbarkeit ist eingeschraenkt — Batch 1 verzerrt den Gesamt-Bias.")

    print()
    print(sep)
    print()
    print("  Empfehlungen:")
    for s in suggestions:
        print(f"  → {s}")
    print(sep)

    # --- Detailtabelle ---
    print()
    print("  Einzelspiele:")
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
    model_check: dict,
    batches: list[dict],
):
    path = DATA_DIR / "calibration_report.txt"
    n = len(scored)
    total_pts = scored["points"].sum()
    lines = [
        "WM 2026 – Kalibrierungsbericht",
        "",
        "MODELL-STATUS",
        f"  mu_current       = {model_check['mu_current']:.4f}  (Basis-Lambda {model_check['base_lambda']:.3f})",
        f"  home_adv         = {model_check['home_adv']:.4f}",
        f"  Kalibriert       = {'Ja' if model_check['is_calibrated'] else 'Nein'}",
    ]
    if not model_check["is_calibrated"] and model_check.get("gap_to_hist"):
        lines += [
            f"  Luecke (hist.)   = +{model_check['gap_to_hist']:.3f}",
            f"  Ziel mu (hist.)  = {model_check['mu_target_hist_wm']:.3f}",
            f"  Schnell-Fix      : MU_INTERCEPT += {model_check['gap_to_hist']:.3f} in wm_simulation.py",
        ]
    lines += [
        "",
        "GESAMT",
        f"  Gespielte Spiele: {n}",
        f"  Punkte: {total_pts} / {n*4} ({total_pts/max(n*4,1)*100:.1f}%)",
        f"  Tore/Spiel: echt={bias['actual_goals_per_game']:.2f} getippt={bias['tipped_goals_per_game']:.2f}",
        "",
        "NACH GENERATION",
    ]
    for b in batches:
        lines.append(f"  {b['label']}: {b['total_pts']}/{b['max_pts']} ({b['pct']:.1f}%) — "
                     f"Tore getippt/echt: {b['avg_tipped_goals']:.2f}/{b['avg_actual_goals']:.2f}")
    lines += ["", "EMPFEHLUNGEN"] + [f"  → {s}" for s in suggestions]
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n  Bericht gespeichert: {path.name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    tips    = load_tip_history()
    results = load_actual_results()

    if tips.empty:
        return

    if results.empty:
        print("Noch keine echten Ergebnisse vorhanden.")
        print("Bitte zuerst: python3 update_results.py")
        return

    model_cfg = load_model_config()
    scored    = score_tips(tips, results)
    if scored.empty:
        print("Keine Spiele konnten verglichen werden (Match-Nummern stimmen moeglicherweise nicht ueberein).")
        return

    bias         = analyze_bias(scored)
    model_check  = check_model_params(bias, model_cfg)
    batches      = analyze_by_generation(tips, results)
    suggestions  = suggest_calibration(bias, scored, model_check)

    print_report(scored, bias, suggestions, model_check, batches)
    save_report(scored, bias, suggestions, model_check, batches)


if __name__ == "__main__":
    main()
