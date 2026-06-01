"""
Schaetzt Teamstaerken fuer die WM 2026 aus zwei Quellen:
  1. Poisson-Modell (Angriff/Abwehr) aus historischen Laenderspielen
  2. Elo-Ratings (aus ALLEN Laenderspielen seit 1872 kumuliert)

Eingabe:
  data/results.csv        - Kaggle-Dataset "International football results"
  data/squad_values.csv   - Ausgabe von scraper_transfermarkt.py (optional)

Ausgabe:
  data/team_strengths.csv
"""

import numpy as np
import pandas as pd
from pathlib import Path
from scipy.optimize import minimize

DATA_DIR = Path(__file__).resolve().parent / "data"

CUTOFF_YEAR = 2015          # Poisson-Modell: nur Spiele ab diesem Jahr
DECAY_HALFLIFE_YEARS = 3.0  # Halb-Wertzeit fuer Zeitgewichtung

MATCH_WEIGHTS = {
    "FIFA World Cup": 4.0,
    "UEFA Euro": 3.0,
    "Copa America": 3.0,
    "Africa Cup of Nations": 2.5,
    "AFC Asian Cup": 2.5,
    "CONCACAF Gold Cup": 2.5,
    "UEFA Nations League": 2.0,
    "FIFA World Cup qualification": 2.0,
    "UEFA Euro qualification": 1.5,
    "Friendly": 0.5,
}
DEFAULT_WEIGHT = 1.2


def load_results(cutoff_year: int = None) -> pd.DataFrame:
    path = DATA_DIR / "results.csv"
    if not path.exists():
        raise FileNotFoundError(f"Nicht gefunden: {path}")
    df = pd.read_csv(path, parse_dates=["date"])
    df = df.dropna(subset=["home_score", "away_score"]).copy()
    df["home_score"] = pd.to_numeric(df["home_score"], errors="coerce")
    df["away_score"] = pd.to_numeric(df["away_score"], errors="coerce")
    df = df.dropna(subset=["home_score", "away_score"])
    if cutoff_year:
        df = df[df["date"].dt.year >= cutoff_year]
    return df.reset_index(drop=True)


def compute_elo(df_full: pd.DataFrame, k_base: float = 20.0) -> pd.Series:
    """
    Elo aus ALLEN historischen Spielen (kumuliert seit 1872).
    Verwendet gewichtetes K (wichtigere Spiele = groesserer K-Faktor).
    """
    elo = {}
    for row in df_full.sort_values("date").itertuples(index=False):
        h, a = row.home_team, row.away_team
        r_h = elo.get(h, 1500.0)
        r_a = elo.get(a, 1500.0)

        importance = MATCH_WEIGHTS.get(row.tournament, DEFAULT_WEIGHT)
        k = k_base * importance

        e_h = 1.0 / (1.0 + 10.0 ** ((r_a - r_h) / 400.0))
        g_h, g_a = row.home_score, row.away_score
        s_h = 1.0 if g_h > g_a else (0.5 if g_h == g_a else 0.0)

        delta = k * (s_h - e_h)
        elo[h] = r_h + delta
        elo[a] = r_a - delta

    return pd.Series(elo, name="elo")


def fit_poisson_model(df: pd.DataFrame) -> pd.DataFrame:
    """
    Vektorisiertes Poisson-Modell (Dixon-Coles-Ansatz):
      log(lambda_home) = att[home] + def[away] + home_adv * (1 - neutral)
      log(lambda_away) = att[away] + def[home]
    Optimierung via L-BFGS-B mit Parameterbegrenzung gegen Overflow.
    """
    today = pd.Timestamp.today()

    # Vektorisierte Gewichtung
    years_ago = (today - df["date"]).dt.days / 365.25
    time_w = 0.5 ** (years_ago / DECAY_HALFLIFE_YEARS)
    importance = df["tournament"].map(MATCH_WEIGHTS).fillna(DEFAULT_WEIGHT)
    weights = (time_w * importance).values

    teams = sorted(set(df["home_team"]) | set(df["away_team"]))
    team_idx = {t: i for i, t in enumerate(teams)}
    n = len(teams)

    home_idx = df["home_team"].map(team_idx).values.astype(int)
    away_idx = df["away_team"].map(team_idx).values.astype(int)
    goals_h = df["home_score"].values.astype(float)
    goals_a = df["away_score"].values.astype(float)
    # neutral=TRUE -> kein Heimvorteil
    neutral = df["neutral"].astype(str).str.upper().eq("TRUE").values.astype(float)

    print(f"  {n} Teams, {len(df)} Spiele im Poisson-Modell.")

    def neg_log_likelihood(params):
        att  = params[:n]
        def_ = params[n:2*n]
        h_adv = params[2*n]

        # Geclipt gegen Overflow (exp(>10) -> inf)
        log_lam_h = np.clip(att[home_idx] + def_[away_idx] + h_adv * (1 - neutral), -8, 8)
        log_lam_a = np.clip(att[away_idx] + def_[home_idx], -8, 8)
        lam_h = np.exp(log_lam_h)
        lam_a = np.exp(log_lam_a)

        ll = weights * (
            goals_h * log_lam_h - lam_h +
            goals_a * log_lam_a - lam_a
        )
        return -np.nansum(ll)

    x0 = np.zeros(2 * n + 1)
    x0[2 * n] = 0.25
    bounds = [(-3.0, 3.0)] * (2 * n) + [(0.0, 0.8)]

    print("  Optimiere Parameter (L-BFGS-B) ...")
    result = minimize(
        neg_log_likelihood,
        x0,
        method="L-BFGS-B",
        bounds=bounds,
        options={"maxiter": 2000, "ftol": 1e-9},
    )
    print(f"  Konvergiert: {result.success} | Iterationen: {result.nit}")

    att  = result.x[:n]
    def_ = result.x[n:2*n]
    att -= att.mean()   # Identifizierbarkeit: Mittelwert = 0

    return pd.DataFrame({
        "team": teams,
        "att": att,
        "def": def_,
    })


def combine_strengths(
    poisson_df: pd.DataFrame,
    elo: pd.Series,
    squad_df: pd.DataFrame,
) -> pd.DataFrame:
    df = poisson_df.set_index("team").copy()
    df["elo"] = elo

    if not squad_df.empty:
        df = df.join(squad_df.set_index("team")[["squad_value_eur"]], how="left")
    else:
        df["squad_value_eur"] = np.nan

    def norm(s: pd.Series) -> pd.Series:
        mn, mx = s.min(), s.max()
        return (s - mn) / (mx - mn) if mx > mn else pd.Series(0.5, index=s.index)

    df["att_norm"]   = norm(df["att"])
    df["elo_norm"]   = norm(df["elo"])

    has_market = df["squad_value_eur"].notna().any()
    if has_market:
        df["mv_norm"] = norm(df["squad_value_eur"].fillna(df["squad_value_eur"].median()))
        df["strength_combined"] = 0.35 * df["att_norm"] + 0.45 * df["elo_norm"] + 0.20 * df["mv_norm"]
    else:
        df["strength_combined"] = 0.40 * df["att_norm"] + 0.60 * df["elo_norm"]

    return df.reset_index().rename(columns={"index": "team"})


def save_checkpoints(poisson_df: pd.DataFrame, elo: pd.Series):
    """Speichert Poisson-Parameter und Elo-Werte als Checkpoints."""
    poisson_df.to_csv(DATA_DIR / "poisson_params.csv", index=False)
    elo.reset_index().rename(columns={"index": "team"}).to_csv(
        DATA_DIR / "elo_checkpoint.csv", index=False
    )
    print(f"  Checkpoints gespeichert: poisson_params.csv, elo_checkpoint.csv")


def update_elo_with_new_matches(new_matches: pd.DataFrame, k_base: float = 20.0) -> int:
    """
    Inkrementeller Elo-Update: laedt Checkpoint, wendet neue Spiele an, speichert.
    Gibt Anzahl verarbeiteter Spiele zurueck. Sehr schnell (< 1 Sek).
    """
    checkpoint_path = DATA_DIR / "elo_checkpoint.csv"
    if not checkpoint_path.exists():
        print("Kein Elo-Checkpoint. Bitte erst strength_model.py ausfuehren.")
        return 0

    elo_df = pd.read_csv(checkpoint_path)
    elo = elo_df.set_index("team")["elo"].to_dict()

    count = 0
    for row in new_matches.sort_values("date").itertuples(index=False):
        h, a = row.home_team, row.away_team
        r_h = elo.get(h, 1500.0)
        r_a = elo.get(a, 1500.0)
        importance = MATCH_WEIGHTS.get(row.tournament, DEFAULT_WEIGHT)
        k = k_base * importance
        e_h = 1.0 / (1.0 + 10.0 ** ((r_a - r_h) / 400.0))
        g_h, g_a = row.home_score, row.away_score
        s_h = 1.0 if g_h > g_a else (0.5 if g_h == g_a else 0.0)
        delta = k * (s_h - e_h)
        elo[h] = r_h + delta
        elo[a] = r_a - delta
        count += 1

    pd.DataFrame({"team": list(elo.keys()), "elo": list(elo.values())}).to_csv(
        checkpoint_path, index=False
    )
    rebuild_team_strengths()
    return count


def rebuild_team_strengths():
    """Baut team_strengths.csv aus Poisson-Checkpoint + aktuellem Elo neu auf (schnell)."""
    poisson_path = DATA_DIR / "poisson_params.csv"
    elo_path = DATA_DIR / "elo_checkpoint.csv"
    squad_path = DATA_DIR / "squad_values.csv"

    if not poisson_path.exists() or not elo_path.exists():
        return

    poisson = pd.read_csv(poisson_path)
    elo = pd.read_csv(elo_path).set_index("team")["elo"]
    squad = pd.read_csv(squad_path) if squad_path.exists() else pd.DataFrame()

    strengths = combine_strengths(poisson, elo, squad)
    strengths.to_csv(DATA_DIR / "team_strengths.csv", index=False)


def main():
    print("Lade Ergebnisse (vollstaendig fuer Elo) ...")
    results_full = load_results()
    print(f"  {len(results_full)} Spiele gesamt geladen.")

    print("Berechne Elo-Ratings (alle Spiele) ...")
    elo = compute_elo(results_full)
    print(f"  Elo fuer {len(elo)} Teams berechnet.")

    print(f"Lade Spiele ab {CUTOFF_YEAR} fuer Poisson-Modell ...")
    results_model = load_results(cutoff_year=CUTOFF_YEAR)
    print(f"  {len(results_model)} Spiele ab {CUTOFF_YEAR}.")

    print("Passe Poisson-Modell an ...")
    poisson = fit_poisson_model(results_model)

    print("Speichere Checkpoints ...")
    save_checkpoints(poisson, elo)

    squad_path = DATA_DIR / "squad_values.csv"
    if squad_path.exists():
        squad = pd.read_csv(squad_path)
        print(f"Marktwerte fuer {squad['team'].nunique()} Teams geladen.")
    else:
        print("squad_values.csv nicht gefunden -> nur Poisson + Elo.")
        squad = pd.DataFrame()

    print("Kombiniere Staerken ...")
    strengths = combine_strengths(poisson, elo, squad)
    strengths.to_csv(DATA_DIR / "team_strengths.csv", index=False)
    print(f"Gespeichert: team_strengths.csv")

    top = strengths.sort_values("strength_combined", ascending=False).head(20)
    print("\nTop 20 Teams:")
    print(top[["team", "att", "elo", "strength_combined"]].to_string(index=False))


if __name__ == "__main__":
    main()
