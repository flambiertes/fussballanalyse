"""Reproducible, chronological 15-season half-life and form experiment.

Run from the repository parent with ``python -m bundesliga.half_life_study``.
The protocol is Review/HALBWERTSZEIT_PROTOKOLL.md. Live defaults are untouched.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from .backtest import match_round, prediction_groups
from .config import DATA_DIR
from .database import load_matches, latest_market_values, consensus_bookmaker_probabilities
from .features import feature_adjustments
from .form_residuals import residual_form
from .model import DynamicDixonColes
from .predict import LIVE_CONFIG
from .priors import lower_league_priors
from .scoring import score_matrix, blend_outcome_probabilities, outcome_probabilities, tip_points

POLICIES = [f"fixed_{h}" for h in (90, 120, 180, 270, 420, 630, 840)] + [
    "season_630_180", "season_180_630"]
FORMS = {"none": 0.0, "legacy": 0.1, "residual_005": 0.05,
         "residual_010": 0.1, "residual_020": 0.2}
BASE_CONFIG = replace(LIVE_CONFIG, form_weight=0.0, bookmaker_weight=0.0,
                      bookmaker_snapshot_type="opening")


def half_life(policy: str, matchday: int) -> float:
    if not 1 <= matchday <= 34:
        raise ValueError("Official matchday must be between 1 and 34")
    if policy.startswith("fixed_"):
        return float(policy.split("_")[1])
    _, start, end = policy.split("_")
    return float(start) + (float(end) - float(start)) * (matchday - 1) / 33


def scoring_table() -> tuple[list[tuple[int, int]], np.ndarray]:
    tips = [(h, a) for h in range(6) for a in range(6)]
    actuals = [(h, a) for h in range(11) for a in range(11)]
    return tips, np.array([[tip_points(t, a) for a in actuals] for t in tips])


TIPS, POINTS_TABLE = scoring_table()


def fast_tip(matrix: np.ndarray) -> tuple[int, int]:
    expectations = POINTS_TABLE @ matrix.ravel()
    index = max(range(len(TIPS)), key=lambda i: (
        expectations[i], matrix[TIPS[i]], -sum(TIPS[i]), -abs(TIPS[i][0] - TIPS[i][1])))
    return TIPS[index]


def prepare(output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    upper = load_matches(competition="D1", finished_only=True)
    lower = load_matches(competition="D2", finished_only=True)
    selected = upper.loc[upper.season.between(2010, 2025)].copy()
    counts = selected.groupby(["season", "matchday"]).size()
    if len(counts) != 16 * 34 or not counts.eq(9).all() or selected.matchday.isna().any():
        raise ValueError("Need 16 complete seasons, including warmup")
    source_files = ["half_life_study.py", "form_residuals.py", "model.py", "features.py",
                    "priors.py", "scoring.py", "backtest.py", "config.py", "predict.py",
                    "database.py", "rounds.py", "Review/HALBWERTSZEIT_PROTOKOLL.md"]
    digest = hashlib.sha256()
    for name in source_files:
        digest.update((Path(__file__).parent / name).read_bytes())
    digest.update(pd.util.hash_pandas_object(upper, index=False).values.tobytes())
    digest.update(pd.util.hash_pandas_object(lower, index=False).values.tobytes())
    # Include external inputs; do not silently reuse cache after changed odds/markets.
    from .database import connect
    with connect() as con:
        for table in ("bookmaker_odds", "market_values"):
            frame = pd.read_sql_query(f"SELECT * FROM {table} ORDER BY rowid", con)
            digest.update(pd.util.hash_pandas_object(frame, index=False).values.tobytes())
    fingerprint = digest.hexdigest()
    manifest_path = output / "manifest.json"
    if manifest_path.exists():
        old = json.loads(manifest_path.read_text())
        if old["fingerprint"] != fingerprint:
            raise ValueError("Study inputs/code changed; choose a fresh --output directory")
        if (output / "inputs.pkl").exists():
            print("Validated cached inputs", flush=True)
            return
    manifest_path.write_text(json.dumps({
        "fingerprint": fingerprint, "config": asdict(BASE_CONFIG),
        "policies": POLICIES, "forms": FORMS, "evaluation_seasons": [2011, 2025],
        "warmup_season": 2010, "rolling_selection_years": 5,
        "primary_selection_metric": "log_loss", "rounds": 510, "games": 4590,
    }, indent=2))
    _, groups = prediction_groups(selected, "expected-points")
    batches = []
    for i, (as_of, group) in enumerate(groups):
        cutoff = as_of - pd.Timedelta(days=365.25 * BASE_CONFIG.lookback_years)
        training = upper.loc[upper.match_date.lt(as_of) & upper.match_date.ge(cutoff)]
        markets = latest_market_values(as_of)
        eligible = group.match_date.map(match_round).eq(as_of)
        odds = consensus_bookmaker_probabilities(
            group.loc[eligible, "match_id"], snapshot_type="opening", as_of=as_of
        ).set_index("match_id")
        rows = []
        for m in group.itertuples(index=False):
            ah, aa, f = feature_adjustments(training, m.home_team, m.away_team,
                                             as_of, BASE_CONFIG, markets)
            book = odds.loc[m.match_id] if m.match_id in odds.index else {}
            rows.append({"match_id": m.match_id, "season": int(m.season),
                         "matchday": int(m.matchday), "as_of": as_of,
                         "match_date": m.match_date, "home_team": m.home_team,
                         "away_team": m.away_team, "home_goals": int(m.home_goals),
                         "away_goals": int(m.away_goals), "other_home": ah,
                         "other_away": aa, "legacy_home": f["home_form"],
                         "legacy_away": f["away_form"],
                         **{c: book.get(c, np.nan) for c in
                            ("book_prob_home", "book_prob_draw", "book_prob_away")}})
        batches.append((as_of, pd.DataFrame(rows)))
        if (i + 1) % 34 == 0:
            print(f"Prepared {i + 1}/544 round inputs", flush=True)
    pd.to_pickle((upper, lower, batches), output / "inputs.pkl")


def fit_policy(policy: str, output_string: str) -> str:
    output = Path(output_string)
    upper, lower, batches = pd.read_pickle(output / "inputs.pkl")
    by_season = {}
    for as_of, group in batches:
        by_season.setdefault(int(group.season.iloc[0]), []).append((as_of, group))
    for season, season_batches in sorted(by_season.items()):
        path = output / f"base_{policy}_{season}.csv"
        if path.exists():
            continue
        records = []
        for as_of, group in season_batches:
            config = replace(BASE_CONFIG, half_life_days=half_life(policy, int(group.matchday.iloc[0])))
            model = DynamicDixonColes(config).fit(upper, as_of)
            if config.use_lower_league_priors:
                lower_league_priors(model, lower, upper,
                                   set(group.home_team) | set(group.away_team), as_of, config)
            for row in group.to_dict("records"):
                ha, hd = model.team_parameters(row["home_team"])
                aa, ad = model.team_parameters(row["away_team"])
                lh = model.intercept + model.home_advantage + ha + ad + row["other_home"]
                la = model.intercept + aa + hd + row["other_away"]
                row.update(base_log_home=lh, base_log_away=la,
                           base_home=float(np.exp(np.clip(lh, -4, 3))),
                           base_away=float(np.exp(np.clip(la, -4, 3))), rho=model.rho,
                           half_life_days=config.half_life_days,
                           training_matches=len(model.training_matches),
                           fit_iterations=int(model.optimization_result.nit))
                records.append(row)
        temporary = path.with_suffix(".tmp")
        pd.DataFrame(records).to_csv(temporary, index=False)
        temporary.replace(path)
        print(f"FIT {policy} {season}/{season + 1}: 34 rounds complete", flush=True)
    return policy


def evaluate_policy(policy: str, output_string: str) -> str:
    output = Path(output_string)
    path = output / f"metrics_{policy}.csv"
    if path.exists():
        return policy
    base = pd.concat([pd.read_csv(output / f"base_{policy}_{s}.csv",
                                 parse_dates=["match_date", "as_of"])
                      for s in range(2010, 2026)], ignore_index=True)
    records = []
    for (_, _, as_of), group in base.loc[base.season.ge(2011)].groupby(["season", "matchday", "as_of"]):
        teams = set(group.home_team) | set(group.away_team)
        forms = {team: residual_form(base, team, as_of) for team in teams}
        for row in group.itertuples(index=False):
            actual = (int(row.home_goals), int(row.away_goals))
            outcome = 0 if actual[0] > actual[1] else (1 if actual[0] == actual[1] else 2)
            one_hot = np.eye(3)[outcome]
            book = (row.book_prob_home, row.book_prob_draw, row.book_prob_away)
            if not np.isfinite(book).all():
                book = None
            for form, weight in FORMS.items():
                fh, fa = ((row.legacy_home, row.legacy_away) if form == "legacy"
                          else (forms[row.home_team], forms[row.away_team]))
                lh = float(np.exp(np.clip(row.base_log_home + weight * (fh - 0.5 * fa), -4, 3)))
                la = float(np.exp(np.clip(row.base_log_away + weight * (fa - 0.5 * fh), -4, 3)))
                raw = score_matrix(lh, la, row.rho)
                for book_weight in (0.0, 0.5, 1.0):
                    matrix = blend_outcome_probabilities(raw, book, book_weight)
                    probabilities = np.array(outcome_probabilities(matrix))
                    tip = fast_tip(matrix)
                    records.append({"policy": policy, "form": form,
                                    "book_weight": book_weight, "match_id": row.match_id,
                                    "season": row.season, "matchday": row.matchday,
                                    "tip_home": tip[0], "tip_away": tip[1],
                                    "tip_points": tip_points(tip, actual),
                                    "log_loss": -np.log(max(matrix[actual], 1e-15)),
                                    "outcome_log_loss": -np.log(max(probabilities[outcome], 1e-15)),
                                    "brier_score": float(np.square(probabilities - one_hot).sum()),
                                    "quotes_available": book is not None})
    temporary = path.with_suffix(".tmp")
    pd.DataFrame(records).to_csv(temporary, index=False)
    temporary.replace(path)
    print(f"SCORED {policy}: {len(records)} match/variant rows", flush=True)
    return policy


def aggregate(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    scores = frame.groupby(keys).agg(games=("match_id", "size"),
                                    log_loss=("log_loss", "mean"),
                                    outcome_log_loss=("outcome_log_loss", "mean"),
                                    brier_score=("brier_score", "mean"),
                                    points=("tip_points", "sum")).reset_index()
    round_keys = list(dict.fromkeys(keys + ["season", "matchday"]))
    rounds = frame.groupby(round_keys).tip_points.sum().reset_index()
    stats = rounds.groupby(keys).tip_points.agg(rounds="size", mean="mean", maximum="max").reset_index()
    for threshold in (18, 20, 22, 24):
        c = rounds.assign(hit=rounds.tip_points.ge(threshold)).groupby(keys).hit.sum()
        stats = stats.merge(c.rename(f"at_least_{threshold}").reset_index(), on=keys)
    return scores.merge(stats, on=keys)


def rolling_choices(season_metrics: pd.DataFrame, policies: list[str], forms: list[str]) -> pd.DataFrame:
    chosen = []
    eligible = season_metrics.loc[season_metrics.policy.isin(policies) & season_metrics.form.isin(forms)]
    for weight in (0.0, 0.5, 1.0):
        for season in range(2016, 2026):
            past = eligible.loc[eligible.book_weight.eq(weight) & eligible.season.between(season - 5, season - 1)]
            if past.groupby(["policy", "form"]).season.nunique().ne(5).any() or past.empty:
                raise ValueError("Selection requires five complete preceding seasons")
            rank = past.groupby(["policy", "form"])[["log_loss", "brier_score"]].mean().reset_index()
            best = rank.sort_values(["log_loss", "brier_score", "policy", "form"]).iloc[0]
            chosen.append({"book_weight": weight, "season": season, "policy": best.policy,
                           "form": best.form, "selection_start": season - 5,
                           "selection_end": season - 1, "past_log_loss": best.log_loss})
    return pd.DataFrame(chosen)


def summarize_study(output: Path) -> None:
    metrics = pd.concat([pd.read_csv(output / f"metrics_{p}.csv") for p in POLICIES], ignore_index=True)
    keys = ["policy", "form", "book_weight"]
    expected_rows = 4590 * len(POLICIES) * len(FORMS) * 3
    if len(metrics) != expected_rows or metrics.duplicated(keys + ["match_id"]).any():
        raise ValueError("Incomplete or duplicated candidate evaluation")
    if not metrics.groupby(keys + ["season", "matchday"]).size().eq(9).all():
        raise ValueError("Incomplete official rounds")
    seasons = aggregate(metrics, keys + ["season"])
    all_summary = aggregate(metrics, keys)
    all_summary.to_csv(output / "all_candidates.csv", index=False)
    seasons.to_csv(output / "per_season.csv", index=False)
    choices = []
    methods = {"select_fixed": (POLICIES[:7], ["legacy"]),
               "select_half_life": (POLICIES, ["legacy"]),
               "select_half_life_and_form": (POLICIES, list(FORMS))}
    selected_frames = []
    for method, (policies, forms) in methods.items():
        choice = rolling_choices(seasons, policies, forms).assign(method=method)
        choices.append(choice)
        selected_frames.append(metrics.merge(choice[["book_weight", "season", "policy", "form", "method"]],
                                            on=["book_weight", "season", "policy", "form"], validate="many_to_one"))
    baseline = metrics.loc[metrics.policy.eq("fixed_420") & metrics.form.eq("legacy") & metrics.season.ge(2016)]
    selected_frames.append(baseline.assign(method="baseline_420"))
    selected = pd.concat(selected_frames, ignore_index=True)
    selection = pd.concat(choices, ignore_index=True)
    validation = aggregate(selected, ["method", "book_weight"])
    per_test_season = aggregate(selected, ["method", "book_weight", "season"])
    selection.to_csv(output / "rolling_choices.csv", index=False)
    selected.to_csv(output / "rolling_predictions.csv", index=False)
    validation.to_csv(output / "rolling_summary.csv", index=False)
    per_test_season.to_csv(output / "rolling_per_season.csv", index=False)
    metrics["phase"] = np.where(metrics.matchday.le(8), "1-8", np.where(metrics.matchday.le(25), "9-25", "26-34"))
    phases = aggregate(metrics.loc[metrics.form.eq("legacy")], keys + ["phase"])
    phases.to_csv(output / "phases.csv", index=False)
    # Paired season block bootstrap: preserve all within-season dependence.
    rng = np.random.default_rng(20260908)
    indices = rng.integers(0, 10, size=(10000, 10))
    uncertainty = []
    for weight in (0.0, 0.5, 1.0):
        subset = per_test_season.loc[per_test_season.book_weight.eq(weight)]
        baseline_s = subset.loc[subset.method.eq("baseline_420")].set_index("season").sort_index()
        for method in methods:
            candidate = subset.loc[subset.method.eq(method)].set_index("season").sort_index()
            for metric in ("log_loss", "outcome_log_loss", "brier_score", "mean", "at_least_24"):
                diffs = (candidate[metric] - baseline_s[metric]).to_numpy()
                lo, hi = np.quantile(diffs[indices].mean(axis=1), [0.025, 0.975])
                uncertainty.append({"method": method, "book_weight": weight, "metric": metric,
                                    "difference": float(diffs.mean()), "ci_025": lo, "ci_975": hi})
    intervals = pd.DataFrame(uncertainty)
    intervals.to_csv(output / "paired_uncertainty.csv", index=False)
    with pd.ExcelWriter(output / "halbwertszeit_15_saisons.xlsx", engine="openpyxl") as writer:
        for name, frame in (("Rolling-Test", validation), ("Auswahl-je-Saison", selection),
                            ("Test-je-Saison", per_test_season), ("Alle-Kandidaten", all_summary),
                            ("Kandidaten-je-Saison", seasons), ("Saisonphasen", phases),
                            ("Unsicherheit", intervals)):
            frame.to_excel(writer, sheet_name=name, index=False)
            sheet = writer.sheets[name]
            sheet.freeze_panes = "A2"
            sheet.auto_filter.ref = sheet.dimensions
            from openpyxl.utils import get_column_letter
            for i in range(1, len(frame.columns) + 1):
                sheet.column_dimensions[get_column_letter(i)].width = 18
    print(validation.to_string(index=False), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DATA_DIR / "half_life_15_seasons")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--stage", choices=["all", "prepare", "fit", "score", "summarize"], default="all")
    args = parser.parse_args()
    if args.stage in {"all", "prepare"}:
        prepare(args.output)
    for stage, operation in (("fit", fit_policy), ("score", evaluate_policy)):
        if args.stage not in {"all", stage}:
            continue
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            jobs = [pool.submit(operation, policy, str(args.output)) for policy in POLICIES]
            for job in as_completed(jobs):
                job.result()  # A failed fit must fail the study, never drop games.
    if args.stage in {"all", "summarize"}:
        summarize_study(args.output)


if __name__ == "__main__":
    main()
