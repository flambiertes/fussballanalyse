"""Chronological evaluation of the predeclared seven-plus-two risk rule."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from .config import DATA_DIR
from .half_life_study import fast_tip
from .limited_risk import LimitedRiskConfig, limited_risk_tips
from .scoring import score_matrix, blend_outcome_probabilities, tip_points

SIGNALS = (90, 120, 270, 420)
SELECTABLE = ["baseline"] + [f"hybrid_{h}" for h in SIGNALS]


def reconstruct(row) -> np.ndarray:
    lh = np.exp(np.clip(row.base_log_home + .1 * (row.legacy_home - .5 * row.legacy_away), -4, 3))
    la = np.exp(np.clip(row.base_log_away + .1 * (row.legacy_away - .5 * row.legacy_home), -4, 3))
    return score_matrix(float(lh), float(la), row.rho)


def choose_for_season(rounds: pd.DataFrame, season: int) -> dict:
    past = rounds.loc[rounds.season.between(season - 5, season - 1)
                     & rounds.strategy.isin(SELECTABLE)].copy()
    if set(past.strategy) != set(SELECTABLE):
        raise ValueError("Missing selection candidates")
    if not past.groupby(["strategy", "season"]).size().eq(34).all():
        raise ValueError("Incomplete selection seasons")
    if not past.groupby("strategy").season.nunique().eq(5).all():
        raise ValueError("Need five preceding seasons")
    past["ge22"] = past.points.ge(22)
    past["ge24"] = past.points.ge(24)
    ranking = past.groupby("strategy").agg(ge22=("ge22", "sum"), ge24=("ge24", "sum"),
                                           changes=("changes", "sum"), points=("points", "sum")).reset_index()
    best = ranking.sort_values(["ge22", "ge24", "changes", "points", "strategy"],
                               ascending=[False, False, True, False, True]).iloc[0]
    return dict(season=season, strategy=best.strategy, selection_start=season - 5,
                selection_end=season - 1, past_ge22=int(best.ge22), past_ge24=int(best.ge24),
                past_changes=int(best.changes), past_points=int(best.points))


def aggregate(rounds: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    table = rounds.groupby(keys).agg(rounds=("points", "size"), points=("points", "sum"),
              mean=("points", "mean"), maximum=("points", "max"),
              mean_changes=("changes", "mean"), mean_expected_cost=("expected_cost", "mean"),
              better=("better", "sum"), equal=("equal", "sum"), worse=("worse", "sum")).reset_index()
    for threshold in (18, 20, 22, 24):
        totals = rounds.assign(hit=rounds.points.ge(threshold)).groupby(keys).hit.sum()
        table = table.merge(totals.rename(f"at_least_{threshold}").reset_index(), on=keys)
    return table


def run(source: Path, output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    config = LimitedRiskConfig()
    inputs = [source / f"base_fixed_{h}_{s}.csv" for h in SIGNALS for s in range(2011, 2026)]
    inputs += [source / f"metrics_fixed_{h}.csv" for h in SIGNALS]
    digest = hashlib.sha256()
    source_names = ("limited_risk.py", "limited_risk_study.py", "half_life_study.py", "scoring.py",
                    "Review/SIEBEN_PLUS_ZWEI_PROTOKOLL.md")
    for name in source_names:
        digest.update((Path(__file__).parent / name).read_bytes())
    hashes = {}
    for path in inputs:
        hashes[path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
        digest.update(hashes[path.name].encode())
    fingerprint = digest.hexdigest()
    manifest_path = output / "manifest.json"
    if manifest_path.exists() and json.loads(manifest_path.read_text())["fingerprint"] != fingerprint:
        raise ValueError("Changed study inputs/code; choose a new --output directory")
    manifest_path.write_text(json.dumps(dict(fingerprint=fingerprint, inputs=hashes,
                config=asdict(config), signals=SIGNALS, selection_metric=["ge22", "ge24"],
                retrospective=True, source_study=str(source)), indent=2))
    bases = {h: pd.concat([pd.read_csv(source / f"base_fixed_{h}_{s}.csv")
                          for s in range(2011, 2026)]).set_index("match_id") for h in SIGNALS}
    baseline = bases[420].sort_values(["season", "matchday", "match_id"])
    for h in SIGNALS:
        if len(bases[h]) != 4590 or not bases[h].index.is_unique or set(bases[h].index) != set(baseline.index):
            raise ValueError("Incomplete base forecasts")
        aligned = bases[h].loc[baseline.index]
        for column in ("as_of", "match_date", "season", "matchday", "home_goals", "away_goals"):
            if not aligned[column].equals(baseline[column]):
                raise ValueError(f"Misaligned input: {h} / {column}")
    records = []
    for number, ((season, matchday), group) in enumerate(baseline.groupby(["season", "matchday"]), 1):
        if len(group) != 9 or group.as_of.nunique() != 1:
            raise ValueError("Need complete, uniformly frozen rounds")
        ids = group.index.tolist()
        base_matrices = []
        for row in group.itertuples():
            book = (row.book_prob_home, row.book_prob_draw, row.book_prob_away)
            if not np.isfinite(book).all():
                book = None
            base_matrices.append(blend_outcome_probabilities(reconstruct(row), book, 1.0))
        baseline_tips = [fast_tip(matrix) for matrix in base_matrices]
        selections = {"baseline": (baseline_tips, {})}
        for h in SIGNALS:
            signals = [reconstruct(row) for row in bases[h].loc[ids].itertuples()]
            tips, report = limited_risk_tips(base_matrices, signals, ids, config)
            selections[f"hybrid_{h}"] = (tips, {x["match_id"]: x for x in report["changes"]})
        # Actual scores enter only after every strategy has selected its tips.
        for strategy, (tips, changes) in selections.items():
            for row, tip, old_tip in zip(group.itertuples(), tips, baseline_tips):
                actual = (int(row.home_goals), int(row.away_goals))
                detail = changes.get(row.Index, {})
                records.append(dict(strategy=strategy, season=season, matchday=matchday,
                    match_id=row.Index, as_of=row.as_of, home_team=row.home_team, away_team=row.away_team,
                    actual_home=actual[0], actual_away=actual[1], tip_home=tip[0], tip_away=tip[1],
                    baseline_home=old_tip[0], baseline_away=old_tip[1], points=tip_points(tip, actual),
                    baseline_points=tip_points(old_tip, actual), changed=tip != old_tip,
                    expected_cost=detail.get("expected_points_cost", 0.0),
                    probability_edge=detail.get("edge", np.nan),
                    base_probability=detail.get("base_probability", np.nan),
                    signal_probability=detail.get("signal_probability", np.nan),
                    added_draw=detail.get("adds_draw", False)))
        if number % 102 == 0:
            print(f"Selected and scored {number}/510 official rounds", flush=True)
    predictions = pd.DataFrame(records)
    # Independent check against the previous study, including the current quote aggregation.
    old = pd.read_csv(source / "metrics_fixed_420.csv")
    old = old.loc[old.form.eq("legacy") & old.book_weight.eq(1.0)].set_index("match_id")
    check = predictions.loc[predictions.strategy.eq("baseline")].set_index("match_id").loc[old.index]
    for current, previous in (("points", "tip_points"), ("tip_home", "tip_home"), ("tip_away", "tip_away")):
        assert np.array_equal(check[current], old[previous]), current
    predictions.to_csv(output / "predictions.csv", index=False)
    rounds = predictions.groupby(["strategy", "season", "matchday"]).agg(
        points=("points", "sum"), baseline_points=("baseline_points", "sum"),
        changes=("changed", "sum"), expected_cost=("expected_cost", "sum"),
        added_draws=("added_draw", "sum"), games=("match_id", "size")).reset_index()
    assert rounds.games.eq(9).all()
    assert rounds.changes.le(2).all() and rounds.added_draws.le(1).all()
    assert rounds.expected_cost.le(config.expected_points_budget + 1e-12).all()
    # Whole-card short-memory comparisons are descriptive, outside the selection pool.
    comparisons = []
    base_rounds = rounds.loc[rounds.strategy.eq("baseline"), ["season", "matchday", "baseline_points"]]
    for h in SIGNALS[:3]:
        other = pd.read_csv(source / f"metrics_fixed_{h}.csv")
        other = other.loc[other.form.eq("legacy") & other.book_weight.eq(0.0)]
        other = other.merge(check[["tip_home", "tip_away"]], on="match_id", suffixes=("", "_base"), validate="many_to_one")
        other["changed"] = other.tip_home.ne(other.tip_home_base) | other.tip_away.ne(other.tip_away_base)
        total = other.groupby(["season", "matchday"]).agg(points=("tip_points", "sum"), changes=("changed", "sum"), games=("match_id", "size")).reset_index()
        total = total.merge(base_rounds, on=["season", "matchday"], validate="one_to_one")
        comparisons.append(total.assign(strategy=f"full_{h}", expected_cost=np.nan, added_draws=np.nan))
    all_rounds = pd.concat([rounds, *comparisons], ignore_index=True)
    all_rounds["better"] = all_rounds.points.gt(all_rounds.baseline_points)
    all_rounds["equal"] = all_rounds.points.eq(all_rounds.baseline_points)
    all_rounds["worse"] = all_rounds.points.lt(all_rounds.baseline_points)
    choices = pd.DataFrame([choose_for_season(rounds, s) for s in range(2016, 2027)])
    chosen = all_rounds.merge(choices.loc[choices.season.le(2025), ["season", "strategy"]],
                              on=["season", "strategy"], validate="many_to_one")
    chosen["selected_signal"] = chosen.strategy
    chosen["strategy"] = "rolling_selection"
    test_rounds = pd.concat([all_rounds.loc[all_rounds.season.ge(2016)], chosen], ignore_index=True)
    all_summary = aggregate(all_rounds, ["strategy"])
    test_summary = aggregate(test_rounds, ["strategy"])
    per_season = aggregate(all_rounds, ["strategy", "season"])
    test_seasons = aggregate(test_rounds, ["strategy", "season"])
    rng = np.random.default_rng(20260909)
    indices = rng.integers(0, 10, size=(10000, 10))
    anchor = test_seasons.loc[test_seasons.strategy.eq("baseline")].set_index("season").sort_index()
    uncertainty = []
    for strategy, group in test_seasons.loc[~test_seasons.strategy.eq("baseline")].groupby("strategy"):
        group = group.set_index("season").sort_index()
        for metric in ("mean", "at_least_22", "at_least_24"):
            delta = (group[metric] - anchor[metric]).to_numpy()
            low, high = np.quantile(delta[indices].mean(axis=1), [.025, .975])
            uncertainty.append(dict(strategy=strategy, metric=metric, difference=float(delta.mean()),
                                    ci_025=low, ci_975=high))
    intervals = pd.DataFrame(uncertainty)
    for name, table in (("all_rounds", all_rounds), ("all_summary", all_summary),
                        ("per_season", per_season), ("rolling_choices", choices),
                        ("test_rounds", test_rounds), ("test_summary", test_summary),
                        ("test_per_season", test_seasons), ("uncertainty", intervals)):
        table.to_csv(output / f"{name}.csv", index=False)
    with pd.ExcelWriter(output / "sieben_plus_zwei.xlsx", engine="openpyxl") as writer:
        for name, table in (("15-Saisons", all_summary), ("10-Testsaisons", test_summary),
                            ("Auswahl", choices), ("Je-Saison", per_season),
                            ("Unsicherheit", intervals), ("Spieltage", test_rounds),
                            ("Geaenderte-Tipps", predictions.loc[predictions.changed])):
            table.to_excel(writer, sheet_name=name, index=False)
            sheet = writer.sheets[name]
            sheet.freeze_panes = "A2"
            sheet.auto_filter.ref = sheet.dimensions
    print(test_summary.to_string(index=False), flush=True)
    print("Selection 2026/27:", choices.iloc[-1].to_dict(), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DATA_DIR / "half_life_15_seasons")
    parser.add_argument("--output", type=Path, default=DATA_DIR / "limited_risk_15_seasons")
    args = parser.parse_args()
    run(args.source, args.output)


if __name__ == "__main__":
    main()
