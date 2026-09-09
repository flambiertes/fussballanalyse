"""Replay tip selection on saved walk-forward probabilities without refitting."""
from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from .contest import ContestConfig, contest_tips
from .database import load_matches
from .rounds import attach_matchdays, round_groups
from .scoring import blend_outcome_probabilities, expected_points_tip, score_matrix, tip_points


def replay(input_path: Path, output_path: Path) -> dict:
    predictions = pd.read_csv(input_path)
    required = {
        "as_of", "match_id", "lambda_home", "lambda_away", "rho", "prob_home",
        "prob_draw", "prob_away", "actual_home", "actual_away", "tip_home", "tip_away",
        "competition", "season", "home_team", "away_team", "match_date",
    }
    if not required <= set(predictions):
        raise ValueError(f"Fehlende Spalten: {sorted(required - set(predictions))}")
    if predictions.duplicated(["as_of", "match_id"]).any():
        raise ValueError("Doppelte Prognosen im Eingabelauf")
    if "matchday" not in predictions or predictions.matchday.isna().any():
        predictions = attach_matchdays(predictions, load_matches())
    config = ContestConfig()
    rounds = []
    _, groups = round_groups(predictions)
    # Reject stale mixed-time replays before any expensive simulation work.
    if any(group.as_of.nunique() != 1 for _, group in groups):
        raise ValueError("Spieltag enthaelt mehrere Prognosezeitpunkte; offiziellen Backtest erneut berechnen")
    if any(pd.Timestamp(group.as_of.iloc[0]) > pd.to_datetime(group.match_date).min() for _, group in groups):
        raise ValueError("Prognosezeitpunkt liegt nach Beginn des Spieltags")
    for number, (_, group) in enumerate(groups, 1):
        if len(group) != 9:
            continue
        as_of = group.as_of.iloc[0]
        matrices = [blend_outcome_probabilities(
            score_matrix(row.lambda_home, row.lambda_away, row.rho),
            (row.prob_home, row.prob_draw, row.prob_away), 1.0,
        ) for row in group.itertuples()]
        tips, report = contest_tips(matrices, config)
        # Actual results enter only AFTER the portfolio has been chosen.
        actuals = [(int(row.actual_home), int(row.actual_away)) for row in group.itertuples()]
        # Use the same baseline as the contest report, independent of input tips.
        baseline = [expected_points_tip(matrix) for matrix in matrices]
        rounds.append({
            "as_of": as_of,
            "competition": group.competition.iloc[0],
            "season": int(group.season.iloc[0]),
            "matchday": int(group.matchday.iloc[0]),
            "baseline_points": sum(tip_points(t, a) for t, a in zip(baseline, actuals)),
            "contest_points": sum(tip_points(t, a) for t, a in zip(tips, actuals)),
            "changed_tendencies": report["changed_tendencies"],
            "evaluation_log_objective": report["evaluation_log_objective"],
            "baseline_evaluation_log_objective": report["baseline_evaluation_log_objective"],
            "scenarios": json.dumps(report["scenarios"]),
            "tips": json.dumps([
                {"match_id": mid, "tip_home": tip[0], "tip_away": tip[1]}
                for mid, tip in zip(group.match_id, tips)
            ]),
        })
        if number % 10 == 0:
            print(f"{number}/{len(groups)} Kalenderfenster bearbeitet", flush=True)
    result = pd.DataFrame(rounds)
    if result.empty:
        raise ValueError("Keine vollstaendigen Neun-Spiele-Fenster")
    summary = {
        "source": str(input_path), "config": asdict(config),
        "complete_rounds": len(result), "skipped_incomplete_matchdays": len(groups) - len(result),
        "matchday_grouping": "official_matchdays",
        "interpretation": "Historical points and assumed crowd scenarios; no real winning rates",
        "mean_changed_tendencies": float(result.changed_tendencies.mean()),
        "fraction_evaluation_objective_above_baseline": float((
            result.evaluation_log_objective > result.baseline_evaluation_log_objective
        ).mean()),
    }
    for strategy in ("baseline", "contest"):
        scores = result[strategy + "_points"]
        summary[strategy] = {
            "mean_points": float(scores.mean()), "std_points": float(scores.std(ddof=0)),
            "best_points": int(scores.max()),
            **{f"rounds_ge_{threshold}": int((scores >= threshold).sum())
               for threshold in (18, 20, 22, 24)},
        }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)
    output_path.with_suffix(".json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(replay(args.input, args.output), indent=2))


if __name__ == "__main__":
    main()
