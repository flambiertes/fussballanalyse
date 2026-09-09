from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import asdict, replace
from pathlib import Path

import numpy as np
import pandas as pd

from .config import DB_PATH, ModelConfig
from .contest import ContestConfig, contest_tips
from .database import (
    consensus_bookmaker_probabilities,
    latest_market_values,
    load_matches,
    save_predictions,
)
from .model import MODEL_VERSION, DynamicDixonColes
from .priors import lower_league_priors
from .rounds import round_groups
from .scoring import evaluate_prediction, target_points_tips


def match_round(date: pd.Timestamp) -> pd.Timestamp:
    """Dienstag bis Montag bilden einen konservativen Bundesliga-Spieltag."""
    days_since_tuesday = (date.weekday() - 1) % 7
    return date.normalize() - pd.Timedelta(days=days_since_tuesday)


def prediction_groups(test: pd.DataFrame, tip_strategy: str):
    """Freeze each official round at its earliest fixture's Tuesday cutoff."""
    scope, groups = round_groups(test)
    if scope != "official_matchdays" and tip_strategy != "expected-points":
        raise ValueError("Turnierstrategien benoetigen offizielle Spieltagsnummern")
    batches = [(match_round(group.match_date.min()), group) for _, group in groups]
    return scope, sorted(batches, key=lambda pair: (pair[0], pair[1].match_date.min()))


def run_backtest(
    competition: str,
    test_seasons: list[int],
    config: ModelConfig | None = None,
    db_path: Path | str = DB_PATH,
    persist: bool = True,
    run_id: str | None = None,
    verbose: bool = True,
    tip_strategy: str = "expected-points",
    target_points: int = 24,
    contest_config: ContestConfig | None = None,
    matchdays: list[int] | None = None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    config = config or ModelConfig()
    if tip_strategy not in {"expected-points", "target-score", "contest"}:
        raise ValueError("tip_strategy muss expected-points, target-score oder contest sein")
    contest_config = contest_config or ContestConfig()
    all_matches = load_matches(competition=competition, finished_only=True, path=db_path)
    lower_matches = (
        load_matches(competition="D2", finished_only=True, path=db_path)
        if competition == "D1" and config.use_lower_league_priors
        else pd.DataFrame()
    )
    if all_matches.empty:
        raise ValueError(f"Keine abgeschlossenen Spiele fuer {competition} in {db_path}")
    test = all_matches[all_matches["season"].isin(test_seasons)].copy()
    if matchdays is not None:
        test = test[test["matchday"].isin(matchdays)].copy()
    if test.empty:
        raise ValueError(f"Keine Testspiele fuer Saisons {test_seasons}")
    test["as_of"] = test["match_date"].map(match_round)
    records = []

    grouping_scope, grouped = prediction_groups(test, tip_strategy)
    for number, (round_start, round_matches) in enumerate(grouped, start=1):
        training = all_matches[all_matches["match_date"] < round_start].copy()
        model = DynamicDixonColes(config).fit(training, as_of=round_start)
        promoted_priors = {}
        if config.use_lower_league_priors:
            current_teams = set(round_matches["home_team"]) | set(round_matches["away_team"])
            promoted_priors = lower_league_priors(
                model, lower_matches, all_matches, current_teams,
                pd.Timestamp(round_start), config,
            )
        # Auch bei Gewicht 0 laden: So speichert ein Baseline-Lauf das rohe
        # Marktwertsignal und mehrere Gewichte koennen ohne erneuten Fit
        # verglichen werden.
        markets = latest_market_values(round_start, path=db_path)
        # Untimestamped opening/closing quotes for a postponed fixture cannot be
        # assumed known at the original round cutoff. Captures are filtered by as_of.
        eligible = round_matches["match_date"].map(match_round).eq(round_start)
        odds_ids = round_matches["match_id"] if config.bookmaker_snapshot_type == "captured" else round_matches.loc[eligible, "match_id"]
        round_odds = consensus_bookmaker_probabilities(
            odds_ids,
            snapshot_type=config.bookmaker_snapshot_type,
            as_of=round_start,
            path=db_path,
        ).set_index("match_id")
        if verbose:
            print(
                f"[{number:>3}/{len(grouped)}] {round_start.date()} | "
                f"Training {len(model.training_matches):>4} | "
                f"rho {model.rho:+.3f} | Heim {model.home_advantage:+.3f} | "
                f"Aufsteiger-Priors {len(promoted_priors)}"
            )
        round_records = []
        round_matrices = []
        for match in round_matches.itertuples(index=False):
            bookmaker = round_odds.loc[match.match_id] if match.match_id in round_odds.index else None
            prediction = model.predict(
                match.home_team, match.away_team, markets,
                bookmaker_probabilities=bookmaker,
            )
            actual = (int(match.home_goals), int(match.away_goals))
            tip = (int(prediction["tip_home"]), int(prediction["tip_away"]))
            metrics = evaluate_prediction(prediction["score_matrix"], tip, actual)
            round_matrices.append(prediction["score_matrix"])
            round_records.append(
                {
                    "match_id": match.match_id,
                    "competition": match.competition,
                    "season": int(match.season),
                    "matchday": match.matchday,
                    "match_date": match.match_date,
                    "as_of": round_start,
                    "round_grouping": grouping_scope,
                    "odds_withheld_for_late_fixture": (
                        config.bookmaker_snapshot_type != "captured"
                        and match_round(pd.Timestamp(match.match_date)) != round_start
                    ),
                    "home_team": match.home_team,
                    "away_team": match.away_team,
                    "actual_home": actual[0],
                    "actual_away": actual[1],
                    "lambda_home": prediction["lambda_home"],
                    "lambda_away": prediction["lambda_away"],
                    "prob_home": prediction["prob_home"],
                    "prob_draw": prediction["prob_draw"],
                    "prob_away": prediction["prob_away"],
                    "tip_home": tip[0],
                    "tip_away": tip[1],
                    **metrics,
                    **prediction["features"],
                    "home_external_prior": match.home_team in model.external_team_parameters,
                    "away_external_prior": match.away_team in model.external_team_parameters,
                    "rho": model.rho,
                    "home_advantage": model.home_advantage,
                    "training_matches": len(model.training_matches),
                }
            )

        # Every official round uses one common cutoff, including postponed games.
        for record in round_records:
            record["tip_strategy"] = "expected-points"
        if tip_strategy in {"target-score", "contest"} and len(round_records) == 9:
            if tip_strategy == "contest":
                tips, report = contest_tips(round_matrices, contest_config)
                target_probability = np.nan
            else:
                tips, target_probability = target_points_tips(
                    round_matrices, target_points=target_points
                )
            for record, matrix, tip in zip(round_records, round_matrices, tips):
                actual = (int(record["actual_home"]), int(record["actual_away"]))
                record["tip_home"], record["tip_away"] = tip
                record.update(evaluate_prediction(matrix, tip, actual))
                record["target_probability"] = target_probability
                record["tip_strategy"] = tip_strategy
                if tip_strategy == "contest":
                    record["contest_changed_tendencies"] = report["changed_tendencies"]
                    record["contest_evaluation_log_objective"] = report["evaluation_log_objective"]
                    record["contest_baseline_log_objective"] = report["baseline_evaluation_log_objective"]
        else:
            for record in round_records:
                record["target_probability"] = np.nan
        records.extend(round_records)

    predictions = pd.DataFrame(records).sort_values(["match_date", "match_id"]).reset_index(drop=True)
    run_id = run_id or f"{competition.lower()}-{uuid.uuid4().hex[:12]}"
    if persist:
        persisted_config = {
            **asdict(config),
            "tip_strategy": tip_strategy,
            "target_points": target_points,
            "contest_config": asdict(contest_config) if tip_strategy == "contest" else None,
        }
        save_predictions(predictions, run_id, MODEL_VERSION, persisted_config, path=db_path)
    summary = summarize(predictions, run_id, config)
    summary["tip_strategy"] = tip_strategy
    summary["target_points"] = target_points
    if tip_strategy == "contest":
        summary["contest_config"] = asdict(contest_config)
        summary["contest_crowd_source"] = "assumed; no observed participant data"
        summary["contest_tie_convention"] = "equal prize share / uniform lottery"
    return predictions, summary


def summarize(predictions: pd.DataFrame, run_id: str, config: ModelConfig) -> dict[str, object]:
    actual_diff = predictions["actual_home"] - predictions["actual_away"]
    tip_diff = predictions["tip_home"] - predictions["tip_away"]
    grouping_scope, groups = round_groups(predictions)
    complete_groups = [group for _, group in groups if len(group) == 9]
    matchday_points = pd.Series([group["tip_points"].sum() for group in complete_groups], dtype=float)
    summary = {
        "run_id": run_id,
        "matches": len(predictions),
        "points": int(predictions["tip_points"].sum()),
        "points_per_match": round(float(predictions["tip_points"].mean()), 4),
        "points_percent": round(float(predictions["tip_points"].mean() / 4 * 100), 2),
        "exact_rate": round(float((predictions["tip_points"] == 4).mean()), 4),
        "correct_tendency_rate": round(float((np.sign(actual_diff) == np.sign(tip_diff)).mean()), 4),
        "mean_log_loss": round(float(predictions["log_loss"].mean()), 4),
        "mean_brier_score": round(float(predictions["brier_score"].mean()), 4),
        "actual_goals_per_match": round(float((predictions["actual_home"] + predictions["actual_away"]).mean()), 4),
        "expected_goals_per_match": round(float((predictions["lambda_home"] + predictions["lambda_away"]).mean()), 4),
        "complete_matchdays": int(len(matchday_points)),
        "matchday_grouping": grouping_scope,
        "incomplete_groups": len(groups) - len(complete_groups),
        "rounds_with_multiple_forecast_times": sum(group["as_of"].nunique() > 1 for group in complete_groups),
        "mean_matchday_points": (
            round(float(matchday_points.mean()), 4) if len(matchday_points) else None
        ),
        "matchday_points_std": (
            round(float(matchday_points.std(ddof=0)), 4) if len(matchday_points) else None
        ),
        "best_matchday_points": (
            int(matchday_points.max()) if len(matchday_points) else None
        ),
        "config": asdict(config),
    }
    for threshold in (18, 20, 22, 24):
        hits = int((matchday_points >= threshold).sum())
        summary[f"matchdays_ge_{threshold}"] = hits
        summary[f"matchday_rate_ge_{threshold}"] = round(
            hits / max(len(matchday_points), 1), 4
        )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Leakage-freier Bundesliga-Walk-forward-Backtest")
    parser.add_argument("--league", choices=["D1", "D2"], default="D1")
    parser.add_argument("--seasons", nargs="+", type=int, required=True)
    parser.add_argument("--matchdays", nargs="+", type=int)
    parser.add_argument("--half-life-days", type=float, default=ModelConfig.half_life_days)
    parser.add_argument("--lookback-years", type=float, default=ModelConfig.lookback_years)
    parser.add_argument("--ridge", type=float, default=ModelConfig.ridge)
    parser.add_argument("--form-weight", type=float, default=0.0)
    parser.add_argument("--venue-form-weight", type=float, default=0.0)
    parser.add_argument("--h2h-weight", type=float, default=0.0)
    parser.add_argument("--market-weight", type=float, default=0.0)
    parser.add_argument("--bookmaker-weight", type=float, default=0.0)
    parser.add_argument(
        "--bookmaker-snapshot", choices=["opening", "closing", "captured"],
        default=ModelConfig.bookmaker_snapshot_type,
    )
    parser.add_argument("--lower-league-priors", action="store_true")
    parser.add_argument("--promotion-penalty", type=float, default=0.20)
    parser.add_argument(
        "--tip-strategy", choices=["expected-points", "target-score", "contest"],
        default="expected-points",
    )
    parser.add_argument("--target-points", type=int, default=24)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    config = replace(
        ModelConfig(),
        half_life_days=args.half_life_days,
        lookback_years=args.lookback_years,
        ridge=args.ridge,
        form_weight=args.form_weight,
        venue_form_weight=args.venue_form_weight,
        h2h_weight=args.h2h_weight,
        market_value_weight=args.market_weight,
        bookmaker_weight=args.bookmaker_weight,
        bookmaker_snapshot_type=args.bookmaker_snapshot,
        use_lower_league_priors=args.lower_league_priors,
        promotion_attack_penalty=-abs(args.promotion_penalty),
        promotion_defense_penalty=abs(args.promotion_penalty),
    )
    predictions, summary = run_backtest(
        args.league, args.seasons, config=config, verbose=not args.quiet,
        tip_strategy=args.tip_strategy, target_points=args.target_points,
        matchdays=args.matchdays,
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        predictions.to_csv(args.output, index=False)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
