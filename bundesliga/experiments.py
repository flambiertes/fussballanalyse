from __future__ import annotations

import argparse
import json
from dataclasses import asdict, replace
from pathlib import Path

import numpy as np
import pandas as pd

from .backtest import run_backtest, summarize
from .config import ModelConfig
from .scoring import (
    blend_outcome_probabilities,
    evaluate_prediction,
    expected_points_tip,
    outcome_probabilities,
    score_matrix,
)


DEFAULT_VARIANTS = {
    "baseline": {"form_weight": 0.0, "venue_form_weight": 0.0, "h2h_weight": 0.0},
    "form_005": {"form_weight": 0.05, "venue_form_weight": 0.0, "h2h_weight": 0.0},
    "form_010": {"form_weight": 0.10, "venue_form_weight": 0.0, "h2h_weight": 0.0},
    "form_015": {"form_weight": 0.15, "venue_form_weight": 0.0, "h2h_weight": 0.0},
    "form_010_venue_005": {"form_weight": 0.10, "venue_form_weight": 0.05, "h2h_weight": 0.0},
    "h2h_005": {"form_weight": 0.0, "venue_form_weight": 0.0, "h2h_weight": 0.05},
    "h2h_010": {"form_weight": 0.0, "venue_form_weight": 0.0, "h2h_weight": 0.10},
    "form_010_h2h_005": {"form_weight": 0.10, "venue_form_weight": 0.0, "h2h_weight": 0.05},
    "market_005": {"market_value_weight": 0.05},
    "market_010": {"market_value_weight": 0.10},
    "market_015": {"market_value_weight": 0.15},
    "market_020": {"market_value_weight": 0.20},
    "market_030": {"market_value_weight": 0.30},
    "market_040": {"market_value_weight": 0.40},
    "bookmaker_025": {"bookmaker_weight": 0.25},
    "bookmaker_050": {"bookmaker_weight": 0.50},
    "bookmaker_075": {"bookmaker_weight": 0.75},
    "bookmaker_100": {"bookmaker_weight": 1.00},
    "form_h2h_market_005": {
        "form_weight": 0.10,
        "venue_form_weight": 0.0,
        "h2h_weight": 0.05,
        "market_value_weight": 0.05,
    },
    "form_h2h_market_010": {
        "form_weight": 0.10,
        "venue_form_weight": 0.0,
        "h2h_weight": 0.05,
        "market_value_weight": 0.10,
    },
    "form_h2h_market_020": {
        "form_weight": 0.10,
        "venue_form_weight": 0.0,
        "h2h_weight": 0.05,
        "market_value_weight": 0.20,
    },
    "form_h2h_market_030": {
        "form_weight": 0.10,
        "venue_form_weight": 0.0,
        "h2h_weight": 0.05,
        "market_value_weight": 0.30,
    },
    "form_h2h_market_book_025": {
        "form_weight": 0.10,
        "venue_form_weight": 0.0,
        "h2h_weight": 0.05,
        "market_value_weight": 0.05,
        "bookmaker_weight": 0.25,
    },
    "form_h2h_market_book_050": {
        "form_weight": 0.10,
        "venue_form_weight": 0.0,
        "h2h_weight": 0.05,
        "market_value_weight": 0.05,
        "bookmaker_weight": 0.50,
    },
    "form_h2h_market_book_075": {
        "form_weight": 0.10,
        "venue_form_weight": 0.0,
        "h2h_weight": 0.05,
        "market_value_weight": 0.05,
        "bookmaker_weight": 0.75,
    },
}


def apply_feature_variant(
    baseline: pd.DataFrame,
    config: ModelConfig,
) -> pd.DataFrame:
    result = baseline.copy()
    home_adjustment = (
        config.form_weight * (result["home_form"] - 0.5 * result["away_form"])
        + config.venue_form_weight * result["home_venue_form"]
        + config.h2h_weight * result["h2h"]
        + 0.5 * config.market_value_weight * result["market_log_ratio"] * result["market_decay"]
    )
    away_adjustment = (
        config.form_weight * (result["away_form"] - 0.5 * result["home_form"])
        + config.venue_form_weight * result["away_venue_form"]
        - config.h2h_weight * result["h2h"]
        - 0.5 * config.market_value_weight * result["market_log_ratio"] * result["market_decay"]
    )
    result["lambda_home"] = result["lambda_home"] * np.exp(home_adjustment)
    result["lambda_away"] = result["lambda_away"] * np.exp(away_adjustment)

    computed = []
    for row in result.itertuples(index=False):
        matrix = score_matrix(row.lambda_home, row.lambda_away, row.rho, config.max_goals)
        bookmaker = tuple(
            float(getattr(row, column, np.nan))
            for column in ("book_prob_home", "book_prob_draw", "book_prob_away")
        )
        matrix = blend_outcome_probabilities(
            matrix,
            bookmaker if np.all(np.isfinite(bookmaker)) else None,
            config.bookmaker_weight,
        )
        prob_home, prob_draw, prob_away = outcome_probabilities(matrix)
        tip = expected_points_tip(matrix)
        metrics = evaluate_prediction(matrix, tip, (int(row.actual_home), int(row.actual_away)))
        computed.append((prob_home, prob_draw, prob_away, tip[0], tip[1], metrics))
    result["prob_home"] = [item[0] for item in computed]
    result["prob_draw"] = [item[1] for item in computed]
    result["prob_away"] = [item[2] for item in computed]
    result["tip_home"] = [item[3] for item in computed]
    result["tip_away"] = [item[4] for item in computed]
    for metric in ("tip_points", "log_loss", "brier_score"):
        result[metric] = [item[5][metric] for item in computed]
    return result


def compare_variants(
    baseline: pd.DataFrame,
    base_config: ModelConfig | None = None,
    variants: dict[str, dict[str, float]] | None = None,
) -> pd.DataFrame:
    base_config = base_config or ModelConfig()
    variants = variants or DEFAULT_VARIANTS
    rows = []
    for name, changes in variants.items():
        config = replace(base_config, **changes)
        predictions = apply_feature_variant(baseline, config)
        summary = summarize(predictions, name, config)
        rows.append(
            {
                "variant": name,
                "points": summary["points"],
                "points_per_match": summary["points_per_match"],
                "exact_rate": summary["exact_rate"],
                "correct_tendency_rate": summary["correct_tendency_rate"],
                "mean_log_loss": summary["mean_log_loss"],
                "mean_brier_score": summary["mean_brier_score"],
                **changes,
            }
        )
    return pd.DataFrame(rows).sort_values(
        ["mean_brier_score", "mean_log_loss", "points_per_match"],
        ascending=[True, True, False],
    ).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Form- und H2H-Features auf Validierungssaisons vergleichen")
    parser.add_argument("--league", choices=["D1", "D2"], default="D1")
    parser.add_argument("--seasons", nargs="+", type=int, required=True)
    parser.add_argument("--baseline-csv", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    config = ModelConfig()
    if args.baseline_csv and args.baseline_csv.exists():
        baseline = pd.read_csv(args.baseline_csv, parse_dates=["match_date", "as_of"])
    else:
        baseline, _ = run_backtest(
            args.league, args.seasons, config=config, persist=False, verbose=False
        )
        if args.baseline_csv:
            args.baseline_csv.parent.mkdir(parents=True, exist_ok=True)
            baseline.to_csv(args.baseline_csv, index=False)
    comparison = compare_variants(baseline, config)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        comparison.to_csv(args.output, index=False)
    print(comparison.to_string(index=False))


if __name__ == "__main__":
    main()
