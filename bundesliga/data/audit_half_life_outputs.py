"""Independent output checks against the previously frozen eight-season audit."""
from pathlib import Path
import json
import numpy as np
import pandas as pd
from bundesliga.half_life_study import fast_tip
from bundesliga.scoring import score_matrix, blend_outcome_probabilities
from bundesliga.database import connect

root = Path(__file__).resolve().parent
output = root / "half_life_15_seasons"
metrics = pd.read_csv(output / "metrics_fixed_420.csv")
baseline = metrics.loc[metrics.form.eq("legacy") & metrics.book_weight.eq(1.0)]
report = {}
for period in ("2018_2021", "2022_2025"):
    old = pd.read_csv(root / f"backtest_d1_{period}_official_frozen.csv")
    merged = old.merge(baseline, on="match_id", suffixes=("_old", "_new"), validate="one_to_one")
    assert len(merged) == 1224
    years = range(int(period[:4]), int(period[-4:]) + 1)
    base = pd.concat([pd.read_csv(output / f"base_fixed_420_{s}.csv") for s in years])
    pair = old.merge(base, on="match_id", suffixes=("_old", "_new"), validate="one_to_one")
    home = np.exp(np.clip(pair.base_log_home + .1 * (pair.legacy_home - .5 * pair.legacy_away), -4, 3))
    away = np.exp(np.clip(pair.base_log_away + .1 * (pair.legacy_away - .5 * pair.legacy_home), -4, 3))
    assert np.max(abs(home - pair.lambda_home)) < 1e-12
    assert np.max(abs(away - pair.lambda_away)) < 1e-12
    assert np.max(abs(pair.rho_new - pair.rho_old)) < 1e-12
    old_quote_array = pair[[f"book_prob_{o}_old" for o in ("home", "draw", "away")]].to_numpy()
    new_quote_array = pair[[f"book_prob_{o}_new" for o in ("home", "draw", "away")]].to_numpy()
    assert np.array_equal(np.isnan(old_quote_array), np.isnan(new_quote_array))
    quote_error = np.nan_to_num(np.abs(old_quote_array - new_quote_array)).max(axis=1)
    changed = quote_error > 1e-12
    changed_ids = set(pair.loc[changed, "match_id"])
    if changed_ids:
        assert pair.loc[changed, "season_old"].eq(2018).all()
        # Historical CSV used a mean over all opening entries, including BbMx.
        # The current helper prefers BbAv, the actual market average.
        with connect() as con:
            raw = pd.read_sql_query("SELECT o.* FROM bookmaker_odds o JOIN matches m USING(match_id) "
                                    "WHERE m.competition='D1' AND m.season=2018 "
                                    "AND o.snapshot_type='opening'", con)
        inverse = 1 / raw[["home_odds", "draw_odds", "away_odds"]].to_numpy()
        inverse /= inverse.sum(axis=1, keepdims=True)
        raw[["h", "d", "a"]] = inverse
        previous_mean = raw.groupby("match_id")[["h", "d", "a"]].mean()
        old_quotes = old.set_index("match_id").loc[previous_mean.index,
                    ["book_prob_home", "book_prob_draw", "book_prob_away"]]
        assert np.max(abs(previous_mean.to_numpy() - old_quotes.to_numpy())) < 1e-12
    unchanged = merged.loc[~merged.match_id.isin(changed_ids)]
    for name in ("tip_home", "tip_away", "tip_points"):
        assert unchanged[f"{name}_old"].eq(unchanged[f"{name}_new"]).all(), (period, name)
    # Hold old quotes fixed: the same model reproduces ALL historical tips.
    for r in old.itertuples(index=False):
        matrix = blend_outcome_probabilities(score_matrix(r.lambda_home, r.lambda_away, r.rho),
            (r.book_prob_home, r.book_prob_draw, r.book_prob_away), r.bookmaker_weight)
        assert fast_tip(matrix) == (r.tip_home, r.tip_away)
    errors = {}
    for name in ("log_loss", "brier_score"):
        error = float(np.abs(unchanged[f"{name}_old"] - unchanged[f"{name}_new"]).max())
        assert error < 1e-6, (period, name, error)
        errors[name] = error
    report[period] = {"verified_model_predictions": len(merged), "max_absolute_errors_same_quotes": errors,
                      "changed_quote_aggregation_games": len(changed_ids),
                      "changed_tips_due_to_quotes": int((merged.tip_home_old.ne(merged.tip_home_new)
                                                        | merged.tip_away_old.ne(merged.tip_away_new)).sum())}

bases = pd.concat([pd.read_csv(p, parse_dates=["as_of", "match_date"])
                   for p in output.glob("base_*.csv")], ignore_index=True)
assert len(bases) == 9 * 16 * 306
assert bases.as_of.le(bases.match_date).all()
assert bases.fit_iterations.gt(0).all()
assert np.isfinite(bases[["base_home", "base_away", "rho"]]).all().all()
report["fits"] = {"completed": 9 * 16 * 34, "match_forecasts_including_warmup": len(bases),
                  "max_iterations": int(bases.fit_iterations.max()),
                  "min_iterations": int(bases.fit_iterations.min())}
report["tests"] = "pytest tests, plus chronological selection and residual leakage tests"
(output / "independent_audit.json").write_text(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
