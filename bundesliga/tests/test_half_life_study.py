from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from bundesliga.form_residuals import residual_form
from bundesliga.half_life_study import half_life, fast_tip, rolling_choices
from bundesliga.scoring import score_matrix, expected_points_tip


def test_seasonal_memory_endpoints_and_direction():
    assert half_life("season_630_180", 1) == 630
    assert half_life("season_630_180", 34) == 180
    assert half_life("season_180_630", 1) == 180
    assert half_life("season_180_630", 34) == 630
    assert half_life("season_630_180", 10) > half_life("season_630_180", 20)
    assert half_life("fixed_420", 34) == 420
    with pytest.raises(ValueError):
        half_life("season_630_180", 35)


def history():
    return pd.DataFrame([dict(match_id="one", home_team="A", away_team="B",
                              match_date=pd.Timestamp("2020-01-04"),
                              as_of=pd.Timestamp("2020-01-01"),
                              home_goals=1, away_goals=1, base_home=0.6, base_away=2.4)])


def test_draw_against_strong_opponent_is_positive_residual():
    matches = history()
    as_of = pd.Timestamp("2020-01-10")
    strong = residual_form(matches, "A", as_of)
    assert strong > 0
    assert residual_form(matches, "B", as_of) == pytest.approx(-strong)
    weaker = matches.assign(base_home=2.4, base_away=0.6)
    assert residual_form(weaker, "A", as_of) < 0


def test_unplayed_and_post_match_forecasts_cannot_enter_form():
    matches = history()
    as_of = pd.Timestamp("2020-01-10")
    expected = residual_form(matches, "A", as_of)
    future_result = matches.assign(match_id="two", match_date=pd.Timestamp("2020-02-01"), home_goals=9)
    invalid_forecast = matches.assign(match_id="three", as_of=pd.Timestamp("2020-01-05"), home_goals=9)
    combined = pd.concat([matches, future_result, invalid_forecast], ignore_index=True)
    assert residual_form(combined, "A", as_of) == expected
    assert residual_form(future_result, "A", as_of) == 0
    assert residual_form(matches.assign(home_goals=np.nan), "A", as_of) == 0


def test_date_only_same_day_forecast_is_usable_after_match_day():
    matches = history()
    matches["as_of"] = matches.match_date
    assert residual_form(matches, "A", pd.Timestamp("2020-01-10")) > 0


def test_form_does_not_reintroduce_results_outside_training_lookback():
    assert residual_form(history(), "A", pd.Timestamp("2027-01-10")) == 0


def test_fast_tip_matches_existing_scoring_on_varied_distributions():
    rng = np.random.default_rng(53)
    for _ in range(60):
        h, a = rng.uniform(0.15, 4.5, size=2)
        matrix = score_matrix(h, a, rng.uniform(-0.15, 0.05))
        assert fast_tip(matrix) == expected_points_tip(matrix)


def test_rolling_selection_never_uses_current_or_future_season():
    rows = []
    for year in range(2011, 2026):
        for weight in (0.0, 0.5, 1.0):
            for policy in ("fixed_180", "fixed_420"):
                rows.append(dict(season=year, book_weight=weight, policy=policy,
                                 form="legacy", brier_score=0.6,
                                 log_loss=3.0 if policy == "fixed_420" else 3.1))
    metrics = pd.DataFrame(rows)
    policies = ["fixed_180", "fixed_420"]
    original = rolling_choices(metrics, policies, ["legacy"])
    metrics.loc[metrics.season.ge(2020) & metrics.policy.eq("fixed_180"), "log_loss"] = 0
    changed = rolling_choices(metrics, policies, ["legacy"])
    pd.testing.assert_frame_equal(original.loc[original.season.le(2020)],
                                  changed.loc[changed.season.le(2020)])
    assert changed.loc[changed.season.eq(2021), "policy"].eq("fixed_180").all()
    assert changed.selection_end.eq(changed.season - 1).all()
