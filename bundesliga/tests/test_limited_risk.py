from dataclasses import replace

import numpy as np
import pandas as pd
import pytest

from bundesliga.limited_risk import LimitedRiskConfig, limited_risk_tips
from bundesliga.limited_risk_study import SELECTABLE, choose_for_season


def matrix(home, draw, away):
    m = np.zeros((11, 11))
    m[1, 0], m[1, 1], m[0, 1] = home, draw, away
    return m


def test_keeps_seven_and_respects_joint_budget():
    base = [matrix(.4, .28, .32)] * 9
    signal = [matrix(.35, .2, .45)] * 9
    tips, report = limited_risk_tips(base, signal, [str(i) for i in range(9)])
    assert sum(t != (1, 0) for t in tips) == 2
    assert report["expected_points_cost"] == pytest.approx(.64)
    assert {r["match_id"] for r in report["changes"]} == {"0", "1"}
    _, restricted = limited_risk_tips(base, signal, list(map(str, range(9))),
                                    replace(LimitedRiskConfig(), expected_points_budget=.5))
    assert restricted["changes_count"] == 1


def test_only_one_added_draw_and_can_choose_non_favorite():
    base = [matrix(.4, .35, .25)] * 9
    signal = [matrix(.3, .5, .2)] * 9
    _, report = limited_risk_tips(base, signal, list(map(str, range(9))))
    assert report["changes_count"] == 1 and report["added_draws"] == 1
    base = [matrix(.5, .15, .35)] * 9
    signal = [matrix(.48, .09, .43)] * 9
    tips, report = limited_risk_tips(base, signal, list(map(str, range(9))))
    assert report["changes_count"] == 1  # second change would exceed budget
    assert (0, 1) in tips  # away is supported, although home remains model favorite


def test_no_forced_changes_and_input_order_does_not_break_ties():
    base = [matrix(.4, .28, .32)] * 9
    ids = list(map(str, range(9)))
    _, report = limited_risk_tips(base, base, ids)
    assert report["changes_count"] == 0
    signal = [matrix(.35, .2, .45)] * 9
    _, reordered = limited_risk_tips(base[::-1], signal[::-1], ids[::-1])
    assert {r["match_id"] for r in reordered["changes"]} == {"0", "1"}
    # Even a large model edge cannot authorize an outcome below the base floor.
    base = [matrix(.8, .1, .1)] * 9
    signal = [matrix(.1, .1, .8)] * 9
    _, report = limited_risk_tips(base, signal, ids)
    assert report["changes_count"] == 0


def test_rejects_incomplete_round_and_invalid_limits():
    m = matrix(.5, .2, .3)
    with pytest.raises(ValueError):
        limited_risk_tips([m] * 8, [m] * 8, list(map(str, range(8))))
    with pytest.raises(ValueError):
        limited_risk_tips([m] * 9, [m] * 9, list(map(str, range(9))),
                          replace(LimitedRiskConfig(), max_changes=3))


def selection_data():
    rows = []
    for strategy in SELECTABLE:
        for year in range(2011, 2027):
            for day in range(1, 35):
                rows.append(dict(strategy=strategy, season=year, matchday=day,
                                 points=22 if day == 1 else 10,
                                 changes=0 if strategy == "baseline" else 1))
    return pd.DataFrame(rows)


def test_selection_prefers_unchanged_baseline_on_tail_tie_and_ignores_future():
    rounds = selection_data()
    assert choose_for_season(rounds, 2016)["strategy"] == "baseline"
    rounds.loc[(rounds.strategy == "hybrid_120") & (rounds.season >= 2016), "points"] = 36
    assert choose_for_season(rounds, 2016)["strategy"] == "baseline"
    assert choose_for_season(rounds, 2017)["strategy"] == "hybrid_120"


def test_selection_optimizes_upper_tail_before_average_points():
    rounds = selection_data()
    mask = rounds.strategy.eq("hybrid_270") & rounds.season.lt(2016)
    rounds.loc[mask, "points"] = 0
    rounds.loc[mask & rounds.matchday.le(2), "points"] = 22
    assert choose_for_season(rounds, 2016)["strategy"] == "hybrid_270"
