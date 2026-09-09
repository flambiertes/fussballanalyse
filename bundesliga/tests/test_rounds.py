from __future__ import annotations

import pandas as pd
import pytest

from bundesliga.backtest import prediction_groups, summarize
from bundesliga.config import ModelConfig
from bundesliga.rounds import attach_matchdays


def two_rounds():
    rows = []
    for day, date in [(1, '2025-01-07'), (2, '2025-01-10')]:
        for i in range(9):
            rows.append(dict(competition='D1', season=2024, matchday=day,
                             match_id=f'{day}-{i}', home_team=f'H{i}', away_team=f'A{i}',
                             match_date=pd.Timestamp(date), as_of=pd.Timestamp('2025-01-07'),
                             actual_home=2, actual_away=1, tip_home=2, tip_away=1,
                             tip_points=4, log_loss=1., brier_score=.2,
                             lambda_home=2., lambda_away=1.))
    return pd.DataFrame(rows)


def test_two_official_rounds_in_one_week_are_counted_separately():
    x = two_rounds()
    scope, groups = prediction_groups(x, 'contest')
    assert scope == 'official_matchdays'
    assert [len(group) for _, group in groups] == [9, 9]
    summary = summarize(x, 'test', ModelConfig())
    assert summary['complete_matchdays'] == 2
    assert summary['mean_matchday_points'] == 36
    assert summary['matchday_grouping'] == 'official_matchdays'


def test_postponement_does_not_advance_the_official_round_cutoff():
    x = two_rounds()
    x.loc[8, 'match_date'] = pd.Timestamp('2025-02-01')
    _, groups = prediction_groups(x, 'target-score')
    first = next((as_of, group) for as_of, group in groups if group.matchday.iloc[0] == 1)
    assert first[0] == pd.Timestamp('2025-01-07')
    assert len(first[1]) == 9
    assert first[1].match_date.max() == pd.Timestamp('2025-02-01')


def test_missing_round_metadata_is_explicit_and_contest_rejects_it():
    x = two_rounds().drop(columns='matchday')
    scope, groups = prediction_groups(x, 'expected-points')
    assert scope == 'calendar_windows'
    assert len(groups) == 1 and len(groups[0][1]) == 18
    with pytest.raises(ValueError, match='offizielle'):
        prediction_groups(x, 'contest')


def test_metadata_join_rejects_ambiguity_and_conflicts():
    original = two_rounds().head(9)
    official = original.copy()
    stripped = original.drop(columns='matchday')
    assert attach_matchdays(stripped, official).matchday.eq(1).all()
    with pytest.raises(ValueError, match='Mehrdeutige'):
        attach_matchdays(stripped, pd.concat([official, official]))
    official['matchday'] = 3
    with pytest.raises(ValueError, match='Widerspruechliche'):
        attach_matchdays(original, official)


def test_backtest_withholds_untimestamped_postponed_odds(monkeypatch):
    import numpy as np
    import bundesliga.backtest as backtest
    x = two_rounds()
    x['home_goals'] = x.actual_home
    x['away_goals'] = x.actual_away
    x.loc[8, 'match_date'] = pd.Timestamp('2025-02-01')
    odds_calls = []
    fit_calls = []

    class Model:
        rho = 0.
        home_advantage = 0.
        external_team_parameters = {}
        def __init__(self, config):
            pass
        def fit(self, training, as_of):
            self.training_matches = training
            fit_calls.append((training.copy(), as_of))
            return self
        def predict(self, *args, bookmaker_probabilities=None):
            matrix = np.zeros((11, 11))
            matrix[2, 1] = 1.
            return dict(lambda_home=2., lambda_away=1., prob_home=1., prob_draw=0.,
                        prob_away=0., tip_home=2, tip_away=1, score_matrix=matrix,
                        features={'bookmaker_weight': float(bookmaker_probabilities is not None)})

    def odds(ids, **kwargs):
        ids = list(ids)
        odds_calls.append((ids, kwargs['as_of']))
        return pd.DataFrame({'match_id': ids, 'book_prob_home': [1.] * len(ids)})

    monkeypatch.setattr(backtest, 'load_matches', lambda **kwargs: x.copy())
    monkeypatch.setattr(backtest, 'DynamicDixonColes', Model)
    monkeypatch.setattr(backtest, 'latest_market_values', lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(backtest, 'consensus_bookmaker_probabilities', odds)
    result, summary = backtest.run_backtest('D1', [2024], persist=False, verbose=False)
    assert summary['complete_matchdays'] == 2
    assert all(as_of == pd.Timestamp('2025-01-07') for _, as_of in fit_calls)
    assert all(training.empty for training, _ in fit_calls)
    assert '1-8' not in odds_calls[0][0]
    delayed = result[result.match_id.eq('1-8')].iloc[0]
    assert delayed.odds_withheld_for_late_fixture
    assert delayed.bookmaker_weight == 0
    assert delayed.as_of == pd.Timestamp('2025-01-07')
