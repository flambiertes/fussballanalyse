"""Official matchday metadata and explicit grouping for backtests."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from .config import DB_PATH
from .database import connect, load_matches


def attach_matchdays(predictions: pd.DataFrame, official: pd.DataFrame) -> pd.DataFrame:
    keys = ['competition', 'season', 'home_team', 'away_team']
    if official.duplicated(keys).any():
        raise ValueError('Mehrdeutige offizielle Spieltagszuordnung')
    mapping = official[keys + ['matchday']].rename(columns={'matchday': '_official_matchday'})
    result = predictions.merge(mapping, on=keys, how='left', validate='many_to_one', sort=False)
    if result['_official_matchday'].isna().any():
        raise ValueError('Offizielle Spieltagsnummern fehlen fuer einen Teil der Spiele')
    if 'matchday' in result:
        conflict = result.matchday.notna() & result.matchday.ne(result._official_matchday)
        if conflict.any():
            raise ValueError('Widerspruechliche Spieltagsnummern')
    result['matchday'] = result.pop('_official_matchday').astype(int)
    return result


def round_groups(predictions: pd.DataFrame):
    """Never label calendar windows as official matchdays; never mix seasons."""
    if 'matchday' in predictions and predictions.matchday.notna().all():
        keys = [name for name in ['competition', 'season', 'matchday'] if name in predictions]
        return 'official_matchdays', list(predictions.groupby(keys, sort=True))
    keys = [name for name in ['competition', 'season', 'as_of'] if name in predictions]
    return 'calendar_windows', list(predictions.groupby(keys, sort=True))


def import_cached_matchdays(seasons: list[int], cache_dir: Path, db_path: Path = DB_PATH) -> int:
    from .data_sources import parse_openligadb_matches
    frames = [parse_openligadb_matches(
        json.loads((cache_dir / f'bl1_{season}.json').read_text(encoding='utf-8')),
        'D1', season,
    ) for season in seasons]
    official = pd.concat(frames, ignore_index=True)
    original = load_matches('D1', seasons=seasons, path=db_path)
    linked = attach_matchdays(original, official)
    # Check identities and results before changing metadata; preserve primary keys,
    # odds, results, dates, prediction history and original result provenance.
    checked = original.merge(official, on=['competition', 'season', 'home_team', 'away_team'],
                             suffixes=('_stored', '_official'), validate='one_to_one')
    for column in ['home_goals', 'away_goals']:
        both = checked[column + '_stored'].notna() & checked[column + '_official'].notna()
        if not checked.loc[both, column + '_stored'].eq(checked.loc[both, column + '_official']).all():
            raise ValueError('Ergebnisse der Quellen stimmen nicht ueberein')
    with connect(db_path) as connection:
        connection.executemany('UPDATE matches SET matchday=? WHERE match_id=?',
                               [(int(row.matchday), row.match_id) for row in linked.itertuples()])
    return len(linked)


def main() -> None:
    parser = argparse.ArgumentParser(description='Offizielle Spieltage aus gecachten OpenLigaDB-Saisons zuordnen')
    parser.add_argument('--seasons', type=int, nargs='+', required=True)
    parser.add_argument('--cache-dir', type=Path, required=True)
    args = parser.parse_args()
    # Reuse the downloaded official fixture list; fetch one season per request
    # only when it is not cached. No match results or odds are overwritten.
    import requests
    args.cache_dir.mkdir(parents=True, exist_ok=True)
    for season in args.seasons:
        path = args.cache_dir / f'bl1_{season}.json'
        if not path.exists():
            response = requests.get(f'https://api.openligadb.de/getmatchdata/bl1/{season}', timeout=30)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list) or not payload:
                raise ValueError(f'Keine offiziellen Spiele fuer Saison {season}')
            path.write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
    print(f'{import_cached_matchdays(args.seasons, args.cache_dir)} Spieltagszuordnungen aktualisiert')


if __name__ == '__main__':
    main()
