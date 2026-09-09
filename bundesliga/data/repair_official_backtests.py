from pathlib import Path
from dataclasses import replace,asdict
import json
import pandas as pd
from bundesliga.backtest import run_backtest,summarize
from bundesliga.database import load_matches
from bundesliga.predict import LIVE_CONFIG
from bundesliga.rounds import attach_matchdays
config=replace(LIVE_CONFIG,bookmaker_snapshot_type='opening')
lookup=load_matches('D1')
for start in [2018,2022]:
 old=pd.read_csv(f'data/backtest_d1_{start}_{start+3}_live_config_with_odds.csv',parse_dates=['match_date','as_of'])
 old=attach_matchdays(old,lookup)
 counts=old.groupby(['season','matchday']).as_of.nunique()
 affected=counts[counts.gt(1)].index.tolist()
 old['original_forecast_reused']=True
 old['round_grouping']='official_matchdays'
 old['odds_withheld_for_late_fixture']=False
 parts=[old]
 for season,day in affected:
  fresh,_=run_backtest('D1',[int(season)],config=config,persist=False,verbose=False,tip_strategy='expected-points',matchdays=[int(day)])
  assert len(fresh)==9 and fresh.as_of.nunique()==1
  expected=set(old[old.season.eq(season)&old.matchday.eq(day)].match_id)
  assert set(fresh.match_id)==expected
  fresh['original_forecast_reused']=False
  parts[0]=parts[0][~parts[0].match_id.isin(expected)]
  parts.append(fresh)
  print('REPAIRED',season,day,'asof',fresh.as_of.iloc[0],'withheld odds',int(fresh.odds_withheld_for_late_fixture.sum()),flush=True)
 result=pd.concat(parts,ignore_index=True).sort_values(['match_date','match_id']).reset_index(drop=True)
 assert len(result)==1224 and not result.match_id.duplicated().any()
 groups=result.groupby(['season','matchday'])
 assert len(groups)==136 and groups.size().eq(9).all()
 assert groups.as_of.nunique().eq(1).all()
 assert all(group.as_of.iloc[0]<=group.match_date.min() for _,group in groups)
 out=Path(f'data/backtest_d1_{start}_{start+3}_official_frozen.csv')
 result.to_csv(out,index=False)
 summary=summarize(result,f'official-audit-{start}',config)
 summary['source_csv']=f'data/backtest_d1_{start}_{start+3}_live_config_with_odds.csv'
 summary['recomputed_rounds']=[list(map(int,pair)) for pair in affected]
 summary['validation_note']='Already examined historical periods; no new blind test. Untimestamped opening odds timing within the original calendar week remains an assumption.'
 out.with_suffix('.json').write_text(json.dumps(summary,indent=2,ensure_ascii=False),encoding='utf-8')
 print('COMPLETE',start,'points',summary['points'],'best',summary['best_matchday_points'],flush=True)
