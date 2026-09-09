from pathlib import Path
import json,pandas as pd,numpy as np
from bundesliga.data_sources import parse_openligadb_matches
from bundesliga.scoring import tip_points,score_matrix,blend_outcome_probabilities,outcome_probabilities
frames=[]
for season in range(2018,2026):
 path=Path(f'data/raw/draw_matchday_review/bl1_{season}.json')
 frames.append(parse_openligadb_matches(json.loads(path.read_text()),'D1',season))
official=pd.concat(frames,ignore_index=True).rename(columns={'home_goals':'official_home','away_goals':'official_away','match_date':'official_date'})
candidates=[(h,a) for h in range(6) for a in range(6)]
point_table=np.array([[tip_points(t,(h,a)) for h in range(11) for a in range(11)] for t in candidates])
def best(matrix):
 scores=point_table@matrix.ravel()
 i=max(range(len(candidates)),key=lambda i:(scores[i],matrix[candidates[i]],-sum(candidates[i]),-abs(candidates[i][0]-candidates[i][1])))
 return candidates[i]
reports=[]
for start in [2018,2022]:
 x=pd.read_csv(f'data/backtest_d1_{start}_{start+3}_live_config_with_odds.csv')
 y=x.merge(official[['season','home_team','away_team','matchday','official_home','official_away']],on=['season','home_team','away_team'],how='left',validate='one_to_one',indicator=True)
 assert y._merge.eq('both').all(),y[y._merge.ne('both')][['home_team','away_team']].to_string()
 assert y.actual_home.eq(y.official_home).all() and y.actual_away.eq(y.official_away).all()
 calc=[tip_points((r.tip_home,r.tip_away),(r.actual_home,r.actual_away)) for r in y.itertuples()]
 assert (np.array(calc)==y.tip_points).all()
 y['current']=y.tip_points
 names=['odds_fixed_2_1','odds_fixed_1_0','internal_only','half_market']
 for name in names:y[name]=0
 tip_details=[]
 for i,r in enumerate(y.itertuples()):
  probs=(r.prob_home,r.prob_draw,r.prob_away)
  favourite=int(np.argmax(probs))
  raw=score_matrix(r.lambda_home,r.lambda_away,r.rho)
  tips={
   'odds_fixed_2_1':[(2,1),(1,1),(1,2)][favourite],
   'odds_fixed_1_0':[(1,0),(1,1),(0,1)][favourite],
   'internal_only':best(raw),
   'half_market':best(blend_outcome_probabilities(raw,probs,.5)),
  }
  for name,tip in tips.items():
   y.loc[i,name]=tip_points(tip,(r.actual_home,r.actual_away))
  tip_details.append({name:f'{tip[0]}:{tip[1]}' for name,tip in tips.items()})
  assert best(blend_outcome_probabilities(raw,probs,1))==(r.tip_home,r.tip_away)
 for name in names:y[name+'_tip']=[d[name] for d in tip_details]
 grouped=y.groupby(['season','matchday']).agg(n=('match_id','size'),points=('tip_points','sum'),asofs=('as_of','nunique'),last_asof=('as_of','max'),first_match=('match_date','min'))
 assert len(grouped)==136 and grouped.n.eq(9).all()
 sizes=y.groupby('as_of').size();complete_ids=sizes[sizes.eq(9)].index
 groups=y[y.as_of.isin(complete_ids)].groupby('as_of')
 mixed=[(a,g.matchday.unique().tolist()) for a,g in groups if len(g[['season','matchday']].drop_duplicates())!=1]
 summary={'period':f'{start}/{start+1} to {start+3}/{start+4}','full_official_rounds':len(grouped),'calendar_window_sizes':{str(k):int(v) for k,v in sizes.value_counts().sort_index().items()},'mixed_nine_windows':mixed,'official_rounds_multiple_forecast_times':int(grouped.asofs.gt(1).sum()),'official_rounds_all_forecasts_before_first_game':int((grouped.last_asof<grouped.first_match).sum()),'variants':{}}
 for name in ['current',*names]:
  p=y.groupby(['season','matchday'])[name].sum()
  summary['variants'][name]={'points':int(y[name].sum()),'points_per_game':float(y[name].mean()),'mean_round':float(p.mean()),'best_round':int(p.max()),**{f'ge{n}':int(p.ge(n).sum()) for n in [18,20,22,24,26]}}
 reports.append(summary)
 print(json.dumps(summary,indent=2),flush=True)
 print('TOP',grouped.sort_values('points',ascending=False).head(6).to_string(),flush=True)
 grouped.to_csv(f'data/official_round_audit_{start}_{start+3}.csv')
 y.drop(columns='_merge').to_csv(f'data/official_predictions_{start}_{start+3}.csv',index=False)
Path('data/official_round_model_audit.json').write_text(json.dumps(reports,indent=2),encoding='utf-8')
