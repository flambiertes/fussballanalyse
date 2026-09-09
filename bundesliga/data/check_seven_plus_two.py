from pathlib import Path
import json
import numpy as np
import pandas as pd
from bundesliga.scoring import score_matrix,blend_outcome_probabilities,outcome_probabilities,tip_points

# Exploratory rule fixed before scoring: keep >=7 original scorelines. An
# alternative must be the internal model's most likely 1X2 outcome, differ from
# the original tip, have >=20% in the original forecast, and positive model-market
# disagreement. Select at most 2, ranked by that disagreement. No tuning loop.
candidates=[(h,a) for h in range(6) for a in range(6)]
points=np.array([[tip_points(t,(h,a)) for h in range(11) for a in range(11)] for t in candidates])
def tendency(t):return 0 if t[0]>t[1] else 1 if t[0]==t[1] else 2

def apply(group):
 result=group.copy().reset_index(drop=True)
 options=[]
 for i,r in enumerate(result.itertuples()):
  raw=score_matrix(r.lambda_home,r.lambda_away,r.rho)
  p=np.array([r.prob_home,r.prob_draw,r.prob_away]);internal=np.array(outcome_probabilities(raw))
  matrix=blend_outcome_probabilities(raw,tuple(p),1.)
  baseline=(int(r.tip_home),int(r.tip_away));alternative=int(np.argmax(internal))
  if alternative==tendency(baseline) or p[alternative]<.20 or internal[alternative]<=p[alternative]:continue
  expected=points@matrix.ravel()
  ids=[idx for idx,t in enumerate(candidates) if tendency(t)==alternative]
  best=max(ids,key=lambda idx:(expected[idx],matrix[candidates[idx]],-sum(candidates[idx]),-abs(candidates[idx][0]-candidates[idx][1])))
  options.append((internal[alternative]-p[alternative],i,candidates[best],float(p[alternative]),float(internal[alternative])))
 result['hybrid_home']=result.tip_home;result['hybrid_away']=result.tip_away
 result['internal_alternative_probability']=np.nan;result['original_alternative_probability']=np.nan
 for _,i,tip,p,internal in sorted(options,key=lambda o:(-o[0],o[1]))[:2]:
  result.loc[i,['hybrid_home','hybrid_away']]=tip
  result.loc[i,'internal_alternative_probability']=internal
  result.loc[i,'original_alternative_probability']=p
 # Actual results are only used after all selections are fixed.
 result['original_points']=[tip_points((int(r.tip_home),int(r.tip_away)),(int(r.actual_home),int(r.actual_away))) for r in result.itertuples()]
 result['hybrid_points']=[tip_points((int(r.hybrid_home),int(r.hybrid_away)),(int(r.actual_home),int(r.actual_away))) for r in result.itertuples()]
 assert (result.tip_home.ne(result.hybrid_home)|result.tip_away.ne(result.hybrid_away)).sum()<=2
 return result

history=pd.read_csv('data/backtest_d1_2018_2021_live_config_with_odds.csv')
frames=[apply(g) for _,g in history.groupby('as_of',sort=True) if len(g)==9]
result=pd.concat(frames,ignore_index=True)
result.to_csv('data/seven_plus_two_development.csv',index=False)
summary={}
for label in ['original','hybrid']:
 totals=result.groupby('as_of')[label+'_points'].sum()
 summary[label]={'rounds':len(totals),'mean':float(totals.mean()),'best':int(totals.max()),'ge18':int(totals.ge(18).sum()),'ge20':int(totals.ge(20).sum()),'ge22':int(totals.ge(22).sum()),'ge24':int(totals.ge(24).sum())}
summary['mean_changed_games']=float((result.tip_home.ne(result.hybrid_home)|result.tip_away.ne(result.hybrid_away)).groupby(result.as_of).sum().mean())
print(json.dumps(summary,indent=2),flush=True)
Path('data/seven_plus_two_development.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')

old=pd.read_csv('data/contest_rueckblick_spieltage_1_2_2026.csv')
recent=[]
for day,path in [(1,'data/tipps_spieltag_1_2026.xlsx'),(2,'data/tipps_spieltag_2_2026_risk.xlsx')]:
 source=pd.read_excel(path)
 merged=source.drop(columns=['rho'],errors='ignore').merge(old[['match_id','rho','actual_home','actual_away']],on='match_id',validate='one_to_one',sort=False)
 selected=apply(merged);recent.append(selected)
 changes=selected[selected.tip_home.ne(selected.hybrid_home)|selected.tip_away.ne(selected.hybrid_away)]
 print('DAY',day,'OLD',selected.original_points.sum(),'HYBRID',selected.hybrid_points.sum(),flush=True)
 print(changes[['home_team','away_team','tip_home','tip_away','hybrid_home','hybrid_away','original_alternative_probability','internal_alternative_probability']].to_string(index=False),flush=True)
pd.concat(recent,ignore_index=True).to_csv('data/seven_plus_two_matchdays_1_2.csv',index=False)
x=recent[1]
a=x[x.home_team.eq('Eintracht Frankfurt')].iloc[0]
print('AUGSBURG FORM MULTIPLIERS HOME/AWAY',np.exp(.1*(a.home_form-.5*a.away_form)),np.exp(.1*(a.away_form-.5*a.home_form)),flush=True)
print('P24 CONSTANT 34 MATCHDAYS',1-(1-float(a.target_probability))**34,flush=True)
