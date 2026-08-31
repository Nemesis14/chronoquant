---
epic: epic_050
id: t5
title: Short modell teljes pipeline (sample→FE→search→train→predict)
assignee: modeling_agent
status: done
blocks: [t6]
blocked_by: [t3]
---

## Goal

`lgbm_solusdt_s_fw60_2101_2607` teljes offline pipeline futtatása. Párhuzamosítható t4-gyel.

## Scope

`src/modeling/pipeline.py --model lgbm_solusdt_s_fw60_2101_2607`.

## Acceptance Criteria

- [x] setup/sample/feature_engineering/search/train/predict lépések rendben lefutnak
- [x] `model.pkl`, `features.json`, `params.json` artifact létrejön
- [x] `reg.models` status=predicted
- [x] `model."<id>__pred"` tábla létrejön a lab.duckdb-ben (rows=2933276)

## Notes

sample: rows=48884 (train=44563, valid=4321), n_input=208 n_selected=208.
Egy harmadik, súlyos bug derült ki itt (ld. epic.md Key Decisions): a
`step_search()` sosem adta át a `direction` paramétert, így a short modell
keresése végig a "long" objektívet optimalizálta (mindig 0.0 — vak keresés).
Javítva `pipeline.py`-ban (`direction` levezetve `target_name`-ből) +
`_persist_completed()` mindkét ratio mezőt perzisztálja. A régi (érvénytelen)
search/ mappa törölve, a keresés helyes iránnyal újrafutott: 100 trial,
trial_no=63 lett a legjobb, objective_score=-1.5225, valid_ratio_p075=1.5225
(értelmes, változó metrika). Train: n_features=137, n_estimators=79.
Predict: 2933276 sor.
