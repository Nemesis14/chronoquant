---
epic: epic_050
id: t4
title: Long modell teljes pipeline (sample→FE→search→train→predict)
assignee: modeling_agent
status: done
blocks: [t6]
blocked_by: [t3]
---

## Goal

`lgbm_solusdt_l_fw60_2101_2607` teljes offline pipeline futtatása.

## Scope

`src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2101_2607`.

## Acceptance Criteria

- [x] setup/sample/feature_engineering/search/train/predict lépések rendben lefutnak
- [x] `model.pkl`, `features.json`, `params.json` artifact létrejön
- [x] `reg.models` status=predicted
- [x] `model."<id>__pred"` tábla létrejön a lab.duckdb-ben (rows=2933276)

## Notes

sample: rows=48884 (train=44563, valid=4321), n_input=208 n_selected=208 (FE nem szűrt).
Két blokkoló bug derült ki és lett javítva futás közben (ld. epic.md Key
Decisions): (1) `step_search` hiányzó `feature_selection`/`feature_key`
paraméter, (2) `_select_best_trial`/`_check_patience` stale `valid_top10_lift`
mezőre szűrt a tényleges `objective_score` helyett. A search 100 trial-ból
állt (trial_no=60 lett a legjobb, objective_score=-2.255, gap=0.386). Train:
n_features=133, n_estimators=1522. Predict: 2933276 sor.
