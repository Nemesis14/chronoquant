---
epic: epic_021
id: t1
title: Fit function refactor — search artifacts konsumálása
assignee: modeling_agent
status: pr
blocks: [t2, t3]
blocked_by: []
---

## Goal

Refaktorálni a LightGBM fit logikát (`src/modeling/training/lightgbm_model.py` vagy új
`fit_lgbm.py`) hogy a search outputjait konsumálja, ne végezzen saját CV sweepet.

## Scope

- `src/modeling/training/fit_lgbm.py` — új fájl (az old `lightgbm_model.py` megmarad referenciaként)
- `src/modeling/training/train.py` — dispatcher update: a `train_lightgbm_binary` mellett/helyett az új fit-et hívja
- `src/modeling/training/artifacts.py` — esetleg bővítés (save_fit_artifacts)

**Érintett artifact bemenetek:**
- `artifact_dir/search/best_params.json` — 13 tuned param
- `artifact_dir/search/search_best.json` — fold summary, best_iteration per fold
- `artifact_dir/feature_engineering/feature_set.json` — `selected` kulcs

## Acceptance Criteria

- [ ] `fit_lightgbm_from_search(model_id)` (vagy hasonló) beolvassa a search artifact-ot
- [ ] `n_estimators` deriválása: `round(mean(fold["best_iteration"]) * 1.1)` a search_best.json folds-ból
- [ ] Feature lista: `feature_set.json["selected"]` (nem all feat_* from DuckDB)
- [ ] Adatbetöltés: `sample_train_valid.parquet` (purge kizárva), feature join quant_train-ből
  - A parquet adja az `open_time` és `target` értékeket
  - A quant_train DuckDB join adja a feature oszlopokat az adott open_time-okra
  - `row_stride` alkalmazható (60 a 2021-es modellnél)
- [ ] Egyetlen LightGBM illesztés (nincs fold loop)
- [ ] Kimenetek artifact-ba: `model.pkl`, `features.json`, `params.json`
- [ ] `params.json` tartalmazza a teljes param diktot (fixed + best_params merged)
- [ ] `ruff check` + `pyright` tiszta

## Notes

**Implementálva:** `src/modeling/training/fit_lgbm.py` létrehozva — `fit_lightgbm_from_search(model_id)`.
- n_estimators = round(mean(fold best_iterations) × 1.1)
- Adatbetöltés: sample parquet (train+valid) + quant_train year-bounded query_range, inner merge
- _FIXED_PARAMS + best_params merge, egyetlen LGBMClassifier.fit()
- Kimenet: model.pkl, features.json, params.json az artifact_dir-be
- train.py dispatcher frissítve: lightgbm_model.py megmarad referenciaként
