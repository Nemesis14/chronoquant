---
epic: epic_021
id: t1
title: Fit function refactor — search artifacts konsumálása
assignee: modeling_agent
status: todo
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

**n_estimators kérdés:** A search early stopping-gal talált best_iteration-t fold-onként
(266–2997 között a 2021-es modellnél, mean ~576). A végső fithez javasolt: mean × 1.1
vagy max(best_iteration) — döntés implementáláskor.

**Fixed params:** A search `_FIXED_PARAMS`-t is használt (objective, boosting_type, metric,
subsample_freq, force_col_wise, verbosity, n_jobs). Ezeket a fit is alkalmazni kell.
Forrás: `src/modeling/search/lgbm_search.py` `_FIXED_PARAMS` dict.

**Régi kod:** `lightgbm_model.py` CV kódját NEM kell törölni — maradhat referenciaként.
Az új `fit_lgbm.py` a tiszta implementáció.

**Adatbetöltés pontosítás:**
Sample parquet filter: `segment in ("train", "valid")` → kapunk open_time listát.
DuckDB join: `quant_train WHERE open_time IN (...)` — csak a selected feature oszlopok.
Target a sample parquet-ből jön (long_mfe_fw60 vagy short_mfe_fw60 a model target_name-től).
