# Epic 021 — Fit model refactor: artifact-based pipeline

## Goal

Refactor `03_fit_model.py` és a training réteg (`src/modeling/training/`) hogy az új
pipeline struktúrát kövesse:

- **Search eredményeinek konsumálása:** `best_params.json` + `search_best.json` (n_estimators)
- **Feature set az artifact-ból:** `feature_engineering/feature_set.json["selected"]`
- **Sample parquet alapú adatbetöltés:** csak a yearly sample train+valid sorai, purge kizárva
- **OOS scoring:** a következő naptári évet scorolja, kimenete `sample_oos.parquet` az artifact-ban
- **Nincs CV sweep:** a search már megtalálta a legjobb paramétereket; a fit egyszer illeszt

## Context

A jelenlegi `train_lightgbm_binary()` (lightgbm_model.py) maga végez CV-t és `num_leaves`
sweepet — ez az új flow-ban a `02_hyper_param_search.py` szerepe. A fit-nek csak egyszer
kell illesztenie a search által megtalált paraméterekkel.

**Search outputs (már megvan a 2021-es modellnél):**
- `artifacts/<model_id>/search/best_params.json` — 13 tuned hyperparameter
- `artifacts/<model_id>/search/search_best.json` — fold summary-vel, best_iteration per fold
- `artifacts/<model_id>/feature_engineering/feature_set.json` — selected feature list

**Sample forrás:** `database/solusdt/samples/solusdt_fw60_yearly_2021/sample_train_valid.parquet`
Columns: `open_time | long_mfe_fw60 | short_mfe_fw60 | segment`

## Tasks

| ID | Cím | Assignee | Depends on |
|----|-----|----------|------------|
| t1 | Fit function refactor — search artifacts konsumálása | modeling_agent | — |
| t2 | OOS scoring — sample_oos.parquet | modeling_agent | t1 |
| t3 | Pipeline.py + 03_fit_model.py CLI update | modeling_agent | t1, t2 |
| t4 | Validation | validator_agent | t3 |
