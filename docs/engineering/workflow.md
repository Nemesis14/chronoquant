# ChronoQuant Engineering Workflow

This legacy workflow was retired after the yearly-modeling refactor.

Use the current `_doc_` documentation as the source of truth:

- `_doc_/0000_project_overview.md` — cross-domain project and pipeline overview
- `_doc_/1000_database.md` — DuckDB schema and data-layer contract
- `_doc_/2000_features.md` — feature layer methodology
- `_doc_/3000_targets.md` — target layer methodology
- `_doc_/4000_quant_train.md` — model-ready join table contract
- `_doc_/5000_modelling.md` — active yearly modeling pipeline
- `_doc_/5500_hyper_param_search.md` — LightGBM hyperparameter search

Current model development uses `artifacts/<model_id>/` and yearly model IDs such as
`lgbm_solusdt_l_fw60_q90_2021`.
