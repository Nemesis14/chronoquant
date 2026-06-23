---
epic: epic_033
id: t36
title: modeling 5xxx — sampling audit + training/search/pipeline/predict/provenance kód-ref
assignee: code_doc_agent
status: done
blocks: [t39]
blocked_by: []
---

## Goal
A `src/modeling/` lefedetlen részeinek kód-referenciája (jelenleg csak sampling 5100–5420 van),
és a meglévő sampling oldalak auditja.

## Scope
- Audit: 5100/5200/5300/5410/5420 (sampling) vs `src/modeling/sampling/`
- Új kód-ref: `training/` (train, cv, datasets, metrics, reports, training_windows),
  `search/`, `02_hyper_param_search.py`, `pipeline.py`, `predict.py`, `provenance.py`,
  `03_fit_model.py`, `00_create_sample.py`
- Számozás: 5500/5600 tartomány (methodology szám alatt marad az invariáns szerint)

## Acceptance Criteria
- [x] Minden modeling .py-hoz kód-ref: Overview (flowchart/sequenceDiagram) + függvény-szintű leírás
- [x] Min. 2–3 Mermaid/fájl; nincs methodology-duplikáció
- [x] Felfelé link a meglévő methodology-ra (`../methodology_doc/5000_modelling.md`, 5400, 5500)
- [x] Entry Gate: ha hiányzó methodology X100 kell (pl. training) → `todo_` a methodology_agent-nek,
      és addig a meglévő overview-ra linkelünk (nem írunk methodology-t)

## Notes

### Audit — drift javítások (2026-06-22)

**5100_sampling_config.md:**
- Hozzáadva `WalkForwardSamplingConfig` (teljesen hiányzott — kód tartalmazza)
- Hozzáadva `n_folds` mező (kódban létezik, doksiból hiányzott)
- Javítva forrás link path (`../src/` -> `../../src/`)
- Metodológiai felfelé link hozzáadva

**5200_sampling_artifacts.md:**
- Javítva forrás link path
- Metodológiai felfelé link hozzáadva

**5300_create_sample.md:**
- Javítva forrás link path-ok
- Metodológiai felfelé link hozzáadva

**5410_sampling_splits.md:**
- TELJES ÚJRAÍRÁS: a régi doksi `splits.py` funkciókat dokumentált (nem létező fájl
  `quantitative/sampling/splits.py`). Az aktuális `yearly_sampler.py` teljesen más
  API-t tartalmaz: `select_hourly_observations`, `assign_fold_ids`,
  `generate_walk_forward_folds`, `assign_walk_forward_fold_ids`.

**5420_sampling_audit.md:**
- Javítva forrás link path (`quantitative/sampling/audit.py` → `sampling/audit.py`)
- Metodológiai felfelé link hozzáadva

### Új dokumentumok (2026-06-22)

**5510_training.md** — `src/modeling/training/` teljes lefedés:
- train.py (train_model dispatcher)
- datasets.py (ModelingDataset, load_modeling_dataset)
- cv.py (PurgedEmbargoCV)
- training_windows.py (DatasetSplit, fold_split, final_train_test_split)
- metrics.py (binary_classification_metrics, lift_at_percentiles, calibration_table)
- artifacts.py (save_training_artifacts, register_training_artifacts)
- reports.py (write_training_report, cv_summary, validation_calibration_summary)
- fit_lgbm.py (fit_lightgbm_from_search, _load_train_data)

**5520_search.md** — `src/modeling/search/lgbm_search.py` + `02_hyper_param_search.py`:
- run_search (fő belépési pont, stage defaults)
- _load_search_dataset (snap ⋈ model.__sample JOIN)
- _load_model_sample_meta (fold struktúra rekonstrukció)
- _compute_objective (Top10 Lift - 0.5×std objektív)
- _run_one_trial (per-fold LightGBM fit, rank metrikák)
- _fold_split_walk_forward / _fold_split_4fold
- Paraméter tér tábla, output fájlok, CLI argumentumok

**5530_pipeline_predict_provenance.md** — pipeline.py + predict.py + provenance.py + 03_fit_model.py:
- pipeline.py minden step (setup, sample, feature_engineering, search, train, predict)
- predict_offline (snap→pred tábla, immutability ellenőrzés)
- provenance.py teljes API (model lifecycle, feature_set, search_run, artifact regisztrálás)
- 03_fit_model.py CLI wrapper

### Entry Gate státusz
Nincs szükség új methodology todo-ra — minden szükséges methodology doc létezik:
5400_sampling.md, 5500_hyper_param_search.md, 5600_model_training.md,
5700_offline_prediction.md, 5000_modelling.md.

### Blocker
Nincs.
