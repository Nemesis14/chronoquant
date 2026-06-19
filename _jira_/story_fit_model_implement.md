---
id: story_fit_model_implement
title: 03_fit_model — OOS flow implementálás
assignee: modeling_agent
status: todo
blocked_by: [story_hyper_param_search_refactor]
---

## Goal

A `03_fit_model.py` implementálása az új OOS flow alapján. A teljes yearly sample-n refit, majd a következő évet scorolja OOS-ként.

## Scope

- `src/modeling/03_fit_model.py`
- `src/modeling/training/train.py`
- `src/modeling/training/lightgbm_model.py`
- `src/modeling/training/artifacts.py`

## Acceptance Criteria

- [ ] Input: `--year 2021 --oos-year 2022 --asset-id solusdt --model-id <id>`
- [ ] Refit: `sample_train_valid.parquet` összes train+valid sora (purge kizárva)
- [ ] Model artifact: `models/<model_id>/model.pkl` + `features.json`
- [ ] OOS output: `database/solusdt/samples/solusdt_fw60_yearly_<year>/sample_oos.parquet`
  - Oszlopok: `open_time | pred_long | pred_short | long_mfe_fw60 | short_mfe_fw60`
- [ ] `ruff check` + `pyright` tiszta

## Notes

Spec forrás: `_doc_/0000_project_overview.md` — "OOS evaluation" szekció.
Az OOS a teljes következő év (percenkénti) — trading modul inputja.
