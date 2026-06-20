---
epic: epic_024
id: t4
title: 2021-es modell pipeline újrafuttatása
assignee: modeling_agent
status: todo
blocked_by: [t1, t2]
blocks: [t5]
---

## Goal

Az lgbm_solusdt_l_fw60_2021 modell teljes pipeline-ját újrafuttatni a refaktorált
4-fold CV kóddal: sample → search (explore) → train.

## Scope

- `artifacts/lgbm_solusdt_l_fw60_2021/` — teljes törlés és újraépítés
- Kivéve: `artifacts/lgbm_solusdt_l_fw60_2021/feature_engineering/` — ezt NEM töröljük

## Lépések

```bash
# 1. Régi search artifacts törlése (12-fold incompatibilis)
rm -rf artifacts/lgbm_solusdt_l_fw60_2021/search/

# 2. Sample lépés (új 4-fold struktúra)
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step sample

# 3. Search (explore stage — 60 trial, 4 fold)
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step search --stage explore

# 4. Train (final fit + OOS scoring)
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step train
```

## Elvárt output

- `artifacts/lgbm_solusdt_l_fw60_2021/metadata.json` — `fold_week_assignments`, `fold_row_counts`, `n_folds: 4`
- `artifacts/lgbm_solusdt_l_fw60_2021/sample_train_valid.parquet` — `fold_id` oszlop, nincs `segment`
- `artifacts/lgbm_solusdt_l_fw60_2021/search/search_best.json` — 4 fold summary
- `artifacts/lgbm_solusdt_l_fw60_2021/model.pkl` + `features.json` + `params.json`
- `artifacts/lgbm_solusdt_l_fw60_2021/sample_oos.parquet` — 2022-es OOS predictions

## Acceptance Criteria
- [ ] sample lépés sikeresen fut, metadata.json tartalmaz `n_folds: 4`
- [ ] search explore fut, search_best.json tartalmaz 4 fold_summary entry-t
- [ ] train fut, model.pkl és sample_oos.parquet létezik
- [ ] Pipeline status: `train_done`

## Notes
