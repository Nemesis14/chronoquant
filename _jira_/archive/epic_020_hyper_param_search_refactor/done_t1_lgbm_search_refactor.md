---
epic: epic_020
id: t1
title: lgbm_search.py + 02_hyper_param_search.py + pipeline.py refaktor
assignee: modeling_agent
status: pr
blocks: [t3]
---

## Goal

A search modul teljes refaktorálása: legacy kód eltávolítása, yearly sample + feature_set.json
alapú adatbetöltés, helyes bináris label generálás, CV a selected_valid_weeks alapján.

## Scope

- `src/modeling/search/lgbm_search.py` — teljes újraírás
- `src/modeling/02_hyper_param_search.py` — CLI refaktor
- `src/modeling/pipeline.py` — step_search illesztés

## Acceptance Criteria

- [ ] `_load_feature_cols(artifact_dir)` olvassa a `feature_set.json["selected"]`-t
- [ ] `_load_search_dataset` polars parquet-betöltés + DuckDB join (query_range_pl)
- [ ] Bináris label: quantile thresholding a train segment alapján
- [ ] CV: 12 fold a selected_valid_weeks-ből (egy fold = egy hét validation set, teljes train segment)
- [ ] Stage defaults: smoke=5 trial/2 fold, explore=60/all, refine=30/all, row_stride=1 mindenhol
- [ ] Output: search_best.json + best_params.json (artifact_dir/search/)
- [ ] CLI: --model (nem --model-id), --stage, --n-trials, --timeout-hours, --fold-limit, --retry-failed
- [ ] Minden legacy kód eltávolítva (Stage 0 audit, champion guardrail, folds.json loading, _KNOWN_DUPLICATES)
- [ ] pipeline.py step_search továbbítja a stage + n_trials + fold_limit + timeout_hours argokat

## Notes

Implementáció állapota: in_progress
