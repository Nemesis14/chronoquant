---
epic: epic_017
id: t113
title: Create one DuckDB table per sample
assignee: database_agent
status: pr
blocks: [t114]
blocked_by: [t111, t112]
---

## Goal
Create one DuckDB table per sample definition, derived from `quant_train` and the selected feature set.

## Scope
- sample SQL generation
- DuckDB table naming convention
- rebuild behavior
- metadata link to feature-set JSON

## Acceptance Criteria
- [x] Each sample is written as a DuckDB table named `sample_<sample_id>`.
- [x] Columns include `open_time`, `fold_id`, `segment`, selected `feat_*`, `long_mfe_fw60`, and `short_mfe_fw60`.
- [x] Multiple folds can exist in one sample table using `fold_id`.
- [x] Rebuild is deterministic and safe to rerun.
- [x] Existing JSON or parquet artifacts may remain as secondary outputs.

## Notes
The table should be the primary modeling handoff after this epic is complete.

Változtatások:
- `src/database/store/duckdb_store.py`: `materialize_sample_table(conn, sample_id, segment_df)` hozzáadva — CREATE OR REPLACE TABLE `sample_<sample_id>`; kolumn-sorrend: open_time, fold_id, segment, feat_* (sorted), target cols.
- `src/modeling/quantitative/sampling/create_sample.py`: import hozzáadva; `sample_table_name` mező a metadata dictbe; write_yearly_artifacts után read-write kapcsolaton materializálja a táblát.
- `src/modeling/quantitative/00_create_sample.py`: `sample_table` sor a CLI outputban.
