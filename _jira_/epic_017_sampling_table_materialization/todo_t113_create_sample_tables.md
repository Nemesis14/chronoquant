---
epic: epic_017
id: t113
title: Create one DuckDB table per sample
assignee: database_agent
status: todo
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
- [ ] Each sample is written as a DuckDB table named `sample_<sample_id>`.
- [ ] Columns include `open_time`, `fold_id`, `segment`, selected `feat_*`, `long_mfe_fw60`, and `short_mfe_fw60`.
- [ ] Multiple folds can exist in one sample table using `fold_id`.
- [ ] Rebuild is deterministic and safe to rerun.
- [ ] Existing JSON or parquet artifacts may remain as secondary outputs.

## Notes
The table should be the primary modeling handoff after this epic is complete.
