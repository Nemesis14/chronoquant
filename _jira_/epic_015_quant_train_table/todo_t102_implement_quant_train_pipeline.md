---
epic: epic_015
id: t102
title: Implement quant_train build pipeline
assignee: database_agent
status: todo
blocks: [t103]
blocked_by: [t101]
---

## Goal
Implement the database pipeline that materializes `quant_train` from `feat_ohlcv_quant` joined with `target`.

## Scope
- new module: `src/database/sync_tables/sync_quant_train.py`
- update: `src/database/02_sync_pipeline.py`
- update: `src/database/store/duckdb_store.py`
- update: `src/database/store/duckdb_query.py` if needed

## Acceptance Criteria
- [ ] `quant_train` can be created/rebuilt from DuckDB data.
- [ ] It includes `open_time`, selected `feat_*`, `long_mfe_fw60`, and `short_mfe_fw60`.
- [ ] It excludes prediction columns.
- [ ] Rows with missing required targets are excluded or explicitly handled according to the schema contract.
- [ ] The unified sync CLI supports `--tables quant_train`.
- [ ] Re-running the pipeline is idempotent and deterministic.

## Notes
The initial implementation may use all `feat_*` columns. Later feature engineering selection will provide a narrowed feature set.
