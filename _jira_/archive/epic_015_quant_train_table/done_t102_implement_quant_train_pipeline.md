---
epic: epic_015
id: t102
title: Implement quant_train build pipeline
assignee: database_agent
status: done
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

**Implemented:**
- `src/database/sync_tables/sync_quant_train.py` — `sync_quant_train(asset_id, start_time, end_time)`
- `src/database/03_build_quant_train.py` — standalone ad-hoc CLI (nem pipeline!)
- `src/database/02_sync_pipeline.py` — **NEM módosult**: quant_train szándékosan nincs a live sync pipeline-ban (user döntés: ad-hoc, training előtt futtatandó)
- A `--tables quant_train` criterion helyett: `uv run python src/database/03_build_quant_train.py [--start ...] [--end ...]`
- Idempotens: full rebuild mindig CREATE OR REPLACE, range rebuild DELETE+INSERT
