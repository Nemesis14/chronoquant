---
epic: epic_015
id: t101
title: Define quant_train schema and rebuild contract
assignee: database_agent
status: done
blocks: [t102, t103, t104]
blocked_by: []
---

## Goal
Define the canonical `quant_train` DuckDB table schema and the rebuild semantics used by the database and modeling pipelines.

## Scope
- `src/database/store/duckdb_store.py`
- `src/database/store/duckdb_query.py`
- `_doc_/1000_database.md`
- new or updated quant train documentation under `_doc_/`

## Acceptance Criteria
- [ ] Table name is `quant_train`.
- [ ] Required columns are documented: `open_time`, selected `feat_*`, `long_mfe_fw60`, `short_mfe_fw60`.
- [ ] The table is defined as a model-ready join of `feat_ohlcv_quant` and `target`.
- [ ] Rebuild semantics are explicit: deterministic full/range rebuild, not append-only only.
- [ ] Handling of null targets and null features is documented.

## Notes
Use the existing fw60 outcome schema as source of truth. Do not reintroduce legacy boolean `trg_*` targets in this task.

**Implemented:** `rebuild_quant_train()` added to `src/database/store/duckdb_store.py`.
- Schema: `open_time` + összes `feat_*` col (dinamikusan felfedezve feat_ohlcv_quant-ból) + `long_mfe_fw60` + `short_mfe_fw60`
- Full rebuild: `CREATE OR REPLACE TABLE` (determinisztikus)
- Range rebuild: DELETE + INSERT a megadott open_time ablakra
- NULL target sorok INNER JOIN + explicit NULL filter által kizárva
- `duckdb_query.py` nem igényelt módosítást (`query_range`/`query_range_pl` már tetszőleges táblanevet kezel)
