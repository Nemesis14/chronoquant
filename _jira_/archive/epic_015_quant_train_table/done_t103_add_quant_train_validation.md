---
epic: epic_015
id: t103
title: Add quant_train validation and stats
assignee: database_agent
status: pr
blocks: []
blocked_by: [t101, t102]
---

## Goal
Add validation and stats reporting for the new `quant_train` DuckDB table.

## Scope
- `src/database/store/validate.py`
- `src/database/store/duckdb_stats.py`
- `src/database/01_validate_stats.py`
- database tests under `src/database/tests/`

## Acceptance Criteria
- [x] Row count, min/max `open_time`, and duplicate `open_time` count are reported.
- [x] Null counts are reported for `long_mfe_fw60` and `short_mfe_fw60`.
- [x] Feature null-rate summary is available for `feat_*` columns.
- [x] Validation fails or warns on duplicate `open_time` rows.
- [x] Existing validation for `ohlcv`, `target`, `feat_ohlcv_quant`, and `predictions` still works.

## Notes
This task should not change sampling logic. It only validates the new intermediate training table.

**Implemented:**
- `validate.py`: `check_quant_train_no_duplicates(db_path)` — raises AssertionError ha dup open_time van; skips gracefully ha a tábla/fájl hiányzik
- `duckdb_stats.py`:
  - `TableStats` dataclass: új `dup_count: int = 0` mező
  - `collect_duckdb_stats_report()` defaults: `"quant_train"` hozzáadva (ohlcv, target, feat_ohlcv_quant, predictions, quant_train)
  - quant_train-specifikus null_ratios: mindig tartalmazza `long_mfe_fw60` + `short_mfe_fw60` + első 3 `feat_*` col
  - minden tábla kap `dup_count` számítást (COUNT(*) - COUNT(DISTINCT open_time))
  - `format_duckdb_stats_report()`: `dups={dup_count}` megjelenik minden tábla sorában
- `01_validate_stats.py`: nem igényelt módosítást — quant_train automatikusan bekerül a report-ba az új default miatt
