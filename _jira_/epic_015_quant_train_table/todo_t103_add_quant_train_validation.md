---
epic: epic_015
id: t103
title: Add quant_train validation and stats
assignee: validator_agent
status: todo
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
- [ ] Row count, min/max `open_time`, and duplicate `open_time` count are reported.
- [ ] Null counts are reported for `long_mfe_fw60` and `short_mfe_fw60`.
- [ ] Feature null-rate summary is available for `feat_*` columns.
- [ ] Validation fails or warns on duplicate `open_time` rows.
- [ ] Existing validation for `ohlcv`, `target`, `feat_ohlcv_quant`, and `predictions` still works.

## Notes
This task should not change sampling logic. It only validates the new intermediate training table.
