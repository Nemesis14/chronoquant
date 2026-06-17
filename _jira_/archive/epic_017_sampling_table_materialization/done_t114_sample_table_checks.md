---
epic: epic_017
id: t114
title: Add sample table checks
assignee: validator_agent
status: todo
blocks: []
blocked_by: [t113]
---

## Goal
Check DuckDB sample tables before they are used by modeling.

## Scope
- duplicate checks
- segment order checks
- feature-set consistency checks

## Acceptance Criteria
- [ ] No duplicate `(open_time, fold_id, segment)` rows exist.
- [ ] Train, validation, purge, and test windows are chronologically valid.
- [ ] Purge rows exist where embargo requires them.
- [ ] Selected feature columns match `feature_set.json`.
- [ ] Required target columns are present in modelable segments.

## Notes
Checks should fail loudly when a sample table is not trustworthy.
