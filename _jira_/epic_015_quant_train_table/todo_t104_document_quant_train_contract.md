---
epic: epic_015
id: t104
title: Document quant_train data contract
assignee: code_doc_agent
status: todo
blocks: []
blocked_by: [t101, t102]
---

## Goal
Document the `quant_train` table and how downstream feature engineering and sampling should consume it.

## Scope
- `_doc_/1000_database.md`
- new doc candidate: `_doc_/3300_quant_train.md`
- relevant modeling/sampling docs

## Acceptance Criteria
- [ ] End-to-end flow is documented: `feat_ohlcv_quant + target -> quant_train -> feature engineering -> sample tables -> modeling`.
- [ ] `quant_train` schema and source columns are documented.
- [ ] Rebuild semantics are documented.
- [ ] The documentation clearly states that `quant_train` uses `long_mfe_fw60` and `short_mfe_fw60` as the initial target columns.
- [ ] Legacy boolean target naming is not used for this new layer.

## Notes
Keep the documentation aligned with the current fw60 log-return outcome schema.
