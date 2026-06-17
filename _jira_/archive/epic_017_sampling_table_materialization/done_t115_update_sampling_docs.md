---
epic: epic_017
id: t115
title: Update sampling and modeling docs
assignee: code_doc_agent
status: todo
blocks: []
blocked_by: [t111, t112, t113, t114]
---

## Goal
Document the new modeling data flow and sample table contract.

## Scope
- `_doc_/3100_sampling.md`
- `_doc_/3120_sampling_splits.md`
- new or updated feature engineering docs
- new or updated quant train docs

## Acceptance Criteria
- [ ] The flow is documented: `quant_train -> feature_set.json -> sample_<sample_id> -> modeling`.
- [ ] Segment values are documented: `train`, `valid`, `purge`, `test`.
- [ ] Sample table naming is documented.
- [ ] Required columns are documented.
- [ ] The role of JSON and parquet artifacts is clarified.

## Notes
Keep docs aligned with the new `quant_train` table name.

Változtatások:
- `_doc_/3101_sampling_yearly.md`: flowchart frissítve (`quant_train` input + DuckDB tábla output); `test` szegmens hozzáadva; validációs checklist frissítve (`sample_table_name`, `check_sample_table`).
- `_doc_/1140_validate.md`: `check_sample_table` dokumentálva (5 invariáns, paraméterek, hiba típusok).
- `_doc_/1260_quant_train.md`: sample tábla materialization szekció hozzáadva (oszlopok, rebuild szemantika, artifact szerepek); flowchart frissítve.
