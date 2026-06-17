---
epic: epic_015
id: t104
title: Document quant_train data contract
assignee: code_doc_agent
status: pr
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
- [x] End-to-end flow is documented: `feat_ohlcv_quant + target -> quant_train -> feature engineering -> sample tables -> modeling`.
- [x] `quant_train` schema and source columns are documented.
- [x] Rebuild semantics are documented.
- [x] The documentation clearly states that `quant_train` uses `long_mfe_fw60` and `short_mfe_fw60` as the initial target columns.
- [x] Legacy boolean target naming is not used for this new layer.

## Notes
Keep the documentation aligned with the current fw60 log-return outcome schema.

**Implemented:**
- `_doc_/1000_database.md`: ER diagram frissítve (target tábla fw60 oszlopokra, quant_train hozzáadva a megfelelő INNER JOIN relációkkal), új `### quant_train` szekció (séma, rebuild szemantika, CLI, kód referencia)
- `_doc_/1260_quant_train.md` (új): részletes kód-referencia fájl — end-to-end flowchart, séma tábla, rebuild szemantika diagram, CLI usage, implementáció és kapcsolódó dokumentumok
- Megjegyzés: `3300_quant_train.md` helyett `1260_quant_train.md` — a `3300` szám foglalt (`3300_targets.md`); quant_train a database domain (1000-es tartomány) része
- Legacy `trg_*` boolean naming nincs használva; minden target referencia `long_mfe_fw60` / `short_mfe_fw60`
