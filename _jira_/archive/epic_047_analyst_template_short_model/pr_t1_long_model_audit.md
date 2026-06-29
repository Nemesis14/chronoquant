---
epic: epic_047
id: t1
title: Long model doc + notebook audit
assignee: analyst_agent
status: todo
blocks: [t2]
blocked_by: []
---

## Goal
Megvizsgálni hogy a long model 4 analysis notebookja (`01_sampling`, `02_feature_engineering`,
`03_hyperparameter_search`, `04_strategy`) helyesen hivatkozza-e a kapcsolódó doc-okat,
reprodukálható-e (hardcoded path-ok, helyes DB séma-nevek), és azonosítani a közös kódrészeket
amelyeket t2-ben `analyst/lib/`-be kell emelni.

## Scope
- `artifacts/lgbm_solusdt_l_fw60_2101_2605/analysis/*.ipynb` — 4 notebook olvasása
- `_doc_/database_and_code_doc/5510_training.md` és kapcsolódó doc-ok referencia-ellenőrzése
- `analyst/lib/` jelenlegi tartalmának felmérése

## Acceptance Criteria
- [ ] Minden notebook reprodukálhatóságát megvizsgálta (path-ok, DB sémák)
- [ ] Hardcoded MODEL_ID, TARGET, DB path helyeket azonosítva és listázva a Notes-ban
- [ ] Közös kódrészek (colors, sns setup, DB loading pattern, plot wrappers) listázva
- [ ] Doc referencia-eltérések (ha van) azonosítva

## Notes
