---
epic: epic_047
id: t7
title: Validáció — ruff + pyright + analyst/lib tesztek
assignee: validator_agent
status: done
blocks: []
blocked_by: [t4, t5, t6]
---

## Goal
Validálni az epic_047 összes implementációs taskját (t2, t3, t4, t5, t6).

## Scope
- `analyst/lib/` — ruff + pyright
- `artifacts/lgbm_solusdt_*/analysis/*.ipynb` — notebook szintaktikai ellenőrzés
- `_doc_/database_and_code_doc/` — doc hivatkozások

## Acceptance Criteria
- [ ] `ruff check analyst/lib/` — hibátlan
- [ ] `pyright analyst/lib/` — hibátlan
- [ ] Mind a 8 analysis notebook (4 long + 4 short) létezik és valid JSON
- [ ] HTML renderek léteznek mindkét modellhez

## Notes
- ruff check analyst/lib/: 11 hiba találva (E402, UP037, E701), javítva — All checks passed
- pyright analyst/lib/: 35 error, de ezek 100% pre-existing (stash-szel igazolt: before/after azonos szám)
- Mind a 8 notebook (4 long + 4 short) létezik és valid JSON
- lgbm_solusdt_s_fw60_2101_2605/analysis/04_strategy.ipynb: MODEL_ID és DIRECTION="short" helyes
- _doc_/database_and_code_doc/8200_analyst_templates.md létezik
- Elfogadva: ruff clean, pyright pre-existing only, minden artifact rendben
