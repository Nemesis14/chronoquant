---
epic: epic_014
id: t5
title: project_overview.md sampling section frissítése
assignee: code_doc_agent
status: pr
story_points: 2
blocks: []
blocked_by: [t2]
---

## Goal

A `_doc_/0000_project_overview.md` sampling-related részét frissíteni az új yearly
random-hour stratégiának megfelelően. A régi expanding-window referenciák eltávolítása.

## Scope

- `_doc_/0000_project_overview.md` — ML Models / Model pipeline szekció
- Esetleg `.agent/` agent manifestek ha a sampling API-t referálják

## Acceptance Criteria

- [x] `_doc_/0000_project_overview.md`-ban nincs hivatkozás expanding-window CV logikára
- [x] A sampling leírás tükrözi az új yearly stratégiát:
  - sample ID formátum: `solusdt_fw60_yearly_{year}`
  - sample tartalom: `metadata.json`, `sample.parquet`, `audit.json`
  - sample.parquet oszlopok: open_time, segment, long_mfe_fw60, short_mfe_fw60
  - CLI: `00_create_sample.py --year {year} --asset-id solusdt`
- [x] A model pipeline diagram/leírás konzisztens az új flow-val
- [x] Agent manifestek nem hivatkoznak régi sampling API-ra — nem igényelt változtatást

## Notes

Nem kell terjedelmes rewrite — csak a tény-szintű hibák javítása. A módszertani
rationale-t a t1 (methodology_agent) kezeli, nem ez a task.

**Done 2026-06-17:** `_doc_/0000_project_overview.md` frissítve — `folds.json` eltávolítva
a repository layout listából; `build_expanding_window_splits` referencia lecserélve
yearly random-hour CLI-leírásra + sample.parquet oszlopok + sample ID formátum.
Agent manifestek ellenőrizve, nem hivatkoznak régi API-ra.
