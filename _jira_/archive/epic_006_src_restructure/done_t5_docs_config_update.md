---
epic: epic_006
id: t5
title: Docs, config és CLAUDE.md frissítése az új struktúrának megfelelően
assignee: doc_agent
status: todo
blocked_by: [t4]
---

## Goal
Az összes dokumentáció és konfig fájl frissítése hogy tükrözze az új `src/` struktúrát.

## Scope

**`_doc_/project_overview.md`:**
- Repository Layout szekció frissítése (database/, modeling/quantitative/, modeling/elliott/, trading/, ui/)
- Agent Ownership tábla frissítése
- Testing Rules parancsok frissítése (új path-ok)

**`CLAUDE.md`:**
- Delegation Table frissítése:
  - `database_agent` → `src/database/`
  - `modeling_agent` → `src/modeling/`
  - `ui_agent` → `src/ui/`, `src/trading/`

**`_doc_/README.md`:**
- Struktúra diagram frissítése az új számozásnak és lapos struktúrának megfelelően

**`_tests/`** — ellenőrizni hogy a test path hivatkozások helyesek-e az új struktúrában.

## Acceptance Criteria
- [ ] `project_overview.md` Repository Layout helyes
- [ ] `project_overview.md` Agent Ownership helyes
- [ ] `CLAUDE.md` Delegation Table helyes
- [ ] `_doc_/README.md` struktúra diagram helyes
- [ ] `_tests/` path hivatkozások konzisztensek

## Notes
Nem ír alkalmazáskódot — csak dokumentáció és konfig.
