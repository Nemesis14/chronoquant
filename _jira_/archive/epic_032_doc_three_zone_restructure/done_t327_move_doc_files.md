---
epic: epic_032
id: t327
title: Meglévő _doc_ fájlok átmozgatása a három zónába
assignee: code_doc_agent
status: done
blocks: [t328, t329]
blocked_by: [t325]
---

## Goal
A jelenlegi `_doc_/` fájlok fizikai átmozgatása a három zóna-alkönyvtárba a t325-ben
definiált szabály szerint, a téma-számozás megtartásával és a kereszthivatkozások javításával.

## Scope
- `_doc_/` X110+ kód-referencia → implementation zóna
- `_doc_/` X000/X100 metodológia → methodology zóna
- `_doc_/*.ipynb` analyst notebookok + `_doc_/analysis/` → results zóna
- belső kereszthivatkozások (relatív linkek) javítása
- `_doc_/_plans_/` — döntés: marad-e külön vagy methodology alá kerül
- Hivatkozás: t325 célstruktúra

## Acceptance Criteria
- [ ] minden _doc_ fájl a helyes zónában van
- [ ] téma-számozás megtartva zónán belül
- [ ] belső linkek nem töröttek
- [ ] 0000_project_overview.md repository-layout szakasza frissítve

## Notes
- 46 .md áthelyezve (git mv): 33 → database_and_code_doc/, 13 → methodology_doc/.
  Besorolás tartalom szerint (user-döntés): kód-jellegű X100-ak (2100/2200/3100/4100) +
  teljes 1xxx blokk a kód-zónába; X000/X010/methodology-X100 a methodology-zónába.
- `archieve/` (6 fájl) → `models_doc/archive/` (elnevezés-javítva); üres `archieve/` törölve.
- 0000 és 0001 a `_doc_` gyökérben maradtak (globális); `_plans_/` külön maradt (user-döntés).
- Kereszthivatkozások javítva: relatív linkek zónán belül bare, zónák közt `../<zóna>/`.
  5 pre-existing törött link (0221–0225 régi számozás) is javítva. 82 link ellenőrizve, 0 törött.
- 0000 repository-layout szakasz a 3-zónás struktúrára frissítve.
