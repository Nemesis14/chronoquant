---
epic: epic_047
id: t6
title: Code doc frissítés — analyst template struktúra
assignee: code_doc_agent
status: todo
blocks: [t7]
blocked_by: [t3]
---

## Goal
Dokumentálni az `analyst/lib/` új template struktúráját és az analysis notebookok
konvencióját a `_doc_/database_and_code_doc/` zónában, hogy jövőbeli modellekhez
az analyst_agent tudja mit kell követni.

## Scope
- `_doc_/database_and_code_doc/` — új vagy frissített analyst doc (pl. `8200_analyst_templates.md`)
- `analyst/lib/` — modul-szintű docstring frissítés (ha szükséges)

## Acceptance Criteria
- [ ] Dokumentálva van: analyst/lib/ modulok listája és felelősségük
- [ ] Dokumentálva van: artifact/analysis/ notebook template struktúra (01-04)
- [ ] Dokumentálva van: hogyan kell új modellhez notebookot készíteni (paraméter csere)
- [ ] Belső hivatkozások helyes path-okra mutatnak

## Notes
t3 után fut — template struktúra akkor válik véglegesé.
