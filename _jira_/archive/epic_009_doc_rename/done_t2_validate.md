---
epic: epic_009_doc_rename
id: t2
title: Validálás — epic_8 rename
assignee: validator_agent
status: todo
blocked_by: [t1]
---

## Goal

Validálni, hogy az átnevezés teljes és konzisztens.

## Acceptance Criteria

- [ ] Mind a 17 régi fájl (`0001_`–`0232_` prefix) eltűnt a `_doc_/` gyökérből
- [ ] Mind a 17 új fájl (`1000_`–`1320_` prefix) létezik és nem üres
- [ ] `_doc_/0000_project_overview.md` érintetlen
- [ ] Nincs dangling kereszthivatkozás: `grep -r "000[1-9]_\|002[1-9]_\|023[1-9]_" _doc_/` → üres
