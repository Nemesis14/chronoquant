---
epic: epic_7
id: t9
title: Validálás — epic_7 összes pr_ ticket
assignee: validator_agent
status: todo
blocked_by: [t1, t2, t3, t4, t5, t6, t7, t8]
---

## Goal

Validálni az epic_7 összes elkészült dokumentációs és skill-frissítési munkáját.

## Scope

Minden `pr_` státuszú epic_7 ticket.

## Acceptance Criteria

- [ ] Minden `_doc_/3xxx_*.md` fájl létezik és nem üres
- [ ] Minden Mermaid diagram szintaxisa helyes (column 0 rule)
- [ ] `docs_skill.md` és `doc_agent.md` módosításai konzisztensek egymással
- [ ] `ruff check .agent/ --fix` — nincs hiba (nem Python, de ellenőrzés)
- [ ] Nincs broken kereszthivatkozás a doc fájlok között
