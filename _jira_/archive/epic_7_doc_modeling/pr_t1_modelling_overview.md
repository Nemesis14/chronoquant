---
epic: epic_7
id: t1
title: Hozd létre _doc_/3000_modelling.md
assignee: doc_agent
status: pr
blocks: [t2, t3, t4, t5, t6, t7]
---

## Goal

Létrehozni a modeling domain főfejezet-áttekintőjét az új 1000/1100/1110 számozási séma
szerint. Ez a fájl a fejezet belépési pontja — nem megy bele modul-szintű részletekbe.

## Scope

- Létrehozandó: `_doc_/3000_modelling.md`
- Forrás kontextus: `_doc_/0000_project_overview.md` (Modeling szekció)
- Hivatkozott src: `src/modeling/quantitative/`

## Acceptance Criteria

- [ ] Rövid leírás a modeling domain szerepéről a projektben (LightGBM pipeline, v4 modellek)
- [ ] `flowchart TD` áttekintő diagram: ohlcv → features → sampling → train → predictions
- [ ] Táblázat: az alfejezetekre mutató linkekkel (3100, 3200 stb. — a jövőbeli fejezetek is felsorolva mint "tervezett")
- [ ] Docs skill `flow/module doc` struktúra szerint
