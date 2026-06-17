---
epic: epic_7
id: t2
title: Hozd létre _doc_/3100_sampling.md
assignee: doc_agent
status: pr
blocked_by: [t1]
blocks: [t3, t4, t5, t6, t7]
---

## Goal

Létrehozni a sampling almodul áttekintő dokumentációját. Ez a fájl a 3.1-es alfejezet
belépési pontja — megmagyarázza a koncepciót és megmutatja a modul belső struktúráját,
majd linkeket ad a részletes fájldokumentációkhoz.

## Scope

- Létrehozandó: `_doc_/3100_sampling.md`
- Forrás: `src/modeling/quantitative/sampling/__init__.py`, teljes mappa struktúra

## Acceptance Criteria

- [ ] Koncepcióleírás: mi az expanding window CV és miért van szükség embargóra
- [ ] `flowchart TD` diagram: a 4 modul (config → audit → splits → artifacts) kapcsolata
- [ ] Alfejezet-index táblázat: 3110–3150 linkekkel
- [ ] Artifact output leírás: `database/<asset>/samples/<id>/` struktúra (3 JSON fájl)
