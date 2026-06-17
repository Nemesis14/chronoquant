---
epic: epic_7
id: t4
title: Hozd létre _doc_/3120_sampling_splits.md
assignee: doc_agent
status: pr
blocked_by: [t2]
---

## Goal

Dokumentálni a `build_expanding_window_splits` függvényt és az expanding window logikát.

## Scope

- Létrehozandó: `_doc_/3120_sampling_splits.md`
- Forrás: `src/modeling/quantitative/sampling/splits.py`

## Acceptance Criteria

- [ ] `flowchart TD` az expanding window mechanizmusáról: data_start → min_train → fold rolling → test cutoff
- [ ] `sequenceDiagram`: caller → build_expanding_window_splits → return dict
- [ ] Paraméterek táblázata
- [ ] Return dict struktúra: `folds[]` és `test` mezők leírva
- [ ] Embargo logika kiemelve: miért kell gap train és valid között
- [ ] ValueError feltételek dokumentálva
