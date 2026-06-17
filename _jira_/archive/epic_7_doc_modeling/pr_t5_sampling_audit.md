---
epic: epic_7
id: t5
title: Hozd létre _doc_/3130_sampling_audit.md
assignee: doc_agent
status: pr
blocked_by: [t2]
---

## Goal

Dokumentálni az `audit_feature_table` függvényt és a mögöttes SQL auditot.

## Scope

- Létrehozandó: `_doc_/3130_sampling_audit.md`
- Forrás: `src/modeling/quantitative/sampling/audit.py`

## Acceptance Criteria

- [ ] `sequenceDiagram`: audit_feature_table → DuckDB → _run_audit → return dict
- [ ] Return dict mezők táblázata: key | type | leírás
- [ ] `data_start_safe` és `data_end_safe` deriválási logikája kiemelve (miért fontos a safe boundary)
- [ ] Gap detection SQL logika magyarázva (LAG window function)
- [ ] `_run_audit` internal függvény dokumentálva (nem public, de fontos a megértéshez)
