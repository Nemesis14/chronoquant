---
epic: epic_010_analyst_sampling
id: t3
title: Sampling dokumentáció frissítése — parquet artifact
assignee: doc_agent
status: done
blocks: []
blocked_by: [t2]
---

## Goal

A `_doc_/3100_sampling.md` (és szükség esetén `_doc_/3110_sampling_config.md`) frissítése:
dokumentálja, hogy a sample creation parquet fájlt is létrehoz, leírva annak struktúráját,
elérési útját és a `segment` oszlop értékkészletét.

## Scope

- `_doc_/3100_sampling.md` — fő sampling dokumentáció
- `_doc_/3110_sampling_config.md` — ha releváns
- `_doc_/0000_project_overview.md` — ha a sample artifact listát is tartalmazza, frissítendő

## Acceptance Criteria

- [x] A parquet artifact leírva: fájlnév, elérési út, struktúra
- [x] `segment` oszlop értékkészlete dokumentálva (`fold_N_train`, `fold_N_valid`, `test`)
- [x] A doc konzisztens a t2-ben ténylegesen implementált megoldással

## Notes

t2 (modeling_agent) után futhat — a tényleges fájlnév és struktúra onnan derül ki.

### 2026-06-16 — Implementáció

- `_doc_/3100_sampling.md` frissítve:
  - Overview szöveg parquet-tel bővítve
  - Mermaid flowchart: `_write_sample_parquet` lépés hozzáadva
  - Artifact output tábla: `sample.parquet` sor hozzáadva
  - `sample.parquet` struktúra + `segment` értékkészlet dokumentálva
  - Polars lazy frame usage példa hozzáadva
  - Validációs checklist parquet-tel bővítve
- `_doc_/3110_sampling_config.md` — nem érintett (SamplingConfig nem változott)
- `_doc_/0000_project_overview.md` — nem érintett (artifact path ott nincs)
