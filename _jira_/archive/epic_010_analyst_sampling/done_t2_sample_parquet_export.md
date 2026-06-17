---
epic: epic_010_analyst_sampling
id: t2
title: Sample creation parquet export
assignee: modeling_agent
status: done
blocks: [t3, t4]
blocked_by: []
---

## Goal

A `00_create_sample.py` és a mögöttes sampling modul kibővítése: a meglévő JSON artifactok
(metadata.json, folds.json, audit.json) mellé kerüljön egy `.parquet` fájl is, amely az
összes sort tartalmazza egy `segment` oszloppal (`fold_1_train`, `fold_1_valid`, ..., `test`).
A parquet legyen az elsődleges input a model traininghez és az elemzésekhez.

## Scope

- `src/modeling/quantitative/sampling/` — orchestrator és/vagy splits modul
- `src/modeling/quantitative/00_create_sample.py` — parquet írás hívása
- `database/solusdt/samples/<sample_id>/` — ide kerül a parquet fájl
- Futtatás: `uv run python src/modeling/quantitative/00_create_sample.py` az aktuális sample-re

## Acceptance Criteria

- [x] Parquet fájl létrejön a sample mappában (`sample.parquet`)
- [x] `segment` oszlop tartalmazza a fold + split azonosítót (`fold_1_train`, `fold_1_valid`, ..., `test`)
- [x] Minden sor benne van (features + targets + segment)
- [x] A script sikeresen lefut, nincs exception
- [ ] `ruff check` és `pyright` tiszta az érintett modulokra (validator_agent)

## Notes

Parquet fájlnév konvenció és elérési út egyeztetendő a modeling_agent döntése alapján.

### 2026-06-16 — Implementáció

- `_write_sample_parquet()` hozzáadva `create_sample.py`-hoz
- DuckDB `COPY ... TO` streaming megközelítés — nem materializál memóriában
- UNION ALL 11 szegmens (5 × train + 5 × valid + test)
- Kimenet: `database/solusdt/samples/solusdt_fw60_2010_2605/sample.parquet` (10.3 GB, ZSTD)
- Sorok: 9,669,301; `segment` értékek: fold_1_train..fold_5_valid, test
