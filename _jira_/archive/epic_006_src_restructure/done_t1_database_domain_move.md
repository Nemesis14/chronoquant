---
epic: epic_006
id: t1
title: Database domain — fájlok mozgatása és script számozás
assignee: database_agent
status: todo
blocks: [t4]
---

## Goal
A jelenlegi `src/store/` és `src/data_pipeline/` modulokat beköltöztetni a `src/database/` domain alá. A `scripts/store/` és `scripts/data_pipeline/` scripteket számozva a `src/database/` gyökerébe mozgatni.

## Scope

**Mozgatások:**
- `src/store/*` → `src/database/store/`
- `src/data_pipeline/*` → `src/database/data_pipeline/`

**Scripts (számozva, `src/database/` gyökerébe):**
- `scripts/store/validate_duckdb_stats.py` → `src/database/01_validate_stats.py`
- `scripts/data_pipeline/sync_ohlcv.py` → `src/database/02_sync_ohlcv.py`
- `scripts/data_pipeline/rebuild_derived.py` → `src/database/03_rebuild_derived.py`
- `scripts/store/benchmark_duckdb.py` → `src/database/04_benchmark_duckdb.py`

**NE frissítsd az import path-okat** — az t4 feladata.

## Acceptance Criteria
- [ ] `src/database/store/` tartalmaz minden fájlt ami `src/store/`-ban volt
- [ ] `src/database/data_pipeline/` tartalmaz minden fájlt ami `src/data_pipeline/`-ban volt
- [ ] A 4 script számozva megvan `src/database/` alatt
- [ ] `src/store/` és `src/data_pipeline/` törölt
- [ ] `scripts/store/` és `scripts/data_pipeline/` törölt

## Notes
Import path-ok szándékosan érintetlenek maradnak — a kód t4-ig broken állapotban lesz.
