---
epic: epic_008
id: t2
title: data_pipeline/ átnevezése sync_tables/-re
assignee: database_agent
status: done
blocks: [t3]
---

## Goal
A `data_pipeline/` mappa neve félrevezető — a "pipeline" szó az orchestrációra illik, nem az egyedi tábla-szintű sync függvényekre. Átnevezés `sync_tables/`-re.

## Scope
- `src/database/data_pipeline/` → `src/database/sync_tables/`
- Összes `from database.data_pipeline.*` import frissítése a teljes `src/`-ban
- `_doc_/` referenciák frissítése ha vannak

## Acceptance Criteria
- [x] Mappa átnevezve
- [x] Minden import frissítve (`database.data_pipeline` → `database.sync_tables`)
- [x] `uv run pyright src/database/` hibamentes (validator_agent feladata)
- [x] `ruff check src/database/ --fix` hibamentes (validator_agent feladata)

## Notes
Végrehajtva 2026-06-15.

- `src/database/data_pipeline/` → `src/database/sync_tables/` (6 fájl: `__init__.py`, `sync_ohlcv.py`, `sync_features.py`, `sync_targets.py`, `sync_predictions.py`, `_features_polars.py`)
- Belső self-import frissítve: `sync_features.py` → `database.sync_tables._features_polars`
- Külső importok frissítve: `src/database/02_sync_ohlcv.py`, `src/database/03_rebuild_derived.py`, `src/ui/sync.py`
- `src/database/store/maintenance.py` — git-ben törölt fájl, fizikai fájl nem létezik, nem érintett
- Teszt fájlok frissítve: `test_sync_ohlcv.py`, `test_sync_features.py`, `test_sync_predictions.py`, `test_sync_targets.py`, `test_leak_prevention.py`, `test_pipeline_integration.py`, `test_target_window.py`
- `_doc_/` frissítve: `0000_project_overview.md`, `0002_database_module.md`, `0022_data_pipeline.md`, `0221_sync_ohlcv.md`, `0222_sync_features.md`, `0223_sync_predictions.md`, `0224_sync_targets.md`, `0225_features_polars.md`
- Régi `src/database/data_pipeline/` mappa törölve
- `database.data_pipeline` referencia nem maradt a `src/` alatt (grep ellenőrzve)

[validator] done — 2026-06-15
ruff: clean. pyright: clean (0 errors). Tests: 112/112 pass.
No `data_pipeline` import references found in tests (grep verified).
