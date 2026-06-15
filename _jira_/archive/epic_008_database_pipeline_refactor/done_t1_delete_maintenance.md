---
epic: epic_008
id: t1
title: maintenance.py törlése, hasznos függvények átköltöztetése
assignee: database_agent
status: done
blocks: [t3]
---

## Goal
A halott `maintenance.py` törlése. A két hasznos függvény (`raw_manifest_audit`, `log_dataset_check`) kerüljön át `store/duckdb_stats.py`-ba, ahol a többi stats/audit logika van.

## Scope
- `src/database/store/maintenance.py` — törlés
- `src/database/store/duckdb_stats.py` — `raw_manifest_audit` + `log_dataset_check` hozzáadása

## Acceptance Criteria
- [x] `maintenance.py` törölve
- [x] `raw_manifest_audit` és `log_dataset_check` működőképesen elérhető `duckdb_stats.py`-ból
- [x] Nincs törött import sehol
- [x] `_chunk_date_ranges` duplikáció megszűnt (a maintenance-beli példány elveszik)

## Notes
`backfill_predictions` és `rebuild_derived_tables` — ezeket semmi sem hívja, egyszerűen törlendők.

2026-06-15: Végrehajtva. `maintenance.py` törölve. `raw_manifest_audit` és `log_dataset_check` áthelyezve `duckdb_stats.py`-ba. Szükséges importok (`logging`, `duckdb_query` függvények) hozzáadva. Grep-pel ellenőrizve: más fájlban nem volt `maintenance`-re hivatkozó import. `_chunk_date_ranges` duplikáció megszűnt a törlésével.

[validator] done — 2026-06-15
ruff: clean. pyright: clean (0 errors). Tests: 112/112 pass.
New smoke tests: `src/database/tests/store/smoke/test_duckdb_stats_audit.py` (6 tests for `raw_manifest_audit` + `log_dataset_check`).
