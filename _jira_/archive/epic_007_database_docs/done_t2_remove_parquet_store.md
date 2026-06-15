---
epic: epic_007
id: t2
title: Remove dead parquet_store.py
assignee: database_agent
status: pr
---

## Goal
Törölni a régi Parquet-alapú write layer-t, ami a DuckDB-re való átállás után halott kóddá vált.

## Scope
- `src/database/store/parquet_store.py` — törölve

## Acceptance Criteria
- [x] A fájl törölve
- [x] Nincs más fájl `src/`-ban ami importálta (ellenőrizve: nulla referencia)

## Notes
A fájl a KAN-35 / KAN-54 ticketek idejéből maradt, napi és Hive-stílusú Parquet partícionálást valósított meg. Az egész codebase DuckDB-re állt át, a parquet_store-ra semmilyen import nem mutatott.
