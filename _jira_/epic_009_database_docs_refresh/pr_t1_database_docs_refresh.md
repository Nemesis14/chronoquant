---
epic: epic_009
id: t1
title: Database modul dokumentáció frissítése az új szerkezetre
assignee: doc_agent
status: pr
---

## Goal
A `src/database/` nagy átalakítása (epic_008) után elavult `_doc_/` fájlok frissítése a tényleges kódbázis alapján.

## Scope
- `_doc_/` — 0002-től kezdődő database doc fájlok

## Acceptance Criteria
- [x] `0216_parquet_store.md` törölve (parquet_store.py törölve lett)
- [x] `0021_store.md` — parquet_store szekció és flowchart node eltávolítva
- [x] `0023_tests.md` — `data_pipeline/` → `sync_tables/` javítva, `sync_pipeline/` hozzáadva
- [x] `0232_pipeline_tests.md` — title/path javítva, `sync_pipeline/smoke/test_sync_pipeline_helpers.py` dokumentálva
- [x] `0212_duckdb_query.md` — `ohlcv_time_stats` helytelen visszatérési típus javítva (`dict` → `tuple[int, str|None, str|None]`)
- [x] `0225_features_polars.md` — `T_MINUS_1_SKIP` értékek javítva; feature name táblák pontosítva (sma_ratio, ema_ratio, macd_diff, stb.)
- [x] `0211_duckdb_store.md` — `_AnyDF` típus, migráció (feat_ohlcv_quant is érintett) pontosítva
- [x] `0231_store_tests.md` — `test_duckdb_stats_audit.py` hozzáadva

## Notes
[doc_agent] 2026-06-15 — Elvégezve
- Törölt fájl: `0216_parquet_store.md`
- Módosított fájlok: `0021_store.md`, `0023_tests.md`, `0232_pipeline_tests.md`, `0212_duckdb_query.md`, `0225_features_polars.md`, `0211_duckdb_store.md`, `0231_store_tests.md`
- A `0002_database_module.md` és `0022_sync_tables.md` tartalmuk alapján pontosak maradtak — nem igényeltek módosítást
