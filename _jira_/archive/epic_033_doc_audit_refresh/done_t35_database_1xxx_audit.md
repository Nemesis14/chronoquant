---
epic: epic_033
id: t35
title: 1xxx database/store/sync/tests + snapshots/registry kód-ref audit
assignee: code_doc_agent
status: done
blocks: [t39]
blocked_by: []
---

## Goal
Az 1xxx tartomány (database infrastruktúra) kód-ref doksiainak ellenőrzése és szinkronba hozása
a tényleges kóddal; redundancia eltávolítása; felfelé-linkek ellenőrzése.

## Scope
- 1000/1001, 1100–1150 (store: duckdb_store, query, stats, validate, toolkit)
- 1200/1210/1230 (sync_tables: sync_ohlcv, sync_predictions)
- 1300/1310/1320 (tests)
- 1410 (snapshots_code) ↔ `store/snapshots.py`, 1510 (registry_code) ↔ `store/registry.py`
- Ellenőrzés: tényleges függvény-aláírások, CLI-k, séma egyezése a doksival

## Acceptance Criteria
- [ ] Minden 1xxx doksi a tényleges kódot tükrözi (függvények, paraméterek, séma)
- [ ] Nincs duplikált methodology-tartalom — felfelé link a `../methodology_doc/`-ra
- [ ] Megtalált anomáliák/elavult részek javítva
- [ ] Más zóna nincs módosítva (csak link)

## Notes
Az infra CLI-k (migrations, registry_validator, sync_quant_train, 04/06) a t38-ban kapnak új doksit;
itt a meglévő 1xxx oldalak auditja a cél.

### Elvégzett munka (2026-06-22, code_doc_agent)

**Fő drift: minden doksi `src/database/` path-ot tartalmazott — a valódi modul `src/data_handling/`.**

Javított fájlok és főbb változások:

- **1001_database_module.md**: path javítás; modul struktúra kiegészítve (03–06 scriptek); `01_validate_stats.py` `--asset-id` flag + helyes tábla lista; `02_sync_pipeline.py` path; függőségi sorrend pontosítás
- **1000_database.md**: `predictions` tábla séma kiegészítve: `long_mfe_fw60`, `short_mfe_fw60`, `long_model_id`, `short_model_id` oszlopok; SQL DDL frissítve; legacy migráció leírás javítva
- **1100_store.md**: path javítás; `ensure_tables` leírás (migrációk); `backfill_predictions`, `rebuild_quant_train` hozzáadva; írási módok táblája kiegészítve
- **1110_duckdb_store.md**: path javítás; flowchart kiegészítve; `ensure_tables` → v1–v5 migration tábla; `_insert_append_only` `COALESCE` + `int` visszatérés; `insert_ohlcv/feat/predictions` visszatérés típus; `insert_target` DELETE szemantika javítva (IN, nem range); `insert_predictions` séma; `backfill_predictions` + `rebuild_quant_train` új szekciók
- **1120_duckdb_query.md**: path javítás; `asof_join_predictions_features` `feature_cols` típus javítva (`list[str] | None`)
- **1130_duckdb_stats.md**: path javítás; `TableStats.dup_count` mező hozzáadva; default tábla lista `quant_train`-nel kiegészítve; example kimenet `dups=` mezővel
- **1140_validate.md**: path javítás; `check_quant_train_no_duplicates` új szekció; `check_no_future_features` raises `FileNotFoundError` javítva; `check_target_no_current_bar` — nem dob `AssertionError` (csak log); import path javítva
- **1150_toolkit.md**: path javítás; `get_time_range` / `get_dataset_columns` / `get_row_count` szignatúra javítva (`dataset` első param); return type `str | None` (nem `pd.Timestamp`); példa kimenet frissítve
- **1200_sync_tables.md**: `sync_predictions` `backfill` param + deploy detektálás megemlítve
- **1210_sync_ohlcv.md**: path javítás (CLI példák)
- **1230_sync_predictions.md**: path javítás; `backfill` param hozzáadva; `_load_model_artifacts` path leírás javítva (`artifact_dir`); output szerkezet kiegészítve (fw60 + model_id oszlopok); Deploy/Cutover szekció hozzáadva
- **1300_tests.md**: path javítás (pytest parancsok)
- **1310_store_tests.md**: path javítás; `test_quant_train.py` smoke szekció hozzáadva; sanity `test_quant_train.py` hozzáadva; `test_quant_train_timing.py` perf szekció hozzáadva
- **1320_pipeline_tests.md**: path javítás; `test_sync_predictions.py` teszt nevek javítva (tényleges függvénynevekre)
- **1410_snapshots_code.md**: `compute_content_sha256` algoritmus leírás javítva (SUM(hash) fingerprint, nem string_agg+to_json)
- **1510_registry_code.md**: `_migration_002_deployments_lifecycle` (v2) + `REG_MIGRATIONS` lista hozzáadva
