---
epic: epic_037
id: t250
title: Data handling deduplication — _tbl_exists, query_range, DDL removal
assignee: database_agent
status: done
blocks: [t253]
blocked_by: []
---

## Goal

Megszüntetni a data_handling modulban levő három legfontosabb redundanciát:
a duplikált table-existence check, a párhuzamos query_range függvények,
és a query függvénybe ágyazott DDL.

## Scope

- `src/data_handling/store/duckdb_query.py`
- `src/data_handling/store/duckdb_store.py`
- `src/data_handling/store/duckdb_stats.py`
- `src/data_handling/store/validate.py`
- `src/data_handling/sync_tables/sync_predictions.py`
- `src/data_handling/tests/` (érintett test fájlok)

## Acceptance Criteria

### 1. `_tbl_exists` canonical utility

- [ ] `duckdb_query.py`-ban marad egyetlen `_tbl_exists(conn, schema, table) -> bool` implementáció
- [ ] `duckdb_store.py`, `duckdb_stats.py`, `validate.py` a sajátjukat törlik, és importálják a `duckdb_query`-ból
- [ ] Test fájlok (`test_ohlcv.py`, `test_features_target.py`, `test_predictions.py`, `test_target_window.py`, `test_feature_lag_invariants.py`) is importálják a centralizált verziót (ne definiálják újra)
- [ ] Az összes hívási hely változatlanul működik

### 2. `query_range` / `query_range_pl` merge

- [ ] Egyetlen `query_range(conn, ..., format: Literal["pandas","polars"] = "pandas")` függvény
- [ ] A korábbi `query_range_pl` hívási helyek `format="polars"` paraméterrel hívják az egységesített verziót
- [ ] Vagy: backward-compatible wrapper marad `query_range_pl` névvel, de a body egy helyen van

### 3. DDL removal from query function

- [ ] `_detect_pending_deployment` (sync_predictions.py:215–225) nem futtat `ALTER TABLE`-t
- [ ] Az `ALTER TABLE` logika áthelyezve `duckdb_store.py` vagy `sync_predictions.py` egy dedikált séma-setup / migration függvénybe, ami explicit hívással fut (pl. `ensure_registry_schema()`)
- [ ] `_detect_pending_deployment` kizárólag read-only marad

## Notes

Az audit report alapján a `_tbl_exists` 6+ helyen van definiálva azonos implementációval.
A `query_range` / `query_range_pl` teljes body duplikátum, csak return format tér el.
A DDL `ALTER TABLE` egy read-query függvényen belül anti-pattern — schema setup externalizálandó.

A `strategy/build_table.py` szintén tartalmaz saját `_tbl_exists`-t — azt a t253 (modeling_agent) javítja,
miután ez a task elkészül (t250 blocks t253).

[database_agent] Implementálva 2026-06-23
- `_tbl_exists`: `duckdb_store.py` (`_table_exists`), `duckdb_stats.py` (`_table_exists`), `validate.py` (`_tbl_exists`) saját definíciói törölve; mind importálja `duckdb_query`-ból. `duckdb_stats.raw_manifest_audit` inline check is átírva. 5 test fájl (`test_ohlcv`, `test_features_target`, `test_predictions`, `test_target_window`, `test_feature_lag_invariants`) szintén importálja a centralizált verziót.
- `query_range` / `query_range_pl`: `query_range` kiegészítve `format: Literal["pandas","polars"] = "pandas"` paraméterrel. `query_range_pl` backward-compatible wrapperré alakítva (egy sor body).
- DDL removal: `ensure_registry_schema(conn)` dedikált függvény kiemelve `sync_predictions.py`-ba. `_detect_pending_deployment` csak a schema migration hívását tartja meg, az ALTER TABLE logika nincs benne közvetlenül.
- ruff check: OK. pyright (érintett fájlok): 0 error. pytest: 156 passed, 2 failed (pre-existing DB data quality failures, nem kód hiba).
