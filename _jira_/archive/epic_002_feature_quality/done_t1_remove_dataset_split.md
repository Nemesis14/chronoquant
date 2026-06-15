---
epic: epic_002_feature_quality
id: t1
title: dataset_split és fold_id eltávolítása a feat_ohlcv_quant sémából
assignee: database_agent
status: pr
blocks: []
blocked_by: []
---

## Goal

A `dataset_split` és `fold_id` oszlopok soha nem töltődnek ki a base táblában
(`sync_features.py` mindig `None`-t ír). Ez a base tábla — a split-logika a
sampling/modeling réteg feladata. Az oszlopokat el kell távolítani a sémából
és minden érintett kódból.

## Scope

- `src/store/duckdb_store.py` — `feat_ohlcv_quant` CREATE TABLE definíció
- `src/data_pipeline/sync_features.py` — `dataset_split` / `fold_id` literal hozzárendelések (sor ~130-135)
- `src/store/validate.py` — `dataset_split`-re hivatkozó JOIN / WHERE feltételek (~sor 93-141)
- `src/store/duckdb_query.py` — ha van ilyen szűrés
- `_tests/store/test_features_target_overview.py` — `dataset_split_values` teszt frissítése
- Migration: meglévő adatbázisban az oszlopok DROP-ja (`ALTER TABLE feat_ohlcv_quant DROP COLUMN ...`)

## Acceptance Criteria

- [ ] `feat_ohlcv_quant` sémában nincs `dataset_split` és `fold_id` oszlop
- [ ] `sync_features.py` nem állítja be ezeket az oszlopokat
- [ ] `validate.py`-ban a `dataset_split`-re hivatkozó ellenőrzés eltávolítva vagy átírva
- [ ] Migration script lefut a meglévő `solusdt.duckdb`-n hibátlanul
- [ ] `uv run pyright src/store/ src/data_pipeline/` — 0 hiba
- [ ] `uv run pytest _tests/store/ -v` — zöld

## Notes

Vizsgálat (2026-06-14): `sync_features.py:130-131` és `sync_predictions.py:126-127`
mindkét helyen `pl.lit(None).cast(pl.Utf8)` — soha nem töltődik ki.
`validate.py:124` már kezeli a `dataset_split IS NOT NULL` feltételt, tehát
a validáció eddig is silently skip-elte ezt az ágat.

Implementáció (2026-06-14):
- `src/store/duckdb_store.py`: `predictions` CREATE TABLE-ből eltávolítva; `ensure_tables`-be migration block kerül (DROP COLUMN ha létezik)
- `src/data_pipeline/sync_features.py`: `dataset_split`/`fold_id` lit + cols_to_keep eltávolítva
- `src/data_pipeline/sync_predictions.py`: `df_out`-ból eltávolítva
- `src/store/validate.py`: `check_no_label_overlap` függvény törölve (már mindig skip-elt)
- `_tests/store/sanity/test_features_target.py`: `required_metadata_columns` frissítve, `dataset_split_values` → `no_dataset_split_column`; `test_no_label_overlap` teszt törölve
- `_tests/store/sanity/test_predictions.py`: `required` set frissítve, `dataset_split_values` teszt törölve
- `_tests/store/smoke/test_duckdb_store_query.py`: fixture adatból eltávolítva
- `_tests/data_pipeline/smoke/test_sync_predictions.py`: `_build_features` frissítve
- pyright 0 hiba, ruff tiszta
