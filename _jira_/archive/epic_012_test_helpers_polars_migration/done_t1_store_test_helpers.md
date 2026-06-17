---
epic: epic_012
id: t1
title: Store test helper-ek Polars-ra állítása
assignee: database_agent
status: done
blocks: [t4]
---

## Goal

A `insert_ohlcv`, `insert_feat_ohlcv_quant`, `insert_predictions` függvények `pl.DataFrame`-t várnak, de a store tesztekben a segédfüggvények `pd.DataFrame`-t adnak vissza — `AttributeError: 'DataFrame' object has no attribute 'select'` hibát okoz.

## Scope

### `src/database/tests/store/smoke/test_duckdb_store_query.py`

- `_ohlcv_frame()` (sor ~38): `pd.DataFrame` + `pd.to_datetime(...)` → `pl.DataFrame` + `datetime` list
- `_feature_frame()` (sor ~61): `pd.DataFrame` + `pd.to_datetime(...)` → `pl.DataFrame`
- `_prediction_frame()` (sor ~92): `pd.DataFrame` + `pd.to_datetime(...)` → `pl.DataFrame`
- `import pandas as pd` sor eltávolítható, ha más pandas használat nincs
- `pd.Timestamp("2024-01-01 00:02:00")` összehasonlítás (`latest_open_time` return) → `datetime` string-re vagy `datetime` objektumra

### `src/database/tests/store/smoke/test_duckdb_stats.py`

- `_build_store()` helper (sor ~34): `pd.DataFrame({ "open_time": pd.date_range(...) })` → `pl.DataFrame`
  - Pl. `pl.datetime_range(...)` vagy `[datetime(2024,1,1) + timedelta(hours=i) for i in range(48)]`
- Minden egyéb `pd.Timestamp` / `pd.Timedelta` → `datetime.datetime` + `datetime.timedelta`

### `src/database/tests/store/smoke/test_duckdb_stats_audit.py`

- `_insert_rows()` helper (sor ~27): `pd.DataFrame({ "open_time": pd.date_range(...) })` → `pl.DataFrame`
- Polars datetime list + oszlopok (`open`, `high`, `low`, `close`, `volume`, stb.)

### `src/database/tests/store/perf/test_query_timing.py`

- `test_timing_insert_100k_rows` (sor ~98-100): `pd.DataFrame` → `pl.DataFrame`
  - `ts = pd.date_range(...)` → `[datetime(2020,1,1) + timedelta(minutes=i) for i in range(n_rows)]`
- A `pd.Timestamp` / `pd.Timedelta` sorok (47-48, 146-147, 163-164, stb.) query_range outputhoz tartoznak (pandas) — azok maradhatnak, vagy `datetime.datetime`-ra válthatók

## Acceptance Criteria

- [ ] Mind a 4 fájlban az `insert_*` hívások előtt `pl.DataFrame` épül
- [ ] `uv run pytest src/database/tests/store/ -v` — 0 FAILED (csak store/ alkönyvtár)
- [ ] `uv run pyright src/database/tests/store/` — 0 új error (pre-existing pandas/polars hibák javultak)
- [ ] `ruff check src/database/tests/store/ --fix` — clean

## Notes
