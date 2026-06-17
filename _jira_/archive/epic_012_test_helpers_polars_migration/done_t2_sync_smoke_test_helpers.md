---
epic: epic_012
id: t2
title: Sync smoke test helper-ek Polars-ra állítása
assignee: database_agent
status: done
blocks: [t4]
---

## Goal

A `sync_features` és `sync_predictions` smoke tesztek `_build_ohlcv()` (és `_build_features()`) helper-ei `pd.DataFrame`-t adnak vissza, de `insert_ohlcv` / `insert_feat_ohlcv_quant` `pl.DataFrame`-t vár.

## Scope

### `src/database/tests/sync_tables/smoke/test_sync_features.py`

- `_build_ohlcv(rows)` (sor ~22-31): teljes átírás Polarsra
  - `pd.date_range(...)` → datetime list (`from datetime import datetime, timedelta`)
  - `pd.Series(range(rows), dtype="float64")` + `pd.concat([...]).max(axis=1)` → egyszerű list comprehension-ök
  - `pd.DataFrame({...})` → `pl.DataFrame({...})`
- `import pandas as pd` sor eltávolítható ha nincs más pandas referencia

### `src/database/tests/sync_tables/smoke/test_sync_predictions.py`

- `_build_ohlcv(rows)` (sor ~45-57): `pd.date_range` + `pd.DataFrame` → `pl.DataFrame`
- `_build_features(ohlcv, feat_cols)` (sor ~65-73): jelenleg pandas `ohlcv` input feltételez
  - Ha az `ohlcv` paraméter Polars lesz, a függvényt is Polars-ra kell írni, vagy `pl.DataFrame`-t ad át
  - `insert_feat_ohlcv_quant` `pl.DataFrame`-t vár — a `_build_features` return-je `pl.DataFrame` legyen
- `_build_target(ohlcv)` (sor ~75) — **már `pl.DataFrame`-t ad vissza** (epic_011-ben javítva), csak az `ohlcv` input típusa változik
- `predict_proba(self, X: pd.DataFrame)` — ez a Mock model, ez maradhat pandas (a `sync_predictions` kódja pandas-t ad a modellnek)
- `import pandas as pd` sor maradhat ha `predict_proba` stub-ban szükséges

## Acceptance Criteria

- [ ] `_build_ohlcv()` mindkét fájlban `pl.DataFrame`-t ad vissza
- [ ] `_build_features()` (ahol van) `pl.DataFrame`-t ad vissza
- [ ] `uv run pytest src/database/tests/sync_tables/smoke/ -v` — `test_sync_features` és `test_sync_predictions` PASSED
- [ ] `uv run pyright src/database/tests/sync_tables/smoke/test_sync_features.py src/database/tests/sync_tables/smoke/test_sync_predictions.py` — 0 új error
- [ ] `ruff check src/database/tests/sync_tables/smoke/ --fix` — clean

## Notes
