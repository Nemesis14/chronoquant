---
epic: epic_012
id: t3
title: Pipeline integration test helper-ek Polars-ra állítása
assignee: database_agent
status: done
blocks: [t4]
---

## Goal

A `test_pipeline_integration.py` `_build_ohlcv()` és `_build_features()` helper-ei `pd.DataFrame`-t adnak, de `insert_ohlcv` / `insert_feat_ohlcv_quant` `pl.DataFrame`-t vár. Három integrációs teszt bukik emiatt.

## Scope

### `src/database/tests/sync_tables/integration/test_pipeline_integration.py`

- `_build_ohlcv(rows)` (sor ~41-58): `pd.date_range` + `pd.DataFrame` → `pl.DataFrame`
  - Struktúra: `open_time`, `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `trades`, `taker_buy_base`, `taker_buy_quote`
- `_build_features(ohlcv)` (sor ~61-69): pandas `ohlcv` input → Polars input, return `pl.DataFrame`
  - Polars slicing: `ohlcv["open_time"]`, `ohlcv["close"]` helyett `ohlcv.select(...)` vagy `ohlcv["open_time"].to_list()`
- `_build_target(ohlcv)` (sor ~71) — **már `pl.DataFrame`-t ad vissza** (epic_011-ben javítva), csak az `ohlcv` input típusa változik
- `predict_proba(self, X: pd.DataFrame)` (sor ~37) — Mock model, maradhat pandas típusmegjelöléssel
- `insert_ohlcv` és `insert_feat_ohlcv_quant` hívások (sor ~143-144, ~238-239, ~263): ezek automatikusan javulnak ha a builder-ek Polars-t adnak

### Érintett tesztek

- `test_ohlcv_to_predictions_cross_layer_alignment`
- `test_features_close_matches_ohlcv_close`
- `test_target_open_time_subset_of_ohlcv`

## Acceptance Criteria

- [ ] `_build_ohlcv()` és `_build_features()` `pl.DataFrame`-t ad vissza
- [ ] `uv run pytest src/database/tests/sync_tables/integration/ -v` — mind a 3 korábban failing teszt PASSED
- [ ] `uv run pyright src/database/tests/sync_tables/integration/test_pipeline_integration.py` — 0 új error
- [ ] `ruff check src/database/tests/sync_tables/integration/ --fix` — clean

## Notes
