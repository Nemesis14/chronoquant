# tests/sync_tables/ + sync_pipeline/ — Pipeline Tesztek

`src/database/tests/sync_tables/` és `src/database/tests/sync_pipeline/`

Pipeline tesztek három szinten: smoke (mock adattal), sanity (lookahead bias ellenőrzés), integration (cross-layer teljes pipeline flow). A `sync_pipeline/smoke/` a CLI belépési pont helper függvényeit teszteli.

---

## sync_tables/smoke/ — Pipeline Funkciók Mock Adattal

Minden smoke teszt szintetikus adattal és mocked Binance API-val fut. Nem igényelnek éles DB-t.

### `test_sync_ohlcv.py`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_sync_ohlcv_inserts_rows` | Mocked Binance → 5 sor kerül az ohlcv táblába |
| `test_sync_ohlcv_idempotent` | Újrafuttatásra 0 új sor (append-only guard) |
| `test_sync_ohlcv_stale_guard` | Ha `open_time_ms_from` régebbi mint a DB max-a, a guard véd |
| `test_sync_ohlcv_column_count` | 10 oszlop kerül a táblába (nem 12) |

---

### `test_sync_features.py`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_sync_features_expected_columns` | `available_ts` és `lookback_end_ts` jelen van az outputban |
| `test_sync_features_feat_prefix` | Minden feature oszlop `feat_` prefix-szel kezdődik |
| `test_sync_features_idempotent` | Újrafuttatásra 0 új sor (append-only guard) |
| `test_sync_features_solusdt_profile` | `solusdt_fw60` profil által előírt feature-ök jelen vannak |

---

### `test_sync_predictions.py`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_sync_predictions_long_pred_written` | `long_pred` oszlop jelen van a táblában |
| `test_sync_predictions_short_pred_written` | `short_pred` oszlop jelen van a táblában |
| `test_sync_predictions_idempotent` | Újrafuttatásra 0 új sor |
| `test_sync_predictions_scores_in_range` | `long_pred`, `short_pred` ∈ [0, 1] a szintetikus mock modellre |

---

### `test_sync_targets.py`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_sync_targets_label_columns` | `trg_l_fw60_q90` és `trg_s_fw60_q10` jelen van |
| `test_sync_targets_null_tail` | Az utolsó 60 sor `NULL` label (horizon=60) |
| `test_sync_targets_label_distribution` | Pozitív label arány 5-20% között (egészséges osztályeloszlás) |

---

## sync_tables/sanity/ — Lookahead Bias Ellenőrzések

Szintetikus adattal, de valódi `compute_features_polars` hívással. Az összes teszt a feature pipeline determinizmusát és lookahead mentességét ellenőrzi.

### `test_leak_prevention.py`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_day_range_position_no_intraday_future_leak` | `feat_day_range_position` nem változik ha jövőbeli barak kerülnek hozzá |
| `test_ohlcv_features_independent_of_appended_future_bars` | OHLCV-alapú feature-ök (RSI, ROC, SMA, BB) determinisztikusak, nem változnak jövőbeli adatsoron |
| `test_deterministic_time_features_stable_across_dataset_sizes` | Timestamp-alapú feature-ök (`T_MINUS_1_SKIP` tagjai) ugyanazok különböző méretű DataFrame-en |

**Teszt módszer — future-bar append:**
1. Feature-ök számítása N soros DataFrame-en
2. Feature-ök számítása N+100 soros DataFrame-en (azonos sor 0..N-1)
3. Ellenőrzés: a közös sorok értékei numerikusan azonosak

Ez a teszt közvetlenül azt ellenőrzi, amit a t-1 lag garantál.

---

## sync_tables/integration/ — Cross-Layer Pipeline Flow

Szintetikus adattal és mocked modellel (nincs szükség éles DB-re vagy real model artifact-ra). Az összes pipeline réteg együtt fut.

### `test_pipeline_integration.py`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_ohlcv_to_predictions_cross_layer_alignment` | Teljes pipeline: ohlcv → features → target → predictions; cross-layer timestamp egyezés |
| `test_features_close_matches_ohlcv_close` | `feat_ohlcv_quant.close == ohlcv.close` minden közös `open_time`-ra |
| `test_target_open_time_subset_of_ohlcv` | `target.open_time ⊆ ohlcv.open_time` |

**Ellenőrzött cross-layer invariánsok (`test_ohlcv_to_predictions_cross_layer_alignment`):**

```
predictions.open_time ⊆ ohlcv.open_time
predictions.open_time ⊆ feat_ohlcv_quant.open_time
predictions.close == ohlcv.close (ABS diff < 1e-8)
predictions.long_pred ∈ [0, 1]
predictions.short_pred ∈ [0, 1]
```

**Mock setup:**
- `_MockModel`: `predict_proba` mindig `[0.35, 0.65]`-öt ad vissza
- `monkeypatch` a `utils.load_asset_config`, `champion_models_for_asset`, `_load_model_artifacts` függvényekre
- 250 szintetikus OHLCV bar (2024-01-01 00:00 – 04:09)

---

## sync_pipeline/smoke/ — CLI Helper Tesztek

`02_sync_pipeline.py` belső helper függvényeinek tesztjei. Nincs DB vagy Binance hívás — tisztán logikai tesztek.

### `test_sync_pipeline_helpers.py`

`importlib` segítségével tölti be a `02_sync_pipeline.py`-t (a számos prefix miatt közvetlen import nem lehetséges).

**Tesztelt függvények:** `_monthly_chunks`, `_resolve_tables`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_monthly_chunks_single_month` | 1 hónapnál rövidebb tartomány → 1 tuple |
| `test_monthly_chunks_splits_correctly` | 6 hónapos tartomány, chunk=3 → pontosan 2 chunk |
| `test_monthly_chunks_contiguous` | Minden chunk vége == következő chunk kezdete (nincs rés) |
| `test_monthly_chunks_equal_start_end_returns_empty` | Azonos start/end → üres lista |
| `test_monthly_chunks_returns_list_of_tuples` | Return type: `list[tuple[str, str]]` |
| `test_resolve_tables_default_returns_all` | `--tables` nélkül: teljes tábla szett |
| `test_resolve_tables_subset` | `--tables=ohlcv,features` → csak ez a kettő |
| `test_resolve_tables_skip_ohlcv_removes_ohlcv` | `--skip-ohlcv` → ohlcv nincs a szettben |
| `test_resolve_tables_unknown_table_exits` | Ismeretlen tábla → `sys.exit(1)` |
| `test_resolve_tables_whitespace_stripped` | Szóközök az elemek körül elfogadottak |
