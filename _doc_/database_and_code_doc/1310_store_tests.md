# tests/store/ — Store Tesztek

`src/database/tests/store/`

Store réteg tesztjei három szinten: smoke (szintaxis + alapfunkció), sanity (éles DB adatintegritás), perf (wall-clock teljesítmény).

---

## smoke/ — Gyors Funkcionális Ellenőrzések

Szintetikus adattal futnak, nem igényelnek éles DB-t.

### `test_duckdb_store_query.py`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_ensure_tables_creates_tables` | `ensure_tables()` létrehozza `ohlcv`, `target`, `predictions` táblákat |
| `test_insert_ohlcv_append_only` | 5 sor beírás, újrafuttatásra 0 új sor (idempotens) |
| `test_insert_ohlcv_filters_columns` | Extra oszlopok csendesen ignorálódnak |
| `test_query_range_returns_correct_rows` | `query_range()` visszaad 7 napot timestamp BETWEEN-nel |
| `test_query_range_pl_returns_polars` | `query_range_pl()` Polars DataFrame-et ad vissza |
| `test_asof_join_predictions_features` | ASOF join helyes sorpárosítást ad |
| `test_dataset_exists_false_on_empty` | Üres tábla → `dataset_exists() = False` |
| `test_latest_open_time_returns_max` | `latest_open_time()` a beírt sorok maximumát adja |

---

### `test_duckdb_stats.py`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_collect_report_skips_missing_db` | Hiányzó DB-re `DuckDBStatsReport` üres táblákkal tér vissza |
| `test_collect_report_row_count` | 100 sor betöltés után `row_count == 100` |
| `test_collect_report_time_range` | `min_ts` és `max_ts` helyesek |
| `test_collect_report_null_ratios` | 0.0 null arány tiszta adatra |
| `test_format_report_contains_table_names` | `format_report()` tartalmazza a tábla neveket |

---

### `test_validate.py`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_assert_zero_passes` | 0 sort visszaadó SQL → átmegy |
| `test_assert_zero_fails` | >0 sort visszaadó SQL → `AssertionError` |

---

### `test_duckdb_stats_audit.py`

`raw_manifest_audit` és `log_dataset_check` szintetikus adaton és hiányzó DB/tábla esetén.

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_raw_manifest_audit_happy_path` | Nem dob hibát ha az adat megvan |
| `test_raw_manifest_audit_missing_db` | Nem dob hibát ha a DB fájl hiányzik |
| `test_raw_manifest_audit_missing_table` | Nem dob hibát ismeretlen táblanévre |
| `test_log_dataset_check_ohlcv_happy_path` | `log_dataset_check` ohlcv-re nem dob hibát |
| `test_log_dataset_check_missing_db` | Nem dob hibát ha a DB hiányzik |
| `test_log_dataset_check_missing_dataset` | Nem dob hibát üres táblára |

---

## sanity/ — Éles DB Adat-invariánsok

**Előfeltétel:** Éles `database/solusdt/solusdt.duckdb` DB megléte. Ha hiányzik, minden teszt `pytest.skip()`.

### `test_ohlcv.py`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_ohlcv_table_exists` | Az ohlcv tábla létezik és nem üres |
| `test_ohlcv_row_count_positive` | Legalább 1 sor |
| `test_ohlcv_required_columns` | Mind a 10 oszlop jelen van |
| `test_ohlcv_date_range_reasonable` | min_ts >= 2022-01-01, max_ts <= ma |
| `test_ohlcv_no_nulls` | Nincs null érték egyik oszlopban sem |
| `test_ohlcv_invariant` | `open`, `high`, `low`, `close` konzisztencia (high >= low, stb.) |
| `test_ohlcv_volume_positive` | `volume > 0` minden sorra |
| `test_ohlcv_1min_cadence` | Egymást követő timestampek 60s különbséggel |
| `test_ohlcv_no_duplicate_timestamps` | Nincs duplikált `open_time` |

---

### `test_features_target.py`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_feat_ohlcv_quant_table_exists` | `feat_ohlcv_quant` tábla létezik |
| `test_feat_ohlcv_quant_required_metadata_columns` | `open_time`, `close`, `available_ts`, `lookback_end_ts` jelen van |
| `test_feat_ohlcv_quant_available_ts_no_lookahead` | `available_ts <= open_time` minden sorban |
| `test_feat_ohlcv_quant_row_count` | feature sorok száma a várható tartományban van |
| `test_target_required_columns` | `long_mfe_fw60`, `short_mfe_fw60` és fw60 outcome oszlopok jelen vannak |
| `test_target_long_mfe_range` | `long_mfe_fw60` értékek az elvárt tartományban (logreturn) |
| `test_target_short_mfe_range` | `short_mfe_fw60` értékek az elvárt tartományban (logreturn) |

---

### `test_feature_lag_invariants.py`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_available_ts_equals_open_time` | `available_ts == open_time` minden sorra |
| `test_first_row_ohlcv_feats_null` | Az első sor OHLCV-alapú feature-jei `NULL` (t-1 lag) |
| `test_p2_features_not_null_first_row` | `T_MINUS_1_SKIP` tagjai az első sorban `NOT NULL` |
| `test_rsi_lag_correlation` | RSI t. bar korrelál az ohlcv t-1 bar close-ával |
| `test_close_position_differs_from_current_bar` | `feat_close_position` != jelenlegi bar `(close-low)/(high-low)` |

---

### `test_predictions.py`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_predictions_table_exists` | `predictions` tábla létezik |
| `test_predictions_required_columns` | `long_pred`, `short_pred` jelen van |
| `test_predictions_score_range` | `long_pred`, `short_pred` ∈ [0, 1] |
| `test_predictions_label_end_ts` | `label_end_ts > open_time` minden sorra |
| `test_predictions_alignment_with_ohlcv` | `predictions.open_time ⊆ ohlcv.open_time` |

---

### `test_target_window.py`

| Teszt | Mit ellenőriz |
|-------|---------------|
| `test_target_1_following_boundary` | `ROWS BETWEEN 1 FOLLOWING` — az aktuális bar nincs a forward window-ban |
| `test_null_count_equals_horizon` | Pontosan 60 NULL sor az utolsó soroknál |
| `test_nulls_at_tail` | NULL-ok csak a target tábla végén vannak |
| `test_null_symmetry` | Long és short NULL count egyenlő |
| `test_synthetic_10_bar` | Szintetikus 10 barios adaton forward window ellenőrzés |

---

## perf/ — Teljesítmény Benchmarkok

Wall-clock idő mérés az éles DB-n. Skippel ha a DB hiányzik.

### `test_query_timing.py`

| Teszt | Limit |
|-------|-------|
| `test_timing_ohlcv_count` | COUNT(*) < 2s |
| `test_timing_ohlcv_range_query_7d` | 7 napos range query < 3s |
| `test_timing_ohlcv_daily_aggregation` | Napi agg (GROUP BY day) < 5s |
| `test_timing_ohlcv_rolling_sma60` | Rolling SMA60 teljes history < 15s |
| `test_timing_insert_100k_rows` | 100k sor INSERT (szintetikus temp DB) < 10s |
| `test_timing_feat_count` | feat COUNT(*) < 2s |
| `test_timing_feat_range_query_7d` | feat 7 napos range < 3s |
| `test_timing_feat_range_query_30d` | feat 30 napos range < 5s |
| `test_timing_feat_groupby_year` | feat GROUP BY year < 3s |
| `test_timing_feat_groupby_month` | feat GROUP BY month < 3s |
| `test_timing_target_count` | target COUNT(*) < 2s |
| `test_timing_target_label_groupby` | target label GROUP BY < 3s |
| `test_timing_target_range_query_30d` | target 30 napos range < 3s |
| `test_timing_predictions_count` | predictions COUNT(*) < 2s |
| `test_timing_predictions_range_query_7d` | predictions 7d range < 3s |
| `test_timing_predictions_range_query_30d` | predictions 30d range < 5s |
| `test_timing_predictions_groupby_year` | predictions GROUP BY year < 3s |
| `test_timing_predictions_groupby_month` | predictions GROUP BY month < 3s |
| `test_timing_asof_join` | ASOF JOIN predictions⋈features < 10s |
