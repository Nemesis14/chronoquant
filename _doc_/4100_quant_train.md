# 4100 — quant_train Table

Model-ready join tábla: `feat_ohlcv_quant` + `target` → tanítási adatforrás.

---

## Áttekintés

A `quant_train` tábla az ML pipeline egyetlen stabil belépési pontja. Feature engineering, sampling és LightGBM tanítás ebből dolgozik — nem a nyers `feat_ohlcv_quant` + `target` join-ból.

```mermaid
flowchart LR
  A[feat_ohlcv_quant\nopen_time + feat_*] -->|INNER JOIN\non open_time| C[quant_train\nopen_time + feat_* +\nlong_mfe_fw60 +\nshort_mfe_fw60]
  B[target\nopen_time + fw60 outcomes] -->|INNER JOIN\non open_time| C
  C --> D[00_create_sample.py\nyearly random-hour\nsegment assign]
  D --> E[database/.../samples/<sample_id>/\nmetadata.json + audit.json +\nsample_train_valid.parquet]
  E --> F[03_fit_model.py\nLightGBM]
```

**NULL target policy:** Az INNER JOIN automatikusan kizárja azokat a sorokat, ahol `long_mfe_fw60 IS NULL OR short_mfe_fw60 IS NULL`. Ezek a sorok sosem kerülnek be a `quant_train`-be.

**Nem pipeline:** A `quant_train` nem része a live sync pipeline-nak (`02_sync_pipeline.py`). Kizárólag ad-hoc rebuild — tanítás előtt futtatandó.

---

## Yearly sample artifact handoff

A `00_create_sample.py` (`create_yearly_sample`) az éves mintavétel után statikus
parquet/json artifactokat ír a `database/<asset>/samples/<sample_id>/` könyvtárba.
Az aktív yearly sampling pipeline nem hoz létre `sample_<id>` DuckDB táblát.

**Könyvtár példa:** `database/solusdt/samples/solusdt_fw60_yearly_2024/`

**`sample_train_valid.parquet` oszlopok:**

| Oszlop | Típus | Leírás |
|--------|-------|--------|
| `open_time` | `TIMESTAMP` | Bar nyitási ideje |
| `segment` | `VARCHAR` | `train`, `valid`, vagy `purge` |
| `fold_id` | `BIGINT \| NULL` | 0-alapú index a validációs héthez; NULL ha nem valid sor |
| `long_mfe_fw60` | `DOUBLE` | Long target |
| `short_mfe_fw60` | `DOUBLE` | Short target |

Feature oszlopok nem kerülnek a sample parquetba; a modeling lépések a szükséges
feature-öket DuckDB-ből töltik vissza a sample `open_time` soraival joinolva.

**Artefaktok szerepe:**
- `sample_train_valid.parquet`: elsődleges yearly sample handoff — ebből jönnek az `open_time`, `segment`, `fold_id` és target sorok
- `metadata.json`: konfigurációs és auditálási metaadatok; tartalmazza a `selected_valid_weeks` listát
- `audit.json`: adatminőségi metrikák

---

## Séma

| Oszlop | Típus | Forrás | Leírás |
|--------|-------|--------|--------|
| `open_time` | `TIMESTAMP` (PK) | `feat_ohlcv_quant` | Bar nyitási ideje, UTC. INNER JOIN garantálja az egyediséget. |
| `feat_*` | `DOUBLE` | `feat_ohlcv_quant` | Összes `feat_` prefixű feature oszlop. T-1 lag már alkalmazva. |
| `long_mfe_fw60` | `DOUBLE` | `target` | `log(max_price_fw60 / close[t])` — fw60 long outcome. |
| `short_mfe_fw60` | `DOUBLE` | `target` | `log(min_price_fw60 / close[t])` — fw60 short outcome. |

**Kizárt oszlopok:** `close`, `available_ts`, `lookback_end_ts` (feat táblából), `fw60_close`, `fw60_max`, `fw60_min` és egyéb fw60 oszlopok (target táblából), `long_pred`, `short_pred` (predictions tábla).

**Legacy naming:** A `trg_l_fw60_q90` / `trg_s_fw60_q10` boolean elnevezés NEM szerepel ebben a rétegben. A target oszlopok kizárólag `long_mfe_fw60` és `short_mfe_fw60`.

---

## Rebuild szemantika

```mermaid
flowchart TD
  A{rebuild típus?} -->|full| B[CREATE OR REPLACE TABLE quant_train\nAS SELECT ...]
  A -->|range| C[DELETE FROM quant_train\nWHERE open_time BETWEEN start AND end]
  C --> D[INSERT INTO quant_train\nSELECT ... WHERE open_time BETWEEN start AND end]
  B --> E[determinisztikus eredmény]
  D --> E
```

| Mód | SQL | Mikor |
|-----|-----|-------|
| **Full rebuild** | `CREATE OR REPLACE TABLE quant_train AS SELECT ...` | Kezdeti feltöltés, teljes újraépítés |
| **Range rebuild** | `DELETE + INSERT` a megadott `open_time` ablakra | Inkrementális frissítés |

Mindkét mód **idempotens** — többszöri futtatás azonos eredményt ad.

---

## CLI

```powershell
# Full rebuild (alapértelmezett)
uv run python src/data_handling/03_build_quant_train.py

# Range rebuild
uv run python src/data_handling/03_build_quant_train.py --start 2024-01-01 --end 2024-12-31
```

---

## Implementáció

| Fájl | Szerepe |
|------|---------|
| [`src/data_handling/store/duckdb_store.py`](src/data_handling/store/duckdb_store.py) | `rebuild_quant_train(conn, start_time, end_time)` — core rebuild logika |
| [`src/data_handling/sync_tables/sync_quant_train.py`](src/data_handling/sync_tables/sync_quant_train.py) | `sync_quant_train(asset_id, start_time, end_time)` — asset-szintű wrapper |
| [`src/data_handling/03_build_quant_train.py`](src/data_handling/03_build_quant_train.py) | Standalone CLI |

---

## Kapcsolódó dokumentumok

- [`_doc_/1000_database.md`](_doc_/1000_database.md) — teljes DuckDB séma áttekintő
- [`_doc_/1110_duckdb_store.md`](_doc_/1110_duckdb_store.md) — store réteg
- [`_doc_/3100_sync_targets.md`](_doc_/3100_sync_targets.md) — target tábla és fw60 outcome-ok
- [`_doc_/3000_targets.md`](_doc_/3000_targets.md) — target layer módszertani háttér
- [`_doc_/5010_sampling_yearly.md`](_doc_/5010_sampling_yearly.md) — aktív yearly sampling metodológia
