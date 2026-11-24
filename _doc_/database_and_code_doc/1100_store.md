# store/ — DuckDB Store Réteg

A `src/data_handling/store/` könyvtár kezeli az összes alacsony szintű DuckDB interakciót: séma létrehozást, adatbeírást, lekérdezést, validációt és statisztikákat.

---

## Áttekintés

```mermaid
flowchart TD
    APP["Hívó kód\n(sync_*, 02_sync_pipeline, UI)"]
    STORE["duckdb_store.py\nírás, séma, migráció"]
    QUERY["duckdb_query.py\nolvasás, range, ASOF join"]
    STATS["duckdb_stats.py\nstat gyűjtés, formázás"]
    VALID["validate.py\nintegritás ellenőrzés"]
    DB[("solusdt.duckdb")]

    APP --> STORE & QUERY
    STORE --> DB
    QUERY --> DB
    STATS --> DB
    VALID --> DB
```

---

## Fájlok

### [duckdb_store.py](1110_duckdb_store.md)

DuckDB kapcsolat kezelés, séma inicializálás és adatbeírás.

**Kulcs funkciók:**
- `get_connection(db_path)` — DuckDB kapcsolat megnyitása, parent dir létrehozása
- `ensure_tables(conn)` — Verziózott migrációk futtatása (`LIVE_DB_MIGRATIONS` v1–v8, idempotens; v8 = `scout_signals`)
- `_ensure_feat_ohlcv_quant_table(conn, df)` — Dinamikus séma: tábla létrehozása vagy oszlopbővítés
- `_insert_append_only(conn, table, df)` — Core append logika `MAX(open_time)` alapon
- `insert_ohlcv(conn, df)` — OHLCV beírás (10 oszlop szűrés + append)
- `insert_feat_ohlcv_quant(conn, df)` — Feature beírás (dinamikus séma + append)
- `insert_target(conn, df)` — Target beírás (DELETE WHERE IN + INSERT)
- `insert_predictions(conn, df)` — Prediction beírás (séma DB-ből, append)
- `backfill_predictions(conn, df)` — Historikus gap-fill (INSERT OR IGNORE)
- `rebuild_quant_train(conn, start, end)` — quant_train teljes vagy range rebuild

---

### [duckdb_query.py](1120_duckdb_query.md)

Read-only lekérdezések pandas és Polars DataFrame kimenettel.

**Kulcs funkciók:**
- `_connect(db_path)` — Read-only kapcsolat, `None` ha a fájl hiányzik
- `query_range(db_path, dataset, start, end, columns)` → pandas DataFrame
- `query_range_pl(db_path, dataset, start, end, columns)` → Polars DataFrame (zero-copy)
- `dataset_columns(db_path, dataset)` → oszlopnév lista
- `dataset_exists(db_path, dataset)` → bool (tábla létezik ÉS van benne sor)
- `latest_open_time(db_path, dataset)` → `pd.Timestamp | None`
- OHLCV shortcut-ok: `ohlcv_dataset_exists`, `ohlcv_row_count`, `ohlcv_latest_open_time`, `ohlcv_time_stats`

---

### [duckdb_stats.py](1130_duckdb_stats.md)

DB egészség-statisztikák gyűjtése, formázása és dataset audit.

**Dataclass-ok:** `TableStats`, `TimedMetric`, `DuckDBStatsReport`

**Kulcs funkciók:**
- `collect_duckdb_stats_report(db_path, tables)` — Sorok, időtartomány, null arányok, timing smoke (1d/1w/1mo/full), GROUP BY year
- `format_duckdb_stats_report(report)` → szöveg riport stdout-ra

---

### [validate.py](1140_validate.md)

Integritás invariánsok ellenőrzése AssertionError-ral.

**Kulcs funkciók:**
- `assert_zero(con, sql, msg)` — SQL futtat, AssertionError ha `count > 0`
- `check_no_future_features(db_path)` — `available_ts <= open_time` mindenhol
- `check_target_no_current_bar(db_path)` — NULL tail sorok megléte

---

## Írási módok összefoglalója

| Tábla | Írási mód | Függvény |
|-------|-----------|----------|
| `ohlcv` | Append-only (`MAX(open_time)` alap) | `insert_ohlcv` |
| `feat_ohlcv_quant` | Append-only + dinamikus séma | `insert_feat_ohlcv_quant` |
| `target` | DELETE WHERE IN + INSERT (teljes rebuild); a live worker (`sync_full`) NEM frissíti — retrainkor `02_sync_pipeline.py --tables targets`, ezért a vége elmaradhat a `feat_ohlcv_quant` mögött (t949) | `insert_target` |
| `predictions` | Append-only | `insert_predictions` |
| `predictions` | Gap-fill (INSERT OR IGNORE) | `backfill_predictions` |
| `quant_train` | Full rebuild (CREATE OR REPLACE) / Range (DELETE+INSERT) | `rebuild_quant_train` |
| `quant_train__<profile>` | Mint `quant_train`, challenger profilnál (`extra_feature_tables` LEFT JOIN) | `rebuild_quant_train(..., extra_feature_tables, table_name)` |
| `feat_levels` | Idempotens upsert (`INSERT OR REPLACE BY NAME`, PK `open_time`) | `upsert_feat_levels` (`sync_levels.py`) |
| `scout_signals` | Forward-log: INSERT új eseményre; immutable predikció, set-once tervek, egyszeri kimenet (`open` → `resolved`/`invalid`), nincs törlés | `upsert_scout_signals` (`sync_scout.py`) |
| `snap."<snapshot_id>"` (lab DB) + `reg.snapshots` | Immutable CTAS, egyszer; a reg sor `content_sha256` (id/reuse) + additív `values_sha256` (érték-fingerprint, t949) | `snapshots.create_snapshot` → [1410](1410_snapshots_code.md) |
