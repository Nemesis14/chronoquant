# duckdb_query.py — Lekérdezések

`src/database/store/duckdb_query.py`

Read-only lekérdezési réteg. Minden függvény önálló db_path-on dolgozik — nem tart fenn nyitott kapcsolatot. Pandas és Polars kimenet egyaránt elérhető.

---

## `_connect(db_path)`

**Célja:** Read-only DuckDB kapcsolat megnyitása.

**Visszatérési érték:** `duckdb.DuckDBPyConnection | None` — `None` ha a fájl nem létezik.

Minden publikus lekérdező függvény ezt hívja belül, és gracefully kezeli a hiányzó DB esetet (üres DataFrame / None visszatérés).

---

## `query_range(db_path, dataset, start, end, columns)`

**Célja:** Időtartomány-alapú lekérdezés pandas DataFrame-ként.

**Paraméterek:**

| Paraméter | Típus | Leírás |
|-----------|-------|--------|
| `db_path` | `str` | DuckDB fájl elérési útja |
| `dataset` | `str` | Tábla neve (`ohlcv`, `feat_ohlcv_quant`, stb.) |
| `start` | `str \| None` | Kezdő timestamp (`YYYY-MM-DD HH:MM:SS`), `None` = teljes history |
| `end` | `str \| None` | Záró timestamp, `None` = legújabbig |
| `columns` | `list[str] \| None` | Lekérdezett oszlopok, `None` = összes |

**Visszatérési érték:** `pd.DataFrame` — üres DataFrame ha DB hiányzik vagy nincs találat.

**SQL:** `SELECT {cols} FROM {dataset} WHERE open_time BETWEEN ? AND ? ORDER BY open_time`

---

## `query_range_pl(db_path, dataset, start, end, columns)`

**Célja:** Ugyanaz mint `query_range`, de **Polars DataFrame** kimenettel (zero-copy DuckDB → Arrow → Polars).

**Visszatérési érték:** `pl.DataFrame`

A feature computation (`sync_features.py`) ezt használja a OHLCV adatok betöltéséhez Polars pipeline-ba.

---

## `dataset_columns(db_path, dataset)`

**Célja:** Tábla oszlopneveinek lekérdezése.

**Visszatérési érték:** `list[str]` — üres lista ha DB vagy tábla hiányzik.

**Felhasználás:** `insert_predictions` hívja, hogy a séma a DB-ből olvasódjon (nem DataFrame-ből inferálva).

---

## `dataset_exists(db_path, dataset)`

**Célja:** Ellenőrzi, hogy a tábla létezik-e ÉS van-e benne legalább 1 sor.

**Visszatérési érték:** `bool`

**Felhasználás:** Sync függvények ellenőrzik a szülő tábla meglétét (pl. sync_features ellenőrzi, hogy az ohlcv tábla létezik) mielőtt futnak.

---

## `row_count(db_path, dataset)`

**Célja:** Sorok száma egy táblában.

**Visszatérési érték:** `int` — `0` ha DB vagy tábla hiányzik.

---

## `asof_join_predictions_features(db_path, feature_cols, start, end)`

**Célja:** ASOF JOIN a `predictions` és `feat_ohlcv_quant` táblák között, modeling-ready snapshothoz.

**Paraméterek:**

| Paraméter | Típus | Leírás |
|-----------|-------|--------|
| `db_path` | `str` | DuckDB fájl elérési útja |
| `feature_cols` | `list[str]` | Feature oszlopok listája (`feat_*`) |
| `start` | `str \| None` | Időtartomány kezdete |
| `end` | `str \| None` | Időtartomány vége |

**Visszatérési érték:** `pd.DataFrame`

**SQL logika:**
```sql
SELECT p.open_time, p.close, p.long_pred, p.short_pred,
       p.trg_l_fw60_q90, p.trg_s_fw60_q10,
       f.feat_col1, f.feat_col2, ...
FROM predictions p
ASOF LEFT JOIN feat_ohlcv_quant f
    ON p.open_time >= f.available_ts
WHERE p.open_time BETWEEN ? AND ?
ORDER BY p.open_time
```

Az `available_ts` biztosítja, hogy minden predikció sorhoz csak már elérhető feature-ök csatlakoznak.

---

## `latest_open_time(db_path, dataset)`

**Célja:** A tárolt adatok legújabb timestampje.

**Visszatérési érték:** `pd.Timestamp | None` — `None` ha a tábla üres vagy hiányzik.

**Felhasználás:** Minden sync függvény innen határozza meg, hol folytassa az inkrementális szinkront.

---

## OHLCV shortcut függvények

Kényelmi wrapperek az `ohlcv` táblához:

| Függvény | Visszatérési érték |
|----------|-------------------|
| `ohlcv_dataset_exists(db_path)` | `bool` |
| `ohlcv_row_count(db_path)` | `int` |
| `ohlcv_latest_open_time(db_path)` | `str \| None` — `YYYY-MM-DD HH:MM:SS` formátum |
| `ohlcv_time_stats(db_path)` | `tuple[int, str \| None, str \| None]` — `(count, min_ts, max_ts)` |

---

## Kapcsolat kezelés

Minden lekérdező függvény belül:
1. `_connect(db_path)` → kapcsolat vagy `None`
2. Ha `None`: üres/default visszatérési értékkel tér vissza (nem dob hibát)
3. Minden esetben `conn.close()` a `finally` blokkban

A read-only mód (`read_only=True`) megakadályozza, hogy a lekérdező kód véletlenül módosítsa az adatbázist. Párhuzamos olvasást támogat több process-ből.
