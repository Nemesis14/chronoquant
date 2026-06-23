# toolkit.py — Dataset Inspekciós Segédek

`src/data_handling/store/toolkit.py`

DS workflow segédek gyors dataset inspekciókhoz. Nem üzleti logika — kényelmi wrapperek notebook és REPL használatra.

---

## `resolve_db_path(asset_id)`

**Célja:** Asset ID-ból DuckDB fájl elérési út lekérése a configból.

**Paraméterek:**

| Paraméter | Típus | Leírás |
|-----------|-------|--------|
| `asset_id` | `str \| None` | Asset azonosító (`solusdt`), `None` = config default |

**Visszatérési érték:** `str` — abszolút db_path

**Belső hívás:** `utils.load_asset_config(asset_id)["database"]["db_path"]`

---

## `list_datasets(asset_id)`

**Célja:** Az asset DB-ben adatot tartalmazó dataset-ek listázása.

**Visszatérési érték:** `list[str]` — pl. `["ohlcv", "feat_ohlcv_quant", "predictions", "target"]`

**Logika:** `dataset_exists(db_path, dataset)` hívás minden ismert táblára.

---

## `get_dataset_columns(dataset, asset_id)`

**Célja:** Dataset oszlopnevei.

**Visszatérési érték:** `list[str]`

---

## `get_row_count(dataset, asset_id)`

**Célja:** Sorok száma egy datasetben.

**Visszatérési érték:** `int`

---

## `get_time_range(dataset, asset_id)`

**Célja:** Az adott dataset időtartományának lekérdezése.

**Paraméterek:**

| Paraméter | Típus | Leírás |
|-----------|-------|--------|
| `dataset` | `str` | Dataset neve |
| `asset_id` | `str \| None` | Asset azonosító (`None` = config default) |

**Visszatérési érték:** `tuple[str | None, str | None]` — `(min_ts, max_ts)` string formátumban, vagy `(None, None)` ha üres

---

## `print_summary(asset_id)`

**Célja:** Gyors összefoglaló az összes dataset állapotáról stdout-ra.

**Példa kimenet:**
```
DB path: database/solusdt/solusdt.duckdb
------------------------------------------------------------
ohlcv                 rows= 1,234,567  cols=  10  2022-01-01 00:00:00 -> 2026-06-14 23:59:00
feat_ohlcv_quant      rows= 1,230,000  cols= 115  2022-01-15 00:01:00 -> 2026-06-14 23:59:00
target                rows= 1,234,507  cols=  11  2022-01-01 00:00:00 -> 2026-06-14 22:59:00
predictions           rows= 1,180,000  cols=   9  2023-06-01 00:00:00 -> 2026-06-14 23:59:00
```

**Felhasználás:**
```python
from data_handling.store.toolkit import print_summary
print_summary("solusdt")
```
