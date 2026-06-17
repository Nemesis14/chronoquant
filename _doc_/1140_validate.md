# validate.py — Integritás Ellenőrzés

`src/database/store/validate.py`

Adatintegritás invariánsok ellenőrzése. Minden függvény `AssertionError`-t dob ha az ellenőrzés sikertelen. A `check_*` függvények a `01_validate_stats.py` pipeline részeként futnak és CI-ban is futtathatók.

---

## `assert_zero(con, sql, msg)`

**Célja:** Alap ellenőrző primitív — SQL-t futtat, `AssertionError`-t dob ha a visszaadott szám nem nulla.

**Paraméterek:**

| Paraméter | Típus | Leírás |
|-----------|-------|--------|
| `con` | `duckdb.DuckDBPyConnection` | Nyitott kapcsolat |
| `sql` | `str` | SQL lekérdezés, egyetlen COUNT(*) értéket ad vissza |
| `msg` | `str` | Hiba üzenet `AssertionError`-ban |

**Viselkedés:**
- Ha az SQL 0-t ad vissza: átmegy (sikeres)
- Ha az SQL > 0-t ad vissza: `AssertionError(f"{msg}: {count} sor sért feltételt")`

**Felhasználás:** A `check_*` függvények mind erre épülnek.

---

## `check_no_future_features(db_path)`

**Célja:** Ellenőrzi, hogy a `feat_ohlcv_quant` táblában `available_ts <= open_time` minden sorra teljesül.

**Invariáns:** Ha `available_ts > open_time`, az azt jelenti, hogy egy feature értéke egy jövőbeli bartól függ — ez lookahead bias.

**SQL belül:**
```sql
SELECT COUNT(*) FROM feat_ohlcv_quant
WHERE available_ts > open_time
```

**Hiba esetén:** `AssertionError` — a pipeline megáll és a hibát javítani kell.

---

## `check_target_no_current_bar(db_path)`

**Célja:** Ellenőrzi, hogy a `target` tábla utolsó `horizon` (=60) sorában a target oszlopok `NULL`-ok.

**Invariáns:** Az utolsó 60 bar jövőbeli záróára még nem ismert — a labelek `NULL`-ok kell legyenek.

**Hiba esetén:** `AssertionError` — az utolsó sorok NULL kell legyenek.

---

## `check_sample_table(db_path, sample_id, expected_feat_cols=None)`

**Célja:** Integritás ellenőrzések a materializált `sample_<sample_id>` DuckDB táblán, mielőtt a modellező pipeline felhasználja.

**Paraméterek:**

| Paraméter | Típus | Leírás |
|-----------|-------|--------|
| `db_path` | `str` | Abszolút path a .duckdb fájlhoz |
| `sample_id` | `str` | Sample azonosító (`sample_<sample_id>` a tábla neve) |
| `expected_feat_cols` | `list[str] \| None` | Opcionális: elvárt feat_* oszlopok listája (pl. `feature_set.json`-ból) |

**Ellenőrzések:**

| # | Invariáns | Hiba |
|---|-----------|------|
| 1 | Nincs duplikált `(open_time, fold_id, segment)` sor | `AssertionError: duplicate (open_time, fold_id, segment) rows` |
| 2 | A `test` sorok minden `non-test` sort időben megelőznek | `AssertionError: non-test rows not strictly before test rows` |
| 3 | Legalább egy `purge` sor létezik (embargo buffer jelen van) | `AssertionError: no purge rows found` |
| 4 | A `train` és `valid` soroknál `long_mfe_fw60` és `short_mfe_fw60` nem NULL | `AssertionError: NULL target columns in train/valid rows` |
| 5 | Ha `expected_feat_cols` megadva: minden oszlop jelen van a táblában | `AssertionError: expected feature columns missing: [...]` |

**Raises:**
- `FileNotFoundError` — ha a .duckdb fájl nem létezik, vagy a tábla hiányzik
- `AssertionError` — ha bármely invariáns sérül

**Felhasználás:**
```python
from database.store.validate import check_sample_table

# Alap ellenőrzés
check_sample_table("database/solusdt/solusdt.duckdb", "solusdt_fw60_yearly_2024")

# Feature set konzisztencia-ellenőrzéssel
check_sample_table(
    "database/solusdt/solusdt.duckdb",
    "solusdt_fw60_yearly_2024",
    expected_feat_cols=["feat_rsi_14", "feat_roc_14", ...],
)
```

---

## Futtatás

```bash
# Standalone (01_validate_stats.py részeként)
uv run python src/database/01_validate_stats.py

# Direkt hívás Python-ból
from database.store.validate import (
    check_no_future_features,
    check_quant_train_no_duplicates,
    check_sample_table,
    check_target_no_current_bar,
)
check_no_future_features("database/solusdt/solusdt.duckdb")
check_quant_train_no_duplicates("database/solusdt/solusdt.duckdb")
check_sample_table("database/solusdt/solusdt.duckdb", "solusdt_fw60_yearly_2024")
```
