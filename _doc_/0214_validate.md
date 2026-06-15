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

**Invariáns:** Az utolsó 60 bar jövőbeli záróára még nem ismert — a labelek `NULL`-ok kell legyenek. Ha valamelyik `NOT NULL`, a forward window SQL sérült.

**SQL belül:**
```sql
SELECT COUNT(*) FROM (
    SELECT open_time, trg_l_fw60_q90 FROM target
    ORDER BY open_time DESC LIMIT 60
) sub
WHERE trg_l_fw60_q90 IS NOT NULL OR trg_s_fw60_q10 IS NOT NULL
```

**Hiba esetén:** `AssertionError` — az utolsó sorok NULL kell legyenek.

---

## Futtatás

```bash
# Standalone (01_validate_stats.py részeként)
uv run python src/database/01_validate_stats.py

# Direkt hívás Python-ból
from database.store.validate import check_no_future_features, check_target_no_current_bar
check_no_future_features("database/solusdt/solusdt.duckdb")
check_target_no_current_bar("database/solusdt/solusdt.duckdb")
```
