# duckdb_stats.py — DB Statisztikák és Audit

`src/data_handling/store/duckdb_stats.py`

DB egészség-ellenőrzés: sorok, időtartományok, null arányok és lekérdezési teljesítmény minden táblára. Dataset integritás audit és logolás. A `01_validate_stats.py` CLI script és a `02_sync_pipeline.py` hívja.

---

## Dataclass-ok

### `TableStats`

Egy tábla pillanatképe:

| Mező | Típus | Leírás |
|------|-------|--------|
| `table` | `str` | Tábla neve |
| `status` | `str` | `"OK"`, `"EMPTY"`, `"SKIP_TABLE_MISSING"`, `"SKIP_DB_MISSING"` |
| `row_count` | `int` | Sorok száma |
| `min_open_time` | `str \| None` | Legkorábbi `open_time` |
| `max_open_time` | `str \| None` | Legújabb `open_time` |
| `column_count` | `int` | Oszlopok száma |
| `null_ratios` | `dict[str, float]` | Null arány max 5 nem-`open_time` oszlopra (0.0–1.0) |
| `dup_count` | `int` | Duplikált `open_time` értékek száma (`COUNT(*) - COUNT(DISTINCT ...)`) |

---

### `TimedMetric`

Egy benchmark lekérdezés eredménye:

| Mező | Típus | Leírás |
|------|-------|--------|
| `label` | `str` | Leírás (pl. `"range_ohlcv_1d"`) |
| `status` | `str` | `"OK"` vagy `"SKIP_EMPTY"` |
| `elapsed_ms` | `float` | Futási idő milliszekundumban |
| `row_count` | `int \| None` | Visszaadott sorok száma |
| `detail` | `str` | Részlet (pl. `"rows=1440"`) |

---

### `DuckDBStatsReport`

Teljes riport:

| Mező | Típus | Leírás |
|------|-------|--------|
| `db_path` | `str` | DuckDB fájl elérési útja |
| `tables` | `list[TableStats]` | Táblák stat listája |
| `timings` | `list[TimedMetric]` | Benchmark eredmények |

---

## `collect_duckdb_stats_report(db_path, tables)`

**Célja:** Teljes `DuckDBStatsReport` összeállítása a megadott táblákra.

**Paraméterek:**

| Paraméter | Típus | Leírás |
|-----------|-------|--------|
| `db_path` | `str` | DuckDB fájl elérési útja |
| `tables` | `list[str] \| None` | Vizsgálandó táblák. Alap: `["ohlcv", "target", "feat_ohlcv_quant", "predictions", "quant_train"]` |

**Visszatérési érték:** `DuckDBStatsReport`

**Gyűjtött adatok táblánként:**
- `COUNT(*)` → `row_count`
- `MIN(open_time)` / `MAX(open_time)` → `min_open_time` / `max_open_time`
- Null arány max 5 (nem `open_time`) oszlopra: `COUNT(*) WHERE col IS NULL / COUNT(*)`
- Oszlopszám az `information_schema.columns`-ból

**Timing smoke benchmarkok** (ha `ohlcv` tábla elérhető):
- `range_ohlcv_1d` — utolsó 1 nap COUNT
- `range_ohlcv_1w` — utolsó 1 hét COUNT
- `range_ohlcv_1mo` — utolsó 1 hónap COUNT
- `range_ohlcv_full` — teljes COUNT(*)
- `groupby_ohlcv_year` — éves bontás GROUP BY

Ha a DB fájl vagy a tábla hiányzik, a riport `SKIP_DB_MISSING` / `SKIP_TABLE_MISSING` / `EMPTY` státuszokat ad vissza — nem dob kivételt.

---

## `raw_manifest_audit(db_path, dataset)`

**Célja:** Nyers dataset integritás audit — sorok, időtartomány, null timestamp-ek, duplikált timestamp-ek logolása.

**Paraméterek:**

| Paraméter | Típus | Leírás |
|-----------|-------|--------|
| `db_path` | `str` | DuckDB fájl elérési útja |
| `dataset` | `str` | Vizsgálandó tábla neve (`'ohlcv'`, `'target'`, `'feat_ohlcv_quant'`, `'predictions'`) |

**Visszatérési érték:** `None` — kizárólag `logging.info` / `logging.warning` kimenet.

**Ellenőrzött metrikák (DuckDB native SQL):**

| Kulcs | Leírás |
|-------|--------|
| `row_count` | Összes sor száma (`COUNT(*)`) |
| `min_ts` | Legkorábbi `open_time` |
| `max_ts` | Legújabb `open_time` |
| `null_ts` | Null `open_time` értékű sorok (`SUM(CASE WHEN open_time IS NULL)`) |
| `dup_ts` | Duplikált `open_time` értékek (`COUNT(*) - COUNT(DISTINCT ...)`) |

**Viselkedés:**
- Ha a DB fájl hiányzik: `logger.warning` és korai visszatérés
- Ha a tábla nem létezik: `logger.warning` és korai visszatérés
- Ha `null_ts > 0` vagy `dup_ts > 0`: `logger.warning` (gyanús adat)
- Egyébként: `logger.info` OK üzenet

**Felhasználás:** Deployment utáni ellenőrzés, adatintegritás gyanú esetén. A `log_dataset_check` hívja minden dataset után.

---

## `log_dataset_check(db_path, dataset)`

**Célja:** Sor szám, időtartomány és `raw_manifest_audit` logolása egy dataset-re.

**Paraméterek:**

| Paraméter | Típus | Leírás |
|-----------|-------|--------|
| `db_path` | `str` | DuckDB fájl elérési útja |
| `dataset` | `str` | Dataset neve: `'ohlcv'`, `'target'`, `'feat_ohlcv_quant'`, `'predictions'` |

**Visszatérési érték:** `None` — kizárólag `logging.info` / `logging.warning` kimenet.

**Belső logika:**
- `ohlcv` esetén: `ohlcv_dataset_exists` + `ohlcv_time_stats` (gyors path)
- Egyéb táblák: `dataset_exists` + `query_range_pl(columns=["open_time"])` → min/max számítás
- Mindkét ágban: `raw_manifest_audit(db_path, dataset)` meghívva a részletes audithoz

---

## `format_duckdb_stats_report(report)`

**Célja:** `DuckDBStatsReport` ember által olvasható szöveggé alakítása.

**Visszatérési érték:** `str` — többsoros riport stdout-ra vagy logba.

**Példa kimenet:**
```
DuckDB statistics smoke report
db_path: database/solusdt/solusdt.duckdb
informational: timing metrics do not fail validation by themselves

Tables:
- ohlcv: status=OK rows=1234567 min=2022-01-01 00:00:00 max=2026-06-14 23:59:00 cols=10 dups=0
  null_ratios: open=0.000, high=0.000, low=0.000, close=0.000, volume=0.000
- target: status=OK rows=1234567 ... dups=0

Timings:
- range_ohlcv_1d: status=OK elapsed_ms=12.345 row_count=1440 rows=1440
- range_ohlcv_1w: status=OK elapsed_ms=45.678 row_count=10080 rows=10080
- groupby_ohlcv_year: status=OK elapsed_ms=23.100 row_count=5 rows=5
```
