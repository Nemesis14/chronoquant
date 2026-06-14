---
epic: epic_001_db_health
id: t1
title: OHLCV completeness check + query benchmark
assignee: database_agent
status: pr
blocks: []
blocked_by: []
---

## Goal
Ellenőrizni az ohlcv tábla teljességét és a DuckDB alaplekérdezések sebességét.

## Scope
- `scripts/benchmark_duckdb.py` (bugfix: data_dir → db_path, Windows encoding)
- `src/store/duckdb_stats.py` (olvasva, nem módosítva)

## Acceptance Criteria
- [x] ohlcv sorok száma és időtartomány ellenőrizve
- [x] Gap analízis lefutott
- [x] Query benchmark lefutott

## Notes

### Completeness check eredménye (2026-06-14)

| Tábla | Státusz | Sorok | Min | Max |
|-------|---------|-------|-----|-----|
| ohlcv | OK | 3,022,861 | 2020-09-14 07:00 | 2026-06-14 12:00 |
| target | EMPTY | 0 | — | — |
| feat_ohlcv_quant | MISSING | — | — | — |
| predictions | EMPTY | 0 | — | — |

**Gap analízis:** 0 gap esemény, 0 hiányzó bar — az ohlcv adat folyamatos.

**Null ratio:** open/high/low/close/volume = 0.000 (tiszta adat).

### Query benchmark eredménye

| Query | Idő |
|-------|-----|
| INSERT 100k sor (synthetic) | ~219 ms |
| Range query last 7 days | 48.3 ms |
| Full scan 3M sor (open_time, close) | 374.7 ms |
| Daily OHLCV aggregation (GROUP BY day) | 99.0 ms |
| Rolling SMA60 + STD60 (window func) | 1.05 s |
| ASOF JOIN predictions ⋈ features | SKIP (feat tábla hiányzik) |

### Javítások a benchmark scriptben
- `cfg["database"]["data_dir"]` → `cfg["database"]["db_path"]` (config séma változott)
- `Path(data_dir).with_suffix(".duckdb")` → `Path(data_dir)` (már .duckdb path)
- `bench_insert`: temp db path `bench.duckdb` fájlnévvel adódik át
- Dupla `conn.close()` eltávolítva
- `→` unicode karakter → `->` (Windows cp1250 encoding fix)

### Következő lépés
A derived táblák (target, feat_ohlcv_quant, predictions) üresek / hiányoznak — a pipeline nincs lefuttatva. Ha szükséges: `sync_ohlcv.py` → feature sync → target sync → prediction sync sorrendben kell futtatni.
