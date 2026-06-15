---
epic: epic_007
id: t1
title: src/database/ teljes dokumentáció
assignee: doc_agent
status: pr
---

## Goal

A `src/database/` modul teljes dokumentálása a `_doc_/` könyvtárban, flat struktúrában, numerikus névvel ahol a szám mélysége tükrözi a mappa mélységét.

## Scope

**Létrehozott fájlok:**

| Fájl | Tartalom |
|------|---------|
| `_doc_/0001_database.md` | DuckDB séma: ER diagram, 4 tábla definíció, konvenciók |
| `_doc_/0002_database_module.md` | `src/database/` modul áttekintés, 3 entry script leírás |
| `_doc_/0021_store.md` | `store/` könyvtár áttekintés, 7 fájl összefoglaló |
| `_doc_/0022_data_pipeline.md` | `data_pipeline/` könyvtár áttekintés, pipeline flow |
| `_doc_/0023_tests.md` | Teszt áttekintés, pytest markok, fixtures, futtatás |
| `_doc_/0211_duckdb_store.md` | `duckdb_store.py` összes függvény dokumentálva |
| `_doc_/0212_duckdb_query.md` | `duckdb_query.py` összes függvény dokumentálva |
| `_doc_/0213_duckdb_stats.md` | `duckdb_stats.py` dataclass-ok + függvények |
| `_doc_/0214_validate.md` | `validate.py` összes függvény dokumentálva |
| `_doc_/0215_maintenance.md` | `maintenance.py` összes függvény dokumentálva |
| `_doc_/0216_parquet_store.md` | `parquet_store.py` Hive layout + összes függvény |
| `_doc_/0217_toolkit.md` | `toolkit.py` DS inspekciós segédek |
| `_doc_/0221_sync_ohlcv.md` | `sync_ohlcv.py` Binance szinkron flow |
| `_doc_/0222_sync_features.md` | `sync_features.py` feature számítás és t-1 lag |
| `_doc_/0223_sync_predictions.md` | `sync_predictions.py` inference pipeline |
| `_doc_/0224_sync_targets.md` | `sync_targets.py` target SQL és kvantilis küszöbök |
| `_doc_/0225_features_polars.md` | `_features_polars.py` 30+ indikátor csoport |
| `_doc_/0231_store_tests.md` | `tests/store/` smoke+sanity+perf részletes |
| `_doc_/0232_pipeline_tests.md` | `tests/data_pipeline/` smoke+sanity+integration |

**Törölt fájlok:**
- `_doc_/0101_ohlcv_schema.md` (beolvadt 0001_database.md-be)
- `_doc_/0201_sync_ohlcv.md` (beolvadt 0221_sync_ohlcv.md-be)

## Acceptance Criteria

- [x] Minden `src/database/` source fájl saját `.md` dokuval rendelkezik
- [x] ER diagram mermaid formátumban az összes 4 táblával
- [x] Minden publikus függvény dokumentálva (paraméter táblák, visszatérési érték)
- [x] Magasabb szintű mappák (`store/`, `data_pipeline/`, `tests/`) áttekintő dokuval
- [x] Teszt szintek leírva (smoke, sanity, perf, integration) + futtatási parancsok
- [x] Régi doku fájlok törölve
- [x] Flat `_doc_/` struktúra, nincs almappa

## Notes

- Két session-ra osztódott a munka (context limit)
- A `feat_ohlcv_quant` dinamikus séma és a t-1 lag mechanizmus részletesen dokumentálva
- Az ASOF join `available_ts` szemantika minden érintett helyen magyarázva
- `T_MINUS_1_SKIP` frozenset és P2 feature-ök kivételei dokumentálva
- Stílus: Mermaid diagram (erDiagram, flowchart, sequenceDiagram), paraméter táblák, SQL blokkok
