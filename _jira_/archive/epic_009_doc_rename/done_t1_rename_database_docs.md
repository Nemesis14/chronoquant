---
epic: epic_009_doc_rename
id: t1
title: Meglévő _doc_ fájlok átnevezése 1000/1100/1110 sémára
assignee: doc_agent
status: pr
blocked_by: []
---

## Goal

A meglévő `_doc_/` fájlokat az új hierarchikus számozási sémára átnevezni.
A séma definícióját lásd: `.agent/skills/docs_skill.md` (epic_7/t8 után frissítve).

**Megjegyzés:** ez a task futtatható epic_7 párhuzamosan, de a séma
definíció (epic_7/t8) elvégzése után érdemes futtatni a konzisztencia érdekében.

## Scope

Érintett fájlok — régi → új:

| Régi | Új |
|------|----|
| `_doc_/0001_database.md` | `_doc_/1000_database.md` |
| `_doc_/0002_database_module.md` | `_doc_/1001_database_module.md` |
| `_doc_/0021_store.md` | `_doc_/1100_store.md` |
| `_doc_/0022_sync_tables.md` | `_doc_/1200_sync_tables.md` |
| `_doc_/0023_tests.md` | `_doc_/1300_tests.md` |
| `_doc_/0211_duckdb_store.md` | `_doc_/1110_duckdb_store.md` |
| `_doc_/0212_duckdb_query.md` | `_doc_/1120_duckdb_query.md` |
| `_doc_/0213_duckdb_stats.md` | `_doc_/1130_duckdb_stats.md` |
| `_doc_/0214_validate.md` | `_doc_/1140_validate.md` |
| `_doc_/0217_toolkit.md` | `_doc_/1150_toolkit.md` |
| `_doc_/0221_sync_ohlcv.md` | `_doc_/1210_sync_ohlcv.md` |
| `_doc_/0222_sync_features.md` | `_doc_/1220_sync_features.md` |
| `_doc_/0223_sync_predictions.md` | `_doc_/1230_sync_predictions.md` |
| `_doc_/0224_sync_targets.md` | `_doc_/1240_sync_targets.md` |
| `_doc_/0225_features_polars.md` | `_doc_/1250_features_polars.md` |
| `_doc_/0231_store_tests.md` | `_doc_/1310_store_tests.md` |
| `_doc_/0232_pipeline_tests.md` | `_doc_/1320_pipeline_tests.md` |

`_doc_/0000_project_overview.md` — **NEM nevezendő át** (globális reserved).
`_doc_/analysis/` — **NEM érintett** (analyst_agent domain, külön struktúra).

## Acceptance Criteria

- [x] Mind a 17 fájl átnevezve (régi fájlok törlése, új névvel létrehozás)
- [x] `_doc_/0000_project_overview.md` változatlan
- [x] Kereszthivatkozások frissítve: ha valamelyik doc fájl hivatkozik másik doc fájlra
  régi névvel, azt is frissíteni kell
- [x] `0002_database_module.md` → `1001_database_module.md` mapping ellenőrizve:
  ha a tartalma alapján inkább alfejezet (1100 range), döntsd el és dokumentáld a Notes-ban

## Notes

**Elvégezve:** 2026-06-16

**`0002_database_module.md` mapping döntés:** `1001_database_module.md` marad (nem 1100 range).
Indok: a fájl az egész `src/database/` domain modul-szintű áttekintője (struktúra, adatfolyam,
entry point scriptek, konfiguráció) — nem egy specifikus almodul (store / sync_tables / tests).
Az 1100 range a store almodulnak van fenntartva. A `1001` a `1000_database.md` (schema doc)
melletti domain-szintű companion file-ként kezelt.

**Kereszthivatkozások frissítve:**
- `1001_database_module.md`: 4 link frissítve (0021→1100, 0022→1200, 0023→1300, 0001→1000)
- `1300_tests.md`: 2 link frissítve (0231→1310, 0232→1320)

**Módszer:** `git mv` — history megőrzött.
