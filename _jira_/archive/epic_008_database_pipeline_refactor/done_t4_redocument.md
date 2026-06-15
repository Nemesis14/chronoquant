---
epic: epic_008
id: t4
title: _doc_/ újradokumentálása az új struktúrára
assignee: doc_agent
status: done
blocked_by: [t3]
blocks: [t5]
---

## Goal
A refactor után a `_doc_/` dokumentációs oldalak tükrözzék az új struktúrát: `sync_tables/`, `sync_pipeline.py`, `maintenance.py` eltűnése, `duckdb_stats.py` bővülése.

## Scope
Érintett doc oldalak (ellenőrizni és frissíteni):
- `_doc_/0002_database_module.md` — modul struktúra leírás
- `_doc_/0021_store.md` — store réteg leírás
- `_doc_/0022_data_pipeline.md` → átnevezni/újraírni mint `sync_tables`
- `_doc_/0213_duckdb_stats.md` — új függvények (raw_manifest_audit, log_dataset_check)
- `_doc_/0215_maintenance.md` — törölni (a modul megszűnt)
- `_doc_/0221_sync_ohlcv.md` .. `0224_sync_targets.md` — elérési utak frissítése
- `_doc_/0001_database.md` — ha hivatkozik régi struktúrára

## Acceptance Criteria
- [ ] Minden doc oldal az új mappastruktúrát (`sync_tables/`) és a `sync_pipeline.py`-t tükrözi
- [ ] `_doc_/0215_maintenance.md` törölve vagy archivált
- [ ] Nincs `data_pipeline` szöveg a doc-okban (vagy explicit "renamed" megjegyzés)
- [ ] `_doc_/0022_data_pipeline.md` → `_doc_/0022_sync_tables.md` (átnevezve és frissítve)

## Notes
A doc agent töltse be az érintett _doc_/ oldalakat és a `sync_pipeline.py` végleges kódját mielőtt dokumentál.

2026-06-15: Elvégezve (doc_agent). Minden érintett doc oldal frissítve:
- `0002_database_module.md`: script név javítva `02_sync_pipeline.py`-re, modul struktúra frissítve
- `0021_store.md`: flowchart `02_sync_pipeline` hivatkozással frissítve
- `0022_data_pipeline.md` → `0022_sync_tables.md` átnevezve; `maintenance.py` referencia eltávolítva, `02_sync_pipeline.py` beillesztve
- `0213_duckdb_stats.md`: `sync_pipeline.py` → `02_sync_pipeline.py` javítva
- `0215_maintenance.md`: törölve
- `0221_sync_ohlcv.md`: régi `02_sync_ohlcv.py` Futtatás szekció felváltva `02_sync_pipeline.py` példákkal
- `0023_tests.md`: `sync_pipeline/` teszt mappa hozzáadva a struktúratérképhez

2026-06-15: Validálva (validator_agent). Minden acceptance criteria teljesül:
- `_doc_/0215_maintenance.md`: nem létezik (törölve) ✓
- `_doc_/0022_sync_tables.md`: létezik ✓
- `_doc_/0022_data_pipeline.md`: nem létezik (átnevezve) ✓
- `_doc_/0213_duckdb_stats.md`: tartalmaz `raw_manifest_audit` és `log_dataset_check` dokumentációt ✓
- Megjegyzés: `data_pipeline` szöveg előfordul `0023_tests.md`-ben és `0232_pipeline_tests.md`-ben, de kizárólag a teszt mappa nevére (`src/database/tests/data_pipeline/`) vonatkozó kontextusban — nem a modul elérési útjaként. Elfogadható.
