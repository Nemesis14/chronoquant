---
epic: epic_031
id: t321
title: Kód-referencia X110 — 1410_snapshots_code + 1510_registry_code
assignee: code_doc_agent
status: todo
blocks: []
blocked_by: [t320]
---

## Goal
X110 kód-referencia docok a `snapshots.py` és `registry.py` modulokhoz: függvény-API,
paraméterek, CLI, diagramok. Entry Gate: a szülő X100 (t320) létezik és teljes.

## Scope
- `_doc_/1410_snapshots_code.md` (X110) — snapshots.py + 05_create_snapshot.py
- `_doc_/1510_registry_code.md` (X110) — registry.py + migrations.py
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` 13.1; docs_skill X110 szabályok

## Acceptance Criteria
- [ ] 1410 + 1510 függvényenkénti leírás (param, return, diagram)
- [ ] Overview flowchart/sequenceDiagram mindkettőben
- [ ] cross-reference az X100-ra (nem ismétlés)
- [ ] docs_skill Mermaid szabályok betartva

## Notes

**Entry Gate:** PASSED — `_doc_/methodology_doc/1400_snapshots.md` és `1500_registry.md`
léteznek és teljesek (overview, módszertani háttér, paraméterek, validációs checklist).

**Létrehozott fájlok (database_and_code_doc zóna, X110):**
- `_doc_/database_and_code_doc/1410_snapshots_code.md` — `snapshots.py` + `05_create_snapshot.py`
- `_doc_/database_and_code_doc/1510_registry_code.md` — `registry.py` + `migrations.py`

**Dokumentált függvények — 1410:**
- `snapshots.py`: `SnapshotResult` (dataclass), `ensure_snap_schema`, `format_range`,
  `build_snapshot_id`, `compute_content_sha256`, `compute_feature_set_hash`, `create_snapshot`,
  + belső: `_ordered_feature_columns`, `_resolve_range`, `_snapshot_table_fqn`,
  `_find_reusable_snapshot`. Konstansok: `SNAP_SCHEMA`, `LIVE_SOURCE`, `HASH8_LEN`.
- `05_create_snapshot.py`: CLI argumentumok (`--asset-id`, `--horizon`, `--start`, `--end`),
  használati példák, `main()` flow.

**Dokumentált függvények — 1510:**
- `migrations.py`: `Migration` (dataclass), `applied_versions`, `run_migrations`,
  + belső `_ensure_migrations_table`.
- `registry.py`: `_migration_001_reg_schema` (8 tábla), `get_registry_connection`,
  `ensure_registry`, `open_crud_connection`, `attach_registry`, `attach_live`,
  `open_lab_connection`, `upsert`, `get`, `list_rows`, `set_status`, `delete`,
  + belső `_serialize_value`, `_validate_table`. Konstansok: `REG_SCHEMA`, `REG_TABLES`,
  `STATUS_LIFECYCLE`, `_PK`, `_JSON_COLS`.

**Mermaid diagramok:** 1410 — Overview flowchart, `create_snapshot` sequenceDiagram,
`main()` flowchart (3 db). 1510 — Overview flowchart, migrations sequenceDiagram,
`open_lab_connection` flowchart, `upsert` flowchart (4 db). Minden fence column-0, lowercase
`mermaid`, emoji-mentes node labelek (docs_skill szabályok betartva).

**Cross-reference:** mindkét doc egyirányúan felfelé linkel a methodology_doc X100-ra
(1400/1500) + egymásra; nincs tartalom-ismétlés (a "miérteket" a methodology hordozza).

**Acceptance Criteria:** mind a 4 teljesítve (függvényenkénti leírás param/return/diagram;
Overview flowchart/sequenceDiagram mindkettőben; cross-ref X100-ra ismétlés nélkül;
docs_skill Mermaid szabályok).
