---
epic: epic_031
id: t312
title: Snapshot réteg + create_snapshot CLI
assignee: database_agent
status: todo
blocks: [t313, t320]
blocked_by: [t311]
---

## Goal
Immutable snapshot réteg: a `live.quant_train` egy range-ének befagyasztása
`snap."<snapshot_id>"` DuckDB táblába (CTAS), content-hash számítással, és
regisztráció a `reg.snapshots`-ba. CLI a folyamat indítására.

## Scope
- `src/data_handling/store/snapshots.py` (új) — CTAS immutable snap tábla + sha256 hash
- `src/data_handling/05_create_snapshot.py` (új) — CLI (asset, range)
- reg.snapshots írás (range, row_count, content_sha256, feature_set_hash)
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` 4.1, 5 (1. lépés), 6 (naming)

## Acceptance Criteria
- [ ] `snap."<snapshot_id>"` tábla létrejön, immutable (újrafuttatás nem írja felül)
- [ ] snapshot_id naming: `{asset}_fw{h}_{range}__{hash8}` (6. szakasz)
- [ ] content_sha256 + feature_set_hash számítás; reuse-detektálás azonos tartalomra
- [ ] reg.snapshots sor keletkezik
- [ ] CLI futtatható: `uv run python src/data_handling/05_create_snapshot.py ...`
- [ ] smoke teszt + ruff + pyright tiszta

## Notes

### Elvégezve (database_agent)

**Létrehozott fájlok:**
- `src/data_handling/store/snapshots.py` — immutable snapshot réteg (`snap` séma a lab DB-ben). Publikus API: `create_snapshot(conn, asset_id, horizon=60, start_time=None, end_time=None) -> SnapshotResult`; `ensure_snap_schema(conn)`; `format_range(start, end) -> str`; `build_snapshot_id(asset_id, horizon, range_token, content_sha256) -> str`; `compute_content_sha256(conn, range_start, range_end) -> tuple[str, int]`; `compute_feature_set_hash(conn) -> str`. `SnapshotResult` dataclass (snapshot_id, asset_id, range_start, range_end, row_count, content_sha256, feature_set_hash, reused).
- `src/data_handling/05_create_snapshot.py` — CLI: `--asset-id`, `--horizon` (default 60), `--start`, `--end`. A `utils.open_lab_connection(asset_id)`-on át dolgozik (lab default + live RO + reg). Tartomány nélkül full history.
- `src/data_handling/tests/store/smoke/test_snapshots.py` — 4 smoke teszt (CTAS+reg írás, immutability re-run-on, feature_set_hash csak feat_* oszlopokra, format_range single-year vs span).

**snapshot_id formátum** (plan 6): `{asset}_fw{h}_{range}__{hash8}`.
- Konkrét, valós adaton verifikált példa: `solusdt_fw60_2023__a37d2703` (range 2023-01-01..2023-01-02, 1441 sor).
- `range` token: ha mindkét bound azonos naptári évben → `{year}` (pl. `2023`); egyébként `{YYMM_start}_{YYMM_end}` (pl. `2101_2605`).
- `hash8` = a content_sha256 első 8 hex karaktere.

**Hash-séma:**
- `content_sha256` = `sha256(string_agg(to_json(row), '\n' ORDER BY open_time))` a range minden oszlopára — azonos tartalom → azonos hash → reuse. Üres range esetén `sha256('')`. A fenti példa: `a37d2703ad05d7cec...`.
- `feature_set_hash` = `sha256(','.join(sorted feat_* oszlopnevek))` — a szuperszett logikai feature-listáját azonosítja (open_time/target oszlopok kizárva). Példa: `92ca55e65082...`.

**reg.snapshots oszlopok (írt mezők):** `snapshot_id` (PK), `asset_id`, `range_start`, `range_end`, `row_count`, `content_sha256`, `feature_set_hash`, `status='candidate'` — a t311 `registry.upsert(conn, "snapshots", ...)` CRUD-on át (idempotens).

**Immutability / reuse:** a `snap."<id>"` táblát `CREATE TABLE IF NOT EXISTS` hozza létre. Re-run azonos tartalomra: `(asset_id, content_sha256)` egyezés + meglévő tábla → `reused=True`, nincs felülírás, nincs új reg sor. (Smoke teszt: a befagyasztott táblát üresítve a re-run NEM írta felül.)

**Döntés:** a `snap` valódi DuckDB schema a lab (default) DB-ben — itt nincs ATTACH-alias ütközés (a lab a default DB), így a plan `snap."<id>"` SQL-je közvetlenül működik (a t311 reg nested-schema problémája csak az ATTACH-olt DB-kre vonatkozott).

**Eredmény:**
- `ruff check src/data_handling/ --fix` — tiszta (2 pre-existing import-formázás auto-fixelve: `sync_quant_train.py`, `test_validate.py`).
- `pyright snapshots.py 05_create_snapshot.py` — 0 error, 0 warning.
- `pytest test_snapshots.py` — 4 passed.
- End-to-end CLI valós `live.quant_train`-en (3M sor): kis range befagyasztva (`solusdt_fw60_2023__a37d2703`, 1441 sor), re-run reuse-t detektált. Verifikációs snapshot + reg sor utána takarítva.

**Minden acceptance criterion teljesült.**
