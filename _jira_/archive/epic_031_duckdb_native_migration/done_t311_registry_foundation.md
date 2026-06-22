---
epic: epic_031
id: t311
title: Registry alapok + migrations framework
assignee: database_agent
status: todo
blocks: [t312, t314]
blocked_by: []
---

## Goal
Központi registry (`reg` séma) létrehozása a `database/_registry/registry.duckdb`-ben,
ATTACH-alapú hozzáférési API a `src/utils.py`-ban, és verziózott migrations framework
a sémák kezelésére (kiváltja az `ensure_tables()` inline ALTER/DROP mintát — P4).

## Scope
- `src/data_handling/store/registry.py` (új) — reg.* CRUD + ATTACH helper
- `src/data_handling/store/migrations.py` (új) — verziózott séma-migráció
- `src/utils.py` — registry/ATTACH hozzáférési API (config-gateway elv)
- registry séma: assets, snapshots, feature_sets, models, search_runs, strategies, deployments, artifacts
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` 4.2, 4.3

## Acceptance Criteria
- [ ] `registry.duckdb` létrejön a `reg` séma 8 táblájával (ER a 4.3 szerint)
- [ ] ATTACH helper: live (READ_ONLY) + reg egy connectionből joinolható
- [ ] CRUD függvények minden reg táblára, státusz-mezővel (draft→…→archived)
- [ ] migrations framework: verziózott, idempotens; smoke teszt
- [ ] utils API a registry eléréséhez (nincs közvetlen JSON/DB elérés a hívóknál)
- [ ] ruff + pyright tiszta

## Notes

### Elvégezve (database_agent)

**Létrehozott fájlok:**
- `src/data_handling/store/migrations.py` — verziózott, idempotens migrations framework. `Migration(version, name, apply)` dataclass + `run_migrations(conn, migrations)` (per-migration tranzakció, rollback hibára, `_schema_migrations` bookkeeping tábla, duplikált verzió elutasítása) + `applied_versions(conn)`.
- `src/data_handling/store/registry.py` — reg séma (8 tábla) + ATTACH helperek + generikus CRUD.
- `src/data_handling/tests/store/smoke/test_registry_migrations.py` — 11 smoke teszt (framework idempotencia, rollback, duplikátum-védelem; reg séma 8 tábla; CRUD roundtrip; JSON-oszlop szerializáció; live+reg join).

**reg.* táblák (mind `status` mezővel, default `'draft'`, `created_at`/`updated_at`):**
`assets`, `snapshots`, `feature_sets`, `models`, `search_runs`, `strategies`, `deployments`, `artifacts` — ER a plan 4.3 szerint. PK-k: asset_id / snapshot_id / feature_set_id / model_id / search_run_id / strategy_id / deployment_id / artifact_id. JSON oszlopok: `feature_sets.selected_cols`, `search_runs.best_params`.

**registry.py API:**
- `ensure_registry(registry_path) -> list[int]` — sémát létrehozza/upgradeli a migrations frameworkkel.
- `get_registry_connection(path)` — közvetlen RW connection (migrations futtatáshoz).
- `open_crud_connection(path)` — in-memory + ATTACH `reg`, így a CRUD default `reg.<table>` alias egységesen működik standalone és lab connectionnél is.
- `open_lab_connection(lab_path, live_path, registry_path)` — lab default + `live` (READ_ONLY) + `reg` egy connectionben joinolható (plan 4.2).
- `attach_live(conn, live_path, read_only=True, alias="live")`, `attach_registry(conn, registry_path, read_only=False, alias="reg")`.
- CRUD: `upsert` (INSERT ... ON CONFLICT DO UPDATE, idempotens, updated_at frissül), `get`, `list_rows(status=...)`, `set_status`, `delete`. Konstansok: `REG_TABLES`, `STATUS_LIFECYCLE`, `_PK`, `_JSON_COLS`.

**utils.py API (config-gateway — hívók nem nyúlnak DB/JSON-hoz közvetlenül):**
- `registry_path() -> str` (fix: `database/_registry/registry.duckdb`)
- `lab_db_path(asset_id=None) -> str` (live db_path-ból `_lab` suffix)
- `ensure_registry() -> list[int]`
- `open_registry_connection()` (CRUD connection, `reg.*`)
- `open_lab_connection(asset_id=None)` (lab + live RO + reg)

**Döntések / feltételezések:**
- A reg táblák a registry.duckdb **default (main)** sémájában élnek, NEM nested `reg` schemában. Ok: ATTACH-nál a `reg` alias + nested `reg` schema `reg.reg.assets`-et eredményezett volna; így a plan 4.2 SQL-je (`reg.models`) egyszerűen az ATTACH alias.
- Registry path globális (asset-agnosztikus), nincs az assets.json-ban — a plan 4.2 fix path-ját használtam.
- A migrations framework `_schema_migrations` táblát hoz létre minden DB-ben; reg séma az 1-es verzió (`reg_schema_initial`).

**Eredmény:**
- `ruff check src/data_handling/store/registry.py migrations.py utils.py + teszt` — **tiszta** (a 2 megmaradt SIM108 a utils `_load_json`/`utc_str_to_ms`-ben **pre-existing main-en**, scope-on kívül).
- `pyright registry.py migrations.py utils.py` — **0 error, 0 warning**.
- `pytest test_registry_migrations.py` — **11 passed**.
- End-to-end gateway verifikáció: `utils.ensure_registry()` létrehozta a `database/_registry/registry.duckdb`-t mind a 8 reg táblával; CRUD a gateway-en át OK.

**Minden acceptance criterion teljesült.**
