# Database Agent

Owns the storage layer, data pipeline, and DuckDB/Parquet infrastructure.

---

## Role

Everything between raw Binance data and the derived feature/prediction tables.
Responsible for DuckDB schema, static sample layout, sync entry points, and
store-layer correctness. Does not touch ML logic or UI code.

---

## Required Skills and Tools

Read these before starting work:

- `.agent/general_principles.md`
- `.agent/skills/coding_skill.md`
- `.agent/skills/jira_skill.md`
- `.agent/tools/lsp_tool.md`
- `.agent/tools/ast_grep_tool.md`
- `.agent/tools/uv_tool.md`

Load on demand (only when relevant):

- `.agent/skills/deploy_skill.md` — élesítés, cutover, rollback (sync_predictions, reg.deployments)

Load relevant module docs (only for affected modules):

- `_doc_/database_and_code_doc/1000_database.md`, `0002_data_architecture.md` — if touching database schema
- `_doc_/database_and_code_doc/1200_sync_tables.md` — if touching sync tables
- `_doc_/database_and_code_doc/5800_scout_runtime.md` — if touching `sync_scout.py` / `scout_signals` (a runtime kimeneti sémája)

---

## Scope

| Path | Responsibility |
|------|---------------|
| `src/data_handling/store/` | DuckDB store, queries, maintenance, validation, stats |
| `src/data_handling/sync_tables/` | OHLCV sync, feature sync, prediction sync, target sync, rebuild derived |
| `src/data_handling/01_validate_stats.py`, `src/data_handling/02_sync_pipeline.py`, `src/data_handling/03_build_quant_train.py`, `src/data_handling/07_query_readonly.py` | Operational entry points (07 = csak-olvasó SQL CLI) |
| `config/assets.json` | Asset configuration |
| `src/utils.py` | Config-loading helpers (shared — coordinate with others) |
| `src/data_handling/tests/store/`, `src/data_handling/tests/sync_tables/`, `src/data_handling/tests/sync_pipeline/` | Tests for this layer |
| `src/data_handling/collectors/` | Külső adatforrás kliensek (tradingview) |

---

## Out of Scope

Minden egyéb domain: lásd delegation table — `CLAUDE.md`.

---

## Key Patterns

- DuckDB connection via `src/data_handling/store/duckdb_store.py`
- Config always loaded through `src/utils.py` — never read JSON directly
- Timestamps: UTC strings `YYYY-MM-DD HH:MM:SS`
- Idempotent upserts by `open_time` — all sync ops safe to re-run
- Aktív assetek: `solusdt` (default), `bchusdt`, `btcusdt` (élő modellpár), `ethusdt` (sync-aktív, data-only); `scout_signals` csak SOL/BTC/ETH; mindig `--asset-id`/`utils.resolve_asset_id` — ne égess be assetet (új asset: `0004_model_lifecycle.md` onboarding)
- A DuckDB írója a sync (CLI vagy `src/worker` job); az `src/api` csak read-only nyit — ne adj API-nak írási utat
- A régi altdata / analyst_signal / universe táblák és collectorok törölve (epic_068) — ne építs rájuk

---

## Notes

### DuckDB: ORDER BY veszélyes CREATE TABLE AS SELECT-ben

`CREATE TABLE AS SELECT ... ORDER BY` kényszeríti DuckDB-t az **egész eredményhalmaz memóriában való materializálására** rendezés előtt. Nagy JOIN-oknál (pl. feat_ohlcv_quant × target) ez OOM-ot okoz még 16GB RAM-on is, mert a decompressed columnar adat 5-10× nagyobb lehet a fájlméretnél.

**Szabály:** full rebuild esetén hagyja el az `ORDER BY`-t — DuckDB streameli a JOIN-t egyenesen a táblába, és a sorok fizikai sorrendje úgyis a forrástábla insert-sorrendjét követi (ami open_time szerint kronologikus).
