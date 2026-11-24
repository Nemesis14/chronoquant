---
epic: epic_069
id: t903
title: API asset-kontextus, worker asset-onkénti sync, MCP asset paraméter
assignee: ui_agent
status: done
blocks: ["t904", "t907"]
blocked_by: ["t902"]
---

## Goal
Az API, a worker és az MCP több assetet kezel; default `solusdt` (nincs viselkedésváltozás paraméter nélkül).

## Scope
`src/api/**` (services/market, binance, status, strat, registry, deps), `src/worker/**` (handlers, scheduler, queue dedup), `src/cq_mcp/**`, `src/api/chat/persona_prompt.md`.

## Acceptance Criteria
- [x] Minden `/api/*` olvasó végpont fogad `?asset=<id>`-t (FastAPI dependency `get_asset`), ismeretlen asset -> 404; `GET /api/assets` az aktív asseteket adja
- [x] A Binance fill/pozíció a kiválasztott asset symbolját használja (TTL cache kulcsa asset)
- [x] Worker: sync jobok `asset_id` paraméterrel, a dedup kulcs tartalmazza az assetet, az auto-sync minden aktív assetre fut egymás után (egy futó job egyszerre); a heartbeat/státusz marad
- [x] MCP tooloknak opcionális `asset` paramétere van (default solusdt), a persona prompt asset-tudatos
- [x] Tesztek (api/worker/cq_mcp) zöldek + új tesztek az asset-kontextusra; ruff, pyright
- [x] A fut a worker/API újraindítása a végén (scripts\\stop.ps1 -> start.ps1 -NoBrowser), SOL sync működik tovább

## Notes

### API asset-szerződés (t904 frontendnek)
- Query paraméter neve: **`asset`** (`?asset=<asset_id>`, pl. `bchusdt`). Elhagyva = `default_asset_id` (solusdt), a SOL viselkedés változatlan. Ismeretlen asset = `404 {"detail": "unknown asset: 'x'", "code": "not_found"}`.
- Minden olvasó végpont fogadja: `/api/status`, `/api/market/{config,candles,chart,tables}`, `/api/predictions/*`, `/api/trades/*` (closed, active, journal/*, binance), `/api/equity`, `/api/equity/summary`, `/api/strat/*`, `/api/registry/*`. Nem asset-specifikus (paraméter nélkül): `/api/jobs*`, `/api/reports*`, `/api/logs/*`, `/api/chat*`, `/api/health`, `/api/assets`.
- `GET /api/assets` -> `{"default_asset_id": "solusdt", "items": [{"asset_id", "symbol", "interval", "market", "is_default": bool, "has_data": bool, "active": bool}]}`; a default elöl, utána `config/assets.json` sorrend. Mind a konfigurált asset szerepel; `has_data` = van live DuckDB fájl, `active` = auto-sync aktív (van live DB + `trading.json` `assets.<id>` kiosztás). A frontend dropdownja javaslat: `active` assetek (vagy `has_data`, jelzéssel); az `?asset=` bármely konfigurált idre működik.
- `/api/status` és `/api/market/config` válaszában `asset_id`/`symbol` az asseté; `status.sync` az adott asset sync jobjaira szűr (worker heartbeat és `auto_sync` globális).
- Sync job indítása: `POST /api/jobs {"kind": "sync_full", "params": {"asset_id": "bchusdt"}}` (hiányzó asset_id = default; ismeretlen = 404). Dedup kulcs: `(kind, asset_id)`; a job `params.asset_id` mindig ki van töltve.

### Elvégzett változtatások
- `src/worker/assets.py` (új): `configured_asset_ids`, `require_asset` / `UnknownAssetError`, `sync_asset_ids`, `list_assets`. **Auto-sync szabály**: konfigurált asset ÉS létező live DuckDB fájl ÉS `trading.json` `assets.<id>` session-kiosztás (`utils.trading_asset_ids`). A BCH-nak most van DB fájlja (t901), de nincs `assets.bchusdt` blokk -> az auto-sync NEM fut rá; a blokk felvételével (modell/deploy után, t906) kapcsol be automatikusan, újraindítás nélkül.
- `api/deps.py`: `get_asset` / `AssetDep`, `get_conn` és `ro_connection(asset_id)` asset-specifikus `live`/`lab` attach (reg és journal közös).
- Services: `active_sessions(asset_id)` (`utils.strategy_session_ids`), minden service `asset_id` paraméter; `binance` symbol asset szerint, a pozíció-cache dict asset kulccsal.
- Worker: `queue.enqueue` normalizálja a `params.asset_id`-t sync kindekre és `(kind, asset)` szerint deduplikál; `scheduler.tick` minden sync-aktív assetre enqueue-ol (default elöl; a worker egyszerre egy jobot futtat -> egymás után), asset-onkénti last-minute kulcs (default: régi `auto_sync_last_minute`). A handlerek már asset_id-t olvastak. Heartbeat/státusz változatlan.
- MCP: új `list_assets` tool (19 tool), `asset` opcionális paraméter: `get_status`, `list_tables`, `query_sql`, `get_candles`, `get_predictions`, `get_latest_signal`, `get_active_strategy`, `get_trades`, `get_equity`, `enqueue_sync`. A persona prompt és a `server.INSTRUCTIONS` asset-tudatos.
- `config/trading.json`: a lapos `strategy_session_*_id` kulcsok törölve (az API már az `assets` blokkot olvassa; `utils` legacy fallback marad).
- Dokumentáció: 8300/8310/8320, `ui_agent.md` és overview tool-szám frissítve.
- Tesztek: új `api/tests/test_assets_api.py`, `worker/tests/test_assets.py`, `cq_mcp` `test_asset_parameter`; 2 meglévő teszt kijavítva (params-ban megjelenik az `asset_id`). `uv run pytest src/api src/worker src/cq_mcp -q` 96 passed; ruff, pyright 0 hiba.
- Újraindítás (`scripts\stop.ps1` -> `scripts\start.ps1 -NoBrowser`) megtörtént; a /api/health ok, SOL auto-sync új workerrel fut.

### Ismert korlátok
- A trade journal (`jrn`, `database/trading.duckdb`) asset-független: `trades/journal/*`, `trades/active` journal-ága minden assetre ugyanazt adja (ma csak SOL kereskedik). Asset-szűrés a journal oldalon külön feladat (a `trading_runs.asset_id` megvan, a pozíciók `run_id`-n át kapcsolhatók).
- Egy hosszú BCH bootstrap `sync_full` a FIFO miatt várakoztatná a SOL auto-syncet, ezért a BCH-t a bootstrap alatt nem vesszük auto-syncbe (szabály fent).
