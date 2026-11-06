---
epic: epic_068
id: t880
title: BFF váz (FastAPI) + read endpointok
assignee: ui_agent
status: done
blocks: [t882, t883, t884, t887, t888, t889]
blocked_by: [t879]
---

## Goal
FastAPI BFF `src/api/` a life_plan mintájára; read-only endpointok: predikciók, jelek, trade journal, equity, strat táblák, modell/registry lista. A `src/ui/data.py` és `binance_data.py` logikája ide költözik (nem másolódik).

## Scope
`src/api/**` (új), `src/ui/data.py`, `src/ui/binance_data.py`, `src/utils.py` (config gateway marad).

## Acceptance Criteria
- [x] Endpointok a blueprint szerint, pydantic modellekkel
- [x] DuckDB kapcsolat read-only, nem blokkolja az event loopot
- [x] A statikus frontend build kiszolgálása (mint a life_plan)
- [x] pytest smoke tesztek, ruff, pyright zöld

## Notes

### Elkészült (ui_agent)
- `src/api/` váz: `main.py` (`create_app()` factory, CORS csak dev, `/api/*` JSON-404, `frontend/dist` + SPA fallback, tiszta hely a `/mcp` mounthoz a SPA előtt, `jobs` router opcionális include `importlib`-tal — jelenleg nincs `routers/jobs.py`, ezért kimarad), `deps.py` (rövid életű RO kapcsolat, 4 ATTACH-próba 200 ms backoffal, `DbBusyError` -> 503 `db_busy`, `db_state()`), `errors.py` (egységes `{detail, code}`), `schemas.py` (pydantic), `routers/` (health+status, market, predictions, trades, equity, strat, registry, logs), `services/` (common, predictions, market, trades, binance, strat, registry, status, logs).
- Az endpointok sync `def`-ek -> FastAPI threadpool, az event loop nem blokkol. A `ui/data.py` + `ui/binance_data.py` logikája pandas-mentes JSON-kimenettel portolva (a `src/ui/` érintetlen, t889 törli; addig az ui/data.py duplikátum marad, mert a Streamlit még használja).
- `src/data_handling/store/readonly_query.py`: a `07_query_readonly.py` teljes logikája (validate_statement, open_readonly_connection, run_query, schema_listing, format_*), + `run_readonly_query(sql, limit)` és `describe_schema(...)`. A CLI vékony wrapper, minden korábbi nevet re-exportál (a meglévő teszt `qr.utils` patchelése változatlanul működik).
- `pyproject.toml`: `fastapi`, `uvicorn[standard]` (+ `httpx` dev). `.gitignore`: `frontend/node_modules/`, `frontend/dist/`.
- Tesztek: `src/api/tests/` (15 smoke, TestClient + tmp DuckDB/journal/artifact környezet), `src/data_handling/tests/query_readonly/test_readonly_query_module.py` (3).

### Eltérések a blueprinttől / döntések
1. **Strat session_id**: a `config/trading.json` `strategy_session_*_id` path-szerű (`<model>/strategy`), a valódi `strat."<id>__trades"` tábla-prefix az artifact `session_id` mezője (`strat_solusdt_fw60_long_2101_2607`). A régi `ui.data._load_strat_table` ezért a mai configgal üres eredményt adott; az API az artifactból feloldott prefixet használja (`ActiveSession.table_prefix`). Az `/api/strat/{session_id}` és `session_ids` is ezt a prefixet adja.
2. **Egyetlen RO kapcsolat per kérés**: a blueprint §7 a `duckdb_query` helpereket említi; ezek hívásonként külön, nem újrapróbáló kapcsolatot nyitnának (lock esetén nyers IOException). Helyette `readonly_query.open_readonly_connection` (live/lab/reg + `jrn` = trading journal, mind READ_ONLY) + SQL, így egy kérés = egy kapcsolat, egységes retry/`db_busy`. A `registry.py` olvasó függvényeit sem használom (raw RO SQL, blueprint megengedi).
3. **Bővített `open_readonly_connection`** (`tries`, `delay`, `extra_targets` kw-only paraméterek, default viselkedés változatlan).
4. **`/api/status` worker/sync**: a worker nincs kész (t881), ezért a `services/status.py` a `database/_jobs/jobs.sqlite`-ot read-only, védekezően olvassa a blueprint §5.2 oszlopneveivel (`heartbeat.last_beat`, `jobs.kind/status/finished_at`, `settings.auto_sync`); hiányzó fájl/hiba -> `worker.alive=false`. **t881 ellenőrizze**, hogy a séma/idő-formátum (epoch vagy ISO) egyezik.
5. **Nem készült** (más task hatásköre): `/api/stream` SSE (t881, §8 térkép), `jobs` router (t881), `reports` (t884, nincs stub), chat/MCP (t883), `mcp` függőség (t883 `uv add`).
6. **Aktív modellek** a `live.predictions` legfrissebb `long/short_model_id` bélyegéből jönnek (a `utils.live_model_id` jelenleg `ValueError`: `env.json runtime.model_id` elavult — nem az én hatásköröm, jelzem).
7. `market/chart`: a bars a `focus_hours` ablakot adja (a régi UI `+2 h` lookback-je nélkül), 1 perces sorokkal; a kliens resample-el. `Bar` nem tartalmaz `v`-t.
8. `trades/active`: journal OPEN sor, különben Binance futures pozíció (a régi fallback); a journal táblában nincs `stop_loss/take_profit` oszlop -> null. A fallback hálózati hívás (~4 s) a `/market/chart`-ot is lassítja, ha nincs journal pozíció — t882/t889 dönthet a cache/elhagyás mellett.
9. `signals`: a küszöb-átlépés = pct felfelé átlépi az aktív entry cutoffot (rising edge), oldalanként; a short cutoff a régi chart szerint közvetlenül a `short_pct`-re vonatkozik (nem `1 - pct`).
10. Registry `filter`: substring (ILIKE, `%`/`_` escape-elve) az összes oszlopon.

### Futtatott parancsok
- `uv add fastapi "uvicorn[standard]"` ; `uv add --dev httpx` -> OK
- `uv run ruff check src/api src/data_handling --fix` -> All checks passed
- `uv run pyright src/api src/data_handling/store/readonly_query.py` -> 0 errors, 0 warnings
- `uv run pytest src/api src/data_handling/tests/query_readonly -v` -> 52 passed
- Valós szerver (`uvicorn api.main:app --app-dir src --port 8611`, read-only), curl: `/api/health`, `/api/status` (3.23M ohlcv sor, modellek, `db.state=ok`), `/api/market/chart?window=4h`, `/api/predictions/latest`, `/api/strat/active`, `/api/trades/closed`, `/api/registry/models/<id>`, `/api/trades/binance` (futures fillek) -> mind 200 és helyes adat; ismeretlen `/api/nope` -> 404 `{detail, code}`. Egy futás közben egy külső író (PID 11636, sync) tartotta a live.duckdb zárat: `/api/market/candles` és `/market/tables` 503 `db_busy` volt (a retry/busy ág élesben is működött). A szerver leállítva.


[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
Integráció: scripts\start.ps1 -NoBrowser (CQ_PORT=8698): /api/health, /api/status (worker.alive=true), /api/jobs* 200. A status.py a jobs.sqlite tényleges sémáját olvassa (heartbeat.last_beat, jobs.kind/status/finished_at, settings.auto_sync, ISO-Z idő; a worker/queue.py CREATE TABLE-lel egyezik, fromisoformat-tal parse). Kisebb javítás: GET /api/registry/models/{id} 'reports' mezője be volt kötetlen ([]), bekötve a reports.store-ra (api/services/registry.py), api tesztek 47 passed.
