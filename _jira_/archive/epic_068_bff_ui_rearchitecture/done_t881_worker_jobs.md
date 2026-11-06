---
epic: epic_068
id: t881
title: Worker/job-manager: sync és predikció háttérfolyamatban
assignee: ui_agent
status: done
blocks: [t887, t888, t889]
blocked_by: [t879]
---

## Goal
A sync/predikció külön processzben fut, a UI csak státuszt/logot kap (SSE). Live trading vezérlés NINCS a scope-ban (a user nem használ live tradinget; a UI csak a trade journalt mutatja).

## Scope
`src/worker/**` (új), `src/api/routers/jobs.py`, `src/ui/sync.py`, `sync_runner.py`, `dashboard_logging.py` logikája.

## Acceptance Criteria
- [x] Job indítás/leállítás/státusz endpoint, egyszerre egy sync job (leállítás = csak queued cancel; running 409, blueprint szerint)
- [x] Log és státusz SSE stream (TestClient-tel tesztelve)
- [ ] Futás közben a read API válaszideje nem romlik (kézi ellenőrzés) — NEM ellenőrizve: nincs read API (t880) és élő Binance sync-et nem futtattam; validátor/kézi teszt kell
- [x] Tesztek, ruff, pyright zöld

## Notes

### t881 — ui_agent (2026-11-06)
**Készült**
- `src/worker/`: `queue.py` (SQLite WAL, `jobs`/`settings`/`heartbeat`; enqueue+dedup `BEGIN IMMEDIATE`-tel, FIFO claim `rowid` szerint, egyszerre 1 running, queued-only cancel, orphan recovery, retention 200 job/30 nap, worker_info), `logs.py` (job log fájl + stdout/logging redirect, byte-offset olvasó, takarítás), `runner.py` (fő ciklus + mellék-szál: heartbeat 5 mp, scheduler tick 1 mp), `scheduler.py` (auto-sync: zárt perc + `ohlcv_sync_delay_seconds`, `last_synced_minute` a settings táblában), `handlers/sync_full.py` (a `ui/sync.py::_run_database_sync_locked` portja, `data_handling.sync_tables.*` belépési pontok), `handlers/sync_predictions.py` (csak predikció-pótlás), `__main__.py` (`python -m worker`, logs/worker.log).
- `src/api/routers/jobs.py`: POST/GET/GET {id}/DELETE {id}, `/log?from=`, `/events` (SSE: status, log{offset,line}, done; `?from=` újracsatlakozás), `/worker`, `/auto-sync` GET+PUT. Hibaformátum `{detail, code}`, `503 worker_down`, `409 conflict`.
- Tesztek: `src/worker/tests/` (26 db: queue, dedup+párhuzamos enqueue, cancel, fake job futtatás, hiba, orphan, heartbeat, scheduler, log offset, API+SSE TestClient-tel). Valódi Binance sync nem futott.
**Eltérések / döntések**
- Nem importálom a `ui.sync`-et (t889 törli): a logika portolva a worker handlerbe; a `threading.Lock` helyett a queue (egy running job). A DuckDB kapcsolat-per-lépés a `data_handling` sync függvényekben már így van; nem tartok nyitva kapcsolatot.
- `queue.list` -> `list_jobs` (builtin-árnyékolás miatt). Heartbeat tábla egysoros (`id=1`).
- SSE `log` esemény `offset` = az adott sor UTÁNI byte-offset (ezt kell `?from=`-nak adni). Ping komment (`: ping`) 15 mp-enként.
- Az `/api/jobs/auto-sync` `delay_seconds` a `config/trading.json`-ból jön (`utils.load_trading_config`).
- A `sync_full` handler `params.asset_id`-t elfogad (default: aktív asset).
- Az API a `worker.queue`/`worker.scheduler`/`worker.logs`-t importálja (nem a handlereket, így nem húz pandas/DuckDB-t).
- Nincs új függőség (stdlib sqlite3; fastapi már a pyprojectben).
**main.py bekötés** (`src/api/main.py` még nem létezik, nem nyúltam hozzá): illesztendő:
`from api.routers import jobs` + `app.include_router(jobs.router)` (a router prefixe már `/api/jobs`; SPA-fallback ELÉ). Plusz `uv run python -m worker` indítás a worker scripthez (t891: `python -m worker`, cwd=repo gyökér, `PYTHONPATH=src` vagy `--app-dir` megfelelő; pytest `pythonpath=["src"]`-t használ).
**Parancsok**
- `uv run ruff check src/worker src/api/routers/jobs.py --fix` -> All checks passed
- `uv run pyright src/worker src/api/routers/jobs.py` -> 0 errors
- `uv run pytest src/worker -v` -> 26 passed
**Nyitott**: read API válaszidő futó sync alatt (kézi, t880 után); a worker `python -m worker` éles indítását nem próbáltam (csak import-ellenőrzés a handlerekre).

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
Nyitott AC lezárva (részben): valódi sentiment_refresh worker job futott (BFF+worker egyben, :8698); a read API válaszideje alatta változatlan (status ~280 ms, market/chart ~360 ms, baseline azonos), SSE /api/jobs/{id}/events: status/log/done folyik, a job result/log_size helyes. Nem futott valódi sync_full (élő Binance + DB-írás) — a db_busy ág a t880-ban élesben igazolt. Megjegyzés: az SSE 'status: succeeded' esemény néha a záró log sor előtt érkezik (kozmetikai).
