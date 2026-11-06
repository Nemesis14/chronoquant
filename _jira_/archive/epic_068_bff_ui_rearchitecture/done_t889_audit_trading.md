---
epic: epic_068
id: t889
title: Audit: trading + régi Streamlit UI kivezetése
assignee: ui_agent
status: done
blocks: [t890]
blocked_by: [t880, t881, t882, t883]
---

## Goal
A user nem használ live tradinget, csak a trade journalt nézi. Vizsgáld, mi marad a `src/trading/`-ból (journal olvasás kell; élő order-logika valószínűleg dead). Live execution kódot jóváhagyás nélkül ne törölj: javaslat a Notes-ban, az orchestrátor kérdez. A `src/ui/` Streamlit törlése itt történik, ha t882/t883 paritást ad.

## Scope
`src/trading/**`, `src/ui/**`, `scripts/*`

## Acceptance Criteria
- [x] Javaslat a trading kód sorsáról a Notes-ban
- [x] `src/ui/` Streamlit kód törölve, paritás ellenőrizve
- [x] Tesztek, ruff, pyright zöld

## Notes

### t889 - ui_agent (2026-11-06)

**A) Streamlit kivezetes - paritas-tablazat**
| Regi (`src/ui/`) | Uj hely | Allapot |
|---|---|---|
| `main.py` fo chart + sidebar sync/auto-sync | `frontend` DashboardPage, PredictionPriceChart, SyncPanel; `worker/` + `/api/jobs*` | OK |
| `data.py` (closed_trades, equity, active_position, latest/history pred, table_health, strat artifact) | `api/services/*` + `/api/{trades,equity,strat,predictions,market}` | OK |
| `binance_data.py`, `trade_panel` (fillek, aktiv trade) | `api/services/binance.py` (+cache t893), TradeTabs/SidePanels | OK |
| `trade_panel` Trading State / Auto Trades kartya | TradeTabs: journal signals/positions/orders/errors/runs (csak olvasas) | OK |
| `sync.py`, `sync_runner.py` | `worker/handlers/sync_full.py`, `worker/scheduler.py` | OK |
| `log_panel`, `dashboard_logging.py` | `/api/logs/{api,worker}` + LogPanel.tsx | OK |
| `chat/runner.py`, `persona_prompt.md`, `chat_panel`, `chat/model.py` | `api/chat/*`, ChatPanel/ChatDock (a ChatDock mar benne van a Layout-ban); model.py UI-allapot a frontende | OK |
| `components/charts.py`, `formatting.py` | PredictionPriceChart.tsx, lib/format.ts | OK |
| `tests/chat/smoke/*` | `api/chat/tests/test_chat.py` (t883) | OK (`test_chat_panel` Streamlit AppTest: ertelmetlen) |
| `trading_runner.py` (live start/stop, mod) | NINCS (D-7: szandekosan kikerult) | Dropolt |
| `.streamlit/config.toml` | - | torolve |
Ertekes, de nem portolt: semmi. A regi 80% UI-zoom/t878 meretezes Streamlit-specifikus volt.

**Torolve**: `src/ui/` teljes egeszeben (22 tracked fajl: main, data, binance_data, sync, sync_runner, trading_runner, dashboard_logging, components/*, chat/*, tests/*, `.streamlit/config.toml`), `scripts/start_ui.ps1`, `scripts/stop_ui.ps1`, regi `scripts/_common.ps1` (ujraiva). `streamlit` dependency torolve a pyproject-bol es uv.lock-bol (rg: nincs tobb import; csak szoveges emlites).
**Uj scriptek** (`scripts/`): `_common.ps1` (port 8600, `CQ_PORT` felulirhato, PYTHONPATH=src, worker pidfile `logs/worker.pid`), `dev.ps1` (uvicorn --reload + worker + Vite :5173), `start.ps1` (rejtett BFF + worker, `frontend/dist` kiszolgalasa, `-NoBrowser`, `-Build`, logok `logs/api.*.log`, `worker.*.log`), `stop.ps1`, `install_autostart.ps1` (uj task `ChronoQuantAutostart`; a regi `ChronoQuantUiAutostart`-ot torli, `-Uninstall` mindkettot). Az install parancsot NEM futtattam.
**utils.py (t896)**: `_CHAT_DEFAULTS` (`persona_prompt_path` -> mar nem letezo `src/ui/chat/persona_prompt.md`, `allowed_bash`), `load_chat_config` erintetlen, a `research/tv_report` miatt a t896-e. A `persona_prompt_path` default torott utat mutat; az API runner a sajat `api/chat/persona_prompt.md`-t olvassa, ezert hatastalan.

**B) `src/trading/` audit**
- Az uj API/worker/MCP NEM importal `trading.*`-t. Olvasas: raw RO SQL a `jrn` katalogusbol (`config/trading.json -> db_path` = `database/trading.duckdb`, tablak: `trading_positions/orders/signals/errors/runs`; `api/services/trades.py`, `api/deps.py`). Config-kulcsok az uj kodban: `db_path`, `ohlcv_sync_delay_seconds` (worker/scheduler), `strategy_session_{long,short}_id` (api/services/common.py). Nincs olvaso `trading_reports/`-ra.
- Elo order-logika (csak ezek hasznaljak egymast): `live/service.py` (TradingService, 486 sor, order kuldes + trading_reports export), `live/exchange.py` (BinanceFuturesClient), `live/state.py` (allapotgep), `live/strategy.py` (evaluator), `01_run_service.py` (CLI), tesztek `tests/live/smoke/test_{state,strategy}.py`. Egyetlen inditoja (`ui/trading_runner.py`) torolve -> mostantol csak kezzel (`01_run_service.py`) futtathato. `live/journal.py` (426 sor): schema-DDL (`ensure_tables`) + irok + olvasok; az API nem hasznalja, de a journal-tablak sema-forrasa. Tesztek: 101 passed (benne trading smoke).
- **Javaslat (a user dont, orchestrator kerdez)**: (1) ERDEMES TORLNI: `service.py`, `exchange.py`, `state.py`, `strategy.py`, `01_run_service.py`, `tests/` + `config/trading.json` `mode/leverage/quote_order_qty/poll_seconds/risk/journal` kulcsok (megmaradnak: `db_path`, `ohlcv_sync_delay_seconds`, `strategy_session_*`) + api-kulcs hivatkozasok; ez elo order kodot szuntet meg, es a jovoben ujraiasnal ugyis ujra kell validalni. (2) `journal.py`: vagy torles (az uj API raw SQL-lel olvas, a DDL a trading.duckdb-ben mar letezik), vagy megtartas 'schema reference'-kent; ajanlas: megtartas, amig a trading.duckdb tablait nem migraljuk. (3) Alternativa: mindent megtartani karbantartas nelkul, 'unsupported' jelolessel a docsban. Ajanlas: (1)+(2 megtartas).
- **`trading_reports/`**: 1 tracked `summary.json` (run_20260620_*), irója a service.py:477 -> ha (1) megvalosul: tartalom + `journal.report_dir/export_on_stop` config-kulcsok torlendok (t890); ha a service marad: gitignore-ba. Docs-hivatkozasok (`docs/*`, `_doc_/7xxx`) t891/t890.

**User-teendok**
1. A regi Streamlit szerver (:8501) MEG FUT es lezarolja a `.venv` fajlokat (`uv sync` hibaja: `google/_upb/_message.pyd` Access denied) es a `logs/ui.*.log`, `streamlit_dashboard.log` (52 MB) fajlokat: allitsd le (`taskkill` a :8501 listenerre), majd `uv sync`, es torold a 3 logfajlt.
2. `scripts\install_autostart.ps1` futtatasa (torli a `ChronoQuantUiAutostart`-ot, felveszi `ChronoQuantAutostart`-ot). Addig a regi task a torolt `start_ui.ps1`-re mutat (belepeskor hibazik).
3. Dontes a `src/trading/` es `trading_reports/` sorsarol (fenti javaslat).
Nyitott (t891): `CLAUDE.md`, `.agent/agents/ui_agent.md`, `_doc_/0000_project_overview.md`, `_doc_/database_and_code_doc/80xx_ui*.md`, `docs/*` Streamlit-hivatkozasok; `walk_forward_tb.py:107` hibauzenet es `trading/*` docstringek 'Streamlit' szoveg.

**Ellenorzes**: ruff (api, worker, cq_mcp, trading, utils) tiszta; pyright 0 hiba; pytest 101 passed; `tsc --noEmit` tiszta, `npm run build` OK; `scripts\start.ps1 -NoBrowser` `CQ_PORT=8699`-en: `/api/health` 200, SPA index szolgalva, worker heartbeat alive=true, `stop.ps1` mindent leallitott (a :8600 regi szervert nem bantottam).

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
