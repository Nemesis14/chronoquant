---
epic: epic_068
id: t897
title: Élő order-logika (src/trading/live) törlése, journal.py megtartása
assignee: ui_agent
status: done
blocks: [t890]
blocked_by: []
---

## Goal
A t889 audit (B) javaslata szerint (user jóváhagyta): az élő order-kód törlése; a `live/journal.py` marad a trading.duckdb séma-referenciájaként.

## Scope
`src/trading/live/{service,exchange,state,strategy}.py`, `src/trading/01_run_service.py`, `src/trading/tests/`, `config/trading.json`, `trading_reports/`.

## Acceptance Criteria
- [x] service/exchange/state/strategy.py, 01_run_service.py, tests/ törölve
- [x] `trading_reports/` törölve
- [x] `config/trading.json`: csak az új kód kulcsai maradtak
- [x] `journal.py` importálható, csomag konzisztens
- [x] ruff, pyright, pytest (trading/api/worker/cq_mcp) zöld

## Notes
[ui_agent] Elvégezve.
- config/trading.json megmaradt: `schema_version`, `asset_id`, `strategy_session_{long,short}_id`, `ohlcv_sync_delay_seconds`, `db_path`. Törölve: `mode`, `leverage`, `quote_order_qty`, `poll_seconds`, `risk`, `journal`.
- `utils.load_trading_config` az egyetlen loader, használt (api deps/common, worker scheduler, journal) -> marad.
- `python-binance` marad: `api/services/binance.py` és `data_handling/sync_tables/sync_ohlcv.py` használja. Nincs elhagyható függőség; `uv lock` változatlan (184 csomag).
- Törölt fájlokra mutató hivatkozások (t891/t890): `_doc_/database_and_code_doc/{7000_trading,7110_run_service,7120_trading_service,7140_trading_exchange,7150_trading_state_strategy}.md`, `_doc_/0000_project_overview.md`, `_doc_/_plans_/068_bff_ui.md`, `docs/{reference/artifacts,engineering/project_structure,business/trading_strategy,architecture/overview,architecture/components/live_trading}.md`, `.agent/agents/ui_agent.md` (Scope: `src/trading/service.py`).
- journal.py tartalmaz `mode`-ot a trading_runs sémában (paraméterként, nem configból) – változatlan.
- Ellenőrzés: ruff tiszta, pyright 0 hiba, pytest 86 passed (trading tesztek nincsenek már).

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
