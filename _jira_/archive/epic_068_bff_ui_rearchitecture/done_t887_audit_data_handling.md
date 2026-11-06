---
epic: epic_068
id: t887
title: Dead code + redundancia audit és refaktor: data_handling
assignee: database_agent
status: done
blocks: [t890]
blocked_by: [t880, t881]
---

## Goal
Hatékony, coding standard szerinti kód. Használatlan függvények/fájlok törlése, duplikációk megszüntetése, nevezéktan. Viselkedés nem változik.

## Scope
`src/data_handling/**`

## Acceptance Criteria
- [x] Dead code lista a Notes-ban (bizonyítékkal: referenciák)
- [x] Törlések megtörténtek
- [x] pytest, ruff, pyright zöld

## Notes

### Módszer
Leltár: minden top-level def/class a `src/data_handling/**`-ban (AST) x nevenkénti referencia-keresés a teljes git-követett fájlhalmazon (py/md/json/toml/ts), + `rg` a belépési pontokra, pyproject (nincs data_handling script entry), `.agent`/`_doc_` hivatkozások. `_jira_/archive` nem olvasva.

### Törölt (bizonyított dead)
- `store/toolkit.py` (+ `_doc_/database_and_code_doc/1150_toolkit.md`): 0 import a repóban (src/api, worker, modeling, strategy, trading, ui, tests, notebookok); csak doksi-említés. Doksi-hivatkozások frissítve: `1100_store.md` (diagram+szekció), `.agent/agents/database_agent.md` (scope sor), `docs/architecture/overview.md`.
- `sync_tables/sync_features.py::build_lag_snapshot`: 0 hívó, 0 teszt, csak vékony wrapper az `asof_join_predictions_features` fölött. Doksi-szekció törölve: `2100_sync_features.md`.

### Összevonás / kozmetika
- `store/analyst_signal_store.py::get_connection` volt a `duckdb_store.get_connection` szó szerinti duplikátuma -> import (re-export, a `analyst_signal_store.get_connection` hívók változatlanok).
- `collectors/tradingview/api.py:9` komment frissítve (`_doc_/research/tradingview_poc.md`, a 068 terv kérte).
- ruff --fix: 4 unused import (a fenti törlések következménye).
- Nem nyúltam: `07_query_readonly.py` / `store/readonly_query.py` (t880 munkája), src/api, worker, ui, frontend.

### Megtartott, mert élő/dokumentált
01-07 CLI-k, `store/registry_validator.py` (dokumentált `python -m` CLI, t890/t892 használja), `04_backfill_predictions.py` (dokumentált CLI), `sync_quant_train`, `sync_triple_barrier`, `sync_call_returns`, `sync_universe_*` (modeling/analyst_signal / experiments importálja).

### Kérdések az orchestrátornak (nem eldönthető)
1. `collectors/{reddit,derivatives,universe,tradingview}` (~3700 sor): nincs prod-hívó, de dokumentált kutatási belépési pontok (`python -m`), `research/tv_report` és `modeling/experiments/cross_sectional_probe` importálja. Törölhető-e a reddit/derivatives/universe (altdata kísérletek lezárva)? Ez O-1/t890 függő.
2. `store/analyst_signal_store.py` + `sync_call_returns`/`sync_altdata_features`/`sync_universe_*`: csak az altdata/analyst_signal kutatás használja; marad, amíg az 1. kérdés nem dől el.
3. Csak tesztből hívott: `registry.list_rows`, `duckdb_stats.log_dataset_check`, `duckdb_query.asof_join_predictions_features` (a build_lag_snapshot törlése után már csak teszt hívja). Megtartottam (tesztelt publikus API; az API/MCP olvasó helpernek jó lehet) - törölhetők a tesztjeikkel együtt, ha nem kell.
4. `_utc_now` duplikátum (`collectors/tradingview/api.py`, `collectors/reddit/api.py`) és `_connect_with_retry` (sync_triple_barrier) vs `_attach_with_retry` (readonly_query): eltérő szemantika/kontextus, nem vontam össze. `ui/data.py` átfedés: t889.
5. `_features_polars.py` 1587 sor: bontás kockázatos (számítási mag), nem érintettem.

### Parancsok / eredmények
- Baseline (előtte): `uv run pytest src/data_handling src/api src/worker` -> 308 passed, 4 failed (adatfüggő: test_asof_join perf, no_time_gap, target_row_count, predictions_score_range).
- Utána: `ruff check src/data_handling --fix` zöld; `pyright src/data_handling` 0 hiba; pytest 314 passed, 3 failed (ugyanaz a 3 adatfüggő sanity teszt, a perf teszt flaky volt). Egy köztes futás 71 setup-hibát adott: a live DB-t egy párhuzamos folyamat zárolta (IOException), újrafuttatva eltűnt. Törölt kód tesztje nem volt.

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
