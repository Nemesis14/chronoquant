---
epic: epic_068
id: t894
title: Research-kollektorok és altdata sync-ek törlése (tradingview marad)
assignee: database_agent
status: done
blocks: [t890]
blocked_by: [t886, t895]
---

## Goal
A user döntése (A): a TV-hangulat funkció marad, minden más research-jellegű kód törlődik. A t887 audit jelöltjei törlendők.

## Scope
`src/data_handling/collectors/{reddit,derivatives,universe}` (a `tradingview` MARAD), `analyst_signal_store`, `sync_call_returns`, `sync_altdata_features`, `sync_universe_*`, a hozzájuk tartozó tesztek, config kulcsok, `_doc_/database_and_code_doc` hivatkozások (a doksi-újraírás a t891-é, de a törölt modulok szekcióit töröld), t887 Notes 3. pontjának csak-tesztből-hívott jelöltjei (`registry.list_rows`, `duckdb_stats.log_dataset_check`, `asof_join_predictions_features`) a tesztjeikkel együtt, ha tényleg nincs hívó. Függ a t895-től (a `cross_sectional_probe` importálja a collectorokat).

## Acceptance Criteria
- [x] Törölt modulok listája bizonyítékkal a Notes-ban
- [x] A `tradingview` collector és a tv_report-függőségek érintetlenek, működnek
- [x] DB séma/adat nem törlődik (csak kód); az altdata táblák sorsa javaslatként a Notes-ban
- [x] pytest, ruff, pyright zöld

## Notes

[database_agent] Elvégzve (2026-11-06).

### Törölt (git rm), összesen 4264 sor + 36 teszt
- `collectors/reddit/` (5 fájl, 893 sor), `collectors/derivatives/` (4 fájl, 934), `collectors/universe/` (4 fájl, 911)
- `sync_tables/`: `sync_altdata_features.py` (142), `_altdata_features_polars.py` (246, a sync_altdata/universe_features számítómagja), `sync_call_returns.py` (306), `sync_universe_features.py` (357), `sync_universe_target.py` (329)
- teszt: `tests/sync_tables/smoke/test_altdata_features.py` (150 sor)
- t887 3. pont: `registry.list_rows`, `duckdb_stats.log_dataset_check`, `duckdb_query.asof_join_predictions_features` + a dedikált tesztjeik (3 log_dataset_check teszt, 1 asof teszt). A `list_rows`-t két teszt mellékesen használta (test_snapshots, test_registry_migrations) -> közvetlen SQL COUNT-ra cserélve.
- Mellékes: `analyst_signal_store.upsert_table` és `upsert_call_returns` (csak reddit/aggregate és sync_call_returns hívta); `05_create_snapshot.py --exclude-altdata` kapcsoló (az `ALTDATA_FEATURES`-t importálta, kísérleti control-snapshot célra).
- Config: a `config/*.json`-ban nem volt reddit/altdata/universe kulcs (rg), `utils.py`-ban sincs; nincs módosítás.

### Bizonyíték
`rg` a törlés előtt a teljes repón (`_jira_`, `_doc_`, lock nélkül): a törölt modulokra csak önmagukból/egymásból/tesztjeikből volt hivatkozás, plusz `05_create_snapshot.py` (fent). src/api, worker, cq_mcp, sentiment, modeling, strategy, trading: 0 hivatkozás. Törlés után a rg a törölt nevekre 0 találat a kódban.

### MARADT (szándékosan)
- `collectors/tradingview/` érintetlen; a sentiment (`fetch.py`, `author_backtest.py`) importálja `api`-t és `normalize.SYMBOL_WHITELIST/interval_to_minutes`-t.
- **`store/analyst_signal_store.py` MARAD**: `tradingview/normalize.py` modulszinten importálja (`DB_SUBPATH, ensure_tables, get_connection, make_call_id, upsert_calls`), tehát a sentiment import-láncán van. Csak a két fenti holt író helper lett kivéve; a migrációk (m001-m010) érintetlenek. Ha később a normalize "calls" írás is megy, a store törölhető/vékonyítható (javaslat: külön ticket).
- `duckdb_store.py` quant_train építés: ha létezik `feat_altdata` tábla, LEFT JOIN-nal hozzáveszi a `feat_*` oszlopait (viselkedés változatlan, a tábla a DB-ben van). Ha az altdata táblák törlődnek, ez az ág magától kihal; addig a quant_train csak a következő rebuildnél venné be - döntés a DB-javaslattal együtt.
- Új árva jelölt (nem kértem, nem töröltem): `duckdb_stats.raw_manifest_audit` (a log_dataset_check törlése után csak teszt hívja).
- Doksi: a törölt függvények szekciói kivéve (1100_store, 1120, 1130, 1310, 1510). A collector/sync-szekciók a doksiban korábban sem szerepeltek (0 találat); `.agent/agents/database_agent.md`-ben nem volt hivatkozás. A teljes újraírás t891.

### DB-javaslat (NEM módosítva, az orchestrátor kérdez)
- `database/solusdt/solusdt.duckdb`: `altdata_5m` (497 789 sor), `feat_altdata` (3 127 763 sor), `funding_rate` (5 187; ez lehet, hogy más célra is kell - ellenőrizendő `strategy/costs.py` komment funding_last-ra hivatkozik, kód nem). A `duckdb_store` quant_train join a `feat_altdata`-ra hivatkozik (lásd fent). Javaslat: `feat_altdata` + `altdata_5m` DROP (a solusdt.duckdb jelentősen csökken), `funding_rate` marad amíg a costs ellenőrizve nincs.
- `database/_universe/universe.duckdb` (11 GB, egész könyvtár): ohlcv_1m 81,9 M sor, metrics_5m 13,0 M, ohlcv_5m 16,4 M, premium_5m 16,3 M, target_5m 12,95 M, feat_* táblák, selection/universe/panel_log. Hivatkozó kód már nincs (build_universe/download_panel/sync_universe_* törölve). Javaslat: a teljes `database/_universe/` törlése (visszaállítható Binance Vision-ből, de a collector is törölve - git history).
- `database/analyst_signal/analyst_signal.duckdb` (311 MB): `calls` 17 061 (a tradingview normalize írja - MARAD), `call_returns` 85 305, `author_scores` 39 995, `score_params` 275, `oos_validation` 15, `gate_validation` 200, `gate_power` 2, `social_hourly` 632 592, `social_features` 315 552, `reddit_seen_authors` 2 408 925, `_schema_migrations` 10. Javaslat: DROP `reddit_seen_authors`, `social_hourly`, `social_features`, `call_returns`, `author_scores`, `score_params`, `oos_validation`, `gate_*` (kódjuk törölve, modeling/analyst_signal is); `calls` marad (TV). A séma-migrációk (m002-m010) viszont még létrehoznák az üres táblákat új DB-n - ha a DB-t vágjátok, érdemes a migrációkat is leegyszerűsíteni (külön ticket).
- `database/btcusdt/` 865 MB: BCHUSDT/BTC out of scope, nem vizsgáltam.

### Parancsok / eredmények
- Baseline: `uv run pytest src/data_handling src/api src/worker src/sentiment -q` -> 320 passed, 3 failed (élő-DB sanity: test_feat_ohlcv_quant_no_time_gap, test_target_row_count_matches_features, test_predictions_score_range).
- Utána: 284 passed, ugyanaz a 3 failed (-36 = törölt tesztek); `ruff check src/data_handling --fix` clean (4 unused import javítva); `pyright src/data_handling` 0 hiba.

[validator] Done — 2026-11-06 (t892 epic-validáció)
Futtatva (egész-repo): ruff check src (All checks passed), pyright src (0 errors), pytest src -m 'not perf' (383 passed, 3 failed: élő-DB sanity — test_feat_ohlcv_quant_no_time_gap [14 időrés a live adatban], test_target_row_count_matches_features [target 3 120 401 vs feature 3 231 645, fw60 lemaradás + hiányzó sorok], test_predictions_score_range [nyers regressziós MFE score nem [0,1] — elavult feltételezés]; mind adatfüggő, kódhibára nem utal, az epic előtti ismert állapot), uv lock --check OK, frontend: tsc --noEmit + npm run build OK, registry_validator OK.
