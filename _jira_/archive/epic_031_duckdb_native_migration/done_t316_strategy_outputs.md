---
epic: epic_031
id: t316
title: Strategy kimenetek → strat.* táblák + reg.strategies
assignee: modeling_agent
status: todo
blocks: [t317, t318, t319, t324]
blocked_by: [t315]
---

## Goal
A strategy kalibráció bemenete a `snap ⋈ model_long.__pred ⋈ model_short.__pred` join;
kimenete a parquet helyett `strat."<session_id>__trades/__equity/__cutoffs"` DuckDB táblák,
és a `strategy_artifact.json` fájl marad. Registráció a `reg.strategies`-be.

## Scope
- `src/strategy/strategy/` — build_table join a snap+pred táblákból; trades/equity/cutoffs → strat.* táblák
- `reg.strategies` írás (model_id_long/short, session_id, status); reg.artifacts a fájlokra
- Hivatkozás: `_doc_/_plans_/data_process_architecture.md` 5 (7. lépés), 6 (naming)

## Acceptance Criteria
- [x] strategy a snap+pred join-ból dolgozik (nincs kézi parquet-mozgatás)
- [x] `strat."<session_id>__trades/__equity/__cutoffs"` táblák keletkeznek
- [x] strategy_artifact.json + isotonic/rank_lookup fájl marad; útvonal reg.artifacts-ban
- [x] reg.strategies sor a model-linkekkel
- [x] strategy smoke tesztek zöldek; ruff + pyright tiszta

## Notes

### Elvégezve (modeling_agent)

**Átállított / létrehozott fájlok:**
- `src/strategy/strategy/build_table.py` (**teljes újraírás**) — a régi model.pkl-betöltés + `quant_train`/`ohlcv` query + `strategy_table.parquet` írás **megszűnt**. Új belépési pont: `build_scored_table(long_model_id, short_model_id, asset_id=None, snapshot_id=None) -> pd.DataFrame` — tisztán DuckDB join (`utils.open_lab_connection`), parquet-mozgatás nélkül.
- `src/strategy/strategy/calibrate.py` — `fit_calibration(session_id, scored_df, start, end) -> (calibrated_df, iso_long, iso_short)`. Forrás a parquet helyett a `scored_df`; a kalibrált tábla in-memory tér vissza. A rank/isotonic **fájl-artefaktok maradnak**. A metodológia (percentil rank lookup + isotonic, decile bucketek) változatlan.
- `src/strategy/strategy/optimize.py` — `optimize_strategy(session_id, long_model_id, short_model_id, calibrated_df, start, end, n_trials=200, asset_id=None)`. A parquet-olvasás helyett a `calibrated_df`-fel dolgozik; a `write_realized_outputs` az új strat.* írót hívja; új `_build_cutoffs` a decile cutoffokat építi; a végén `register_strategy` a reg-be ír. A `_simulate_strategy` state-machine + Optuna sweep + same_window eval **változatlan**.
- `src/strategy/strategy/artifacts.py` — `write_realized_outputs(session_id, trades, cutoffs=None, asset_id=None) -> dict` mostantól `strat."<session>__trades/__equity/__cutoffs"` DuckDB táblákat ír (CREATE OR REPLACE, `strat` séma a lab DB-ben). Új `register_strategy(...)` → `reg.strategies` + `reg.artifacts`. Új `strat_table_fqn(session_id, kind)`. A `write_strategy_artifact`/`read_strategy_artifact` (fájl) megmaradt; a JSON `trades_path`/`equity_curve_path`/`summary_path` parquet-mezők helyett most `trades_table`/`equity_table`/`cutoffs_table` strat-tábla FQN-ek (a `rank_lookup_*_path` / `isotonic_*_path` **változatlan** — a live service ezt olvassa).
- `src/strategy/strategy/session_naming.py` — új `derive_session_id(long, short, scope='combo')` a plan 6 mintára (`strat_{asset}_fw{h}_{scope}_{range}`); a régi `derive_strategy_session_id` megmaradt (fallback + live service).
- `src/strategy/00_run_strategy_session.py` (**új CLI**) — egységes orchestrátor: scored join → calibrate → optimize (a 3 lépés in-memory DataFrame-en kommunikál). A régi `00_build_strategy_table.py` / `01_calibrate_scores.py` / `02_optimize_strategy.py` CLI-k **törölve** (a köztes parquet megszűnt).
- Tesztek átállítva: `test_build_table.py` (join smoke szintetikus snap+pred táblákkal), `test_calibrate.py` (scored_df in-memory), `test_optimize.py` (calibrated_df + strat.* + reg.strategies verifikáció patchelt lab connection-nel), `test_artifacts.py` (strat.* írás + register_strategy), `test_session_naming.py` (új derive_session_id).

**build_table join-forrás (plan 5.1 `strat__scored`):**
`snap."<snapshot_id>" s  JOIN model."<long>__pred" pl ON pl.open_time=s.open_time  JOIN model."<short>__pred" ps ON ps.open_time=s.open_time  LEFT JOIN live.ohlcv o ON o.open_time=s.open_time`, ahol a target oszlopok (`long_mfe_fw60`, `short_mfe_fw60`) a snap-ból, a `pred_long_raw`/`pred_short_raw` a 2 pred táblából, a `open/high/low/close` a `live.ohlcv`-ből (RO; ha nincs, NULL price oszlopok). A snapshot_id reg.models-ből (fallback `sampling.snapshot_id`). Kimenet: `pd.DataFrame[open_time, pred_long_raw, pred_short_raw, long_mfe_fw60, short_mfe_fw60, open, high, low, close]`.

**strat.* táblák sémája (CREATE OR REPLACE, `strat` séma a lab DB-ben):**
- `strat."<session>__trades"`: entry_time, exit_time, direction, entry_price, exit_price, hold_minutes, exit_reason, score_pct_at_entry, bucket_mean_mfe.
- `strat."<session>__equity"`: trade_index, entry_time, bucket_mean_mfe, cumulative_mfe.
- `strat."<session>__cutoffs"`: direction, bucket_id, score_raw_lower, score_raw_upper, score_pct_upper, bucket_mean_mfe, bucket_hit_rate (per-direction decile cutoffok a kalibrált scored-table-ből).

**reg.strategies kontraktus:** `strategy_id = session_id`, `model_id_long`, `model_id_short`, `session_id`, `status='candidate'` (a t311 registry CRUD-on át, idempotens upsert).
**reg.artifacts kontraktus:** a 3 strat tábla `kind='strat_trades|strat_equity|strat_cutoffs'`, `path=strat."<session>__<kind>"` (tábla-név); a fájl-artefaktok `kind='strategy_artifact|isotonic_long|isotonic_short|rank_lookup_long|rank_lookup_short'`, `path=<abszolút fájl>` (csak létező fájl). `owner_id=session_id`.

**Megmaradó fájl-artefaktok:** `strategy_artifact.json`, `isotonic_long.pkl`, `isotonic_short.pkl`, `rank_lookup_long.parquet`, `rank_lookup_short.parquet`, `sweep_results.csv` az `artifacts/<session_id>/`-ben. Útvonaluk a `reg.artifacts`-ba kerül. (A korábbi `trades.parquet`/`equity_curve.parquet`/`summary.json` parquet kimenet → strat.* táblákba költözött.)

**session_id példa:** `lgbm_solusdt_l_fw60_2101_2605` + `lgbm_solusdt_s_fw60_2101_2605` → `strat_solusdt_fw60_combo_2101_2605`.

**Live service kompatibilitás:** a `src/trading/live/service.py` a `read_strategy_artifact`-ot + `rank_lookup_*_path` / `isotonic_*_path` + `decision_params` mezőket olvassa — ezek **változatlanok**, a service nem tört el.

**Teszt eredmény:** `pytest src/strategy/tests/ -v` — **17 passed**. `ruff check src/strategy/` — tiszta. `pyright src/strategy/` — 0 error, 0 warning. (A teljes end-to-end a strat.* írást + snap⋈pred join-t + reg.strategies-t szintetikus in-memory táblákon igazolja, mivel valós snap/pred még nincs a lab DB-ben.)

**Döntések / feltételezések:**
- A `close` (entry/exit price) a snap-ban nincs (a `quant_train` nem hordoz price-t) — a join `live.ohlcv`-vel pótolja (RO attach); ha nem elérhető, a price oszlopok NULL-ok (a `_simulate_strategy` close-ja eleve opcionális).
- A 3 lépés in-memory DataFrame-en kommunikál (nincs köztes parquet), ezért egyetlen orchestrátor CLI váltja a 3 régit; a `register_strategy` az optimize végén fut (a session ott teljesül be).
- A `__cutoffs` tartalma a kalibráció decile-cutoffjai (rank lookup bucket-stat) — a plan a 3 strat-táblát megnevezi, a tartalom a kalibrált scored-table-ből logikusan a bucket-küszöbök.

**Acceptance Criteria — mind teljesült:**
- [x] strategy a snap+pred join-ból dolgozik (nincs kézi parquet-mozgatás)
- [x] `strat."<session_id>__trades/__equity/__cutoffs"` táblák keletkeznek
- [x] strategy_artifact.json + isotonic/rank_lookup fájl marad; útvonal reg.artifacts-ban
- [x] reg.strategies sor a model-linkekkel
- [x] strategy smoke tesztek zöldek (17); ruff + pyright tiszta
