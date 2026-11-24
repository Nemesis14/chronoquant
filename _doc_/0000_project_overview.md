# ChronoQuant — Project Overview

Single source of truth for the orchestrator. Detailed module documentation lives
in zone subdirectories under `_doc_/` (database_and_code_doc / methodology_doc / models_doc, plus research/ and reports/); this file is for cross-domain context only.

**Adat-architektúra referenciák (epic_031 DuckDB-natív migráció után):**
- `_doc_/database_and_code_doc/0002_data_architecture.md` — 3-fájlos tárolási topológia, sémák, ATTACH
- `_doc_/database_and_code_doc/0003_runtime_flow.md` — éles folyamat: sync → live predict → UI/API → deploy
- `_doc_/database_and_code_doc/0004_model_lifecycle.md` — modell életciklus: snapshot → sample → FE → search → train → predict → deploy/cutover
- `_doc_/database_and_code_doc/8300_api.md` ... `8360_scripts.md` — BFF/worker/MCP/reports/sentiment/frontend/scripts referencia (epic_068)
- `_doc_/_plans_/068_bff_ui.md` — a BFF/UI architektúra kontraktusa

---

## Business Goal

ChronoQuant is an algorithmic crypto trading system targeting **perpetual futures on Binance**.
Három asset fut élő modellpárral és predikcióval: **solusdt** (SOLUSDT, default), **bchusdt** (BCHUSDT, epic_069) és **btcusdt**
(BTCUSDT, epic_075 t944/t946); az **ethusdt** sync-aktív, de quant modell nélkül (data-only + Impulse Scout; t945 cancelled). Az asset-ek
független DB-vel, modellpárral és stratégiával futnak; az UI `/a/:asset` fülekkel váltogat köztük, a fülsorrend
`config/assets.json` `display_order`: **BTC, BCH, SOL, ETH** (a default asset ettől függetlenül `solusdt`). The core loop:

1. Sync 1-minute OHLCV candles from Binance into a local DuckDB store.
2. Compute quantitative features (momentum, volume, volatility) over that data.
3. Train LightGBM regressors to predict the next 60-bar forward MFE (Maximum
   Favorable Excursion) — a continuous logreturn outcome — for long and short direction.
4. The strategy module calibrates entry/exit rules against model output,
   producing a strategy artifact with isotonic-calibrated scores and Optuna-optimised thresholds.
5. Egy **FastAPI BFF** (`src/api`, :8600) szolgálja ki a **React UI**-t (`frontend/`) és a REST/SSE API-t;
   a dashboard az élő predikciókat, stratégia-szabályokat, tradeket, equity-t, riportokat mutat.
6. Egy különálló **worker** folyamat (`src/worker`) futtatja a hosszú jobokat (OHLCV/funding/Scout/predikció sync, TV hangulat-frissítés),
   így az API nem blokkol és a DuckDB írója egyetlen processz.
7. A UI alján/önálló ablakban **Claude chat** fut (helyi `claude` CLI, előfizetés), ami a `chronoquant` MCP szerveren
   (`src/cq_mcp`, `/mcp/chronoquant`) át, szűk, többnyire csak-olvasó jogosultsággal éri el az adatokat.

A quant looptól függetlenül fut az **Impulse Scout** (`src/scout`, befagyasztott `artifacts/scout_s0c_r1/`; epic_073/074):
mélypont-esemény detektálás + impulzus-valószínűség SOL/BTC/ETH-n, forward-log a live `scout_signals` táblába. **Kísérleti,
NO-GO** — a UI-n „nem kereskedési jel”, a quant modellbe nem kerül be. → `_doc_/database_and_code_doc/5800_scout_runtime.md`

Az éles kereskedés order-logikája (live service, exchange kliens, state machine) **törölve** (epic_068); a UI-ban a trade
journal (`src/trading/live/journal.py`) olvasása és a Binance fill-ek megjelenítése maradt. A korábbi Streamlit UI, a `research/`
sandbox, az `analyst/` + Quarto láncot és a `docs/` mappa megszűnt (a kutatási tartalom: `_doc_/research/*.md`; a git history: commit `73f2156`).
Terv/kontraktus: `_doc_/_plans_/068_bff_ui.md`.

---

## Module Architecture

Termelési modulok, egyértelmű felelősségekkel:

### `src/data_handling/` — Operational data layer
Owns all live data: ingestion, storage, sync, and validation.
- Syncs raw OHLCV from Binance
- Computes and syncs features, targets, predictions into DuckDB
- `07_query_readonly.py`: csak-olvasó SQL CLI (live/lab/registry)
- No modeling logic; no strategy logic

### `src/modeling/` — Offline ML development
Produces model artifacts from historical data. Runs ad-hoc, not in production.
- Snapshot-natív pipeline: `snap."<snapshot_id>"` a forrás; kimenetek DuckDB táblák (`model.__sample`, `model.__pred`) + fájl-artefaktok
- Egyetlen támogatott sampling mód: `train_valid_split`
- Outputs: `model.pkl`, `features.json`, `params.json`, `feature_set.json`, `model."<id>__pred"`; registry provenance minden lépésnél
- A pipeline `feature_engineering` és `analyze` lépései a `src/reports` buildereket hívják (nem notebook)

### `src/strategy/` — Offline strategy calibration
Consumes model artifacts to calibrate trading rules. Runs ad-hoc, not in production.
- Strategy tábla: `snap ⋈ model_long.__pred ⋈ model_short.__pred` JOIN
- Rank percentile lookup + isotonic regression; Optuna/grid search entry/exit
- Outputs: `strat."<session>__trades/__equity/__cutoffs"` (lab.duckdb) + `strategy_artifact.json`, `rank_lookup_*.parquet`, `isotonic_*.pkl`

### `src/scout/` — Impulse Scout runtime (kísérleti, NO-GO)
Befagyasztott artifact (`artifacts/scout_s0c_r1/`: `manifest.json`, `model.pkl`, `train_oof.parquet`; hash-ellenőrzött `load_artifact()`)
+ `run(asset_id, ohlcv, art)` (detektálás → score → kimenet-feloldás). Nincs DB I/O: a `sync_tables/sync_scout.py` írja a `scout_signals`-t,
a worker hívja. Scout assetek: SOL, BTC, ETH (BCH nem). → `5800_scout_runtime.md`

### `src/trading/` — Trade journal (csak olvasás a UI-nak)
Az order-logika törölve. Marad: `src/trading/live/journal.py` (DuckDB trade journal: runs/signals/positions/orders/errors),
amit az API `/api/trades/journal/*` olvas.

### `src/api/` — FastAPI BFF
REST + SSE (`/api/*`, köztük `/api/scout/signals` + `/api/scout/latest`), `/mcp/chronoquant` mount, `frontend/dist` statikus kiszolgálás. Csak olvas (DuckDB RO); írás = worker job.
Chat: `src/api/chat/` (claude CLI stream-json runner + persona). → `_doc_/database_and_code_doc/8300_api.md`

### `src/worker/` — Job worker
Külön processz (`python -m worker`), SQLite WAL job-sor (`database/_jobs/jobs.sqlite`), auto-sync ütemező. Job kindek: `sync_full`, `sync_predictions`, `sentiment_refresh`, `sync_scout`. A `sync_full` lánca: OHLCV → funding (csak esedékeskor, nem fatális) → Scout (throttle 300 s, nem fatális) → features → predictions. → `8310_worker.md`

### `src/cq_mcp/` — chronoquant MCP szerver
20 tool (18 olvasó, köztük `scout_signals` + író `enqueue_sync`, `sentiment_refresh`), FastMCP streamable HTTP az API-ban, stdio-n is futtatható. → `8320_cq_mcp.md`

### `src/reports/` — Riport formátum és builderek
`ReportBuilder` (manifest.json + report.md + charts/*.json (Plotly) + tables/*.json), tároló/olvasó, pipeline-builderek. → `8330_reports.md`

### `src/sentiment/` — TradingView hangulat
`tv_report/` (fetch, summary, author_backtest CLI); kimenet: `data/tv_report/` (gitignore). A worker `sentiment_refresh` jobja és az MCP `sentiment_*` tooljai használják. → `8340_sentiment.md`

### `frontend/` — React UI
Vite + React 18 + TypeScript + Tailwind; oldalak: Dashboard, Reports (Elemzések), Chat (+ ChatDock popout). → `8350_frontend.md`

### `scripts/` — PowerShell indítók
`dev.ps1`, `start.ps1`, `stop.ps1`, `install_autostart.ps1`, `_common.ps1`. → `8360_scripts.md`

---

## Data Flow

A rendszer DuckDB-natív, 3-fájlos architektúrán alapul (epic_031 után).
Részletes leírás: → `_doc_/database_and_code_doc/0002_data_architecture.md` (topológia),
`_doc_/database_and_code_doc/0003_runtime_flow.md` (éles folyamat), `_doc_/database_and_code_doc/0004_model_lifecycle.md` (modell életciklus).

```
Binance API
    │
    ▼
live.duckdb (main séma)
  sync ──▶  ohlcv ──▶ feat_ohlcv_quant ──▶ target ──▶ quant_train
  sync_predictions ──▶ predictions (long/short stamp)
  sync_funding ──▶ funding_rate          sync_scout (scout.run) ──▶ scout_signals (SOL/BTC/ETH)
  02_sync_pipeline --repair-gaps ──▶ feat_ohlcv_quant + predictions belső lyukainak pótlása

lab.duckdb (snap / model / strat sémák)
  05_create_snapshot   ◀── quant_train (ATTACH RO)  ──▶  snap.<snapshot_id> (immutable CTAS)
  create_sample        ◀── snap.<snapshot_id>        ──▶  model.<id>__sample (hourly+fold)
  pipeline FE          ◀── model.__sample             ──▶  reg.feature_sets (logikai szures)
  pipeline search      ◀── model.__train_input VIEW   ──▶  reg.search_runs + search artifacts
  pipeline train       ◀── model.__train_input VIEW   ──▶  model.pkl + artifacts/
  predict_offline      ◀── snap.<snapshot_id>         ──▶  model.<id>__pred (teljes range)
  00_run_strategy_session ◀── snap x model_long.__pred x model_short.__pred JOIN
                        ──▶  strat.<session>__trades/__equity/__cutoffs/__summary/__grid_results (lab.duckdb)
                        ──▶  strategy_artifact.json + rank_lookup*.parquet (artifacts/)

registry.duckdb (reg séma)
  minden lepesnel: reg.snapshots / models / feature_sets / search_runs / strategies / deployments / artifacts

deploy/cutover:
  06_trigger_deploy.py  ──▶  reg.deployments (pending)
  sync_predictions      ──▶  predictions (backfill+swap, atomikus tranzakcio)

api/ (read-only)  ◀──  live.duckdb + lab.duckdb + registry.duckdb (RO) + trade journal + Binance (fills, cache)
                  ◀──  artifacts/<model_id>/reports/ és _doc_/reports/ (report manifest + json)
worker            ──▶  live.duckdb (sync_full / sync_predictions)   ◀── API enqueue (SQLite jobs.sqlite) ◀── UI / MCP
frontend (React)  ◀──  /api/* (REST + SSE)         chat ──▶ claude CLI ──▶ /mcp/chronoquant (cq_mcp)
```

---

## Persistence Rules

| Data type | Where | Reason |
|-----------|-------|--------|
| Live OHLCV, features, targets, predictions | `live.duckdb` main séma | Synced, queryable, updatable |
| `quant_train` | `live.duckdb` main séma | Ad-hoc join table, rebuilt before training |
| Range snapshots | `lab.duckdb` snap séma (`snap."<snapshot_id>"`) | Immutable CTAS + `content_sha256` (rács/sorszám/oszlopok, az id része) + additív `values_sha256` (érték-hash, registry v3); reprodukálható modell-alap |
| Hourly samples + fold_id | `lab.duckdb` model séma (`model."<id>__sample"`) | Kicsi (~tízezer sor); sokszor olvasható |
| Offline predictions (teljes range) | `lab.duckdb` model séma (`model."<id>__pred"`) | Snapshot-tól elkülönítve; snap hash sértetlen marad |
| Strategy tables (trades/equity/cutoffs) | `lab.duckdb` strat séma (`strat."<session>__*"`) | UI és validáció közvetlen query-vel éri el |
| Registry (provenance lánc) | `registry.duckdb` reg séma | Normalizált igazságforrás; assets → snapshots → models → strategies → deployments |
| Modell-riportok (sampling, FE, hyperparam search, stratégia) | `artifacts/<model_id>/reports/<slug>/` (`manifest.json`, `report.md`, `charts/*.json`, `tables/*.json`) | `src/reports` builderek írják (pipeline lépések), a UI és az MCP olvassa |
| Általános riportok (nem modellhez kötött) | `_doc_/reports/<slug>/` (ugyanaz a formátum, `scope: general`) | analyst_agent / builder output |
| Feature set | `artifacts/<model_id>/feature_engineering/feature_set.json` | FE lépés outputja → reg.feature_sets |
| Worker job-tár | `database/_jobs/jobs.sqlite` (WAL) + `logs/jobs/<id>.log` | Multi-process safe; nem üzleti adat |
| TV hangulat riport | `data/tv_report/` (gitignore) | `src/sentiment/tv_report` outputja |
| Model artifacts | `artifacts/<model_id>/` (`manifest.json`, `model.pkl`, `features.json`, `params.json`, `search/`) | Runtime use by prediction sync |
| Impulse Scout artifact | `artifacts/scout_s0c_r1/` (`manifest.json`, `model.pkl`, `train_oof.parquet`) | Befagyasztott, hash-ellenőrzött; a runtime (`src/scout`) olvassa; nincs reg bejegyzés |
| Scout forward-log | `live.duckdb` `scout_signals` (SOL/BTC/ETH) | `sync_scout` egyetlen író; sor nem törlődik |
| Strategy artifacts | `artifacts/<model_id>/strategy/` — `strategy_artifact.json`, `rank_lookup_long/short.parquet`, `isotonic_long/short.pkl`, `grid_results.csv` | Runtime use by live trading loop; útvonalak a reg.artifacts-ban. Külön `strat_*` top-level mappa nem létezik — strategy a modell almappájában él. |

**Rule:** strukturált, queryolható adat → DuckDB (séma + tábla). Blob és ember-report → fájl.
Mindent a registry köt össze. Részletek: → `_doc_/database_and_code_doc/0002_data_architecture.md`

---

## Repository Layout

```
src/
  data_handling/    Operational data layer
    store/            DuckDB store, queries, validation, stats, registry
    sync_tables/      OHLCV, funding, feature, prediction, target, scout sync; repair_gaps.py (belső lyukak pótlása, epic_076); _levels_polars.py = S/R + trendvonal detektor (tiszta könyvtár, epic_071; config: features.json levels_detector)
    collectors/       Külső adatforrás kliensek (tradingview)
    tests/            Tests (store/, sync_tables/ — smoke, sanity, perf, integration)
    01_validate_stats.py ... 07_query_readonly.py  (CLI-k; 07 = csak-olvasó SQL, chat/MCP is használja)

  modeling/         Offline ML development
    sampling/           train_valid_split sampler (snapshot_sampler, create_sample)
    training/           LightGBM trainer, CV, artifacts
    search/             Hyperparameter search (LightGBM + Optuna)
    pipeline.py         Unified pipeline orchestrator (setup|sample|feature_engineering|search|train|predict|analyze)
    predict.py, provenance.py
    00_create_sample.py, 02_hyper_param_search.py, 03_fit_model.py

  strategy/         Offline strategy calibration (ad-hoc)
    strategy/           build_table, calibrate, search, artifacts library
    00_run_strategy_session.py

  scout/            Impulse Scout runtime: runtime.py (load_artifact, run), freeze.py (artifact fagyasztás), tests/
  trading/live/journal.py   Trade journal (DuckDB) — az order-logika törölve

  api/              FastAPI BFF: main.py (app factory), routers/, services/, schemas.py, deps.py, errors.py, chat/ (runner + persona_prompt.md), tests/
  worker/           python -m worker: queue.py (SQLite), runner.py, scheduler.py, logs.py, handlers/, tests/
  cq_mcp/           chronoquant MCP szerver: server.py + tools_{data,models,jobs,reports,sentiment,scout}.py, tests/
  reports/          Riport formátum: schema.py, builder.py, store.py, theme.py, builders/ (sampling, feature_engineering, hyperparameter_search, strategy, target_regime, sr_detection), tests/
  sentiment/        tv_report/ (fetch.py, summary.py, author_backtest.py, paths.py)
  utils.py          All config loading — single entry point, never read JSON directly

frontend/           React UI (Vite, TS, Tailwind): src/{pages,components,api,lib,types}, dist/ (build output, gitignore)
scripts/            dev.ps1, start.ps1, stop.ps1, install_autostart.ps1, _common.ps1
_doc_/              Dokumentáció — zónák + globális gyökér
  0000_*, 0001_*      Globális (project overview, agentic rendszer)
  database_and_code_doc/   ZÓNA 1 — DB séma + kód-referencia (.md) — code_doc_agent
  methodology_doc/         ZÓNA 2 — rationale, döntések, módszertan (.md, kód-mentes) — methodology_agent
  models_doc/              ZÓNA 3 — modell-elemzések (.md) — analyst_agent
  research/                Lezárt kutatások/kísérletek md összefoglalói (a kód a git historyban: 73f2156) — analyst_agent
  reports/                 Általános riportok (report formátum, `scope: general`) — analyst_agent
  _plans_/                 Tervek (068_bff_ui.md = az implementált BFF kontraktusa)
_jira_/             Local task tracking (epics → tasks → stories); jira.json = epic counter
.agent/             Agent rules, skills, tool docs
config/             JSON config files (assets, features, models, strategies, trading, env…)
data/               Lokális, gitignore-os adatok (pl. data/tv_report/)
logs/               api/worker logok, logs/jobs/<id>.log
artifacts/          Model and strategy artifacts
  <model_id>/
    manifest.json                   Pipeline state + model summary
    feature_engineering/feature_set.json; features.json / model.pkl / params.json
    search/                         Hyperparameter search results
    reports/<slug>/                 Report-formátum riportok (manifest.json, report.md, charts/, tables/)
    strategy/                       Strategy artifacts
      strategy_artifact.json        Grid search best setup + decision_params + strat table refs
      rank_lookup_long/short.parquet  Rank percentile lookup
      isotonic_long/short.pkl       Fitted sklearn IsotonicRegression
      grid_results.csv              Grid search results
  scout_s0c_r1/                     Impulse Scout befagyasztott artifact (manifest.json, model.pkl, train_oof.parquet)
database/           DuckDB files (3-fájlos topológia — részletek: _doc_/database_and_code_doc/0002_data_architecture.md)
  solusdt/
    solusdt.duckdb         <- LIVE (main séma)
    solusdt_lab.duckdb     <- LAB (snap / model / strat sémák)
  bchusdt/
    bchusdt.duckdb         <- LIVE (azonos séma)
    bchusdt_lab.duckdb     <- LAB
  btcusdt/, ethusdt/       <- azonos felépítés
  _registry/
    registry.duckdb        <- REGISTRY (reg séma, globális)
  _jobs/jobs.sqlite        <- worker job-tár (SQLite WAL)
```

---

## Database (DuckDB)

**3-fájlos topológia (epic_031 után):**

| Fájl | Séma | Tartalom |
|------|------|---------|
| `database/<asset>/<asset>.duckdb` (pl. `database/solusdt/solusdt.duckdb`) | `main` | LIVE (asset-onként): ohlcv, feat_ohlcv_quant, target, quant_train, predictions (+ funding_rate; scout_signals csak Scout asseten) |
| `database/<asset>/<asset>_lab.duckdb` | `snap`, `model`, `strat`, `research` | LAB (asset-onként): immutable snapshots, sample/pred táblák, strategy táblák; `research` = verziózott kutatási adatkészletek (pl. `research."wave_events_v1"`, `"wave_events_v2"` (epic_072), `"scout_events_v1"` + `"scout_flow_v1"`, Impulse Scout benchmark: `"scout_leaderboard_v1"`, `"scout_oof_v1"`, `"scout_universe_v1"` (epic_073, SOL+BTC+ETH)) |
| `database/_registry/registry.duckdb` | `reg` (default main) | REGISTRY: assets, snapshots, feature_sets, models, search_runs, strategies, deployments, artifacts; migrációk `_schema_migrations` (v3 = `snapshots.values_sha256`, epic_076 / t949) → `1510_registry_code.md` |

Részletes leírás: → `_doc_/database_and_code_doc/0002_data_architecture.md`

Négy sync-aktív asset (`config/assets.json`, `default_asset_id = solusdt`), ebből három élő quant modellpárral (SOL, BCH, BTC; BCH/BTC stratégiája NET negatív → csak jelzés), ETH data-only; Impulse Scout: SOL, BTC, ETH:

| asset_id | Symbol | Interval / market | features_profile | Megjegyzés |
|----------|--------|-------------------|------------------|-----------|
| `solusdt` | SOLUSDT | 1m, futures | `solusdt_fw60` | default; élő sync + predikció |
| `bchusdt` | BCHUSDT | 1m, futures | `bchusdt_fw60` | epic_069; élő sync + predikció; története 2019-12-19-től; `qty_step` 0.001 |
| `btcusdt` | BTCUSDT | 1m, futures | `btcusdt_fw60` | epic_074/t939 (sync 2021-01-01-től) + epic_075: élő sync + predikció (t946, 2026-11-23: deploy cutover + worker gap-fill, folytonos 2021-01-01-től); stratégia NET negatív → csak jelzés-megjelenítés; `qty_step` 0.001 |
| `ethusdt` | ETHUSDT | 1m, futures | `ethusdt_fw60` | **sync-aktív, data-only** (epic_074/t939): OHLCV + features + funding, nincs quant modell (a UI a predikció/stratégia paneleket elrejti), Impulse Scout fut; `qty_step` 0.001 |

Új asset felvétele: `_doc_/database_and_code_doc/0004_model_lifecycle.md` "Új asset onboarding" szekció.
Az auto-sync szabálya: konfigurált asset ÉS létező live DB ÉS `config/trading.json` `assets.<id>` blokk (üres blokk `{}` = sync-aktív, session id-k `None` → az API/UI „nincs aktív session”-ként kezeli; a predikciós lépés modell nélkül warning + no-op).
**Funding:** a `funding_rate` táblát a `sync_funding` tölti (CLI `--tables funding`, worker `sync_full` esedékességkor; epic_074 / t939). Élő sync: SOL, BCH, BTC, ETH (ETH quant modell nélkül, data-only).
**`target` nem frissül a workerben** (egyik assetnél sem; a worker lánc OHLCV → funding → scout → features → predictions): tanítás / elemzés előtt kézzel kell újraépíteni: `uv run python src/data_handling/02_sync_pipeline.py --asset-id <a> --tables targets`.
**Lyukjavítás (epic_076 / t947):** a `sync_features` catch-up módban a feature-tailtől folytat (félbeszakadt job nem hagy lyukat); meglévő belső lyukak: `uv run python src/data_handling/02_sync_pipeline.py --repair-gaps --asset-id <a> [--dry-run]` (feature → predikció backfill). → `1200_sync_tables.md`

### Live táblák (main séma)

| Table | Primary Key | Purpose |
|-------|-------------|---------|
| `ohlcv` | `open_time` TIMESTAMP | Raw 1-minute candles from Binance |
| `target` | `open_time` TIMESTAMP | fw60 forward outcomes: `long_mfe_fw60`, `short_mfe_fw60` + 8 further columns |
| `feat_ohlcv_quant` | `open_time` TIMESTAMP | Quantitative features (`feat_` prefix) |
| `predictions` | `open_time` TIMESTAMP | Model scores + signals + `long_model_id` / `short_model_id` stamp |
| `quant_train` | `open_time` TIMESTAMP | Ad-hoc join: all `feat_*` + `long_mfe_fw60` + `short_mfe_fw60`; NULL targets excluded |
| `feat_levels` | `open_time` TIMESTAMP | S/R + trendvonal feature-ök (50 `feat_sr_*` / `feat_tl_*`, s/m/l lépték, t-1 lag; epic_071, migráció v6); sync: `sync_tables/sync_levels.py`, pipeline lépés `levels` |
| `funding_rate` | `funding_ts` | Binance funding elszámolások (`sync_funding`, epic_074 / t939) |
| `scout_signals` | `event_id` | Impulse Scout forward-log (migráció v8, epic_074 / t941): esemény, predikció, kereskedési tervek, kimenet; csak SOL/BTC/ETH; író: `sync_scout.py` → `1000_database.md#scout_signals` |
| `quant_train__<profile>` | `open_time` TIMESTAMP | Challenger profil quant_train (pl. `quant_train__solusdt_fw60_sr` = champion join + `feat_levels`); `--features-profile` a `03_build_quant_train.py` / `05_create_snapshot.py`-ben |

`quant_train` is rebuilt ad-hoc before training via `src/data_handling/03_build_quant_train.py`.
Full rebuild = `CREATE OR REPLACE TABLE`; range rebuild = DELETE + INSERT.

All timestamps are **UTC, format `YYYY-MM-DD HH:MM:SS`** (naive strings treated as UTC).
Epoch milliseconds used internally as `open_time_ms`.

All sync operations are **idempotent upserts keyed on `open_time`** — safe to re-run.

Config is always accessed via `src/utils.py` — never read JSON config files directly.

---

## Modeling Pipeline

### Model naming convention

```
lgbm_{asset}_{direction}_fw{horizon}_{year}               ← éves modell
lgbm_{asset}_{direction}_fw{horizon}_{start}_{end}        ← multi-year champion
```
pl. `lgbm_solusdt_l_fw60_2021`, `lgbm_solusdt_s_fw60_2023`
pl. `lgbm_solusdt_l_fw60_2101_2607` (train 2021-01-től, valid 2026-07-ig)

**Modellek:** a `config/models.json` (schema v4) az igazság (jelenleg 11 modell, mind `train_valid_split`; asset-onként egy aktuális champion pár):
- SOL: `lgbm_solusdt_l/s_fw60_2101_2607`, artifact: `artifacts/lgbm_solusdt_{l,s}_fw60_2101_2607/`
- BCH: `lgbm_bchusdt_l/s_fw60_2101_2607` (`asset_id: bchusdt`), artifact: `artifacts/lgbm_bchusdt_{l,s}_fw60_2101_2607/`
- BTC: `lgbm_btcusdt_l/s_fw60_2101_2607` (`asset_id: btcusdt`, `active: true`, élő deploy t946: `trading.json` `lgbm_btcusdt_{l,s}_fw60_2101_2607/strategy`), artifact: `artifacts/lgbm_btcusdt_{l,s}_fw60_2101_2607/` (epic_075 t944)

**BCH eredmények (epic_069):** a valid top10 lift pozitív mindkét irányban (long ~1,93x, short ~2,18x az átlaghoz képest, a train-nel konzisztens, nincs jelentős túlillesztés). A stratégia viszont a költségekkel (maker 2 / taker 5 / slip 1,5 bps + funding) NET negatív mind a 200+200 gridpontban (kalibrált long -0,21, short -0,11 net log return, a gross kicsit pozitív). Ezért a BCH-n élő kereskedés nem javasolt, a fül jelzés-vizualizációra szolgál; részletek a modell-doksiban (`_doc_/models_doc/`, készül, epic_069 t908).

**BTC eredmények (epic_075 t944):** valid top10 lift long 2,11x / short 2,02x (SOL 2,02 / 2,02, BCH 1,98 / 2,08), Pearson 0,39 / 0,38; a train-valid gap kicsi. A stratégia költségekkel szintén NET negatív mind a 200+200 gridpontban (long -0,054, short -0,174 net log return; a long gross +0,144), kevésbé, mint a BCH. Élő kereskedés nem javasolt, jelzés-vizualizációra szolgál.

- **Target semantics:** `fw60` = 60-bar forward window; `long_mfe_fw60` = log(max upside / close[t]); `short_mfe_fw60` = log(min downside / close[t]). Folytonos regressziós target — nincs percentilis küszöb, nincs binarizálás.
- **Feature prefix:** `feat_` | **t-1 lag mandatory** on all features (prevents data leakage).
- **Feature engineering target:** only the model's own direction target is used (`l` → `long_mfe_fw60`, `s` → `short_mfe_fw60`).
- **Artifacts:** `artifacts/<model_id>/` — `manifest.json`, `model.pkl`, `features.json`, `feature_engineering/feature_set.json`, `params.json`, `search/`, `reports/`, `strategy/`. A sample a lab.duckdb-ben él (`model."<id>__sample"`), nem parquet.
- **Samples:** model-specifikusak, a lab.duckdb `model` sémájában élnek (`model."<id>__sample"`); a predikció `model."<id>__pred"`.

### Pipeline (runs offline, in order)

```bash
# Snapshot létrehozása (egyszer, vagy újrahasználat a registry-ből):
uv run python src/data_handling/05_create_snapshot.py --asset-id solusdt --start 2021-01-01 --end 2025-12-31

# Teljes pipeline egy modellre:
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2101_2607

# Más asset: --asset-id <asset> (05_create_snapshot, 06_trigger_deploy, 00_run_strategy_session, 02_sync_pipeline)

# Egyes lépések:
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2101_2607 --step setup
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2101_2607 --step sample
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2101_2607 --step feature_engineering
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2101_2607 --step search --stage smoke
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2101_2607 --step train
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2101_2607 --step predict
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2101_2607 --step analyze

# Deploy trigger (dual-session — long és short külön):
uv run python src/data_handling/06_trigger_deploy.py --strategy-session-id strat_solusdt_fw60_long_2101_2607
uv run python src/data_handling/06_trigger_deploy.py --strategy-session-id strat_solusdt_fw60_short_2101_2607
```

| Lépés | Input | Output |
|-------|-------|--------|
| `setup` | `config/models.json` | `manifest.json` (artifact); `reg.models` draft |
| `sample` | `snap."<snapshot_id>"` (lab.duckdb) | `model."<id>__sample"` (lab.duckdb); `reg.feature_sets`, `reg.models` sampled |
| `feature_engineering` | `model.__sample` | `feature_set.json` (artifact) + riport `artifacts/<id>/reports/feature_engineering/` (`reports.builders.feature_engineering`); `reg.feature_sets` link |
| `search` | `model."<id>__train_input"` VIEW | `search/search_best.json`, `search_trials.jsonl` (artifact); `reg.search_runs` |
| `train` | `model."<id>__train_input"` VIEW | `model.pkl`, `features.json`, `params.json` (artifact); `reg.models` trained |
| `predict` | `snap."<snapshot_id>"` + `model.pkl` | `model."<id>__pred"` (lab.duckdb); `reg.models` predicted |
| `analyze` | artifactok + lab/live táblák | report-formátum riportok `artifacts/<id>/reports/` (sampling, hyperparameter_search, strategy) |

### Sampling (train_valid_split — az egyetlen támogatott mód)

A korábbi `yearly_random_hour` és `walk_forward` módok kikerültek a kódból (`create_sample` ValueError-t dob más módra).
Fix dátumhatárok a `config/models.json` → `sampling`-ból (`train_start/end`, `valid_start/end`); `split` TINYINT: 0=train, 1=valid.
Feature lookback embargo: 240 perc a train elején; target purge: utolsó 60 perc a train végéről; valid oldalon nincs embargo.
Seed=42, óránkénti véletlenszerű perc mintavétel. A valid periódus szándékosan egybeesik a strategy calibrációs ablakkal.
Módszertan: `_doc_/methodology_doc/`.

**Search objective:** valid top10_lift maximalizálása (`top10_lift = mean(y_true | score ∈ top 10%) − mean(y_true | all valid)`),
Optuna patience stopping (patience=20, epsilon=0.001, max 100 trial). A train top10_lift diagnosztika. Mentve: `search_best.json`, `search_trials.jsonl`.

### OOS evaluation

A `src/strategy/` modul a snapshot adatot a tanított modell `__pred` tábláján át közvetlenül pontozza —
nincs előre számolt OOS parquet.

---

## Trading Strategy

Strategy calibration is performed offline by `src/strategy/`, then the artifact drives the live service.

**Offline calibration (`src/strategy/`):**
- `00_run_strategy_session.py` — full session CLI: scored join → calibrate → grid search
  - `build_table.build_scored_table` — snap ⋈ model_long.__pred ⋈ model_short.__pred ⋈ live.ohlcv JOIN
  - `calibrate.fit_calibration` — fits rank percentile lookup (primary) + isotonic regression (secondary); new: bucket median + p75 stats
  - `search.search_strategy` — deterministic grid search over (entry_cutoff × tp_spec × sl_spec); objective: max total realized `fact_log_return`
- Produces: `strategy_artifact.json` + `rank_lookup_long/short.parquet` + `isotonic_long/short.pkl` + `sweep_results_grid.csv` in `artifacts/{session_id}/`
- **Signal mode:** `rank_first` — entry based on score percentile in calibration distribution
- **Evaluation mode:** `same_window` — calibration and search use the same session window
- **Search space:** entry_cutoff ∈ [0.90..0.99] × tp_spec (bucket_mean/median/p75/0.75×mean/0.50×mean) × sl_spec (none/0.5×TP/1.0×TP/1.5×TP/2.0×TP) = 200 setups per direction
- **TP/SL execution:** intrabar high/low touch; long TP `high ≥ entry*exp(tp_lr)`, SL `low ≤ entry*exp(-sl_lr)`; same-bar conflict → SL wins (conservative); timeout = 60 min at close; re-entry next bar after exit
- **Short ranking inversion:** `short_mfe_fw60 = log(fw_min/close) < 0` (profitable short = negatív érték) → alacsony `score_pct_short` = legjobb short; entry feltétel: `(1 - score_pct_short) >= entry_cutoff`
- Módszertani részletek: → `_doc_/methodology_doc/6300_strategy_grid_search.md`

**`strategy_artifact.json` contract (epic_035 után):**
- `signal_mode: "rank_first"`, `evaluation_mode: "same_window"`, `fit_period`
- `decision_params`: `entry_cutoff`, `tp_spec`, `sl_spec`, `directions`, `max_hold_minutes: 60`, `same_bar_conflict_rule: "sl_first"`
- `search_info`: `search_type: "grid"`, `n_setups_evaluated`, `best_objective: "total_fact_log_return"`, `best_value`

**strat.* DuckDB táblák (epic_035 után):**
- `strat."<session>__trades"` — trade ledger: entry/exit_time/price, fact_log_return, tp_lr, sl_lr, exit_reason, tp_spec, sl_spec
- `strat."<session>__equity"` — cumulative_fact_log_return equity curve
- `strat."<session>__cutoffs"` — per-direction decile bucket cutoffs
- `strat."<session>__summary"` — 1-soros aggregált összesítő (n_trades, total/avg fact_log_return, compounded_return_pct, win_rate)
- `strat."<session>__grid_results"` — az összes keresési setup eredménye

**Éles végrehajtás:** az order-logika (live service, exchange kliens, state machine) epic_068-ban törölve. A `strategy_artifact.json` és
`config/trading.json` továbbra is a stratégia kontraktusa, de nincs futó végrehajtó. A `trading.json` **asset-blokkokat** tartalmaz: `assets.<asset_id>.strategy_session_{long,short}_id` (a lapos kulcsok megszűntek; olvasás: `utils.strategy_session_ids(asset)`, `utils.trading_asset_ids()`, `utils.model_asset_id(model_id)`);
a UI az aktív stratégiát (`/api/strat/active`), a predikciókat/jeleket és a trade journalt/Binance fill-eket jeleníti meg.

---

## Testing Rules

| What to run | When |
|-------------|------|
| `uv run pyright src/<module>/` | After any type-annotated change |
| `ruff check src/<module>/ --fix` | Before committing any Python file |
| `uv run pytest src/data_handling/tests/ -v` | Store or pipeline changes |
| `uv run pytest src/modeling/ -v` | Modeling changes |
| `uv run pytest src/strategy/tests/ -v` | Strategy calibration changes |
| `uv run pytest src/api src/worker src/cq_mcp src/sentiment src/reports` | BFF / worker / MCP / sentiment / report változás |
| `cd frontend && npx tsc --noEmit && npm run build` | Frontend változás (típusellenőrzés + build) |
| `scripts\dev.ps1` | Fejlesztői mód: `uvicorn --reload` (:8600) + worker + Vite (:5173), megnyitja a böngészőt |
| `scripts\start.ps1` (`-NoBrowser`, `-Build`) / `scripts\stop.ps1` | Éles mód: rejtett API (:8600, a `frontend/dist`-et is szolgálja) + worker; logok: `logs/api.*.log`, `logs/worker.*.log` |
| `scripts\install_autostart.ps1` (`-Uninstall`) | Windows-belépéskor induló Task (`ChronoQuantAutostart`; a régi `ChronoQuantUiAutostart`-ot törli) |

Always run pyright and ruff for the affected module. Never skip for non-trivial changes.

---

## Key Conventions

- **Config gateway:** all config through `src/utils.py` — no raw JSON reads in `src/`
- **Assets:** `solusdt` (default), `bchusdt` és `btcusdt` élő modellpárral, `ethusdt` sync-aktív quant modell nélkül; UI fülsorrend `config/assets.json` `display_order` (BTC, BCH, SOL, ETH); az asset-kontextus mindenhol explicit (`--asset-id`, API `?asset=`, MCP `asset`, worker `params.asset_id`), nincs beégetett asset (az egyetlen fallback: `utils.FALLBACK_ASSET_ID`)
- **Polars for features:** feature computation uses Polars; pandas allowed elsewhere
- **No print() in library code** — use `logging`
- **Upserts only** — no delete/truncate patterns in sync operations
- **DuckDB-natív:** strukturált adat → DuckDB (séma+tábla); blob/report → fájl; registry köti össze
- **3-fájlos topológia:** live.duckdb (sync) / lab.duckdb (modellezés) / registry.duckdb (katalógus); ATTACH-cal egyetlen connectionból joinolható
- **Snapshot = immutable:** a `snap."<snapshot_id>"` tábla tartalma soha nem íródik felül; új adatállapot = új snapshot_id
- **API csak olvas:** az `src/api` DuckDB-t csak read-only nyit; minden írás worker job (`sync_full`, `sync_predictions`, `sentiment_refresh`, `sync_scout`)
- **/api prefix:** minden REST/SSE végpont `/api/*` alatt; MCP: `/mcp/chronoquant`; a SPA fallback nem nyel el `/api/*` 404-et
- **Riport = fájl-formátum, nem notebook:** manifest.json + report.md + Plotly-JSON charts + JSON tables (`src/reports`); nincs Quarto/papermill
- **Chat jogosultság:** Read, WebSearch, WebFetch + `chronoquant` MCP; Edit/Write/Bash tiltott
- **Research függőségek:** `torch` (CPU) + `tslearn` az opcionális `research` dependency-csoportban (`uv run --group research ...`); az alap `uv sync` nem telepíti → `_doc_/database_and_code_doc/0005_tooling_dependencies.md`
- **Kutatás = md:** lezárt kísérletek `_doc_/research/*.md` (a kód a git historyban), nem futtatható sandbox

---

## Agent Ownership

| Agent | Owns |
|-------|------|
| Database Agent | `src/data_handling/`, `config/assets.json`, DuckDB schema |
| Modeling Agent | `src/modeling/`, feature computation, model artifacts; `src/strategy/`; `src/scout/` + `artifacts/scout_s0c_r1/` |
| UI Agent | `src/api/`, `src/worker/`, `src/cq_mcp/`, `src/sentiment/`, `frontend/`, `src/trading/` (journal), `scripts/` |
| Code Doc Agent | `.agent/`, tooling, infra, dependencies; `_doc_/database_and_code_doc/` (DB séma + kód-referencia zóna) |
| Analyst Agent | `src/reports/` builderek (pipeline-lépések riportjai), `_doc_/research/`, `_doc_/models_doc/`, `_doc_/reports/` — user-célból indul, interaktív session |
| Methodology Agent | `_doc_/methodology_doc/` — business rationale, methodological decisions, parameter justification (kód-mentes) |
| Validator Agent | `pr_` ticket validation: ruff + pyright + pytest, then `done_` promotion |
