# ChronoQuant — Project Overview

Single source of truth for the orchestrator. Detailed module documentation lives
in three zone subdirectories under `_doc_/` (database_and_code_doc / methodology_doc / models_doc); this file is for cross-domain context only.

**Adat-architektúra referenciák (epic_031 DuckDB-natív migráció után):**
- `_doc_/database_and_code_doc/0002_data_architecture.md` — 3-fájlos tárolási topológia, sémák, ATTACH
- `_doc_/database_and_code_doc/0003_runtime_flow.md` — éles folyamat: sync → live predict → trade → deploy
- `_doc_/database_and_code_doc/0004_model_lifecycle.md` — modell életciklus: snapshot → sample → FE → search → train → predict → deploy/cutover

---

## Business Goal

ChronoQuant is an algorithmic crypto trading system targeting **SOLUSDT perpetual
futures on Binance**. The core loop:

1. Sync 1-minute OHLCV candles from Binance into a local DuckDB store.
2. Compute quantitative features (momentum, volume, volatility) over that data.
3. Train LightGBM regressors to predict the next 60-bar forward MFE (Maximum
   Favorable Excursion) — a continuous logreturn outcome — for long and short direction.
4. The strategy module calibrates entry/exit rules against model output,
   producing a strategy artifact with isotonic-calibrated scores and Optuna-optimised thresholds.
5. A Streamlit dashboard consumes live predictions and strategy rules, shows
   signals, and exposes live trading controls.

Elliott wave analysis (`research/elliott/`) is a parallel research module —
it does not feed the live trading pipeline.

---

## Module Architecture

Five production modules with clear, non-overlapping responsibilities:

### `src/data_handling/` — Operational data layer
Owns all live data: ingestion, storage, sync, and validation.
- Syncs raw OHLCV from Binance
- Computes and syncs features, targets, predictions into DuckDB
- No modeling logic; no strategy logic

### `src/modeling/` — Offline ML development
Produces model artifacts from historical data. Runs ad-hoc, not in production.
- Snapshot-natív pipeline: `snap."<snapshot_id>"` a forrás; kimenetek DuckDB táblák (`model.__sample`, `model.__pred`) + fájl-artefaktok
- Outputs: `model.pkl`, `features.json`, `params.json`, `model."<id>__pred"` (offline predikció); registry provenance minden lépésnél
- Does not operate live; does not decide thresholds or rules

### `src/strategy/` — Offline strategy calibration
Consumes model artifacts to calibrate trading rules. Runs ad-hoc, not in production.
- Strategy tábla: `snap ⋈ model_long.__pred ⋈ model_short.__pred` JOIN (parquet-mozgatás nélkül)
- Calibrates raw regression scores via rank percentile lookup + isotonic regression
- Optimises entry/exit/cooldown with Optuna (continuous MFE objective)
- Outputs: `strat."<session>__trades/__equity/__cutoffs"` (lab.duckdb) + `strategy_artifact.json`, `rank_lookup_*.parquet`, `isotonic_*.pkl` (fájl); reg.strategies + reg.artifacts bejegyzés

### `src/trading/` — Live operations only
Runs the calibrated strategy live. Does not calibrate or optimise — only executes.
- Reads `predictions` table + `strategy_artifact.json`, applies rules, journals trades
- Owns: live state machine (FLAT → LONG/SHORT → COOLDOWN), position sizing, trade journal

### `src/ui/` — Display layer
Reads from the database and from artifacts; does not write business data.
- Shows live predictions, signals, equity curve, trade journal
- Exposes live trading controls (start/stop, parameter overrides)

### `research/` — Sandbox
Exploratory code, prototypes, and ideas not yet ready for production.
- `research/elliott/` — Elliott wave parser, validators, scanners (isolated)
- No code here feeds the live pipeline

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

trading/
  live loop  ◀──  predictions (live.duckdb)  ◀──  strategy_artifact.json + rank_lookup*.parquet

ui/  ◀──  predictions + strat.* táblák (lab.duckdb ATTACH) + trade journal
```

---

## Persistence Rules

| Data type | Where | Reason |
|-----------|-------|--------|
| Live OHLCV, features, targets, predictions | `live.duckdb` main séma | Synced, queryable, updatable |
| `quant_train` | `live.duckdb` main séma | Ad-hoc join table, rebuilt before training |
| Range snapshots | `lab.duckdb` snap séma (`snap."<snapshot_id>"`) | Immutable CTAS + content-hash; reprodukálható modell-alap |
| Hourly samples + fold_id | `lab.duckdb` model séma (`model."<id>__sample"`) | Kicsi (~tízezer sor); sokszor olvasható |
| Offline predictions (teljes range) | `lab.duckdb` model séma (`model."<id>__pred"`) | Snapshot-tól elkülönítve; snap hash sértetlen marad |
| Strategy tables (trades/equity/cutoffs) | `lab.duckdb` strat séma (`strat."<session>__*"`) | UI és validáció közvetlen query-vel éri el |
| Registry (provenance lánc) | `registry.duckdb` reg séma | Normalizált igazságforrás; assets → snapshots → models → strategies → deployments |
| Feature engineering analysis | `artifacts/<model_id>/feature_engineering/` (`.ipynb`, `.html`, `feature_set.json`) | Per-model, analyst output; feature_set.json → reg.feature_sets |
| Model artifacts | `artifacts/<model_id>/` (`manifest.json`, `model.pkl`, `features.json`, `params.json`, `search/`) | Runtime use by prediction sync |
| Strategy artifacts | `artifacts/<session_id>/` — `strategy_artifact.json`, `rank_lookup_long/short.parquet`, `isotonic_long/short.pkl`, `sweep_results.csv` | Runtime use by live trading loop; útvonalak a reg.artifacts-ban |

**Rule:** strukturált, queryolható adat → DuckDB (séma + tábla). Blob és ember-report → fájl.
Mindent a registry köt össze. Részletek: → `_doc_/database_and_code_doc/0002_data_architecture.md`

---

## Repository Layout

```
src/
  data_handling/    Operational data layer
    store/            DuckDB store, queries, validation, stats
    sync_tables/      OHLCV sync, feature sync, prediction sync, target sync
    tests/            Tests (store/, sync_tables/ — smoke, sanity, perf, integration)
    01_validate_stats.py
    02_sync_pipeline.py

  modeling/         Offline ML development
    sampling/           Yearly sample creation: config, sampler, audit, artifacts
    training/           LightGBM trainer, CV, datasets, metrics, reports, artifacts
    evaluation/         Backtest runner, metrics
    search/             Hyperparameter search (LightGBM + Optuna)
    feature_engineering/  Feature quality, target-relation, redundancy, stability library
    text/               Future placeholder
    00_create_sample.py
    01_feature_engineering.ipynb
    02_hyper_param_search.py
    03_fit_model.py

  strategy/         Offline strategy calibration (ad-hoc, not in production)
    strategy/           build_table, calibrate, search, artifacts library
    tests/              Smoke tests
    00_run_strategy_session.py

  trading/          Live trading operations only
    live/               TradingService, exchange client, journal, state machine, strategy evaluator
    01_run_service.py

  ui/               Streamlit dashboard (pages, components, data loading)
  utils.py          All config loading — single entry point, never read JSON directly

research/           Sandbox — explorations not yet production-ready
  elliott/            Elliott wave parser, validators, scanners, backtest

analyst/            Analyst Python segédmodulok (table_formatting, plot_utils, db_utils, CSS, _quarto.yml, doc_renderer/)
_doc_/              Dokumentáció — három zóna + globális gyökér
  0000_*, 0001_*      Globális (project overview, agentic rendszer)
  database_and_code_doc/   ZÓNA 1 — DB séma + kód-referencia (.md) — code_doc_agent
  methodology_doc/         ZÓNA 2 — rationale, döntések, módszertan (.md, kód-mentes) — methodology_agent
  models_doc/              ZÓNA 3 — modellenkénti report (.ipynb→Quarto) — analyst_agent
  _plans_/                 Draft rendszertervek (nem kanonikus zóna)
_jira_/             Local task tracking (epics → tasks → stories); jira.json = epic counter
.agent/             Agent rules, skills, tool docs
config/             JSON config files (assets, features, models, strategies, trading…)
artifacts/          Model and strategy artifacts
  <model_id>/
    manifest.json                   Pipeline state + model summary
    metadata.json / audit.json      Sample metadata (written by sample step)
    sample_train_valid.parquet      Hourly sample: own target + pred_{dir} + all feat_* + fold_id
    model.pkl / features.json / params.json
    search/                         Hyperparameter search results
    feature_engineering/            01_feature_engineering.ipynb + .html + feature_set.json
  <session_id>/                     Strategy session — naming: strat_solusdt_fw60_combo_{start}_{end}
    isotonic_long.pkl               Fitted sklearn IsotonicRegression (long direction)
    isotonic_short.pkl              Fitted sklearn IsotonicRegression (short direction)
    rank_lookup_long.parquet        Rank percentile lookup — score_raw|score_pct|bucket_id|bucket_mean/median/p75_mfe
    rank_lookup_short.parquet       Rank percentile lookup (short direction)
    strategy_artifact.json          Grid search best setup + decision_params + search_info + strat table refs
    sweep_results_grid.csv          Grid search results: all (direction, entry_cutoff, tp_spec, sl_spec) combinations
database/           DuckDB files (3-fájlos topológia — részletek: _doc_/database_and_code_doc/0002_data_architecture.md)
  solusdt/
    solusdt.duckdb         <- LIVE (main séma)
    solusdt_lab.duckdb     <- LAB (snap / model / strat sémák)
  _registry/
    registry.duckdb        <- REGISTRY (reg séma, globális)
```

---

## Database (DuckDB)

**3-fájlos topológia (epic_031 után):**

| Fájl | Séma | Tartalom |
|------|------|---------|
| `database/solusdt/solusdt.duckdb` | `main` | LIVE: ohlcv, feat_ohlcv_quant, target, quant_train, predictions |
| `database/solusdt/solusdt_lab.duckdb` | `snap`, `model`, `strat` | LAB: immutable snapshots, sample/pred táblák, strategy táblák |
| `database/_registry/registry.duckdb` | `reg` (default main) | REGISTRY: assets, snapshots, feature_sets, models, search_runs, strategies, deployments, artifacts |

Részletes leírás: → `_doc_/database_and_code_doc/0002_data_architecture.md`

Currently only one active asset: **solusdt** (SOLUSDT, 1m, futures).

### Live táblák (main séma)

| Table | Primary Key | Purpose |
|-------|-------------|---------|
| `ohlcv` | `open_time` TIMESTAMP | Raw 1-minute candles from Binance |
| `target` | `open_time` TIMESTAMP | fw60 forward outcomes: `long_mfe_fw60`, `short_mfe_fw60` + 8 further columns |
| `feat_ohlcv_quant` | `open_time` TIMESTAMP | Quantitative features (`feat_` prefix) |
| `predictions` | `open_time` TIMESTAMP | Model scores + signals + `long_model_id` / `short_model_id` stamp |
| `quant_train` | `open_time` TIMESTAMP | Ad-hoc join: all `feat_*` + `long_mfe_fw60` + `short_mfe_fw60`; NULL targets excluded |

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
pl. `lgbm_solusdt_l_fw60_2101_2605` (anchor 2023-01, valid through 2026-05)

**Active models:** 10 éves modell (2021–2025 × long + short) + 2 champion modell (`lgbm_solusdt_l/s_fw60_2101_2605`) — mind `active: false` amíg nem kerül kiválasztásra élő kereskedésre. Config: `config/models.json` (schema v4).

- **Target semantics:** `fw60` = 60-bar forward window; `long_mfe_fw60` = log(max upside / close[t]); `short_mfe_fw60` = log(min downside / close[t]). Folytonos regressziós target — nincs percentilis küszöb, nincs binarizálás.
- **Feature prefix:** `feat_` | **t-1 lag mandatory** on all features (prevents data leakage).
- **Feature engineering target:** only the model's own direction target is used (`l` → `long_mfe_fw60`, `s` → `short_mfe_fw60`).
- **Artifacts:** `artifacts/<model_id>/` — `manifest.json`, `sample_train_valid.parquet`, `model.pkl`, `features.json`, `params.json`, `search/`, `feature_engineering/`. Note: `sample_oos.parquet` is **not** produced by the fit step — strategy calibration reads from `quant_train` (DuckDB) directly.
- **Samples:** model-specifikusak, az artifact mappában élnek. A `sample_train_valid.parquet` a `sample` lépés outputja; a `pred_{dir}` oszlop a `train` lépés után kerül bele.

### Pipeline (runs offline, in order)

```bash
# Snapshot létrehozása (egyszer, vagy újrahasználat a registry-ből):
uv run python src/data_handling/05_create_snapshot.py --asset-id solusdt --start 2021-01-01 --end 2025-12-31

# Teljes pipeline egy modellre:
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021

# Egyes lépések:
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step setup
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step sample
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step feature_engineering
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step search --stage smoke
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step train
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step predict

# Deploy trigger:
uv run python src/data_handling/06_trigger_deploy.py --strategy-session-id strat_solusdt_fw60_combo_2101_2605
```

| Lépés | Input | Output |
|-------|-------|--------|
| `setup` | `config/models.json` | `manifest.json` (artifact); `reg.models` draft |
| `sample` | `snap."<snapshot_id>"` (lab.duckdb) | `model."<id>__sample"` (lab.duckdb); `reg.feature_sets`, `reg.models` sampled |
| `feature_engineering` | `model.__sample` | `feature_engineering/01_fe.ipynb`, `.html`, `feature_set.json` (artifact); `reg.feature_sets` link |
| `search` | `model."<id>__train_input"` VIEW | `search/search_best.json`, `search_trials.jsonl` (artifact); `reg.search_runs` |
| `train` | `model."<id>__train_input"` VIEW | `model.pkl`, `features.json`, `params.json` (artifact); `reg.models` trained |
| `predict` | `snap."<snapshot_id>"` + `model.pkl` | `model."<id>__pred"` (lab.duckdb); `reg.models` predicted |

### Yearly sample model

One sample = one calendar year. Sample ID: `{asset_id}_fw60_yearly_{year}`.

**Két sampling mód** van (`config/models.json` → `sampling.sampling_mode`):

| Mód | `sampling_mode` | CV struktúra | Fold assignment | Státusz |
|-----|----------------|--------------|-----------------|---------|
| Legacy weekly | `yearly_random_hour` (default) | 4-fold random-week, monthly stratified | `fold_id` 1–4 per week | Legacy |
| **Walk-forward** | `walk_forward` | 9m train + 3m valid, 3m shift | `fold_id` 1–4 (valid ablak) / `0` (train-only) | **ACTIVE** |

**Walk-forward CV (ACTIVE):** paraméterek a `config/models.json` → `sampling` szekcióból jönnek (`train_months`, `valid_months`, `shift_months`, `n_folds`). Purge: 240 perc. `metadata.json`-ban: `fold_time_windows` lista (foldonkénti `train_start/end`, `valid_start/end`). `fold_id = 0` a train-only sorokat jelöli.

| Modell típus | train | valid | shift | n_folds |
|---|---|---|---|---|
| Éves (2021–2025) | 9m | 3m | 3m | 4 |
| Champion (`_2101_2605`) | 9m | 8m | 8m | 4 |

Éves modellek anchor éve = `sampling.year`; champion modelleknél `sampling.year` adja az anchor-t (2023), a tényleges adattartomány a fold windows-ból számítódik (2023-01 → 2026-05).

**Search objective (ACTIVE):** Top10 Lift fold-stability penaltyvel:
```
top10_lift = mean(y_true | score ∈ top 10%) − mean(y_true | all valid)
objective  = mean(top10_lift_folds) − 0.5 × std(top10_lift_folds)   [higher is better]
```
Kötelező audit metrikák foldonként: `spearman_rho`, `decile_monotonicity`. Mentve: `search_best.json`, `search_summary.csv`.

```
sample_train_valid.parquet columns (artifacts/<model_id>/):
  l-irányú model: open_time | long_mfe_fw60  | pred_long  | fold_id | feat_* (all)
  s-irányú model: open_time | short_mfe_fw60 | pred_short | fold_id | feat_* (all)
  — pred_{dir} csak a train step után kerül bele
  — walk-forward módban fold_id: Int8, 0–4 (0 = train-only); legacy módban: 1–4
```

Features a sample parquetban tárolódnak (all feat_* a train_valid-ban). Nincs DB join training közben.
Samples are parquet only — no DuckDB materialization.
Methodology details: `_doc_/5010_sampling_yearly.md`.

### OOS evaluation

For **yearly models**, the intended OOS is always a **separate, future calendar year** — not a holdout
month from the training year. This ensures all seasonal effects are represented in
the training data, and the OOS is a genuinely unseen period.

```
2021 model  →  trained on 2021 sample  →  strategy calibration uses 2022 data from quant_train
2022 model  →  trained on 2022 sample  →  strategy calibration uses 2023 data from quant_train
...
```

`uv run python src/modeling/03_fit_model.py --model lgbm_solusdt_l_fw60_2021` produces:
1. Final model refitted on **all rows** of the 2021 sample (fold_id is CV metadata only).
2. Updated `sample_train_valid.parquet` with `pred_{dir}` column added.

The `src/strategy/` module scores any date range directly from `quant_train` (DuckDB) using
the trained `model.pkl` — no pre-computed OOS parquet is needed or produced.

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

**Live state machine (`src/trading/live/`) — epic_036 után:**
- **States:** FLAT → LONG / SHORT → FLAT (COOLDOWN state eltávolítva)
- Entry: `score_pct_long >= entry_cutoff` (long); `(1 - score_pct_short) >= entry_cutoff` (short — invertált ranking, mert `short_mfe_fw60 < 0`)
- Exit: `hold_minutes >= max_hold_minutes` (timeout-only; intrabar TP/SL a live service-ben külön epicben)
- Service loads `strategy_artifact.json` + rank lookup parquets; converts raw predictions to percentiles via `np.interp` at each bar
- `config/trading.json`: `strategy_session_id`
- ⚠️ Intrabar TP/SL monitoring (bracket order) a live service-ben még nem implementált — külön epic szükséges

---

## Testing Rules

| What to run | When |
|-------------|------|
| `uv run pyright src/<module>/` | After any type-annotated change |
| `ruff check src/<module>/ --fix` | Before committing any Python file |
| `uv run pytest src/data_handling/tests/ -v` | Store or pipeline changes |
| `uv run pytest src/modeling/ -v` | Modeling changes |
| `uv run pytest src/strategy/tests/ -v` | Strategy calibration changes |
| `uv run pytest src/trading/tests/ -v` | Live trading service changes |
| `STREAMLIT_CONFIG_DIR=src/ui uv run streamlit run src/ui/main.py` | UI changes (manual smoke test) |

Always run pyright and ruff for the affected module. Never skip for non-trivial changes.

---

## Key Conventions

- **Config gateway:** all config through `src/utils.py` — no raw JSON reads in `src/`
- **Active asset:** `solusdt` — do not spend time on inactive asset paths
- **Polars for features:** feature computation uses Polars; pandas allowed elsewhere
- **No print() in library code** — use `logging` or `st.*`
- **Upserts only** — no delete/truncate patterns in sync operations
- **DuckDB-natív:** strukturált adat → DuckDB (séma+tábla); blob/report → fájl; registry köti össze
- **3-fájlos topológia:** live.duckdb (sync) / lab.duckdb (modellezés) / registry.duckdb (katalógus); ATTACH-cal egyetlen connectionból joinolható
- **Snapshot = immutable:** a `snap."<snapshot_id>"` tábla tartalma soha nem íródik felül; új adatállapot = új snapshot_id
- **Elliott and research are isolated** — nothing in `research/` feeds the live pipeline

---

## Agent Ownership

| Agent | Owns |
|-------|------|
| Database Agent | `src/data_handling/`, `config/assets.json`, DuckDB schema |
| Modeling Agent | `src/modeling/`, feature computation, model artifacts; `src/strategy/` |
| UI Agent | `src/ui/`, `src/trading/` |
| Code Doc Agent | `.agent/`, tooling, infra, dependencies; `_doc_/database_and_code_doc/` (DB séma + kód-referencia zóna) |
| Analyst Agent | `_doc_/models_doc/` (modell-report notebookok, .ipynb→Quarto), `analyst/` (Python segédmodulok: `table_formatting`, `plot_utils`, `db_utils`, CSS, `_quarto.yml`, `doc_renderer/`); user-célból indul, nem spec-ből; interaktív session |
| Methodology Agent | `_doc_/methodology_doc/` — business rationale, methodological decisions, parameter justification (kód-mentes) |
| Validator Agent | `pr_` ticket validation: ruff + pyright + pytest, then `done_` promotion |
