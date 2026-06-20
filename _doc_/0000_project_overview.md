# ChronoQuant — Project Overview

Single source of truth for the orchestrator. Agents load their own module docs
from `_doc_/<module>/`; this file is for cross-domain context only.

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
- Creates yearly samples (parquet), searches hyperparameters, fits final models
- Outputs: `model.pkl`, `features.json`, `params.json`, updated `sample_train_valid.parquet`
- Does not operate live; does not decide thresholds or rules; does not produce OOS scores

### `src/strategy/` — Offline strategy calibration
Consumes model artifacts to calibrate trading rules. Runs ad-hoc, not in production.
- Builds strategy table: both model predictions + targets from `quant_train` (DuckDB)
- Calibrates raw regression scores via isotonic regression → E[mfe | score]
- Optimises entry/exit/cooldown with Optuna (continuous MFE objective)
- Outputs: `strategy_artifact.json`, `isotonic_long/short.pkl`, `strategy_table.parquet`, `sweep_results.csv`

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

```
Binance API
    │
    ▼
database/  ──sync──▶  ohlcv  ──sync──▶  feat_ohlcv_quant  ──sync──▶  target
                                                │                         │
                                                └──────────┬─────────────┘
                                                           │
modeling/                                           quant_train (ad-hoc build)
  0_create_sample   ◀──────────────────────────────────────┘
  1_feature_engineering
  2_hyper_param_search
  3_fit_model  ──────────▶  model artifact (model.pkl + features.json + params.json)
                            sample_train_valid.parquet (pred_{dir} added)

strategy/             ◀──  model artifacts (model.pkl + features.json) × 2 models
  0_build_strategy_table   ◀──  quant_train (DuckDB) + target (DuckDB)
                           ──▶  strategy_table.parquet
  1_calibrate_scores       ◀──  strategy_table.parquet
                           ──▶  isotonic_long/short.pkl + updated strategy_table.parquet
  2_optimize_strategy      ◀──  strategy_table.parquet (calibrated)
                           ──▶  strategy_artifact.json + sweep_results.csv

trading/
  live loop           ◀──  predictions table (database sync)
                      ◀──  strategy_artifact.json + isotonic_long/short.pkl
                      ──▶  trade journal

database/  ◀──  prediction sync uses model artifact  ──▶  predictions table

ui/  ◀──  predictions table + strategy artifact + trade journal
```

---

## Persistence Rules

| Data type | Where | Reason |
|-----------|-------|--------|
| Live OHLCV, features, targets, predictions | DuckDB | Synced, queryable, updatable |
| `quant_train` | DuckDB | Ad-hoc join table, rebuilt before training |
| Yearly samples | Parquet (`artifacts/<model_id>/sample_train_valid.parquet`) | Model-specific; contains all feat_* + own target + pred_{dir} (after train step) |
| Feature engineering analysis | `artifacts/<model_id>/feature_engineering/` (`.ipynb`, `.html`, `feature_set.json`) | Per-model, analyst output |
| Model artifacts | `artifacts/<model_id>/` (`manifest.json`, `model.pkl`, `features.json`, `params.json`, `search/`) | Runtime use by prediction sync |
| Strategy artifacts | `artifacts/{session_id}/` — naming: `strategy_{asset}_fw{horizon}_{start}_{end}` (e.g. `strategy_lgbm_solusdt_fw60_2101_2605`); contains `strategy_artifact.json`, `rank_lookup_long/short.parquet`, `isotonic_long/short.pkl`, `strategy_table.parquet`, `sweep_results.csv`, `trades.parquet`, `equity_curve.parquet`, `summary.json`, `strategy_report.ipynb/.html` | Produced by `src/strategy/`; runtime use by live trading loop |

**Rule:** if a table needs to be synced incrementally → DuckDB. If it is a static
snapshot produced by a modeling or analysis run → Parquet.

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
    strategy/           build_table, calibrate, optimize, artifacts library
    tests/              Smoke tests
    00_build_strategy_table.py
    01_calibrate_scores.py
    02_optimize_strategy.py

  trading/          Live trading operations only
    live/               TradingService, exchange client, journal, state machine, strategy evaluator
    01_run_service.py

  ui/               Streamlit dashboard (pages, components, data loading)
  utils.py          All config loading — single entry point, never read JSON directly

research/           Sandbox — explorations not yet production-ready
  elliott/            Elliott wave parser, validators, scanners, backtest

src/analyst/        Analyst Python segédmodulok (table_formatting, plot_utils, db_utils, CSS, _quarto.yml)
_doc_/              Module documentation + analyst notebooks
  XXXX_*.ipynb        Analyst notebookok (közvetlenül itt)
  XXXX_*.html         Quarto-rendered HTML output
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
  <session_id>/                     Strategy session — naming: strategy_{asset}_fw{horizon}_{start}_{end}
                                    e.g. strategy_lgbm_solusdt_fw60_2101_2605
                                    (asset=solusdt, horizon=60, start=2021-01, end=2026-05; no date suffix)
    strategy_table.parquet          open_time | pred_long_raw | pred_short_raw | pred_long_cal | pred_short_cal | long_mfe_fw60 | short_mfe_fw60
    isotonic_long.pkl               Fitted sklearn IsotonicRegression (long direction)
    isotonic_short.pkl              Fitted sklearn IsotonicRegression (short direction)
    strategy_artifact.json          Entry/exit/cooldown thresholds + metrics + artifact path refs
    sweep_results.csv               Optuna trial log
    trades.parquet                  Trade ledger (entry_time, exit_time, direction, hold_minutes, exit_reason, …)
    equity_curve.parquet            Cumulative MFE per trade (proxy equity curve)
    summary.json                    Session summary: n_trades, win_rate, gross_return, equity_basis
    strategy_report.ipynb           Analyst report notebook
    strategy_report.html            Quarto-rendered HTML report
database/           DuckDB files only (samples no longer stored here)
  solusdt/
    solusdt.duckdb
```

---

## Database (DuckDB)

**One DuckDB file per asset:** `database/<asset_id>/<asset_id>.duckdb`

Currently only one active asset: **solusdt** (SOLUSDT, 1m, futures).

### Tables

| Table | Primary Key | Purpose |
|-------|-------------|---------|
| `ohlcv` | `open_time` TIMESTAMP | Raw 1-minute candles from Binance |
| `target` | `open_time` TIMESTAMP | fw60 forward outcomes: `long_mfe_fw60`, `short_mfe_fw60` + 8 further columns |
| `feat_ohlcv_quant` | `open_time` TIMESTAMP | Quantitative features (`feat_` prefix) |
| `predictions` | `open_time` TIMESTAMP | Model probability scores + signals |
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
# Teljes pipeline egy modellre:
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021

# Egyes lépések:
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step setup
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step sample
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step feature_engineering
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step search --stage smoke
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_2021 --step train
```

| Lépés | Input | Output (artifact-ban) |
|-------|-------|----------------------|
| `setup` | `config/models.json` | `manifest.json` |
| `sample` | `quant_train` (DuckDB) | `sample_train_valid.parquet`, `metadata.json`, `audit.json` |
| `feature_engineering` | `sample_train_valid.parquet` (artifact) | `feature_engineering/01_fe.ipynb`, `.html`, `feature_set.json` |
| `search` | `sample_train_valid.parquet` + `feature_set.json` (artifact) | `search/search_best.json`, `search_trials.jsonl` |
| `train` | `sample_train_valid.parquet` + search results | `model.pkl`, `features.json`, `params.json`, `sample_train_valid.parquet` (pred hozzáadva) |

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
- `00_build_strategy_table.py` — loads both model predictions from `quant_train` (DuckDB) for a date range
- `01_calibrate_scores.py` — fits rank percentile lookup (primary) + isotonic regression (secondary) per direction; single `--start/--end` window
- `02_optimize_strategy.py` — Optuna sweep; objective: mean(bucket_mean_mfe | entry fired); rank-first entry/exit logic; requires `--long-model`, `--short-model`, `--start`, `--end`
- Produces: `strategy_artifact.json` + `rank_lookup_long/short.parquet` + `isotonic_long/short.pkl` + `strategy_table.parquet` + `sweep_results.csv` in `artifacts/{session_id}/`
- **Signal mode:** `rank_first` — entry based on score percentile in calibration distribution, not raw score threshold
- **Evaluation mode:** `same_window` — calibration, optimization, and reported metrics use the same session window

**`strategy_artifact.json` contract:**
- `signal_mode: "rank_first"`, `evaluation_mode: "same_window"`, `fit_period`
- `decision_params`: `long/short_entry_pct`, `min_edge_gap`, `min/max_hold_minutes`, `cooldown_minutes`, `rearm_pct`, `conflict_rule: "highest_edge"`

**Live state machine (`src/trading/live/`):**
- **States:** FLAT → LONG / SHORT → COOLDOWN → FLAT
- **Entry:** `score_pct_long >= long_entry_pct` → ENTER_LONG; conflict → `highest_edge` rule
- **Exit:** `max_hold_minutes` elapsed, or `opposite_edge`, or `signal_decay` (score_pct < rearm_pct)
- **Rearm:** after `cooldown_minutes` AND both `score_pct <= rearm_pct` → FLAT
- Service loads `strategy_artifact.json` + rank lookup parquets; converts raw predictions to percentiles via `np.interp` at each bar
- `config/trading.json`: `strategy_session_id` (replaces legacy `long/short_strategy_id`)

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
- **DuckDB = synced/live, Parquet = static snapshots** — never invert this
- **Elliott and research are isolated** — nothing in `research/` feeds the live pipeline

---

## Agent Ownership

| Agent | Owns |
|-------|------|
| Database Agent | `src/data_handling/`, `config/assets.json`, DuckDB schema |
| Modeling Agent | `src/modeling/`, feature computation, model artifacts; `src/strategy/` |
| UI Agent | `src/ui/`, `src/trading/` |
| Code Doc Agent | `.agent/`, tooling, infra, dependencies; `_doc_/` X110+ code reference files |
| Analyst Agent | `_doc_/XXXX_*.ipynb` (elemzési notebookok), `src/analyst/` (Python segédmodulok: `table_formatting`, `plot_utils`, `db_utils`, CSS, `_quarto.yml`); user-célból indul, nem spec-ből; interaktív session |
| Methodology Agent | `_doc_/` X000, X100 levels — business rationale, methodological decisions, parameter justification |
| Validator Agent | `pr_` ticket validation: ruff + pyright + pytest, then `done_` promotion |
