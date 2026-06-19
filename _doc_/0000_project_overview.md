# ChronoQuant — Project Overview

Single source of truth for the orchestrator. Agents load their own module docs
from `_doc_/<module>/`; this file is for cross-domain context only.

---

## Business Goal

ChronoQuant is an algorithmic crypto trading system targeting **SOLUSDT perpetual
futures on Binance**. The core loop:

1. Sync 1-minute OHLCV candles from Binance into a local DuckDB store.
2. Compute quantitative features (momentum, volume, volatility) over that data.
3. Train LightGBM binary classifiers to predict whether the next 60-bar forward
   return exceeds a directional threshold (long) or falls below one (short).
4. The trading module calibrates entry/exit rules against out-of-sample model
   output, producing a strategy artifact.
5. A Streamlit dashboard consumes live predictions and strategy rules, shows
   signals, and exposes live trading controls.

Elliott wave analysis (`research/elliott/`) is a parallel research module —
it does not feed the live trading pipeline.

---

## Module Architecture

Four production modules with clear, non-overlapping responsibilities:

### `src/data_handling/` — Operational data layer
Owns all live data: ingestion, storage, sync, and validation.
- Syncs raw OHLCV from Binance
- Computes and syncs features, targets, predictions into DuckDB
- No modeling logic; no strategy logic

### `src/modeling/` — Offline ML development
Produces model artifacts from historical data. Runs ad-hoc, not in production.
- Creates yearly samples (parquet), searches hyperparameters, fits final models
- Outputs: `model.pkl`, `features.json`, `sample_oos.parquet`, `model_card.md`
- Does not operate live; does not decide thresholds or rules

### `src/trading/` — Strategy + live operations
Consumes model artifacts to calibrate strategy rules, then runs them live.
- Measures strategy performance on OOF predictions → produces strategy artifact
- Runs the live trading loop: reads `predictions` table, applies rules, journals trades
- Owns: thresholds, cut-offs, position sizing, cooldowns, hold times

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
  3_fit_model  ──────────▶  model artifact (model.pkl + features.json)
               ──────────▶  sample_oos.parquet

trading/
  0_measure_strategy  ◀──  sample_oos.parquet
                      ──▶  strategy artifact (thresholds, rules)
  live loop           ◀──  predictions table (database sync)
                      ◀──  strategy artifact
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
| Yearly samples | Parquet (`database/<asset>/samples/<id>/`) | Static snapshots, not synced |
| OOS predictions | Parquet (`database/<asset>/samples/<sample_id>/sample_oos.parquet`) | Static, per-model OOS output |
| Feature engineering analysis | `artifacts/<model_id>/feature_engineering/` (`.ipynb`, `.html`, `feature_set.json`) | Per-model, analyst output |
| Model artifacts | `artifacts/<model_id>/` (`manifest.json`, `model.pkl`, `features.json`, `params.json`, `search/`) | Runtime use by prediction sync |
| Strategy artifacts | `artifacts/<model_id>/strategy/` | Runtime use by trading loop |

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
    04_generate_model_card.py

  trading/          Strategy calibration + live operations
    00_measure_strategy.py
    01_sweep_strategy.py
    02_run_service.py

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
artifacts/          Model development artifacts — one folder per model_id
  <model_id>/
    manifest.json                   Pipeline state + model summary
    model.pkl / features.json / params.json / model_card.json
    search/                         Hyperparameter search results
    feature_engineering/            01_feature_engineering.ipynb + .html + feature_set.json
database/           DuckDB files + static sample snapshots (read-only source for training)
  solusdt/
    solusdt.duckdb
    samples/<sample_id>/            metadata.json, audit.json, sample_train_valid.parquet
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
lgbm_{asset}_{direction}_fw{horizon}_q{quantile}_{year}
```
pl. `lgbm_solusdt_l_fw60_q90_2021`, `lgbm_solusdt_s_fw60_q10_2023`

**Active models:** 10 éves modell (2021–2025 × long + short) — mind `active: false` amíg nem kerül kiválasztásra élő kereskedésre. Config: `config/models.json` (schema v4).

- **Target semantics:** `fw60` = 60-bar forward window; `long_mfe_fw60` = log(max upside / close[t]); `short_mfe_fw60` = log(min downside / close[t]).
- **Feature prefix:** `feat_` | **t-1 lag mandatory** on all features (prevents data leakage).
- **Artifacts:** `artifacts/<model_id>/` — `manifest.json`, `model.pkl`, `features.json`, `params.json`, `search/`, `feature_engineering/`.
- **Samples:** read-only forrás `database/solusdt/samples/solusdt_fw60_yearly_{year}/`; nem másolódik az artifact-ba, csak hivatkozik rá (`sampling.sample_dir`).

### Pipeline (runs offline, in order)

```bash
# Teljes pipeline egy modellre:
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_q90_2021

# Egyes lépések:
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_q90_2021 --step setup
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_q90_2021 --step feature_engineering
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_q90_2021 --step search --stage smoke
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_q90_2021 --step train
uv run python src/modeling/pipeline.py --model lgbm_solusdt_l_fw60_q90_2021 --step model_card
```

| Lépés | Input | Output (artifact-ban) |
|-------|-------|----------------------|
| `setup` | `config/models.json` | `manifest.json` |
| `feature_engineering` | `samples/{sample_id}/sample_train_valid.parquet` (via DuckDB) | `feature_engineering/01_fe.ipynb`, `.html`, `feature_set.json` |
| `search` | sample parquet + feature_set.json | `search/search_best.json`, `search_trials.jsonl` |
| `train` | sample parquet + search results | `model.pkl`, `features.json`, `params.json` |
| `model_card` | model artifact + OOS results | `model_card.json` |

### Yearly sample model

One sample = one calendar year. Sample ID: `{asset_id}_fw60_yearly_{year}`.

**Segments:** `train` / `valid` / `purge` — no test holdout within the sample.
Test evaluation uses a separate future-year OOS (see OOS Evaluation section).

```
sample_train_valid.parquet columns:
  open_time | feat_* | long_mfe_fw60 | short_mfe_fw60 | segment | fold_id

sample_oos.parquet columns (written by 03_fit_model.py):
  open_time | pred_long | pred_short | long_mfe_fw60 | short_mfe_fw60
```

Features are NOT stored in the sample — loaded from `quant_train` at training time.
Samples are parquet only — no DuckDB materialization.
Methodology details: `_doc_/5010_sampling_yearly.md`.

### OOS evaluation

OOS (out-of-sample) is always a **separate, future calendar year** — not a holdout
month from the training year. This ensures all seasonal effects are represented in
the training data, and the OOS is a genuinely unseen period.

```
2021 model  →  trained on 2021 sample  →  OOS scored on 2022
2022 model  →  trained on 2022 sample  →  OOS scored on 2023
...
```

`3_fit_model.py --year 2021 --oos-year 2022` produces:
1. Final model refitted on all train+valid rows of the 2021 sample.
2. `sample_oos.parquet` — predict_proba applied to the full 2022 dataset.

The trading module uses `sample_oos.parquet` for strategy calibration.

---

## Trading Strategy

The trading module calibrates strategy rules offline, then runs them live.

**Offline calibration (`0_measure_strategy.py`):**
- Reads `sample_oos.parquet` (model probs + actual targets for OOS year)
- Sweeps entry/exit thresholds, hold times, cooldowns
- Produces `strategy_artifact.json` with the optimal rule set

**Live state machine (`src/trading/strategy.py`):**
- **States:** FLAT → LONG / SHORT → COOLDOWN → FLAT
- **Entry:** `pred_long >= entry_threshold` → ENTER_LONG (long has priority if both fire)
- **Exit:** max hold time elapsed OR stop-loss triggered → EXIT, enter COOLDOWN
- **Rearm:** both model probs must cool below `rearm_threshold` before next entry
- All thresholds and cooldowns come from `strategy_artifact.json`

---

## Testing Rules

| What to run | When |
|-------------|------|
| `uv run pyright src/<module>/` | After any type-annotated change |
| `ruff check src/<module>/ --fix` | Before committing any Python file |
| `uv run pytest src/data_handling/tests/ -v` | Store or pipeline changes |
| `uv run pytest src/modeling/ -v` | Modeling changes |
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
| Modeling Agent | `src/modeling/`, feature computation, model artifacts |
| UI Agent | `src/ui/`, `src/trading/` |
| Code Doc Agent | `.agent/`, tooling, infra, dependencies; `_doc_/` X110+ code reference files |
| Analyst Agent | `_doc_/XXXX_*.ipynb` (elemzési notebookok), `src/analyst/` (Python segédmodulok: `table_formatting`, `plot_utils`, `db_utils`, CSS, `_quarto.yml`); user-célból indul, nem spec-ből; interaktív session |
| Methodology Agent | `_doc_/` X000, X100 levels — business rationale, methodological decisions, parameter justification |
| Validator Agent | `pr_` ticket validation: ruff + pyright + pytest, then `done_` promotion |
