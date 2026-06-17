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
4. Emit probability scores as live signals; a threshold-based strategy decides
   ENTER_LONG / ENTER_SHORT / HOLD / EXIT.
5. A Streamlit dashboard exposes the pipeline, predictions, backtests, and
   live trading controls in one UI.

Elliott wave analysis (`src/elliott_waves/`) is a parallel research module —
it does not feed the live trading pipeline.

---

## Repository Layout

```
src/
  database/         DuckDB domain
    store/            DuckDB store, queries, validation, stats
    sync_tables/      OHLCV sync, feature sync, prediction sync, target sync
    tests/            Tests (store/, sync_tables/ — smoke, sanity, perf, integration)
    01_validate_stats.py
    02_sync_pipeline.py

  modeling/         ML modeling domain (family structure)
    quantitative/     Active family: LightGBM, features, CV, evaluation
      sampling/         Sample definitions: config, splits, audit, artifacts, orchestrator
      evaluation/       Backtesting, metrics
      00_create_sample.py
      01_train_model.py
      02_search_lgbm.py
      03_backtest_strategy.py
      04_sweep_strategy.py
      05_generate_model_card.py
    elliott/          Research family: Elliott wave parser, validators, scanners
    text/             Future family placeholder
    blockchain/       Future family placeholder
    assembly/         Future family placeholder

  trading/          Live trading service, strategy, exchange wrapper, journal
    simulation/       Future: strategy-level backtest logic
    01_run_service.py

  ui/               Streamlit dashboard (pages, components, data loading)
  utils.py          All config loading — single entry point, never read JSON directly

_doc_/              Module documentation mirroring src/ (agent-specific, not preloaded)
_jira_/              Local task tracking (epics → tasks → stories); jira.json holds the global epic counter
.agent/             Agent rules, skills, tool docs
config/             JSON config files (assets, features, models, strategies, trading…)
models/             Generated model artifacts
database/           DuckDB files and sample definitions
                      database/solusdt/solusdt.duckdb
                      database/solusdt/samples/<sample_id>/  (metadata.json, audit.json, sample.parquet)
```

---

## Database

**One DuckDB file per asset:** `database/<asset_id>/<asset_id>.duckdb`

Currently only one active asset: **solusdt** (SOLUSDT, 1m, futures).

### Tables

| Table | Primary Key | Purpose |
|-------|-------------|---------|
| `ohlcv` | `open_time` TIMESTAMP | Raw 1-minute candles from Binance |
| `target` | `open_time` TIMESTAMP | fw60 forward outcomes: `long_mfe_fw60`, `short_mfe_fw60` + 8 further fw60 columns |
| `feat_ohlcv_quant` | `open_time` TIMESTAMP | Quantitative features (`feat_` prefix) |
| `predictions` | `open_time` TIMESTAMP | Model probability scores + signals |

All timestamps are **UTC, format `YYYY-MM-DD HH:MM:SS`** (naive strings treated
as UTC). Epoch milliseconds used internally as `open_time_ms`.

All sync operations are **idempotent upserts keyed on `open_time`** — safe to re-run.

Config is always accessed via `src/utils.py` — never read JSON config files directly
in business logic.

---

## ML Models

### Active models (v4)

| Model ID | Direction | Target | Trainer |
|----------|-----------|--------|---------|
| `lgbm_solusdt_l_fw60_q90_local_v4` | Long | `long_mfe_fw60` | `lightgbm_binary` |
| `lgbm_solusdt_s_fw60_q10_local_v4` | Short | `short_mfe_fw60` | `lightgbm_binary` |

- **Target semantics:** `fw60` = 60-bar forward window; `long_mfe_fw60` = log(max upside/close[t]); `short_mfe_fw60` = log(min downside/close[t]).
- **Features profile:** `solusdt_fw60` defined in `config/features.json`.
- **Feature prefix:** `feat_`  |  **Target columns:** `long_mfe_fw60`, `short_mfe_fw60`
- **t-1 lag mandatory** on all features before training (prevents data leakage).
- Artifacts stored under `models/<model_id>/` (model.pkl + features.json).

### Model pipeline

```
ohlcv → feat_ohlcv_quant → 00_create_sample.py → database/<asset>/samples/<id>/
                                                          ↓
                                                     01_train_model.py → model artifact
                                                                              ↓
                              predictions ← sync_predictions ← predict_proba
```

`00_create_sample.py` generates a yearly random-hour sample: selects one random minute
per hour for the given calendar year, assigns monthly validation weeks, applies a
±240-minute purge buffer, and writes `metadata.json`, `audit.json`, `sample.parquet`
into `database/<asset_id>/samples/<sample_id>/`.

Sample ID format: `{asset_id}_fw60_yearly_{year}` (e.g. `solusdt_fw60_yearly_2024`).
`sample.parquet` columns: `open_time`, `segment` (train/valid/purge), `long_mfe_fw60`, `short_mfe_fw60`.
CLI: `uv run python src/modeling/quantitative/00_create_sample.py --year 2024 --asset-id solusdt`

---

## Trading Strategy

Probability-threshold state machine (`src/trading/strategy.py`):

- **States:** FLAT → LONG / SHORT → COOLDOWN → FLAT
- **Entry:** `pred_long >= entry_threshold` → ENTER_LONG (long has priority if both fire)
- **Exit:** max hold time elapsed OR stop-loss triggered → EXIT, enter COOLDOWN
- **Rearm:** both model probs must cool below `rearm_threshold` before next entry
- Thresholds and cooldowns are config-driven (`config/strategies.json`, `config/trading.json`)

---

## Testing Rules

| What to run | When |
|-------------|------|
| `uv run pyright src/<module>/` | After any type-annotated change |
| `ruff check src/<module>/ --fix` | Before committing any Python file |
| `uv run pytest src/database/tests/ -v` | Store or pipeline changes (smoke/sanity/perf) |
| `uv run pytest src/modeling/ -k "feature or model or backtest" -v` | Modeling changes |
| `STREAMLIT_CONFIG_DIR=src/ui uv run streamlit run src/ui/main.py` | UI changes (manual smoke test) |

Always run pyright and ruff for the affected module. Pytest scope depends on which
layer changed. Never skip these for non-trivial changes.

---

## Key Conventions

- **Config gateway:** all config through `src/utils.py` — no raw JSON reads in `src/`
- **Active asset:** `solusdt` — do not spend time on inactive asset paths
- **Polars for features:** feature computation uses Polars; pandas allowed elsewhere
- **No print() in library code** — use `logging` or `st.*`
- **Upserts only** — no delete/truncate patterns in sync operations
- **Elliott waves is isolated** — does not connect to the live prediction or trading flow

---

## Agent Ownership

| Agent | Owns |
|-------|------|
| Database Agent | `src/database/`, `config/assets.json`, DuckDB schema |
| Modeling Agent | `src/modeling/`, feature computation, model artifacts |
| UI Agent | `src/ui/`, `src/trading/` |
| Code Doc Agent | `.agent/`, tooling, infra, dependencies; `_doc_/` X110+ kód-referencia fájlok |
| Analyst Agent | `_doc_/analysis/` — EDA notebooks, sample quality, feature/model analysis |
| Methodology Agent | `_doc_/` X000, X100 szintek — üzleti rationale, módszertani döntések, paraméter indoklás |
