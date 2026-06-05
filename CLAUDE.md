# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

All scripts are run from the repo root. `src/` is always on `sys.path` (inserted by each script).

```bash
# Run tests
pytest tests/

# Run a single test file
pytest tests/test_modeling_metrics.py

# Run a single test by name
pytest tests/test_smoke.py::test_config_loads

# Launch the Streamlit dashboard
streamlit run src/streamlit_app/main.py

# Sync OHLCV data from Binance
python scripts/sync_ohlcv.py

# Rebuild features and predictions tables from existing OHLCV
python scripts/rebuild_derived_tables.py --drop
python scripts/rebuild_derived_tables.py --features-only --asset-id solusdt_fw60

# Create sample splits for model training
python scripts/create_sample_splits.py --sample-id base_fw240_dev
python scripts/create_sample_splits.py --sample-id base_solusdt_fw60_dev --asset-id solusdt_fw60 --target-horizon-minutes 60

# Train a model (model_id from config/models.json)
python scripts/train_model.py --model-id logit_l_fw240_q90_pval_v1

# Run a backtest (strategy_id from config/strategies.json)
python scripts/backtest_strategy.py lasso_long_fw240_q90_managed_v1
```

## Architecture

### Config layer (`config/`, `src/utils.py`)

`src/utils.py` is the single config-loading entry point. All modules call helpers like `load_asset_config(asset_id)`, `load_models_config()`, `load_features_config()`, etc. Never read JSON config files directly.

Key configs:
- `config/assets.json` — defines assets (`bchusdt_fw240`, `solusdt_fw60`), their SQLite DB paths (dev/prod), and table names
- `config/db.json` — legacy default asset config; `active_env` controls dev vs prod DB
- `config/features.json` — indicator definitions (momentum, trend, volatility, volume, price_action, market_structure) and per-asset feature profiles
- `config/models.json` — model registry: trainer type, target column, file paths, training hyperparameters, `active` flag
- `config/strategies.json` — backtest strategy configs: model, date range, entry/exit thresholds, TP/SL/trailing, fees
- `config/env.json` — runtime model override (`runtime.model_id`); determines which active model is the live model

Asset IDs (`bchusdt_fw240`, `solusdt_fw60`) flow through everything. Pass `asset_id=None` to use the default (BCHUSDT). Always pass `asset_id` explicitly when working with multi-asset code.

### Database layer (`src/db/`)

SQLite databases, one per asset per env. Three tables per asset:
- `{symbol}_1m` — raw OHLCV bars (primary key: `open_time`)
- `{symbol}_1m_features` — technical indicators + target columns; all feature columns prefixed `feat_`; target columns prefixed `trg_`
- `{symbol}_1m_predictions` — live model predictions

`src/db/table_ops.py` — low-level DDL helpers (ensure columns, drop rows by open_time, upsert).  
`src/db/toolkit.py` — inspection and notebook helpers (`list_tables`, `print_tables_summary`, `tail`, `validate_open_time`).  
`src/db/maintenance.py` — `rebuild_derived_tables()` orchestrates full or partial feature/prediction rebuilds.

### Data pipeline (`src/data_pipeline/`)

Three independent sync modules, each idempotent by `open_time`:
- `sync_ohlcv.py` — fetches 1m OHLCV bars from Binance API, inserts into ohlcv table
- `sync_features.py` — reads OHLCV, computes all configured technical indicators and targets, writes to features table
- `sync_predictions.py` — loads saved model artifact, runs inference on the features table, writes probability to predictions table

The Streamlit app chains all three at runtime via `src/streamlit_app/sync.py`.

### Modeling (`src/modeling/`)

Three supported trainers, configured per model in `config/models.json`:
- `statsmodels_pvalue_logreg` — iterative p-value elimination logistic regression (`src/modeling/statsmodels_logreg.py`)
- `sklearn_lasso_logreg` — L1 logistic regression with alpha sweep (`src/modeling/lasso_logreg.py`)
- `lightgbm_binary` — LightGBM with `num_leaves` tuning (`src/modeling/lightgbm_model.py`)

Training flow:
1. `scripts/create_sample_splits.py` → generates expanding-window CV splits → `samples/<sample_id>/`
2. `scripts/train_model.py` → calls `src/modeling/train.py` → dispatches to trainer → saves `model.pkl` + `features.json` + HTML report to `models/<model_id>/`

`src/modeling/datasets.py` — `load_modeling_dataset()` loads aligned X/y from the features table with optional embargo and row stride.  
`src/modeling/sampling.py` — `create_expanding_window_splits()` for time-series CV.  
`src/modeling/artifacts.py` — save/load training metadata.  
`src/modeling/metrics.py` — shared evaluation metrics (ROC-AUC, PR-AUC, Brier, log-loss).  
`src/modeling/reports.py` — writes standalone HTML training reports.

### Backtesting (`src/evaluation/backtest.py`)

`run_configured_strategy(strategy_id)` loads a strategy from `config/strategies.json` and simulates a LONG/FLAT strategy. Entry is on probability threshold crossing; exits via take-profit %, stop-loss %, trailing stop, max-hold time, or probability falling below exit threshold. Outputs `trades.csv`, `equity_curve.csv`, `summary.json`, and `report.html` to the strategy's `output_dir`.

### Streamlit dashboard (`src/streamlit_app/`)

Live monitoring dashboard. `main.py` is the entry point. Runs an auto-sync loop (every N seconds) that chains ohlcv → features → predictions. Uses `@st.fragment(run_every="2s")` for the sync status panel and log panel without full rerenders.

- `data.py` — DB reads for the dashboard (latest prediction, prediction history)
- `sync.py` — orchestrates the three-stage sync pipeline
- `sync_runner.py` — manages background sync state in `st.session_state`
- `dashboard_logging.py` — rotating log file read by the log panel
- `components/charts.py` — Plotly price + prediction chart
- `components/formatting.py` — number formatting helpers

### Target and model naming conventions

Targets: `trg_{direction}_fw{window}_{quantile}` (e.g. `trg_l_fw240_q90` = long, 240-bar forward window, 90th percentile).  
Models: `{family}_{asset?}_{direction}_fw{window}_{quantile}_{variant}_v{n}` (e.g. `logit_l_fw240_q90_pval_v1`).  
Prediction columns in the predictions table: `{model_id}_p`.

### Time handling

All timestamps stored as `"YYYY-MM-DD HH:MM:SS"` UTC strings (no timezone suffix). Epoch milliseconds used for Binance API. Helpers in `src/utils.py`: `now_utc_ms()`, `now_utc_str()`, `ms_to_utc_str()`, `utc_str_to_ms()`. Never use local time.
