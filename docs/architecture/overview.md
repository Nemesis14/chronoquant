# ChronoQuant Architecture Overview

ChronoQuant is organized around deterministic market data ingestion, feature
generation, model training, prediction sync, backtesting, and a Streamlit
monitoring dashboard.

## Config Layer

`src/utils.py` is the single config-loading entry point. Modules should call
helpers such as `load_asset_config()`, `load_models_config()`, and
`load_features_config()` instead of reading JSON config files directly.

Key config files live under `config/`:

- `assets.json`: asset definitions, database paths, and table names.
- `db.json`: legacy default asset config and active environment.
- `features.json`: indicator definitions and per-asset feature profiles.
- `models.json`: model registry and trainer configuration.
- `strategies.json`: backtest strategy definitions.
- `env.json`: runtime model override and environment selection.

## Database Layer

DuckDB databases are organized per asset. Each asset has raw
OHLCV, derived features, and prediction tables keyed by `open_time`.

Important modules:

- `src/data_handling/store/duckdb_store.py`: low-level DDL and insert helpers.
- `src/data_handling/store/toolkit.py`: inspection helpers.
- `src/data_handling/02_sync_pipeline.py`: sync orchestration.

## Data Pipeline

`src/data_handling/sync_tables/` owns idempotent sync stages:

- `sync_ohlcv.py`: fetch raw 1m OHLCV bars.
- `sync_features.py`: compute configured indicators and targets.
- `sync_predictions.py`: load model artifacts and write live predictions.

Detailed data documentation lives under `_doc_/1000_database.md`.

## Modeling

`src/modeling/` owns dataset loading, sampling, training, metrics, reports, and
artifacts. The active yearly modeling flow uses LightGBM.

Model artifacts are saved under `artifacts/<model_id>/`.

Modeling workflow documentation lives under `_doc_/5000_modelling.md`.

## Evaluation

`src/modeling/evaluation/` owns model-independent strategy evaluation and backtesting.
Backtest outputs include trades, equity curves, summaries, and reports.

Evaluation workflow documentation lives under `docs/evaluation/`.

## Streamlit Dashboard

`src/ui/` owns the live monitoring dashboard. It chains OHLCV,
feature, and prediction sync at runtime and renders status, logs, price, and
prediction charts.

## Live Trading

`src/trading/` owns trading decisions, runtime state, exchange integration, and
journal persistence. Operational state is stored in `database/trading.db`; report
exports are stored under `trading_reports/`.

## Time Handling

All stored timestamps are UTC strings in `YYYY-MM-DD HH:MM:SS` format. Epoch
milliseconds are used for Binance API calls. Do not use local time in stored
data.
