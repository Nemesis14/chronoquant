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

SQLite databases are organized per asset and environment. Each asset has raw
OHLCV, derived features, and prediction tables keyed by `open_time`.

Important modules:

- `src/db/table_ops.py`: low-level DDL and upsert helpers.
- `src/db/toolkit.py`: inspection helpers.
- `src/db/maintenance.py`: derived table rebuild orchestration.

## Data Pipeline

`src/data_pipeline/` owns idempotent sync stages:

- `sync_ohlcv.py`: fetch raw 1m OHLCV bars.
- `sync_features.py`: compute configured indicators and targets.
- `sync_predictions.py`: load model artifacts and write live predictions.

## Modeling

`src/modeling/` owns dataset loading, sampling, training, metrics, reports, and
artifacts. Supported trainers include statsmodels p-value logistic regression,
L1 logistic regression, and LightGBM.

Model artifacts are saved under `models/<model_id>/`.

## Evaluation

`src/evaluation/` owns model-independent strategy evaluation and backtesting.
Backtest outputs include trades, equity curves, summaries, and reports.

## Streamlit Dashboard

`src/streamlit_app/` owns the live monitoring dashboard. It chains OHLCV,
feature, and prediction sync at runtime and renders status, logs, price, and
prediction charts.

## Time Handling

All stored timestamps are UTC strings in `YYYY-MM-DD HH:MM:SS` format. Epoch
milliseconds are used for Binance API calls. Do not use local time in stored
data.
