# Script Reference

## Data Pipeline

| Script | Purpose |
|---|---|
| `scripts/sync_ohlcv.py` | Sync raw OHLCV |

## Modeling

| Script | Purpose |
|---|---|
| `scripts/create_sample_splits.py` | Create chronological sample/fold definitions |
| `scripts/search_lgbm.py` | Run LightGBM search |
| `scripts/generate_model_card.py` | Generate model card artifact |

## Evaluation

| Script | Purpose |
|---|---|
| `scripts/sweep_strategy.py` | Strategy threshold sweep |
| `scripts/backtest_strategy.py` | Run configured strategy backtest |

## Elliott

Legacy SQLite Elliott scripts were removed. Use maintained `src/` entry points
or create a new script against the current Parquet/DuckDB data layer.
