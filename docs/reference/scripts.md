# Script Reference

## Data Pipeline

| Script | Purpose |
|---|---|
| `src/data_handling/02_sync_pipeline.py` | Run OHLCV/features/targets/predictions sync pipeline |

## Modeling

| Script | Purpose |
|---|---|
| `src/modeling/00_create_sample.py` | Create yearly random-hour sample artifacts |
| `src/modeling/02_hyper_param_search.py` | Run LightGBM hyperparameter search |
| `src/modeling/03_fit_model.py` | Train final model, write model.pkl + sample_oos.parquet |

## Evaluation

No maintained standalone evaluation script is currently documented here.
Use `src/modeling/evaluation/backtest.py` APIs from the modeling/evaluation flow.

## Elliott

Legacy SQLite Elliott scripts were removed. Use maintained `src/` entry points
or create a new script against the current Parquet/DuckDB data layer.
