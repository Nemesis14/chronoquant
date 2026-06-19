# Component: Data Pipeline

The data pipeline owns deterministic market data ingestion, feature generation,
and runtime prediction sync.

## Modules

| Module | Responsibility |
|---|---|
| `src/data_handling/sync_tables/sync_ohlcv.py` | Fetch and upsert raw OHLCV bars |
| `src/data_handling/sync_tables/sync_features.py` | Compute configured features |
| `src/data_handling/sync_tables/sync_targets.py` | Compute fw60 target outcomes |
| `src/data_handling/sync_tables/sync_predictions.py` | Load runtime model artifacts and write predictions |
| `src/data_handling/store/duckdb_store.py` | DDL and insert/rebuild helpers |

## Data Contracts

- Raw bars are keyed by `open_time`.
- Derived features and predictions join on `open_time`.
- Config is loaded through `src/utils.py`.
- Timestamps are stored as UTC strings.
