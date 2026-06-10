# Component: Data Pipeline

The data pipeline owns deterministic market data ingestion, feature generation,
and runtime prediction sync.

## Modules

| Module | Responsibility |
|---|---|
| `src/data_pipeline/sync_ohlcv.py` | Fetch and upsert raw OHLCV bars |
| `src/data_pipeline/sync_features.py` | Compute configured targets and features |
| `src/data_pipeline/sync_predictions.py` | Load runtime model artifacts and write predictions |
| `src/db/maintenance.py` | Rebuild derived tables |
| `src/db/table_ops.py` | DDL and upsert helpers |

## Data Contracts

- Raw bars are keyed by `open_time`.
- Derived features and predictions join on `open_time`.
- Config is loaded through `src/utils.py`.
- Timestamps are stored as UTC strings.

