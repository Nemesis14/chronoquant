# Artifact Reference

| Location | Meaning |
|---|---|
| `database/` | DuckDB databases and static yearly sample snapshots |
| `artifacts/<model_id>/` | Trained model artifacts and per-model analysis outputs |
| `artifacts/<model_id>/search/` | Search results and trial logs |
| `database/<asset>/samples/<sample_id>/` | Yearly sample metadata, audit, and `sample_train_valid.parquet` |
| `backtests/<strategy_id>/` | Strategy backtest artifacts |
| `backtests/sweep_<model_id>.csv` | Strategy sweep summary |
| `trading_reports/<run_id>/` | Runtime trading report exports |
| `logs/` | Runtime logs |

Generated artifacts should be referenced from docs, not pasted wholesale into
docs unless a human decision summary is needed.
