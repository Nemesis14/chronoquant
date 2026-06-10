# Artifact Reference

| Location | Meaning |
|---|---|
| `database/` | SQLite databases |
| `models/<model_id>/` | Trained model artifacts |
| `models/<model_id>/search/` | Search results and trial logs |
| `samples/<sample_id>/` | Modeling sample metadata, folds, optional parquet |
| `backtests/<strategy_id>/` | Strategy backtest artifacts |
| `backtests/sweep_<model_id>.csv` | Strategy sweep summary |
| `trading_reports/<run_id>/` | Runtime trading report exports |
| `logs/` | Runtime logs |

Generated artifacts should be referenced from docs, not pasted wholesale into
docs unless a human decision summary is needed.

