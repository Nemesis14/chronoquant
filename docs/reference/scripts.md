# Script Reference

## Data Pipeline

| Script | Purpose |
|---|---|
| `scripts/sync_ohlcv.py` | Sync raw OHLCV |
| `scripts/rebuild_derived_tables.py` | Rebuild features/predictions |
| `scripts/export_sample_parquet.py` | Export sample for remote training |
| `scripts/feature_audit.py` | Audit feature table |

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

| Script | Purpose |
|---|---|
| `scripts/plot_elliott.py` | Plot Elliott setups |
| `scripts/backtest_elliott.py` | Elliott walk-forward/sweep evaluation |

