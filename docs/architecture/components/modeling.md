# Component: Modeling

The modeling component owns dataset loading, sampling, model training, metrics,
reports, and artifact persistence.

## Modules

| Module | Responsibility |
|---|---|
| `src/modeling/sampling/` | Yearly sample creation |
| `src/modeling/search/lgbm_search.py` | LightGBM hyperparameter search |
| `src/modeling/training/` | Fit, datasets, metrics, reports, artifacts |
| `src/modeling/evaluation/` | Backtest and evaluation helpers |
| `src/modeling/feature_engineering/` | Feature quality and selection helpers |

## Artifact Contract

Generated model artifacts live under `artifacts/<model_id>/`.
