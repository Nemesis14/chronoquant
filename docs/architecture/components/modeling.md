# Component: Modeling

The modeling component owns dataset loading, sampling, model training, metrics,
reports, and artifact persistence.

## Modules

| Module | Responsibility |
|---|---|
| `src/modeling/datasets.py` | Load modeling frames |
| `src/modeling/sampling.py` | Build chronological splits |
| `src/modeling/lgbm_search.py` | LightGBM search |
| `src/modeling/lightgbm_model.py` | LightGBM model helpers |
| `src/modeling/metrics.py` | Metrics |
| `src/modeling/reports.py` | Model reports |
| `src/modeling/artifacts.py` | Artifact read/write helpers |

## Artifact Contract

Generated model artifacts live under `models/<model_id>/`.

