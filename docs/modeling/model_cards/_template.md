# Model Card: `<model_id>`

## Summary

| Field | Value |
|---|---|
| Model ID | |
| Asset ID | |
| Side | |
| Target | |
| Model family | |
| Trainer | |
| Status | candidate / active / retired |

## Intended Use

Describe what decision this model supports and where it is consumed.

## Data

| Field | Value |
|---|---|
| Sample ID | |
| Source table | |
| Data start | |
| Data end | |
| Holdout start | |
| Holdout end | |
| Row stride | |

## Features

- Feature count:
- Feature source:
- Excluded feature notes:
- Live-safety notes:

## Training

- Search stage:
- Trial count:
- Best trial:
- Final fit window:
- Important parameters:

## Metrics

| Metric | Train | Validation | Holdout |
|---|---:|---:|---:|
| Log loss | | | |
| PR AUC | | | |
| ROC AUC | | | |
| Lift | | | |

## Strategy Handoff

- Strategy IDs:
- Sweep artifacts:
- Selected trigger:
- Holdout report:

## Limitations

- Known weak regimes:
- Data limitations:
- Monitoring requirements:

## Artifacts

| Artifact | Path |
|---|---|
| Model | `models/<model_id>/model.pkl` |
| Features | `models/<model_id>/features.json` |
| Params | `models/<model_id>/params.json` |
| Search best | `models/<model_id>/search/search_best.json` |

