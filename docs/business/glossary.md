# Glossary

| Term | Meaning |
|---|---|
| `asset_id` | Internal asset key, for example `solusdt_fw60` |
| OHLCV | Open, high, low, close, volume market bar |
| Feature | Model input column, usually prefixed with `feat_` |
| Target | Supervised label column, usually prefixed with `trg_` |
| Horizon | Forward-looking period used by target, for example 60 minutes |
| Holdout | Final untouched evaluation window |
| Embargo | Gap between train and validation/test windows to prevent leakage |
| Model card | Per-model documentation of data, training, metrics, limits, and use |
| Strategy sweep | Search over trigger/holding parameters for a model |
| Runtime model | Model currently used for dashboard/trading predictions |
| Candidate model | Model under evaluation, not promoted to runtime |

