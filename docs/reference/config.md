# Config Reference

| File | Purpose |
|---|---|
| `config/assets.json` | Asset definitions, database paths, table names |
| `config/db.json` | Legacy/default database config |
| `config/features.json` | Feature and target definitions |
| `config/models.json` | Current model registry |
| `config/model_registry.json` | Legacy/additional model registry data |
| `config/model_params.json` | Legacy/additional parameter config |
| `config/predictions.json` | Prediction/runtime settings |
| `config/strategies.json` | Strategy definitions |
| `config/trading.json` | Trading runtime settings |
| `config/env.json` | Runtime environment/model override |

Rule: reusable business logic should load config through `src/utils.py`.

