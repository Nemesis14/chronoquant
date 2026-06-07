# LightGBM Model Development And Promotion Workflow

This guide is the LightGBM-specific workflow for ChronoQuant models. General
sample/split rules live in `docs/engineering/sampling.md`; trigger and backtest
rules live in `docs/engineering/strategy_evaluation.md`.

## Overview

```text
Sampling check
-> model registry entry
-> feature audit
-> LightGBM search
-> model validation
-> final fit
-> prediction sync
-> strategy evaluation handoff
-> promotion config
-> UI verification
-> report
```

New model development should use LightGBM only. Logistic regression trainers are
legacy baselines.

## 0. Prerequisites

### 0a. Sampling Check

Before starting a new LightGBM run:

- confirm the intended `asset_id`;
- confirm the target column, for example `trg_l_fw60_q90`;
- confirm the `sample_id` and holdout policy in `samples/<sample_id>/`;
- confirm the data range and split rules using `docs/engineering/sampling.md`.

Quick check:

```bash
python -c "import json; d=json.load(open('samples/base_solusdt_fw60_dev/folds.json')); print(len(d['folds']), 'folds, test:', d['test'])"
```

### 0b. Model Registry Entry

Add the candidate to `config/models.json` with `active=false`:

```json
"lgbm_solusdt_l_fw60_q90_local_v3": {
    "asset_id": "solusdt_fw60",
    "target_name": "trg_l_fw60_q90",
    "family": "lightgbm",
    "variant": "local_v3",
    "trainer": "lightgbm_binary",
    "paths": {
        "model_dir": "models/lgbm_solusdt_l_fw60_q90_local_v3",
        "model_file": "model.pkl",
        "features_file": "features.json"
    },
    "predict": { "method": "predict_proba", "proba": true },
    "training": {
        "sample_id": "base_solusdt_fw60_dev",
        "sample_dir": "samples/base_solusdt_fw60_dev",
        "row_stride": 60
    },
    "active": false
}
```

## 1. Feature Audit

Check that planned features and targets exist in the feature table:

```bash
python scripts/feature_audit.py --asset-id solusdt_fw60
```

Manual inspection example:

```python
import sqlite3
import pandas as pd

with sqlite3.connect("database/solusdt_data_dev.db") as conn:
    cols = pd.read_sql_query("PRAGMA table_info(solusdt_1m_features)", conn)

feat_cols = cols[cols["name"].str.startswith("feat_")]
target_cols = cols[cols["name"].str.startswith("trg_")]
print(f"feat_ cols: {len(feat_cols)}")
print(target_cols["name"].tolist())
```

The LightGBM search audit should exclude:

- null-heavy features;
- effectively constant features;
- known duplicate features;
- features not available at prediction time.

The audited search feature list is written to:

```text
models/<model_id>/search/features_search.json
```

Use the same audited feature list for search and final fit.

## 2. Hyperparameter Search

### Smoke Stage

Use smoke stage to validate feature loading, fold slicing, LightGBM fitting, and
metric calculation:

```bash
python scripts/search_lgbm.py \
    --model-id lgbm_solusdt_l_fw60_q90_local_v3 \
    --stage smoke
```

### Explore Stage

Use explore for the main search:

```bash
python scripts/search_lgbm.py \
    --model-id lgbm_solusdt_l_fw60_q90_local_v3 \
    --stage explore \
    --n-trials 60
```

Resume interrupted searches with `--resume` when supported.

### Refine Stage

Use refine only after explore has found a plausible stable region:

```bash
python scripts/search_lgbm.py \
    --model-id lgbm_solusdt_l_fw60_q90_local_v3 \
    --stage refine \
    --n-trials 30
```

Refine can use a lower `row_stride` and narrower distributions around the best
explore candidates.

## 3. Search Review

Review `models/<model_id>/search/search_best.json`:

```bash
python -c "
import json
with open('models/lgbm_solusdt_l_fw60_q90_local_v3/search/search_best.json') as f:
    best = json.load(f)
print(f'Trial #{best[\"trial_no\"]}')
print(f'  mean_valid_ll:    {best[\"mean_valid_ll\"]:.4f}')
print(f'  mean_train_ll:    {best[\"mean_train_ll\"]:.4f}')
print(f'  mean_gap:         {best[\"mean_gap\"]:.4f}')
print(f'  mean_valid_prauc: {best[\"mean_valid_prauc\"]:.4f}')
print(f'  mean_valid_roc:   {best[\"mean_valid_roc\"]:.4f}')
"
```

Promotion-quality candidates should be reviewed for:

- validation log loss and train/valid gap;
- PR AUC and ROC AUC;
- top-percentile lift;
- fold-to-fold stability;
- reasonable model complexity;
- usable prediction distribution around expected strategy thresholds.

Do not promote from validation log loss alone.

## 4. Feature Importance Review

The search may not persist feature importance for every candidate. For a manual
fold-level review:

```python
import json
import sys
from pathlib import Path

import lightgbm as lgb

sys.path.insert(0, "src")
from modeling.datasets import load_modeling_dataset

MODEL_ID = "lgbm_solusdt_l_fw60_q90_local_v3"

with open(f"models/{MODEL_ID}/search/features_search.json") as f:
    features = json.load(f)["features"]
with open(f"models/{MODEL_ID}/search/search_best.json") as f:
    best = json.load(f)

ds = load_modeling_dataset(
    target_col   = "trg_l_fw60_q90",
    feature_cols = features,
    row_stride   = 60,
    asset_id     = "solusdt_fw60",
)

folds = json.loads(Path("samples/base_solusdt_fw60_dev/folds.json").read_text())["folds"]
fd = folds[-1]
mask_tr = (ds.open_time >= fd["train_start"]) & (ds.open_time < fd["train_end"])

params = {
    **best["params"],
    "objective": "binary",
    "metric": "binary_logloss",
    "verbosity": -1,
    "n_jobs": 4,
}
params["n_estimators"] = round(
    sum(f["best_iteration"] for f in best["fold_summary"]) / len(best["fold_summary"]) * 1.12
)

model = lgb.LGBMClassifier(**params)
model.fit(ds.X[mask_tr], ds.y[mask_tr], callbacks=[lgb.log_evaluation(-1)])

gain = model.booster_.feature_importance(importance_type="gain")
for feat, value in sorted(zip(features, gain), key=lambda x: -x[1])[:25]:
    print(f"{feat:<50} {value:>10.1f}")
```

Watch for zero-gain features and features that dominate only one fold.

## 5. Final Fit

Choose the final-fit data window according to `docs/engineering/sampling.md`.
The production fit may include the newest approved data after holdout review,
but it should use the selected feature list and parameters from the research
phase.

Example:

```python
import json
import pickle
import sys
from pathlib import Path

import lightgbm as lgb

sys.path.insert(0, "src")
from modeling.datasets import load_modeling_dataset

MODEL_ID = "lgbm_solusdt_l_fw60_q90_local_v3"
TRAIN_END = "2025-06-04 23:59:00"

with open(f"models/{MODEL_ID}/search/features_search.json") as f:
    features = json.load(f)["features"]
with open(f"models/{MODEL_ID}/search/search_best.json") as f:
    best = json.load(f)

mean_best_iter = sum(f["best_iteration"] for f in best["fold_summary"]) / len(best["fold_summary"])
n_est = round(mean_best_iter * 1.12)

final_params = {
    **best["params"],
    "objective": "binary",
    "boosting_type": "gbdt",
    "metric": "binary_logloss",
    "n_estimators": n_est,
    "subsample_freq": 1,
    "force_col_wise": True,
    "verbosity": -1,
    "n_jobs": 4,
}

ds = load_modeling_dataset(
    target_col   = "trg_l_fw60_q90",
    feature_cols = features,
    row_stride   = 60,
    asset_id     = "solusdt_fw60",
    end          = TRAIN_END,
)

model = lgb.LGBMClassifier(**final_params)
model.fit(ds.X, ds.y, callbacks=[lgb.log_evaluation(-1)])

out = Path(f"models/{MODEL_ID}")
out.mkdir(parents=True, exist_ok=True)
with open(out / "model.pkl", "wb") as f:
    pickle.dump(model, f)
with open(out / "features.json", "w") as f:
    json.dump({"features": features, "input_features": features}, f, indent=4)
with open(out / "params.json", "w") as f:
    json.dump(final_params, f, indent=4)
```

Verify artifact existence:

```bash
python -c "
from pathlib import Path
p = Path('models/lgbm_solusdt_l_fw60_q90_local_v3')
for name in ['model.pkl', 'features.json', 'params.json']:
    fp = p / name
    print(name, fp.stat().st_size if fp.exists() else 'MISSING')
"
```

## 6. Promotion Config

After validation and holdout review, update model/runtime config.

`config/models.json`:

```python
import json
from pathlib import Path

path = Path("config/models.json")
cfg = json.loads(path.read_text())

OLD_ID = "lgbm_solusdt_l_fw60_q90_local_v2"
MODEL_ID = "lgbm_solusdt_l_fw60_q90_local_v3"

cfg["models"][OLD_ID]["active"] = False
cfg["models"][MODEL_ID]["active"] = True

path.write_text(json.dumps(cfg, indent=4), encoding="utf-8")
```

`config/env.json`:

```python
path = Path("config/env.json")
cfg = json.loads(path.read_text())
cfg["runtime"]["models"]["solusdt_fw60"] = MODEL_ID
path.write_text(json.dumps(cfg, indent=4), encoding="utf-8")
```

## 7. Prediction Sync

For manual promotion checks, sync only a bounded range. Avoid loading millions
of rows into memory when a short verification window is enough.

```python
import sys
sys.path.insert(0, "src")

from data_pipeline.sync_predictions import sync_predictions

sync_predictions(
    start_time = "2025-06-01 00:00:00",
    end_time   = None,
    asset_id   = "solusdt_fw60",
)
```

Verification:

```python
import sqlite3
import pandas as pd

with sqlite3.connect("database/solusdt_data_dev.db") as conn:
    df = pd.read_sql_query(
        "SELECT open_time, prediction FROM solusdt_1m_predictions ORDER BY open_time DESC LIMIT 3",
        conn,
    )
print(df)
```

## 8. Strategy Evaluation Handoff

Trigger selection and backtesting are covered by
`docs/engineering/strategy_evaluation.md`.

For the LGBM workflow, hand off:

- promoted `model_id`;
- `asset_id`;
- prediction availability range;
- relevant sample and holdout boundaries;
- intended side (`long` or `short`).

Example sweep command for the strategy evaluation phase:

```bash
python scripts/sweep_strategy.py \
    --model-id lgbm_solusdt_l_fw60_q90_local_v3 \
    --asset-id solusdt_fw60 \
    --start 2024-01-01 \
    --end 2025-12-31 \
    --side long \
    --top-n 20
```

Update `config/strategies.json` only after the strategy guide's selection,
robustness, and holdout checks are complete.

## 9. UI Verification

```python
import sys
sys.path.insert(0, "src")

from streamlit_app.data import load_dashboard_config

cfg = load_dashboard_config(asset_id="solusdt_fw60")
assert cfg["runtime_model_id"] == "lgbm_solusdt_l_fw60_q90_local_v3"
assert cfg["strategy_id"] == "solusdt_long_fw60_q90_local_v3"
print("entry_threshold:", cfg["strategy"]["entry_threshold"])
```

Confirm the dashboard shows the intended model, active strategy, latest
prediction timestamp, and strategy/backtest summary.

## 10. Report

Store completed analysis under `docs/plans/completed/` or `docs/analysis/`.

Minimum report contents:

- data range, sample ID, and holdout range;
- search stage settings and trial count;
- best hyperparameters and why they were accepted;
- CV metrics per fold and aggregate;
- final holdout metrics;
- top feature importance and zero-gain feature notes;
- final model artifact paths;
- strategy evaluation summary and artifact paths;
- config changes.

## Artifact Reference

| File | Role |
|------|------|
| `config/models.json` | Model registry and active flags |
| `config/env.json` | Runtime model per asset |
| `config/strategies.json` | Strategy config selected by UI |
| `models/<id>/model.pkl` | Saved LightGBM object |
| `models/<id>/features.json` | Feature list for prediction |
| `models/<id>/params.json` | Final fit parameters |
| `models/<id>/search/search_best.json` | Best search trial |
| `models/<id>/search/search_trials.jsonl` | Search trial history |
| `models/<id>/search/features_search.json` | Audited search feature list |
| `backtests/<strategy_id>/` | Strategy evaluation artifacts |
| `backtests/sweep_<model_id>.csv` | Strategy sweep results |
| `src/modeling/lgbm_search.py` | Search engine |
| `scripts/search_lgbm.py` | Search CLI wrapper |
| `scripts/sweep_strategy.py` | Strategy sweep CLI |
| `src/evaluation/backtest.py` | Backtest engine |

## Common Issues

| Issue | Likely Cause | Fix |
|-------|--------------|-----|
| `MemoryError` during feature load | Too many rows times features | Increase `row_stride` or use chunked loading |
| Feature missing from search | Audit excluded it | Review null rate, std, and duplicate rules |
| Zero completed trials | Every trial failed | Inspect failed trial logs |
| Poor holdout despite good CV | Overfit or regime-specific candidate | Reject or rerun search with stricter guardrails |
| UI does not show new model | Runtime config not updated | Check `config/env.json` and dashboard config loading |
| UI does not show new strategy | Strategy resolver chose another entry | Check `config/strategies.json` ordering/resolver logic |
