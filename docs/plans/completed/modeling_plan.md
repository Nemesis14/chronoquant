# ChronoQuant Modeling Plan

## Goal

Build a cleaner modeling workflow around a wide feature base and L1-regularized
logistic regression. The feature table should contain a broad but meaningful
candidate feature space. Feature selection should happen inside the model
training workflow through regularization and cross-validation.

The feature base should favor distinct metric types over many near-duplicate
window variants. For each metric family, use at most a small set of informative
windows, typically no more than four, unless a specific modeling result justifies
more.

The same sampling, cross-validation folds, metrics, and cut-off analysis should
be reusable across long and short models. Model-specific code should only define
the target, model family, hyperparameter grid, and output location.

## Principles

- Keep OHLCV as the immutable base table.
- Keep features as deterministic derived columns keyed by `open_time`.
- Do not duplicate rows during sync or rebuild.
- Do not add separate manual feature filters before Lasso selection.
- Use L1 logistic regression as the first feature-selection model.
- Use the same CV split definitions for all comparable models.
- Use the same metric definitions for all model families.
- Keep cut-off analysis model-independent: it receives facts and prediction
  columns, then returns thresholds and validation metrics.
- Keep scripts thin; reusable logic belongs under `src/`.

## Proposed Structure

```text
src/
  data_pipeline/
    sync_ohlcv.py
    sync_features.py
    sync_predictions.py
  db/
    maintenance.py
    table_ops.py
    toolkit.py
  modeling/
    datasets.py
    sampling.py
    training_windows.py
    metrics.py
    artifacts.py
    reports.py
    statsmodels_logreg.py
    lasso_logreg.py
    lightgbm_model.py
    train.py
    registry.py
  evaluation/
    cutoff.py
    backtest.py
  streamlit_app/
    main.py
    data.py
    pages/
scripts/
  rebuild_derived_tables.py
  train_model.py
  update_prediction_signals.py
models/
  <model_id>/
    model.pkl
    scaler.pkl
    features.json
    metrics.json
    params.json
    cv_results.csv
    report.html
config/
  features.json
  models.json
  model_params.json
```

Notes:
- `src/modeling/` owns data preparation, sampling, CV, training, model artifacts,
  and metric calculation.
- `src/evaluation/` owns model-independent post-prediction evaluation such as
  cut-off selection and signal validation.
- `scripts/` only parses CLI args and calls `src` functions.

## Task 1: Feature Base Expansion

Purpose: define and compute the full candidate feature space in the features
table.

Status: **done for the first production-ready expansion**.

Implemented result:
- `config/features.json` now defines the first expanded feature base.
- `src/data_pipeline/sync_features.py` now computes features from full OHLCV:
  `open`, `high`, `low`, `close`, `volume`.
- The dev features table was rebuilt successfully.
- Current dev DB result:
  - rows: 3,407,807
  - duplicate `open_time`: 0
  - total columns: 51
  - feature columns: 47

Interpretation:
- This is not intended to be a 200+ column feature grid.
- The current feature set is a stable first base with diverse metric types.
- Further feature expansion should happen later as a backlog task and should add
  genuinely different metric families, not many almost-identical windows.

Implementation tasks:
- [x] Extend `config/features.json` with candidate feature definitions.
- [x] Update `src/data_pipeline/sync_features.py` to compute all configured
  features.
- [x] Include OHLCV fields required by technical indicators: `open`, `high`,
  `low`, `close`, `volume`.
- [x] Keep current target columns unchanged:
  - `trg_l_fw240_q90`
  - `trg_s_fw240_q10`
- [x] Keep feature names consistently prefixed with `feat_`.
- [x] Keep feature generation deterministic from OHLCV and config.
- [x] Rebuild derived features after implementation and verify:
  - `open_time` is unique in base/features/predictions.
  - row count alignment is expected.
  - no duplicate feature columns.

Initial feature groups:
- Momentum: RSI, ROC, stochastic, CCI, Williams R, ADX.
- Trend: SMA ratio, EMA ratio, WMA ratio, KAMA, MACD diff/signal/hist.
- Volatility: Bollinger width/position, ATR, normalized ATR, historical vol.
- Volume: volume SMA, volume ratio, OBV, OBV ROC, MFI, AD line, CMF.
- Price action: log return, rolling return mean/std, high-low range, close
  position, skewness, kurtosis.
- Market structure: higher high, higher low, lower high, lower low, swing high,
  swing low.

Out of scope for first pass:
- Multi-timeframe features requiring 5m/15m derived tables.
- Exchange order-book or trade-level microstructure features.
- Dense window grids where each metric is repeated across many similar windows.

Future feature expansion rules:
- Add a feature only when it represents a distinct metric concept or a clearly
  different transformation.
- Avoid adding variables that are almost the same measurement with a slightly
  different window.
- For metrics where windowing is useful, target up to four windows. Suggested
  default candidates:
  - short: 5 or 10
  - standard: 14 or 20
  - medium: 50
  - long: 140 or 200
- Do not force every metric to use all windows. Some metrics should have one or
  two natural windows only.
- Let Lasso decide among the resulting candidates, but keep the candidate set
  meaningful and interpretable.

## Backlog: Additional Feature Families

Purpose: expand the feature base later with distinct metrics, not a large
near-duplicate window grid.

Candidate backlog groups:
- Regime metrics:
  - rolling volatility percentile.
  - trend strength regime.
  - volume regime.
- Distance metrics:
  - distance from rolling high/low.
  - distance from rolling VWAP-like proxy.
  - distance from moving average bands.
- Shape metrics:
  - candle body ratio.
  - upper/lower wick ratio.
  - gap-like one-minute displacement metrics.
- Momentum change metrics:
  - first difference of selected momentum indicators.
  - acceleration for a small subset of momentum metrics.
- Cross-metric interactions:
  - volatility-adjusted momentum.
  - volume-adjusted return.
  - trend-strength-adjusted return.

Backlog acceptance criteria:
- Each added metric has a short rationale.
- Each metric uses at most four windows.
- No metric is added only because another nearby window exists.
- The expanded config remains readable.
- Feature count can grow, but interpretability and metric diversity matter more
  than reaching a fixed column target.

## Task 2: Modeling Dataset Builder

Purpose: provide one reusable way to load aligned modeling data for any target
and feature set.

Status: **done for the first reusable implementation**.

Implemented:
- `src/modeling/datasets.py`
- `ModelingDataset`
- `load_modeling_dataset(...)`

The dataset builder reads from the existing SQLite features table. It does not
create a separate SQL train table. This avoids duplicating millions of rows and
keeps `bchusdt_1m_features` as the source of truth.

Responsibilities:
- [x] Load features table from SQLite.
- [x] Select target column and all `feat_` columns.
- [x] Sort by `open_time`.
- [x] Drop rows with missing target.
- [x] Apply a configurable embargo at the end of the sample if needed, because the
  target uses future rolling windows.
- [x] Return a structured dataset object:
  - `open_time`
  - `X`
  - `y`
  - `target_col`
  - `feature_cols`
- [x] Keep this model-family independent.

Important rule:
- Dataset building must not perform feature selection. It prepares the full
  feature matrix; the model workflow decides what survives.

## Task 3: Shared Sampling and CV Splits

Purpose: all model variants should train and validate on identical samples.

Status: **done for the first reusable implementation**.

Implemented:
- `src/modeling/sampling.py`
- `scripts/create_sample_splits.py`
- `samples/base_fw240_dev/folds.json`
- `samples/base_fw240_dev/metadata.json`

Current generated sample:
- `sample_id`: `base_fw240_dev`
- source table: `bchusdt_1m_features`
- data range: `2019-11-28 10:00:00 -> 2026-05-23 14:30:00`
- split type: expanding window
- target horizon / embargo: 240 minutes
- CV folds: 7
- final test range: `2025-05-23 14:30:00 -> 2026-05-23 14:30:00`

These files are intentionally model-independent. Long, short, Lasso, and future
model families should all use the same `sample_id` when we want comparable
results.

Responsibilities:
- [x] Generate deterministic time-based train/validation/test splits.
- [x] Generate deterministic CV folds for model selection.
- [x] Persist split definitions so long and short models can reuse the same fold
  boundaries.
- [x] Avoid random shuffled CV for time series unless explicitly testing a
  non-time-dependent baseline.

Recommended default:
- Use rolling or expanding time-series CV.
- Keep the final test period untouched by hyperparameter selection.
- Store fold definitions by `sample_id`, for example:
  - `samples/base_fw240_2017_2026/folds.json`
  - `samples/base_fw240_2017_2026/metadata.json`

Suggested split artifact:

```json
{
  "sample_id": "base_fw240_2017_2026",
  "target_horizon_minutes": 240,
  "folds": [
    {
      "fold": 1,
      "train_start": "2017-01-01 00:00:00",
      "train_end": "2021-12-31 23:59:00",
      "valid_start": "2022-01-01 00:00:00",
      "valid_end": "2022-06-30 23:59:00"
    }
  ],
  "test": {
    "start": "2025-01-01 00:00:00",
    "end": "2026-01-31 00:00:00"
  }
}
```

Sampling tasks:
- [x] Implement a split generator.
- [x] Add a script to create/recreate fold files.
- [x] Add tests that verify:
  - no overlap between train/validation/test.
  - folds are sorted by time.
  - long and short models can load the same `sample_id`.

## Task 4: Shared Metrics

Purpose: every model type reports metrics with the same definitions.

Status: **done for the first reusable implementation**.

Implemented:
- `src/modeling/metrics.py`
- `binary_classification_metrics(...)`
- `lift_at_percentiles(...)`
- `calibration_table(...)`

Metrics to compute:
- [x] ROC AUC.
- [x] PR AUC.
- [x] log loss.
- [x] Brier score.
- [x] baseline positive rate.
- [x] lift at fixed top percentiles, for example top 1%, 5%, 10%.
- [x] calibration table by prediction decile.

Primary metric recommendation:
- Use **PR AUC** as the main model-selection metric when the target is around
  10/90, because it focuses more directly on the minority positive class.
- Keep ROC AUC as a secondary metric because it is still useful for comparing
  ranking quality.
- Use Brier score as a calibration metric. It answers a different question:
  whether predicted probabilities are numerically close to observed 0/1 facts.
  Lower is better. It is useful in reports, but it is not the primary model
  selection metric for the first Lasso workflow.

Metric tasks:
- [x] Implement one function that accepts `y_true` and `y_pred`.
- [x] Return a plain dict suitable for JSON serialization.
- [x] Ensure the same metric function is used by long, short, and future model
  families.

## Task 5: L1 Logistic Regression Training

Purpose: implement the first standardized model workflow using L1-regularized
logistic regression.

Status: **implemented for the first reusable workflow**.

Implemented:
- `src/modeling/lasso_logreg.py`
- long model first training run:
  - model id: `logit_l_fw240_q90_l1_v1`
  - sample id: `base_fw240_dev`
  - alpha grid: `30.0`, `60.0`, `100.0`, `150.0`, `220.0`, `330.0`,
    `500.0`, `750.0`, `1000.0`
  - selected alpha: `100.0`
  - selected features: 17 / 47
  - report: `models/logit_l_fw240_q90_l1_v1/report.html`
- short model first training run:
  - model id: `logit_s_fw240_q10_l1_v1`
  - sample id: `base_fw240_dev`
  - alpha grid: `30.0`, `60.0`, `100.0`, `150.0`, `220.0`, `330.0`,
    `500.0`, `750.0`, `1000.0`
  - selected alpha: `100.0`
  - selected features: 20 / 47
  - report: `models/logit_s_fw240_q10_l1_v1/report.html`

Important training note:
- The first interactive training run uses `row_stride=60`. The CV fold time
  windows are unchanged, but the loaded rows are deterministically thinned for
  runtime. This is suitable for first model comparison and report generation.
- A later full-data fit can use `row_stride=1` after the alpha grid is narrowed.

Responsibilities:
- [x] Accept a prepared dataset and fold definitions.
- [x] Standardize features using `StandardScaler`.
- [x] Tune regularization strength over a configured grid.
- [x] Use L1 logistic regression:
  - `sklearn.linear_model.LogisticRegression`
  - `l1_ratio=1.0`
  - `solver="liblinear"` for the first binary Lasso workflow
  - `class_weight` configurable.
- [x] Select the regularization parameter by max validation PR AUC by default.
- [x] Store all CV results.
- [x] Refit the final model on the selected training window.
- [x] Store selected features where coefficient is non-zero.
- [x] Save:
  - model
  - preprocessing pipeline
  - selected features
  - CV metrics
  - final test metrics
  - training metadata
  - HTML training report

Regularization direction:
- Start with weak regularization and increase regularization strength.
- Track validation PR AUC and number of selected features.
- Choose the strongest model that is within an acceptable tolerance of the best
  PR AUC if we want a smaller, more stable feature set.

Open decision:
- Exact selection rule:
  - strict max PR AUC, or
  - one-standard-error style rule for a simpler model.

## Task 6: Generic Training Orchestrator

Purpose: avoid separate long/short scripts with duplicated modeling code.

Status: **implemented for Lasso and LightGBM training**.

Implemented:
- `src/modeling/train.py`
- `scripts/train_model.py`
- Lasso and LightGBM candidate entries in `config/models.json`.

Responsibilities:
- [x] Load model config from `config/models.json`.
- [x] Load dataset by `target_name`.
- [x] Load sample splits by `sample_id`.
- [x] Dispatch to the configured model family.
- [x] Save artifacts under `models/<model_id>/`.

Create `scripts/train_model.py`.

CLI examples:

```powershell
uv run python scripts/train_model.py --model-id logit_l_fw240_q90_l1_v1
uv run python scripts/train_model.py --model-id logit_s_fw240_q10_l1_v1
```

Model config should define only model-specific information:
- target name.
- model family.
- sample id.
- hyperparameter grid.
- output path.
- active flag.

## Task 6A: LightGBM Candidate Training

Purpose: add the first tree-based candidate model while keeping data loading,
splits, metrics, artifacts, and reports shared with the Lasso workflow.

Status: **implemented for the first reusable workflow**.

Implemented:
- `config/model_params.json`
- `src/modeling/lightgbm_model.py`
- shared report generation in `src/modeling/reports.py`
- shared artifact writing in `src/modeling/artifacts.py`
- shared CV/final split slicing in `src/modeling/training_windows.py`
- LightGBM candidate entries in `config/models.json`:
  - `lgbm_l_fw240_q90_stable_v1`
  - `lgbm_s_fw240_q10_stable_v1`

First training runs:
- long model:
  - model id: `lgbm_l_fw240_q90_stable_v1`
  - sample id: `base_fw240_dev`
  - iterated parameter: `num_leaves`
  - grid: `7`, `15`, `31`, `63`
  - selected `num_leaves`: `7`
  - report: `models/lgbm_l_fw240_q90_stable_v1/report.html`
- short model:
  - model id: `lgbm_s_fw240_q10_stable_v1`
  - sample id: `base_fw240_dev`
  - iterated parameter: `num_leaves`
  - grid: `7`, `15`, `31`, `63`
  - selected `num_leaves`: `7`
  - report: `models/lgbm_s_fw240_q10_stable_v1/report.html`

Default stabilizing parameter profile:
- profile id: `lightgbm_binary_stable_v1`
- fixed parameters include:
  - `n_estimators`
  - `learning_rate`
  - `max_depth`
  - `min_child_samples`
  - `subsample`
  - `colsample_bytree`
  - `reg_alpha`
  - `reg_lambda`
- primary iterated parameter:
  - `num_leaves`
  - default grid: `7`, `15`, `31`, `63`

Responsibilities:
- [x] Use the same dataset builder as Lasso.
- [x] Use the same persisted `sample_id` folds as Lasso.
- [x] Use the same shared metric definitions.
- [x] Select the best tuning value by mean validation PR AUC.
- [x] Save standard artifacts:
  - `model.pkl`
  - `features.json`
  - `metrics.json`
  - `params.json`
  - `cv_results.csv`
  - `report.html`
- [x] Keep LightGBM-specific parameters outside the trainer in
  `config/model_params.json`.

Important rule:
- New model families should only own their fit/predict/parameter logic. Dataset
  loading, sample windows, metrics, artifact layout, and reports should stay in
  shared modules.

## Task 6B: Statsmodels P-Value Logistic Regression

Purpose: replace the legacy long/short statsmodels dev scripts with one reusable
trainer that fits the same shared workflow as Lasso and LightGBM.

Status: **implemented**.

Implemented:
- `src/modeling/statsmodels_logreg.py`
- removed legacy notebook-conversion files:
  - `src/modeling/logit_l_fw240_q90_pval_v1_dev.py`
  - `src/modeling/logit_s_fw240_q10_pval_v1_dev.py`
- baseline model entries in `config/models.json` now use
  `trainer: statsmodels_pvalue_logreg`.

P-value regularization rule:
- The tuning parameter is `pvalue_rounds`.
- Round `0` fits all currently usable `feat_` columns.
- Each later round removes all variables whose fitted p-value is above
  `p_threshold`, then refits.
- Selection uses the same validation PR AUC metric as the other workflows, while
  preferring p-stable models when available.
- Final artifacts store the remaining feature list, coefficient table, p-value
  path, shared metrics, CV results, and HTML report.

## Task 7: Prediction Compatibility

Purpose: make `sync_predictions.py` work with the new Lasso artifacts and keep
support for existing statsmodels artifacts during transition. The production
predictions table is application-facing, so it stores only the configured
runtime model output.

Implementation tasks:
- Extend `config/models.json` model metadata with artifact type:
  - `statsmodels`
  - `sklearn_lasso_logreg`
- Update `sync_predictions.py` to:
  - load selected `features.json`.
  - load scaler when model requires one.
  - call the right prediction method.
  - write generic live columns: `target`, `prediction`, `signal`.
- Keep production prediction output schema model-agnostic and stable across
  model switches.
- Keep runtime model and target documentation in `config/predictions.json`,
  with the active model selected by `config/env.json`.
- Keep multi-model prediction outputs outside the live predictions table; store
  them as evaluation artifacts when needed.

## Task 8: Model-Independent Cut-off Analysis

Purpose: cut-off selection should not know how a model was trained.

Create `src/evaluation/cutoff.py`.

Inputs:
- `open_time`
- fact columns:
  - target column(s) needed by the evaluated model or model set
- prediction columns:
  - model probability from an evaluation frame or model artifact
  - optional multiple model probabilities when comparing model sets

Responsibilities:
- Search cut-offs for the evaluated model direction.
- Evaluate:
  - signal frequency.
  - positive target rate inside signal zone.
  - lift versus baseline.
  - neutral zone coverage.
- Return thresholds and metrics independent of model family.

Output:
- `cutoff_results.json`
- optional `cutoff_report.md`

Important rule:
- Cut-off analysis consumes predictions and facts. It does not import training
  code and does not know whether predictions came from statsmodels, Lasso, XGBoost,
  or another future model.

## Task 9: Model Registry and Activation

Purpose: keep old and new models comparable and switchable.

Update `config/models.json` to support:
- multiple inactive candidate models.
- exactly one runtime model for production predictions via `config/env.json`.
- model family.
- sample id.
- metric summary.
- artifact paths.

Example model ids:
- `logit_l_fw240_q90_pval_v1`
- `logit_s_fw240_q10_pval_v1`
- `logit_l_fw240_q90_l1_v1`
- `logit_s_fw240_q10_l1_v1`

Activation rule:
- Do not delete baseline models.
- Add Lasso models as inactive first.
- Promote only after rebuild, prediction generation, cutoff analysis, and
  comparison against baseline.

Recommended model id convention for future renames:
- Keep `model_id` compact, stable, and safe as a prediction column prefix.
- Store full human-readable explanation in `config/model_registry.json`, not only in the
  id.
- Suggested pattern:
  - `<family>_<direction>_fw<horizon>_q<target_quantile>_<variant>_v<version>`
- Examples:
  - `logit_l_fw240_q90_pval_v1`
  - `logit_s_fw240_q10_pval_v1`
  - `logit_l_fw240_q90_l1_v1`
  - `logit_s_fw240_q10_l1_v1`
  - `lgbm_l_fw240_q90_stable_v1`
  - `lgbm_s_fw240_q10_stable_v1`

Naming notes:
- Use `l` and `s` only for direction.
- Use `fw240` for the target forward horizon.
- Prefer `q90` / `q10` over `p90` / `p10`, because `p` can also mean
  probability or p-value.
- Use family names that distinguish implementation/model type:
  - `logit` for logistic regression.
  - `lgbm` for LightGBM.
  - `xgb` for XGBoost.
- Put details such as trainer implementation, p-value threshold, alpha grid,
  LightGBM fixed parameters, sample id, and report notes in registry metadata.

Recommended registry metadata:
- `display_name`
- `description`
- `target`:
  - `direction`
  - `horizon_minutes`
  - `quantile`
  - `column`
- `trainer`
- `sample_id`
- `artifact_type`
- `prediction_column`
- `metric_summary`

Prediction storage rule:
- The live `predictions` table is for the application and should keep generic
  columns: `open_time`, `close`, `target`, `prediction`, and `signal`.
- Do not put `model_id` or `target_name` into the live table; document those via
  `config/predictions.json` and `config/env.json`.
- Do not add every candidate model's prediction to the live table.
- For model comparison, store predictions outside the live table, preferably as
  model/evaluation artifacts or a narrow evaluation table with:
  - `open_time`
  - `model_id`
  - `probability`
  - `run_id`
  - `created_at`
- Candidate evaluation volume can justify a separate table later, but it should
  remain separate from the app-facing live predictions table.

## Task 10: Rebuild and Validation Workflow

Purpose: keep derived tables reproducible after feature and model changes.

Workflow:
1. Update feature config and feature code.
2. Rebuild derived features and predictions on a bounded interval.
3. Validate duplicates and row ranges.
4. Train long and short Lasso models using the same `sample_id`.
5. Generate evaluation predictions for baseline and candidate models outside the
   live predictions table.
6. Run model-independent cut-off analysis.
7. Compare candidate vs baseline.
8. Promote model config only if candidate improves agreed metrics.

Validation checks:
- `open_time` uniqueness in every table.
- no missing required feature columns.
- selected features exist in features table.
- prediction columns exist for active models.
- metrics files exist for trained models.
- cutoff results exist before activation.

## Task 11: Tests

Add focused tests for:
- feature config parsing.
- dataset builder column selection.
- sampling fold generation and non-overlap.
- metric calculation on known toy data.
- Lasso training smoke test on a small synthetic dataset.
- LightGBM training smoke test on a small synthetic dataset.
- statsmodels p-value training smoke test on a small synthetic dataset.
- prediction artifact loading for sklearn and existing statsmodels models.
- cutoff analysis on a small deterministic dataset.

## Execution Order

1. [x] Clean `config/features.json` and implement first expanded feature generation.
2. [x] Rebuild features and validate table shape.
3. [x] Implement `modeling/datasets.py`.
4. [x] Implement `modeling/sampling.py` and persist the first shared `sample_id`.
5. [x] Implement `modeling/metrics.py`.
6. [x] Implement `modeling/lasso_logreg.py`.
7. [x] Implement `modeling/train.py` and `scripts/train_model.py`.
8. [x] Train long Lasso model.
9. [x] Train short Lasso model using the same sample folds.
10. [x] Update prediction loading for sklearn pipeline artifacts.
11. [x] Extract shared artifact/report/window helpers.
12. [x] Implement LightGBM trainer with shared samples and metrics.
13. [x] Train long LightGBM model using the same sample folds.
14. [x] Train short LightGBM model using the same sample folds.
15. [x] Replace legacy statsmodels dev scripts with shared trainer.
16. [x] Train long statsmodels baseline with shared workflow.
17. [x] Train short statsmodels baseline with shared workflow.
18. Generate predictions for candidate models.
19. Implement and run model-independent cut-off analysis.
20. Compare baseline vs candidate.
21. Activate candidate models only after validation.

## Deferred Work

- Multi-timeframe feature generation.
- Additional distinct feature families from the backlog section.
- Alternative model families such as XGBoost or random forest.
- Automated quarterly retraining.
- Live drift monitoring.
- Full trading/backtest engine with fees and position sizing.
