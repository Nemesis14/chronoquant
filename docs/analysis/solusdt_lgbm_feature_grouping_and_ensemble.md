# SOLUSDT LightGBM Feature Grouping And Ensemble Analysis

Date: 2026-06-07

## Question

Should the SOLUSDT LightGBM workflow keep the current 200+ quantitative
features in one model, split them into feature-family models, and then combine
those models with an ensemble/stacking layer? The same question matters for
future text, Elliott wave, order-flow, and on-chain modules.

## Current Project State

The current champion long model is `lgbm_solusdt_l_fw60_q90_local_v2`.
It was trained on 202 audited features, using 5 chronological expanding-window
folds with `row_stride=60`.

Key validation metrics:

| Metric | Long local_v2 |
|---|---:|
| mean_valid_log_loss | 0.2632 |
| mean_train_log_loss | 0.2347 |
| mean_gap | 0.0284 |
| std_valid_log_loss | 0.0078 |
| mean_valid_PR_AUC | 0.3637 |
| mean_valid_ROC_AUC | 0.8147 |
| lift @ top 5% | 4.84x |

The current short model, `lgbm_solusdt_s_fw60_q10_local_v2`, uses 200 audited
features and has similar stability:

| Metric | Short local_v2 |
|---|---:|
| mean_valid_log_loss | 0.2687 |
| mean_train_log_loss | 0.2470 |
| mean_gap | 0.0217 |
| std_valid_log_loss | 0.0119 |
| mean_valid_PR_AUC | 0.3531 |
| mean_valid_ROC_AUC | 0.8084 |
| lift @ top 5% | 4.64x |

The selected hyperparameters are conservative: shallow trees, high
`min_child_samples`, strong L1/L2 regularization, row/feature subsampling, and
early stopping. This matters because the existing training already controls
overfitting at the model level.

## External Evidence

LightGBM is designed to work with many features when regularization and
subsampling are used. Its official parameter documentation describes
`feature_fraction` / `colsample_bytree` as random feature selection per tree,
and explicitly notes that it can reduce overfitting and speed training.
It also lists `bagging_fraction`, `min_data_in_leaf`, `lambda_l1`,
`lambda_l2`, `min_gain_to_split`, `max_depth`, `extra_trees`, and
`path_smooth` as overfitting controls.

Sources:

- https://lightgbm.readthedocs.io/en/latest/Parameters.html
- https://lightgbm.readthedocs.io/en/stable/Parameters-Tuning.html

Stacking is a valid ensemble method, but it must use out-of-fold predictions.
The original stacked-generalization idea is to train a second model on the
base models' predictions on held-out data, not on in-sample fitted
predictions. scikit-learn's `StackingClassifier` documentation states the same
operational rule: base estimators can be fit on full `X`, but the final
estimator is trained using cross-validated predictions; using prefit models on
the same data creates high overfitting risk.

Sources:

- Wolpert, "Stacked Generalization", Neural Networks, 1992,
  https://doi.org/10.1016/S0893-6080(05)80023-1
- https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.StackingClassifier.html

Recent financial forecasting work supports multimodal fusion, but the reported
gains are usually conditional on data quality, alignment, and leakage-safe
evaluation. `FinMultiTime` reports that multimodal fusion can yield moderate
gains, while also emphasizing scale, reproducibility, and data quality.

Sources:

- https://arxiv.org/abs/2506.05019
- https://huggingface.co/papers/2506.05019

## Analysis

### 1. Keeping 200+ Features In One LightGBM Is Reasonable

For the current OHLCV-derived feature set, one integrated LightGBM is still the
right champion baseline.

Reasons:

- The model has enough rows relative to feature count: around 3M raw feature
  rows and around 40k sampled training rows at `row_stride=60`.
- The target is sparse but not tiny: roughly 10% positive class by target
  construction.
- The current validation gap is controlled: 0.0284 for long and 0.0217 for
  short.
- LightGBM can naturally discover interactions between feature families, for
  example volatility x session position x return-tail context.
- Splitting feature groups too early can destroy cross-family interactions.

The current evidence does not say "200 features is too many". It says "feature
audit and regularization are mandatory".

### 2. Feature Groups Are Still Valuable

Feature groups should be formalized, but not primarily to replace the champion
model. They should be used for:

- ablation tests;
- feature-family importance reporting;
- controlled feature expansion;
- future module boundaries;
- optional ensemble base learners.

The existing feature expansion plan already has meaningful groups:

- legacy momentum / trend / volatility / volume / price action;
- return and distance;
- volatility and activity regime rank;
- candle shape;
- trend slope;
- momentum interaction;
- time and session;
- GK/Parkinson volatility;
- autocorrelation;
- drawdown timing;
- pattern flags;
- gap;
- efficiency;
- support/resistance;
- tail risk;
- acceleration;
- Ichimoku;
- Donchian;
- linear regression;
- session-relative;
- activity/order-flow derived groups.

These groups should be stored as metadata next to the feature definitions or
as a generated feature manifest. The training code should still load features
through the shared dataset builder.

### 3. A Group-Model Ensemble Could Help, But It Is Not Guaranteed

Training one LightGBM per feature group and combining them can improve
robustness when groups have different noise, latency, missingness, or regime
sensitivity. It is especially useful when future modalities arrive:

- text/news sentiment;
- social signals;
- on-chain data;
- order-flow/microstructure;
- Elliott wave or pattern parser outputs;
- cross-asset market context.

For the current purely local OHLCV feature set, a group-model ensemble is more
likely to improve interpretability and operational flexibility than immediate
accuracy. Accuracy could improve if base learners make uncorrelated errors, but
it could also regress because the current single LightGBM can learn
cross-group interactions directly.

Expected effect:

| Approach | Accuracy impact | Extensibility impact | Risk |
|---|---|---|---|
| Current single LGBM with 200 features | Strong baseline | Medium | Feature expansion can become hard to reason about |
| Single LGBM + group ablation | Usually no direct runtime gain | High for research | Low |
| Separate group LGBMs + weighted average | Possible small gain or no gain | High | Weight overfit if tuned on holdout |
| OOF stacking meta-model | Possible gain if errors differ | Very high | High unless OOF discipline is strict |
| Multimodal module models + OOF stacker | Best long-term architecture | Very high | Data alignment/leakage and operational complexity |

### 4. Recommended Architecture Direction

Use a two-layer architecture, but introduce it experimentally before replacing
the champion:

```text
Feature table / module outputs
        |
        +--> quant_all_lgbm          (current champion baseline)
        +--> quant_group_momentum    (optional base model)
        +--> quant_group_volatility  (optional base model)
        +--> quant_group_session     (optional base model)
        +--> order_flow_model        (future)
        +--> text_sentiment_model    (future)
        +--> onchain_model           (future)
        +--> elliott_wave_model      (future)
                    |
            OOF prediction matrix
                    |
              calibrated stacker
                    |
              final prediction
```

The stacker should be intentionally simple:

- logistic regression or small calibrated LightGBM;
- inputs are base model OOF probabilities plus a few regime flags;
- no raw 200-feature passthrough at first;
- chronological folds only;
- final holdout untouched until the complete ensemble is frozen.

This keeps the architecture expandable without creating a fragile "model of
models" too early.

## Recommended Experiment Plan

### Phase 1: Formalize Feature Groups

Create a feature group manifest for `solusdt_fw60`.

Suggested artifact:

```text
config/feature_groups.json
```

or:

```text
models/<model_id>/search/feature_groups.json
```

Each feature should have:

- `name`;
- `group`;
- `source_type`: `ohlcv`, `activity`, `order_flow`, `text`, `onchain`, etc.;
- `available_at_prediction_time`;
- optional `latency_minutes`;
- optional `requires_backfill`.

### Phase 2: Run Group Ablation Against The Existing Champion

Use the same `sample_id`, folds, target, row stride, and metrics.

Run:

1. current all-feature model;
2. all minus one group;
3. each large group alone;
4. curated all-feature model with known zero-gain / duplicate features removed.

Primary metrics:

- mean validation log loss;
- PR AUC;
- ROC AUC;
- lift at top 1%, 5%, 10%;
- fold-to-fold stability;
- calibration / Brier score;
- strategy threshold distribution.

Decision rule:

- If removing a group improves or preserves PR AUC and reduces gap, exclude it
  from the next champion candidate.
- If a group alone has weak PR AUC but improves the all-feature model, keep it:
  it may be useful only through interactions.
- Do not use final holdout for feature group selection.

### Phase 3: Build OOF Prediction Artifacts

Before any ensemble, add a reusable OOF prediction artifact format.

Suggested artifact:

```text
models/<model_id>/oof_predictions.csv
```

Columns:

```text
open_time,target,prediction,fold,model_id,asset_id,target_name
```

Candidate model predictions must remain separate from the live predictions
table, matching the project rule.

### Phase 4: Test Simple Weighted Ensemble

Start with a simple weighted average of OOF predictions:

```text
p_final = w1*p_quant_all + w2*p_volatility + w3*p_session + ...
```

Weights should be learned only on research folds or nested folds, not on the
final holdout. This is a cheap benchmark and often hard to beat.

### Phase 5: Test Stacking

Train a meta-model on OOF base predictions. Compare it to:

- current champion;
- simple average;
- optimized weighted average.

Promotion gate:

- better or equal validation log loss;
- better PR AUC or top-percentile lift;
- no worse calibration;
- no worse fold stability;
- clean final holdout report;
- strategy sweep remains robust across thresholds.

## Expected Impact

### Accuracy

For the current 200 OHLCV/activity features, the expected accuracy gain from
splitting into group models is uncertain and probably modest. The current
single LightGBM is already strong, regularized, and able to model feature
interactions. A realistic target for an ensemble experiment would be:

- no regression in log loss;
- 0-5% relative PR AUC improvement if base models have diverse errors;
- improved fold stability or calibration even if PR AUC is similar.

Large gains should not be assumed. If a test shows +10% PR AUC, treat it as
possible leakage or holdout reuse until proven otherwise.

### Extensibility

Extensibility should improve materially. Separate module-level models make
sense once inputs have different generation pipelines and latency:

- text can be missing or delayed;
- on-chain data may update on different intervals;
- order-flow data may require exchange-specific fields;
- Elliott wave labels may be parser/version dependent.

For these cases, a modular ensemble lets the project add or disable a signal
source without retraining every raw feature inside one monolithic model.

## Recommendation

Do not replace the current SOLUSDT champion with feature-group ensemble work
yet. Keep `lgbm_solusdt_l_fw60_q90_local_v2` and
`lgbm_solusdt_s_fw60_q10_local_v2` as the accuracy baselines.

Recommended next step:

1. Add feature-group metadata.
2. Run ablation experiments with the existing LightGBM search workflow.
3. Add OOF prediction artifact support.
4. Test weighted average and stacking as inactive candidates.
5. Only promote an ensemble if it beats the current champion on the same
   sample definition and passes untouched holdout plus strategy robustness.

This gives the project the extensibility needed for text, on-chain, order-flow,
and Elliott wave modules without sacrificing the current model's strongest
property: a simple, validated, single-model baseline with controlled
overfitting.
