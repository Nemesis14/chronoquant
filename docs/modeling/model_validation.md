# Model Validation

Model validation decides whether a trained candidate is credible enough for
strategy evaluation and possible promotion.

## Required Evidence

- Source `sample_id`, data range, fold boundaries, and holdout range.
- Target definition and class balance.
- Feature audit result and final feature count.
- Search settings, trial count, and best parameters.
- Per-fold validation metrics.
- Train/validation gap.
- Prediction distribution.
- Holdout performance, clearly separated from selection metrics.
- Known limitations and rejected alternatives.

## Metric Review

Review at least:

- log loss;
- PR AUC;
- ROC AUC;
- top-percentile lift;
- fold-to-fold stability;
- train/validation gap;
- zero-gain or dominant feature behavior.

## Promotion Gate

A model is promotion-ready only after:

1. It has a completed model card.
2. It passes validation and holdout review.
3. Strategy evaluation selects stable thresholds without tuning on holdout.
4. Runtime config updates are reviewed.
5. Dashboard/runtime verification passes.

