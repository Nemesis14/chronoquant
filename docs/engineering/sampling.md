# Sampling And Split Guide

This guide defines how ChronoQuant chooses data ranges, sample IDs,
train/validation folds, and final holdout windows. It is model-family
independent and should be followed before LightGBM search, future model
families, and strategy evaluation.

## Purpose

Sampling answers four questions:

1. What data is available for an asset and target?
2. Which part is allowed for model and trigger selection?
3. Which part is reserved as an untouched final holdout?
4. Which persisted sample definition should comparable models reuse?

## Startup Data Audit

Start every model development run by inspecting:

- first and last available feature timestamp;
- row count in the feature table;
- required target columns for the requested horizon;
- target and feature null rates;
- duplicate `open_time` values;
- time gaps in the feature table;
- target horizon and embargo requirements.

Use `src/utils.py` config helpers from business logic. Do not read JSON config
directly from reusable code.

## Sample Artifacts

Persist sample definitions under:

```text
samples/<sample_id>/metadata.json
samples/<sample_id>/folds.json
```

`metadata.json` should describe the source data, split parameters, and row
range. `folds.json` should contain deterministic train/validation fold
boundaries and the final holdout range.

## Split Policy

Use chronological splits only. Do not use shuffled CV for market time series
unless a specific experiment explicitly documents why it is safe.

Default structure:

1. **Research folds:** expanding-window train/validation folds used for feature,
   model, and hyperparameter selection.
2. **Embargo:** a gap between train and validation/test windows at least as long
   as the target horizon when labels look forward.
3. **Final holdout:** the newest 6-12 months kept untouched while research
   decisions are made.
4. **Promotion fit:** after a candidate passes holdout review, the production
   artifact may be refit on all approved data through the latest safe timestamp.

The final holdout is an exam during development. It is not permanently discarded:
after it has served as an independent check, approved data can be included in
the promoted production fit.

## Parameter Defaults

Use these as starting points, then document deviations:

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `min_train_days` | 730 | Minimum history before the first validation fold |
| `valid_days` | 180 | Validation fold length |
| `step_days` | 180 | Distance between validation windows |
| `test_days` | 365 | Final untouched holdout length |
| `embargo_minutes` | target horizon | Gap between train and evaluation windows |

Shorter holdouts can be justified when the asset has limited history. Longer
holdouts can be justified for regime-heavy research, but they reduce recent data
available for selection.

## Reusing Sample IDs

Reuse the same `sample_id` when models should be comparable:

- long and short variants on the same asset and horizon;
- candidate LightGBM versions for the same target;
- baseline and champion comparisons.

Create a new `sample_id` when any of these change materially:

- asset or table source;
- target horizon;
- label definition;
- feature table rebuild changes the available date range materially;
- split parameters or holdout boundaries;
- data quality fixes alter a meaningful part of the sample.

## Example Layout

```text
data_start -> 2020-08-11
data_end   -> 2026-06-05

Research:
  train/CV/search        2020-08-11 -> pre-holdout data
  trigger validation     pre-holdout validation or OOF predictions
  untouched holdout      newest 6-12 months

Promotion:
  final production fit   2020-08-11 -> latest safe timestamp
  trigger                selected from a stable research-period range
```

## Validation Checklist

- [ ] `metadata.json` and `folds.json` exist for the `sample_id`.
- [ ] Fold ranges are chronological and non-overlapping.
- [ ] Embargo separates train and validation/test rows.
- [ ] Final holdout is not used for model, feature, hyperparameter, or trigger
      selection.
- [ ] Comparable candidates use the same `sample_id`.
- [ ] The sample data range matches the current modeling question.
