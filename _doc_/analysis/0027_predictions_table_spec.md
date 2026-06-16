# Predictions Table Analysis Spec

## Table

`predictions`

Primary key: `open_time`

Expected columns:

- `open_time`
- `close`
- `label_end_ts`
- `trg_l_fw60_q90`
- `trg_s_fw60_q10`
- `long_pred`
- `short_pred`

Project semantics:

- Predictions are generated from `feat_ohlcv_quant` joined with `target`.
- `long_pred` and `short_pred` are model probability scores.
- `label_end_ts` should equal `open_time + 60 minutes` for `fw60`.
- Prediction rows should be idempotent by `open_time`.

## Purpose

The `predictions` table is the bridge between ML model output and trading strategy decisions.

The analyst must verify that prediction scores are temporally valid, calibrated enough for thresholding, aligned with labels, and stable across regimes.

## Required Checks

### 1. Basic coverage and key integrity

Report:

- row count
- first `open_time`
- last `open_time`
- duplicate `open_time` count
- null count for `long_pred`
- null count for `short_pred`
- null count for target columns
- rows missing matching `ohlcv`
- rows missing matching `feat_ohlcv_quant`
- rows missing matching `target`

Critical if:

- duplicate `open_time`
- predictions without matching feature row
- predictions without matching OHLCV row
- prediction gaps during expected covered periods

### 2. Prediction value sanity

Check:

- `0 <= long_pred <= 1`
- `0 <= short_pred <= 1`
- no NaN
- no inf
- no constant predictions
- no all-zero or all-one predictions
- number of unique prediction values
- score distribution by year/month

Report quantiles, extreme probabilities, and correlation between `long_pred` and `short_pred`.

### 3. Label-end timestamp audit

Verify:

- `label_end_ts = open_time + interval 60 minutes`
- label_end_ts does not exceed available OHLCV horizon for evaluated labels
- final horizon rows with unknown target are handled explicitly

Suggested SQL:

```sql
SELECT
    COUNT(*) AS bad_label_end_ts
FROM predictions
WHERE label_end_ts <> open_time + INTERVAL 60 MINUTE;
```

### 4. Alignment with targets

Join predictions to `target` on `open_time`.

For both long and short:

- compare stored prediction target columns with `target`
- count mismatches
- count null mismatches
- verify target labels used for evaluation are identical to source target table

Critical if prediction labels differ from the `target` table.

### 5. Out-of-sample performance

When labels are available, compute for long and short:

- ROC-AUC
- PR-AUC
- log-loss
- Brier score
- precision/recall at candidate thresholds
- confusion matrix at strategy threshold
- lift in top decile / top percentile
- calibration by probability bin

Report by:

- full evaluated period
- year
- month
- volatility regime
- train/validation/test fold, if available

PR-AUC is mandatory for q90/q10 rare-event labels.

### 6. Calibration and threshold-readiness

Create calibration bins, for example:

- 0.00-0.05
- 0.05-0.10
- 0.10-0.20
- 0.20-0.40
- 0.40-0.60
- 0.60-0.80
- 0.80-1.00

For each bin report:

- row count
- average predicted probability
- empirical positive rate
- precision
- lift over base rate

Critical if ranking is inverted or high-score bins show no lift.

### 7. Long/short conflict analysis

Analyze rows where:

- `long_pred` is above long entry threshold
- `short_pred` is above short entry threshold
- both are above threshold
- both are low
- one is high and the opposite target occurs

Report overlap rate, conflict periods, and whether long-priority strategy creates bias.

### 8. Drift and live-stability

Analyze prediction distribution over time:

- rolling mean
- rolling std
- rolling high-score positive rate
- rolling calibration
- daily/weekly threshold triggers

Critical if live scores are outside validation distribution or sharply drift without a model/config change.

### 9. Feature-to-prediction traceability

For active champion models:

- identify model id used for `long_pred`
- identify model id used for `short_pred`
- verify artifact feature list
- verify all features existed at prediction time
- quantify how many prediction rows had null features before fill

## Required Notebook Outputs

1. Prediction coverage summary.
2. Probability sanity table.
3. Label-end timestamp audit.
4. Target alignment audit.
5. Long and short performance metric table.
6. Calibration table by score bin.
7. Threshold/lift table.
8. Long-short conflict table.
9. Prediction drift over time.
10. Feature-to-prediction traceability summary.

## Critical Findings

Mark as critical if any of these occur:

- probabilities outside [0, 1]
- duplicate prediction timestamps
- prediction rows lack matching feature rows
- target labels in predictions differ from `target`
- label_end_ts is not open_time + 60 minutes
- calibration is inverted or unusable
- long and short predictions conflict frequently at strategy thresholds
- live score distribution is outside validation distribution
