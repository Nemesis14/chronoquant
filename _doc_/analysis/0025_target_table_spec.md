# Target Table Analysis Spec

## Table

`target`

Primary key: `open_time`

Expected columns:

- `open_time`
- `close`
- `trg_l_fw60_q90`
- `trg_s_fw60_q10`

Project semantics:

- `fw60` means a 60-bar forward window.
- Long target `trg_l_fw60_q90` means future maximum return over t+1..t+60 is in the top decile.
- Short target `trg_s_fw60_q10` means future minimum return over t+1..t+60 is in the bottom decile.
- Bar `t` must not be included in the forward label window.
- The last 60 rows should have null targets because the future window is incomplete.

## Purpose

The `target` table defines the supervised learning labels.

The analyst must verify that target labels are temporally correct, class-balanced as expected, and not leaking future information into training.

## Required Checks

### 1. Basic coverage and key integrity

Report:

- row count
- first `open_time`
- last `open_time`
- duplicate `open_time` count
- null counts per target column
- target rows missing matching `ohlcv`
- `ohlcv` rows missing target

Expected:

- `target.open_time` aligns to `ohlcv.open_time`
- `target.close` equals `ohlcv.close`
- last `fw60` rows have null target values

### 2. Forward-window correctness

Recompute forward extrema directly from `ohlcv`:

- `future_max_close = MAX(close)` over rows t+1 through t+60
- `future_min_close = MIN(close)` over rows t+1 through t+60
- `future_bar_count`
- `future_max_return = future_max_close / close - 1`
- `future_min_return = future_min_close / close - 1`

Suggested SQL:

```sql
WITH ohlcv_ordered AS (
    SELECT
        open_time,
        close
    FROM ohlcv
    ORDER BY open_time
),
forward_extrema AS (
    SELECT
        open_time,
        close,
        MAX(close) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING
        ) AS future_max_close,
        MIN(close) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING
        ) AS future_min_close,
        COUNT(*) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING
        ) AS future_bar_count
    FROM ohlcv_ordered
)
SELECT *
FROM forward_extrema
ORDER BY open_time;
```

### 3. Quantile threshold audit

Compute empirical thresholds:

- long threshold from non-null `future_max_return` at q90
- short threshold from non-null `future_min_return` at q10

Report:

- computed long threshold
- computed short threshold
- thresholds saved in metadata, if available
- positive rate for long target
- positive rate for short target

Expected:

- q90/q10 positive rate should be approximately 10%
- project tolerance: 8-12%, unless ties or sample-size explain otherwise

Important note:

If the target threshold is computed from full history, document this explicitly. For strict live-simulation analysis, compare against train-only or rolling threshold calibration.

### 4. Class balance by time

Report distribution by:

- year
- month
- volatility regime
- train/validation/test fold, if sample definition is available

Check:

- whether long and short positives occur on the same row
- whether class rate is stable over time
- whether folds have enough positive examples

### 5. Label horizon and embargo audit

For every sample/fold definition:

- identify target horizon
- identify embargo minutes
- compute train-validation gap
- verify gap >= forward horizon

Required rule:

- minimum embargo must be at least `fw60`, i.e. 60 bars/minutes.

### 6. Leakage proxy checks

Search feature/prediction tables for suspicious columns:

- `trg_*`
- `future_*`
- `label_*`
- `return_forward_*`

Critical if future returns, target columns, or label proxies appear in feature inputs.

## Required Notebook Outputs

1. Target coverage summary.
2. Forward-window recomputation check.
3. Threshold audit table.
4. Target positive-rate table by year/month.
5. Fold-level class balance table.
6. Embargo validation table.
7. Long/short overlap analysis.
8. Leakage proxy column scan.

## Critical Findings

Mark as critical if any of these occur:

- target labels do not match recomputed logic
- bar t is included in the forward window
- last horizon rows are not null
- class balance is far outside expected range
- train/validation/test gaps are shorter than horizon
- features include future return or target-like proxy columns
