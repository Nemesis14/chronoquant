# Target Table Analysis Spec

## Table

`target`

Primary key: `open_time`

## Expected columns
<<<<<<< HEAD

Required base columns:

* `open_time`
* `close`

Required fw60 continuous outcome columns:

* `fw60_close`
* `fw60_max`
* `fw60_min`
* `fw60_close_ret`
* `fw60_close_logret`
* `fw60_max_ratio`
* `fw60_min_ratio`
* `long_mfe_fw60`
* `short_mfe_fw60`

Legacy binary target columns may still exist for backward compatibility, but they are not the primary source-of-truth for target analysis:

* `trg_l_fw60_q90`
* `trg_s_fw60_q10`

## Project semantics

=======

Required base columns:

- `open_time`
- `close`

Required fw60 continuous outcome columns:

- `fw60_close`
- `fw60_max`
- `fw60_min`
- `fw60_close_ret`
- `fw60_close_logret`
- `fw60_max_ratio`
- `fw60_min_ratio`
- `long_mfe_fw60`
- `short_mfe_fw60`

Legacy binary target columns may still exist for backward compatibility, but they are not the primary source-of-truth for target analysis:

- `trg_l_fw60_q90`
- `trg_s_fw60_q10`

## Project semantics

>>>>>>> c4cec1aefdedc1f28828173afca5be31c7a7e783
`fw60` means a 60-bar forward window.

The target table stores continuous forward outcomes for the 60-bar horizon. These outcomes are supervised-learning labels or analysis outcomes, not live features.

Forward window rule:

```text
forward window = t+1 .. t+60
```

The current bar `t` must be excluded from all forward-window calculations.

SQL invariant:

```sql
ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING
```

For each row:

```text
close[t]                    = current bar close
close[t+60]                 = fw60_close
max(close[t+1:t+60])        = fw60_max
min(close[t+1:t+60])        = fw60_min
```

Derived continuous outcome definitions:

```text
fw60_close_ret     = fw60_close / close - 1
fw60_close_logret  = log(fw60_close / close)
fw60_max_ratio     = fw60_max / close
fw60_min_ratio     = fw60_min / close
long_mfe_fw60      = log(fw60_max / close)
short_mfe_fw60     = log(fw60_min / close)
```

Important sign convention:

<<<<<<< HEAD
* `long_mfe_fw60` is usually non-negative when the forward max is above current close.
* `short_mfe_fw60` is a signed downside log return and is usually non-positive when the forward min is below current close.
* If a downstream model needs a positive short opportunity target, use `-short_mfe_fw60` as a derived modeling target. The stored source outcome remains `short_mfe_fw60 = log(fw60_min / close)`.
=======
- `long_mfe_fw60` is usually non-negative when the forward max is above current close.
- `short_mfe_fw60` is a signed downside log return and is usually non-positive when the forward min is below current close.
- If a downstream model needs a positive short opportunity target, use `-short_mfe_fw60` as a derived modeling target. The stored source outcome remains `short_mfe_fw60 = log(fw60_min / close)`.
>>>>>>> c4cec1aefdedc1f28828173afca5be31c7a7e783

The last 60 rows should have null fw60 outcome values because the future window is incomplete.

## Purpose

The `target` table defines model-ready continuous forward outcomes for supervised learning and target analysis.

The analyst must verify that target outcomes are temporally correct, numerically consistent with OHLCV, null-handled correctly, and free from target-definition leakage caused by full-history percentile thresholds.

The primary audit goal is no longer class balance of q90/q10 labels. Instead, the primary audit goal is correctness and stability of continuous fw60 forward outcomes.

## Required Checks

### 1. Basic coverage and key integrity

Report:

<<<<<<< HEAD
* row count
* first `open_time`
* last `open_time`
* duplicate `open_time` count
* null counts per target outcome column
* target rows missing matching `ohlcv`
* `ohlcv` rows missing target

Expected:

* `target.open_time` aligns to `ohlcv.open_time`
* `target.close` equals `ohlcv.close`
* last 60 rows have null fw60 outcome values
* non-tail rows should have non-null fw60 outcome values, unless source OHLCV values are invalid
=======
- row count
- first `open_time`
- last `open_time`
- duplicate `open_time` count
- null counts per target outcome column
- target rows missing matching `ohlcv`
- `ohlcv` rows missing target

Expected:

- `target.open_time` aligns to `ohlcv.open_time`
- `target.close` equals `ohlcv.close`
- last 60 rows have null fw60 outcome values
- non-tail rows should have non-null fw60 outcome values, unless source OHLCV values are invalid
>>>>>>> c4cec1aefdedc1f28828173afca5be31c7a7e783

Target outcome columns to audit:

```text
fw60_close
fw60_max
fw60_min
fw60_close_ret
fw60_close_logret
fw60_max_ratio
fw60_min_ratio
long_mfe_fw60
short_mfe_fw60
```

### 2. Forward-window correctness

Recompute forward outcomes directly from `ohlcv` and compare against the stored `target` table.

Recompute:

<<<<<<< HEAD
* `fw60_close = close[t+60]`
* `fw60_max = MAX(close)` over rows t+1 through t+60
* `fw60_min = MIN(close)` over rows t+1 through t+60
* `future_bar_count`
* `fw60_close_ret = fw60_close / close - 1`
* `fw60_close_logret = log(fw60_close / close)`
* `fw60_max_ratio = fw60_max / close`
* `fw60_min_ratio = fw60_min / close`
* `long_mfe_fw60 = log(fw60_max / close)`
* `short_mfe_fw60 = log(fw60_min / close)`
=======
- `fw60_close = close[t+60]`
- `fw60_max = MAX(close)` over rows t+1 through t+60
- `fw60_min = MIN(close)` over rows t+1 through t+60
- `future_bar_count`
- `fw60_close_ret = fw60_close / close - 1`
- `fw60_close_logret = log(fw60_close / close)`
- `fw60_max_ratio = fw60_max / close`
- `fw60_min_ratio = fw60_min / close`
- `long_mfe_fw60 = log(fw60_max / close)`
- `short_mfe_fw60 = log(fw60_min / close)`
>>>>>>> c4cec1aefdedc1f28828173afca5be31c7a7e783

Suggested SQL:

```sql
WITH ohlcv_ordered AS (
    SELECT
        open_time,
        close
    FROM ohlcv
    ORDER BY open_time
),
forward_window AS (
    SELECT
        open_time,
        close,
        NTH_VALUE(close, 60) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING
        ) AS fw60_close,
        MAX(close) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING
        ) AS fw60_max,
        MIN(close) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING
        ) AS fw60_min,
        COUNT(*) OVER (
            ORDER BY open_time
            ROWS BETWEEN 1 FOLLOWING AND 60 FOLLOWING
        ) AS future_bar_count
    FROM ohlcv_ordered
),
recomputed AS (
    SELECT
        open_time,
        close,
        CASE WHEN future_bar_count >= 60 THEN fw60_close ELSE NULL END AS fw60_close,
        CASE WHEN future_bar_count >= 60 THEN fw60_max ELSE NULL END AS fw60_max,
        CASE WHEN future_bar_count >= 60 THEN fw60_min ELSE NULL END AS fw60_min,
        CASE WHEN future_bar_count >= 60 THEN fw60_close / NULLIF(close, 0) - 1 ELSE NULL END AS fw60_close_ret,
        CASE WHEN future_bar_count >= 60 THEN LOG(fw60_close / NULLIF(close, 0)) ELSE NULL END AS fw60_close_logret,
        CASE WHEN future_bar_count >= 60 THEN fw60_max / NULLIF(close, 0) ELSE NULL END AS fw60_max_ratio,
        CASE WHEN future_bar_count >= 60 THEN fw60_min / NULLIF(close, 0) ELSE NULL END AS fw60_min_ratio,
        CASE WHEN future_bar_count >= 60 THEN LOG(fw60_max / NULLIF(close, 0)) ELSE NULL END AS long_mfe_fw60,
        CASE WHEN future_bar_count >= 60 THEN LOG(fw60_min / NULLIF(close, 0)) ELSE NULL END AS short_mfe_fw60
    FROM forward_window
)
SELECT *
FROM recomputed
ORDER BY open_time;
```

Analyst note:

<<<<<<< HEAD
* If DuckDB `NTH_VALUE(close, 60)` does not produce the expected `close[t+60]` under the selected frame semantics, use an explicit row-number self-join to compute `fw60_close`.
* The correctness requirement is semantic, not tied to this exact SQL implementation.
=======
- If DuckDB `NTH_VALUE(close, 60)` does not produce the expected `close[t+60]` under the selected frame semantics, use an explicit row-number self-join to compute `fw60_close`.
- The correctness requirement is semantic, not tied to this exact SQL implementation.
>>>>>>> c4cec1aefdedc1f28828173afca5be31c7a7e783

### 3. Numeric consistency checks

For all non-null fw60 rows, verify:

```text
fw60_close_ret    ~= fw60_close / close - 1
fw60_close_logret ~= log(fw60_close / close)
fw60_max_ratio    ~= fw60_max / close
fw60_min_ratio    ~= fw60_min / close
long_mfe_fw60     ~= log(fw60_max / close)
short_mfe_fw60    ~= log(fw60_min / close)
```

Report maximum absolute mismatch per derived column.

Expected:

<<<<<<< HEAD
* mismatches should be zero or within floating point tolerance
* suggested tolerance: `1e-10` for direct recomputation, unless engine precision explains a larger difference
=======
- mismatches should be zero or within floating point tolerance
- suggested tolerance: `1e-10` for direct recomputation, unless engine precision explains a larger difference
>>>>>>> c4cec1aefdedc1f28828173afca5be31c7a7e783

### 4. Forward-window ordering and boundary checks

For all non-null rows, check:

```text
fw60_max >= fw60_min
fw60_max >= LEAST(fw60_close, fw60_min) is not sufficient; recompute from full window
fw60_min <= GREATEST(fw60_close, fw60_max) is not sufficient; recompute from full window
fw60_max_ratio >= fw60_min_ratio
long_mfe_fw60 >= short_mfe_fw60
```

Expected:
<<<<<<< HEAD

* `fw60_max` equals the maximum close in t+1..t+60
* `fw60_min` equals the minimum close in t+1..t+60
* `fw60_close` equals close at exactly t+60
* the current row close must not be included in max/min/close outcome calculations

### 5. Distribution and stability analysis

Report distributions for:

```text
fw60_close_ret
fw60_close_logret
fw60_max_ratio
fw60_min_ratio
long_mfe_fw60
short_mfe_fw60
```

Report by:

* year
* month
* quarter
* volatility regime, if available
* train/validation/test fold, if sample definition is available

For each period/fold, report:

* count
* null count
* mean
* median
* standard deviation
* min
* max
* p01
* p05
* p10
* p25
* p75
* p90
* p95
* p99

Purpose:

* detect regime drift
* detect abnormal target distribution shifts
* detect corrupted forward outcome calculations
* provide inputs for optional derived binary thresholds later
=======

- `fw60_max` equals the maximum close in t+1..t+60
- `fw60_min` equals the minimum close in t+1..t+60
- `fw60_close` equals close at exactly t+60
- the current row close must not be included in max/min/close outcome calculations

### 5. Distribution and stability analysis

Report distributions for:

```text
fw60_close_ret
fw60_close_logret
fw60_max_ratio
fw60_min_ratio
long_mfe_fw60
short_mfe_fw60
```

Report by:

- year
- month
- quarter
- volatility regime, if available
- train/validation/test fold, if sample definition is available

For each period/fold, report:

- count
- null count
- mean
- median
- standard deviation
- min
- max
- p01
- p05
- p10
- p25
- p75
- p90
- p95
- p99

Purpose:

- detect regime drift
- detect abnormal target distribution shifts
- detect corrupted forward outcome calculations
- provide inputs for optional derived binary thresholds later
>>>>>>> c4cec1aefdedc1f28828173afca5be31c7a7e783

### 6. Legacy q90/q10 binary label audit, if columns exist

If `trg_l_fw60_q90` and `trg_s_fw60_q10` still exist, treat them as legacy derived labels.

Compute empirical thresholds from continuous source outcomes:

```text
legacy long threshold  = q90(long_mfe_fw60 over non-null full available history)
legacy short threshold = q10(short_mfe_fw60 over non-null full available history)
```

Then verify:

```text
trg_l_fw60_q90 ~= long_mfe_fw60 >= legacy long threshold
trg_s_fw60_q10 ~= short_mfe_fw60 <= legacy short threshold
```

Report:

<<<<<<< HEAD
* computed legacy long threshold
* computed legacy short threshold
* thresholds saved in metadata, if available
* positive rate for legacy long target
* positive rate for legacy short target
* mismatch count between stored binary columns and recomputed legacy labels

Important:

* Full-history q90/q10 binary labels are legacy compatibility outputs.
* They must not be treated as the primary target source for strict time-series model validation.
* If binary labels are needed for modeling, prefer fold-train, train-only, rolling, or explicitly fixed trading thresholds derived from continuous outcomes.
=======
- computed legacy long threshold
- computed legacy short threshold
- thresholds saved in metadata, if available
- positive rate for legacy long target
- positive rate for legacy short target
- mismatch count between stored binary columns and recomputed legacy labels

Important:

- Full-history q90/q10 binary labels are legacy compatibility outputs.
- They must not be treated as the primary target source for strict time-series model validation.
- If binary labels are needed for modeling, prefer fold-train, train-only, rolling, or explicitly fixed trading thresholds derived from continuous outcomes.
>>>>>>> c4cec1aefdedc1f28828173afca5be31c7a7e783

### 7. Fold-level target analysis

For every sample/fold definition, report continuous target distributions separately for train and validation:

```text
long target candidate  = long_mfe_fw60
short target candidate = -short_mfe_fw60, if positive short opportunity is needed downstream
```

For each fold:

<<<<<<< HEAD
* train count
* validation count
* train target mean/median/std
* validation target mean/median/std
* train target quantiles
* validation target quantiles
* top-decile threshold in train only
* validation rate above train-only top-decile threshold

Purpose:

* quantify target drift across folds
* support future fold-as-of binary label derivation
* avoid using future validation/test periods to define thresholds
=======
- train count
- validation count
- train target mean/median/std
- validation target mean/median/std
- train target quantiles
- validation target quantiles
- top-decile threshold in train only
- validation rate above train-only top-decile threshold

Purpose:

- quantify target drift across folds
- support future fold-as-of binary label derivation
- avoid using future validation/test periods to define thresholds
>>>>>>> c4cec1aefdedc1f28828173afca5be31c7a7e783

### 8. Label horizon and embargo audit

For every sample/fold definition:

* identify target horizon
* identify embargo minutes
* compute train-validation gap
* verify gap >= forward horizon

Required rule:

* minimum embargo must be at least `fw60`, i.e. 60 bars/minutes.

This rule still applies for continuous outcomes because the label uses forward information from t+1..t+60.

### 9. Leakage proxy checks

Search feature/prediction tables for suspicious columns:

<<<<<<< HEAD
* `trg_*`
* `future_*`
* `fw60_*`
* `*_mfe_*`
* `*_mae_*`
* `label_*`
* `return_forward_*`
=======
- `trg_*`
- `future_*`
- `fw60_*`
- `*_mfe_*`
- `*_mae_*`
- `label_*`
- `return_forward_*`
>>>>>>> c4cec1aefdedc1f28828173afca5be31c7a7e783

Critical if continuous target outcomes, future returns, target columns, or label proxies appear in model feature inputs.

Allowed:

<<<<<<< HEAD
* `fw60_*`, `*_mfe_*`, and legacy `trg_*` columns may exist in `target`, reports, model evaluation artifacts, or prediction evaluation joins.

Forbidden:

* these columns must not be part of live model input features.
=======
- `fw60_*`, `*_mfe_*`, and legacy `trg_*` columns may exist in `target`, reports, model evaluation artifacts, or prediction evaluation joins.

Forbidden:

- these columns must not be part of live model input features.
>>>>>>> c4cec1aefdedc1f28828173afca5be31c7a7e783

## Required Notebook Outputs

1. Target coverage summary.
2. Forward-window recomputation check.
3. Numeric consistency mismatch table.
4. Null-tail validation table.
5. Continuous target distribution tables by year/month/quarter.
6. Fold-level continuous target distribution table.
7. Optional legacy q90/q10 threshold audit table, if legacy binary columns exist.
8. Embargo validation table.
9. Long/short outcome relationship analysis.
10. Leakage proxy column scan.

## Critical Findings

Mark as critical if any of these occur:

<<<<<<< HEAD
* stored fw60 outcomes do not match recomputed OHLCV logic
* bar t is included in the forward window
* `fw60_close` is not close[t+60]
* `fw60_max` is not max(close[t+1:t+60])
* `fw60_min` is not min(close[t+1:t+60])
* last 60 rows are not null for fw60 outcomes
* non-tail rows are unexpectedly null without source data explanation
* derived return/logreturn columns are numerically inconsistent
* train/validation/test gaps are shorter than horizon
* feature tables include fw60 outcome or target-like proxy columns as model inputs
* binary q90/q10 labels are used as primary source targets without documenting the threshold policy
=======
- stored fw60 outcomes do not match recomputed OHLCV logic
- bar t is included in the forward window
- `fw60_close` is not close[t+60]
- `fw60_max` is not max(close[t+1:t+60])
- `fw60_min` is not min(close[t+1:t+60])
- last 60 rows are not null for fw60 outcomes
- non-tail rows are unexpectedly null without source data explanation
- derived return/logreturn columns are numerically inconsistent
- train/validation/test gaps are shorter than horizon
- feature tables include fw60 outcome or target-like proxy columns as model inputs
- binary q90/q10 labels are used as primary source targets without documenting the threshold policy
>>>>>>> c4cec1aefdedc1f28828173afca5be31c7a7e783
