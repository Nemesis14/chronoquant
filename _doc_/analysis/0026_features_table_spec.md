# Features Table Analysis Spec

## Table

`feat_ohlcv_quant`

Primary key: `open_time`

Expected metadata columns:

- `open_time`
- `close`
- `available_ts`
- `lookback_end_ts`

Expected feature columns:

- all model input columns must start with `feat_`

Important project rule:

- All OHLCV-based features must be shifted by t-1.
- Stored feature value at row t must use only market data from bars <= t-1.
- Deterministic time-index features may be exempt from t-1 shifting.

Known deterministic t2 feature exceptions:

- `feat_bars_into_session_norm`
- `feat_hour_sin`
- `feat_hour_cos`
- `feat_dayofweek_sin`
- `feat_dayofweek_cos`
- `feat_weekend`
- `feat_session_asia`
- `feat_session_europe`
- `feat_session_us`

## Purpose

The `feat_ohlcv_quant` table contains engineered predictors used by ML models.

The analyst must verify that features are temporally valid, numerically sane, aligned to OHLCV, and suitable for out-of-sample modeling.

## Required Checks

### 1. Basic coverage and schema

Report:

- row count
- first `open_time`
- last `open_time`
- duplicate `open_time` count
- number of feature columns
- feature column groups by prefix/family
- non-feature columns
- rows missing matching `ohlcv`

Critical if:

- duplicate `open_time`
- feature rows without matching OHLCV
- model input columns not starting with `feat_`
- `trg_*`, `future_*`, `label_*`, or prediction columns appear among features

### 2. Availability timestamp and temporal validity

Check:

- `available_ts <= open_time`
- `lookback_end_ts <= open_time`
- for OHLCV-based features, stored values should not depend on bar t or future bars

Report:

- violation counts
- max/min lag: `open_time - available_ts`
- max/min lookback lag: `open_time - lookback_end_ts`

Critical if any feature row has `available_ts > open_time`.

### 3. T-1 lag audit

For each feature family, recompute a sample from OHLCV and compare expected t-1 behavior.

Examples:

- returns at t should match return ending at t-1
- rolling mean at t should match window ending at t-1
- RSI/ADX/ATR warmup should reflect rolling period plus t-1 lag
- deterministic calendar/session features may match t directly

Required output:

- feature name
- detected window size, if inferable
- expected first valid row
- actual first valid row
- null warmup count
- t-1 comparison pass/fail
- max absolute diff on sampled recomputation

### 4. Null and NaN pattern audit

For every feature column report:

- null count
- NaN count
- infinite count
- percent missing
- first valid timestamp
- last valid timestamp
- longest null run
- null count by year/month

Critical if:

- unexpected full-column nulls
- infinite values
- large unexplained mid-series null blocks
- model input features are silently filled without documenting missingness

### 5. Distribution and scale audit

For every feature report:

- mean
- std
- min
- max
- p01
- p05
- p50
- p95
- p99
- number of unique values
- zero rate
- constant-column flag
- near-constant flag

Check impossible indicator ranges, scale breaks, all-zero features, binary flags outside 0/1, ranks outside 0-1, and exploding ratios.

### 6. Feature redundancy and correlation

Compute:

- pairwise correlation among numeric features
- high-correlation clusters
- duplicate or near-duplicate columns
- correlation with target, if target is joined
- univariate AUC/PR-AUC where useful

Critical if a feature is an implausibly strong target proxy.

### 7. Drift and regime stability

Analyze by:

- year
- month
- volatility regime
- volume regime
- train/validation/test fold, if available

Report top drifting features, date-dependent availability, and missingness shifts.

### 8. Model input consistency

For every active model artifact:

- load `features.json`
- confirm all listed input features exist
- confirm no target/prediction columns are in input list
- confirm model input order is reproducible
- report missing, unused, and extra features

## Required Notebook Outputs

1. Feature schema summary.
2. Timestamp availability audit.
3. T-1 lag audit table.
4. Null/NaN/inf table for all features.
5. Distribution summary for all features.
6. Indicator range violation table.
7. Feature correlation/redundancy table.
8. Drift summary by time period.
9. Model input consistency table.
10. Leakage proxy scan.

## Critical Findings

Mark as critical if any of these occur:

- `available_ts > open_time`
- feature uses bar t or future data when it should be t-1
- target-like columns appear in model features
- infinite values exist
- large unexplained null blocks
- model artifacts reference missing features
- feature distribution changes sharply at train/test boundary
