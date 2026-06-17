---
epic: epic_016
id: t106
title: Implement univariate feature quality analysis
assignee: analyst_agent
status: pr
blocks: [t110]
blocked_by: [t105]
---

## Goal
Analyze each feature independently and flag variables that should be dropped or reviewed before modeling.

## Scope
- feature null rate
- infinite value rate
- zero or near-zero variance
- duplicate or constant values
- outlier ratio
- missingness by time bucket

## Acceptance Criteria
- [x] Per-feature metrics are produced from `quant_train`.
- [x] Each feature receives a decision: keep, drop, or review.
- [x] Drop reasons are explicit and serializable.
- [x] Thresholds are configurable and recorded.

## Notes
Implemented in `src/modeling/feature_engineering/quality.py` — `analyze_quality()`.
One DuckDB query per feature: null_rate, inf_rate, variance, outlier_ratio (|z|>3).
Decision logic: drop if null/inf/variance threshold exceeded; review if outlier ratio high; else keep.
drop_reason is a human-readable string serialized in the DataFrame.
Thresholds read from FeatureEngineeringConfig (max_null_rate, max_inf_rate, min_variance, max_outlier_ratio).
