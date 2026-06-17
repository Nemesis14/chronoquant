---
epic: epic_016
id: t109
title: Implement time stability analysis
assignee: analyst_agent
status: pr
blocks: [t110]
blocked_by: [t105]
---

## Goal
Analyze whether feature behavior and target relationships remain stable across time buckets.

## Scope
- rolling null rates
- rolling mean and standard deviation
- rolling relation with long and short fw60 targets
- train versus recent drift checks
- feature decay flags

## Acceptance Criteria
- [x] Stability metrics are computed by time bucket.
- [x] Features can be flagged as stable, unstable, decayed, or review.
- [x] Long and short target relations are reported separately.
- [x] Results are included in the feature-set JSON and analyst report.

## Notes
Implemented in `src/modeling/feature_engineering/stability.py` — `analyze_stability()`.
Per-feature DuckDB query: DATE_DIFF-based bucket_idx, per-bucket null_rate/mean/std.
Spearman within bucket via RANK() OVER (PARTITION BY bucket_idx) + CORR().
Baseline = global Spearman over all data (separate query per feature).
drift_long/drift_short = |bucket_spearman - baseline|; max_drift determines flag.
Flag logic: decayed if last 2 buckets exceed max_drift_threshold; unstable if earlier; review at 0.5×; else stable.
