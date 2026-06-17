---
epic: epic_016
id: t107
title: Implement feature target relationship analysis
assignee: analyst_agent
status: pr
blocks: [t110]
blocked_by: [t105]
---

## Goal
Evaluate how each feature relates to `long_mfe_fw60` and `short_mfe_fw60`.

## Scope
- correlation with long target
- correlation with short target
- rank correlation
- binned target response
- single-feature signal proxy
- leakage suspicion flags

## Acceptance Criteria
- [x] Metrics are computed separately for long and short targets.
- [x] Weak, unstable, and suspiciously strong relationships are flagged.
- [x] Outputs can be merged with the univariate quality analysis.
- [x] Results are recorded for the analyst report and feature-set JSON.

## Notes
Implemented in `src/modeling/feature_engineering/target_relation.py` — `analyze_target_relation()`.
Returns one row per (feature, target) pair.
Pearson via DuckDB CORR(); Spearman via RANK() window + CORR() of ranks (pure SQL, no scipy).
signal_proxy = |spearman_rho|.
decision: 'leakage' if |ρ| > max_spearman_leakage; 'weak' if |ρ| < min_spearman_abs; else 'keep'.
Output merges with quality_df in generate_outputs (t110) by feature name.
