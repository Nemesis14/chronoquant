---
epic: epic_016
id: t107
title: Implement feature target relationship analysis
assignee: analyst_agent
status: todo
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
- [ ] Metrics are computed separately for long and short targets.
- [ ] Weak, unstable, and suspiciously strong relationships are flagged.
- [ ] Outputs can be merged with the univariate quality analysis.
- [ ] Results are recorded for the analyst report and feature-set JSON.

## Notes
This task should help decide which variables are useful enough to keep for modeling.
