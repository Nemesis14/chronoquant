---
epic: epic_017
id: t111
title: Refactor sampling to consume quant_train and feature_set JSON
assignee: modeling_agent
status: todo
blocks: [t112, t113]
blocked_by: [t102, t110]
---

## Goal
Make sampling consume `quant_train` and the approved feature list from the feature engineering JSON.

## Scope
- `src/modeling/quantitative/sampling/create_sample.py`
- `src/modeling/quantitative/sampling/audit.py`
- `src/modeling/quantitative/sampling/artifacts.py`

## Acceptance Criteria
- [ ] Sampling reads selected feature columns from `feature_set.json`.
- [ ] Sampling uses `quant_train` as the source table.
- [ ] Only approved features and required targets are included.
- [ ] Existing split generation behavior is preserved unless explicitly changed by later tasks.

## Notes
This task creates the handoff from feature engineering to sampling.
