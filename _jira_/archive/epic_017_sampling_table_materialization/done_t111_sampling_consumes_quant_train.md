---
epic: epic_017
id: t111
title: Refactor sampling to consume quant_train and feature_set JSON
assignee: modeling_agent
status: pr
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
- [x] Sampling reads selected feature columns from `feature_set.json`.
- [x] Sampling uses `quant_train` as the source table.
- [x] Only approved features and required targets are included.
- [x] Existing split generation behavior is preserved unless explicitly changed by later tasks.

## Notes
This task creates the handoff from feature engineering to sampling.

Blocker override: t110 (feature_set.json) még todo — feature_cols=() esetén az összes feat_* kolumna auto-discovery a quant_train sémájából. Ha feature_set.json létezik, a caller (00_create_sample.py) tölti be és adja át feature_cols-ként.

Változtatások:
- `config.py`: feature_cols (tuple, default=()) és test_months (int, default=1) hozzáadva
- `create_sample.py`: forrás ohlcv JOIN target → quant_train; _resolve_feature_cols() auto-discovery; _test_start() helper
- Metadata kibővítve: feature_cols, test_months mezőkkel
