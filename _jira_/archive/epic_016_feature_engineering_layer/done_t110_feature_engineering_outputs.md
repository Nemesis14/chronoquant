---
epic: epic_016
id: t110
title: Generate analyst report and feature set JSON
assignee: analyst_agent
status: pr
blocks: [t111]
blocked_by: [t106, t107, t108, t109]
---

## Goal
Generate the final feature engineering outputs consumed by sampling and modeling.

## Scope
- analyst report markdown
- feature-set JSON
- selected feature list
- dropped feature list with reasons
- review list
- analysis parameters

## Acceptance Criteria
- [x] `analyst_report.md` explains the analyses and decisions.
- [x] `feature_set.json` lists selected features and target columns.
- [x] Drop reasons are traceable to quality, relationship, redundancy, or stability checks.
- [x] Output paths are deterministic under `database/<asset_id>/feature_engineering/<run_id>/`.
- [x] Sampling can consume the JSON without manual editing.

## Notes
Implemented in `src/modeling/feature_engineering/reporting.py` — `generate_outputs()`.
Merges quality/relation/redundancy/stability DataFrames per feature into selected/dropped/review lists.
feature_set.json: run_id, asset_id, created_at, target_cols, selected, dropped, review, thresholds.
analyst_report.md: summary table + per-section feature lists + analysis parameters.
Output dir created if absent (mkdir parents=True).
Smoke test: test_generate_outputs_writes_files and test_generate_outputs_feature_set_schema pass.
