---
epic: epic_016
id: t110
title: Generate analyst report and feature set JSON
assignee: analyst_agent
status: todo
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
- [ ] `analyst_report.md` explains the analyses and decisions.
- [ ] `feature_set.json` lists selected features and target columns.
- [ ] Drop reasons are traceable to quality, relationship, redundancy, or stability checks.
- [ ] Output paths are deterministic under `database/<asset_id>/feature_engineering/<run_id>/`.
- [ ] Sampling can consume the JSON without manual editing.

## Notes
The JSON should define which variables may enter sample table materialization.
