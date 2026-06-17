---
epic: epic_016
id: t108
title: Implement redundancy and correlation analysis
assignee: analyst_agent
status: pr
blocks: [t110]
blocked_by: [t105]
---

## Goal
Detect redundant feature groups and recommend which variables should be kept or removed.

## Scope
- Pearson correlation
- Spearman correlation
- high-correlation clusters
- duplicate or near-duplicate columns
- representative feature selection per cluster

## Acceptance Criteria
- [x] Highly correlated feature groups are identified.
- [x] Each group has a recommended representative feature.
- [x] Redundant feature drop recommendations include reasons.
- [x] Output can be written into the analyst report and JSON feature set.

## Notes
Implemented in `src/modeling/feature_engineering/redundancy.py` — `analyze_redundancy()`.
Loads all feat_* columns into Polars, fills NaN/NULL with column mean, converts to numpy.
Full Pearson correlation matrix via np.corrcoef() (vectorised, efficient).
Connected components via union-find (path-compressed): pairs where |r| >= pearson_cluster_thr are merged.
Representative = lowest-index feature per cluster; drop_reason names the representative.
max_spearman column reserved at 0.0 — Spearman pass left as analyst refinement.
