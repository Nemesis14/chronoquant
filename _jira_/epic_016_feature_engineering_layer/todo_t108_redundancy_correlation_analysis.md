---
epic: epic_016
id: t108
title: Implement redundancy and correlation analysis
assignee: analyst_agent
status: todo
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
- [ ] Highly correlated feature groups are identified.
- [ ] Each group has a recommended representative feature.
- [ ] Redundant feature drop recommendations include reasons.
- [ ] Output can be written into the analyst report and JSON feature set.

## Notes
Representative choice should consider feature quality and feature-target relationship metrics when available.
