---
epic: epic_016
id: t106
title: Implement univariate feature quality analysis
assignee: analyst_agent
status: todo
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
- [ ] Per-feature metrics are produced from `quant_train`.
- [ ] Each feature receives a decision: keep, drop, or review.
- [ ] Drop reasons are explicit and serializable.
- [ ] Thresholds are configurable and recorded.

## Notes
This task does not evaluate target relationship; that is handled separately.
