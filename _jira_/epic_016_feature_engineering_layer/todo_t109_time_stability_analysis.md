---
epic: epic_016
id: t109
title: Implement time stability analysis
assignee: analyst_agent
status: todo
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
- [ ] Stability metrics are computed by time bucket.
- [ ] Features can be flagged as stable, unstable, decayed, or review.
- [ ] Long and short target relations are reported separately.
- [ ] Results are included in the feature-set JSON and analyst report.

## Notes
This task should identify variables that look useful in old data but are weaker in recent data.
