---
epic: epic_017
id: t112
title: Add purge segment support
assignee: modeling_agent
status: todo
blocks: [t113]
blocked_by: [t111]
---

## Goal
Represent embargo/purge windows explicitly in sample outputs instead of only excluding them from training.

## Scope
- split representation
- sample artifact metadata
- sample table materialization logic

## Acceptance Criteria
- [ ] Segment values include `train`, `valid`, `purge`, and `test`.
- [ ] Purge rows are generated between train and validation windows where embargo applies.
- [ ] Purge rows are never used for model fitting.
- [ ] Purge semantics are documented in sample metadata.

## Notes
This makes leakage protection visible and auditable in sample tables.
