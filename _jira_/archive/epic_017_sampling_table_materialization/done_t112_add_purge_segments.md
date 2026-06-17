---
epic: epic_017
id: t112
title: Add purge segment support
assignee: modeling_agent
status: pr
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
- [x] Segment values include `train`, `valid`, `purge`, and `test`.
- [x] Purge rows are generated between train and validation windows where embargo applies.
- [x] Purge rows are never used for model fitting.
- [x] Purge semantics are documented in sample metadata.

## Notes
This makes leakage protection visible and auditable in sample tables.

Változtatások:
- `yearly_sampler.py`: assign_segments() kiegészítve fold_id (Int16, nullable) és test_start paraméterrel; select_monthly_validation_weeks() kiegészítve test_months paraméterrel
- fold_id: valid/purge sorokhoz a validation week indexe (0-based); train/test sorokhoz null
- test segment: open_time >= test_start esetén — felülír mindent (utolsóként alkalmazzuk)
- test_months=1 default → az utolsó hónap holdout test
- Tesztek: test_segment_assignment.py és test_monthly_validation_weeks.py kibővítve
