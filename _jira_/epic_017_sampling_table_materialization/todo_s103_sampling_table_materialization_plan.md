---
epic: epic_017
id: s103
title: Sampling table materialization plan
---

## Goal
Refactor sampling so it consumes `quant_train` and the feature engineering JSON, then materializes DuckDB sample tables.

Each sample should become a separate DuckDB table. Rows must include the selected features, the long and short target columns, `open_time`, `fold_id`, and a `segment` marker.

## Tasks
- [ ] t111: Refactor sampling to consume quant_train and feature_set JSON
- [ ] t112: Add purge segment support
- [ ] t113: Materialize one DuckDB table per sample
- [ ] t114: Add sample table validation
- [ ] t115: Update sampling and modeling docs

## Notes
Segment values should include at least `train`, `valid`, `purge`, and `test`.
