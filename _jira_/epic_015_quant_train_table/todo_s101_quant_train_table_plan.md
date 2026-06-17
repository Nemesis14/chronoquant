---
epic: epic_015
id: s101
title: Quant train DuckDB table plan
---

## Goal
Create a canonical `quant_train` DuckDB table that joins the approved feature rows from `feat_ohlcv_quant` with the two fw60 modeling targets from `target`.

The table is the stable source for feature engineering, sampling, and later model training. It should contain one row per `open_time`, all feature columns selected for the training universe, and the long/short target columns.

## Tasks
- [ ] t101: Define quant_train schema and rebuild contract
- [ ] t102: Implement quant_train build pipeline
- [ ] t103: Add quant_train validation and stats
- [ ] t104: Document quant_train data contract

## Notes
- Use table name `quant_train` exactly; do not introduce `quant_train_base`.
- Initial target columns: `long_mfe_fw60` and `short_mfe_fw60`.
- Derived binary targets may be added later only if the modeling config explicitly requires them.
