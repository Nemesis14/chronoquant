# Target Concepts

Current target methodology lives in `_doc_/3000_targets.md` and the code
reference lives in `_doc_/3100_sync_targets.md`.

Active primary target columns:

| Target | Meaning |
|---|---|
| `long_mfe_fw60` | Log maximum favorable excursion over `t+1..t+60` for long models |
| `short_mfe_fw60` | Log minimum favorable excursion over `t+1..t+60` for short models |

Binary event labels are derived downstream per model quantile, not stored as
legacy `trg_*` target columns.
