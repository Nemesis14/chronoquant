---
epic: epic_6
id: t6
title: Create 00_create_sample.py CLI and remove old sampling.py
assignee: modeling_agent
status: pr
blocked_by: [t1, t5]
blocks: []
---

## Goal
A sampling modul utolsó lépése: vékony CLI script a `quantitative/` gyökérbe (a többi
számozott script mellé), majd a régi `sampling.py` törlése és az összes import-referencia
frissítése az új csomagra.

## Scope
- `src/modeling/quantitative/00_create_sample.py` (új CLI script)
- `src/modeling/quantitative/sampling.py` (törlés)
- `src/modeling/quantitative/lgbm_search.py` (import frissítés)
- `src/modeling/quantitative/lightgbm_model.py` (import frissítés)

## Acceptance Criteria
- [ ] `00_create_sample.py` argparse CLI:
  - `--sample-id` (required)
  - `--asset-id` (required)
  - `--target-col` (required)
  - `--target-horizon-minutes` (required, int)
  - `--min-train-days` (optional, default: 730)
  - `--valid-days` (optional, default: 180)
  - `--step-days` (optional, default: 180)
  - `--test-days` (optional, default: 365)
  - `--embargo-minutes` (optional, default: None)
- [ ] Script print-el: sample_dir, n_folds, data_start_safe, data_end_safe a létrehozás után
- [ ] `src/modeling/quantitative/sampling.py` törölve
- [ ] `lgbm_search.py` import frissítve: `from modeling.quantitative.sampling import ...` (ugyanaz, az `__init__.py` gondoskodik)
- [ ] `lightgbm_model.py` import frissítve: azonos — ellenőrizni hogy az új csomag útvonal működik
- [ ] Coding standard: thin script, `# %%` nem kell (runner), de modul docstring igen
- [ ] `uv run pyright src/modeling/quantitative/00_create_sample.py` hibátlan
- [ ] `uv run pyright src/modeling/quantitative/lgbm_search.py` hibátlan
- [ ] `uv run pyright src/modeling/quantitative/lightgbm_model.py` hibátlan

## Notes
Az import path (`from modeling.quantitative.sampling import ...`) nem változik —
az `__init__.py` re-exportálja. Elég ellenőrizni hogy az importok nem törnek.
A régi `sampling.py`-ban volt `create_sample_definition_from_db()` és `features_time_range()`
— ezek az új `create_sample()` és `audit_feature_table()` által lefedve, törölhetők.
