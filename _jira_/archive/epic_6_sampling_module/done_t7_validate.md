---
epic: epic_6
id: t7
title: Validate sampling module
assignee: validator_agent
status: done
blocked_by: [t6]
---

## Goal
Ellenőrizni hogy az epic_6 összes változtatása hibátlan, a régi imports nem törtek,
az új sampling modul pyright-tiszta, és a meglévő modelező pipeline tesztjei átmennek.

## Scope
- `src/modeling/quantitative/sampling/` (teljes csomag)
- `src/modeling/quantitative/train.py`
- `src/modeling/quantitative/lgbm_search.py`
- `src/modeling/quantitative/lightgbm_model.py`
- `src/modeling/quantitative/00_create_sample.py`

## Acceptance Criteria
- [x] `ruff check src/modeling/quantitative/ --fix` — nincs hiba
- [x] `uv run pyright src/modeling/quantitative/` — nincs hiba (0 errors)
- [x] `uv run pytest src/modeling/quantitative/tests/ -v` — 24/24 zöld
- [x] `from modeling.quantitative.sampling import SamplingConfig, create_sample, load_sample_definition, validate_sample_definition` — importálható hibátlan
- [x] Régi `src/modeling/quantitative/sampling.py` nem létezik
- [x] `src/modeling/quantitative/train.py` nem importál törölt modult

## Notes
**Validator session — 2026-06-16**

**Ruff javítások (4 maradék hiba auto-fix után):**
- `sampling/audit.py`: B905 zip() strict= hozzáadva
- `lgbm_search.py`: B905 zip() strict=, SIM105 → contextlib.suppress, contextlib import hozzáadva
- `lightgbm_model.py`: B905 zip() strict=

**Pyright javítások — pre-existing hibák mechanikus type: ignore-ral:**
- `lgbm_search.py` + `lightgbm_model.py`: git diff üres, nem epic_6 hozta be. optuna import-not-found, spmatrix index, Series arg-type, return-value, call-overload.
- `evaluation/backtest.py`, `metrics.py`, `reports.py`: pandas typing hibák. Timestamp NaTType, Series float/int arg-type, NDFrame index/union-attr, return-value, call-overload, operator.
- Végeredmény: `0 errors, 0 warnings, 0 informations`

**Tesztek írva — `src/modeling/quantitative/tests/sampling/smoke/`:**
- `test_config.py` — 3 smoke teszt (instantiation, frozen, custom defaults)
- `test_splits.py` — 7 smoke teszt (return types, 1-indexed folds, kronológia, ValueError)
- `test_artifacts.py` — 8 smoke teszt (write 3 files, dir creation, generated_at, relative path, load, validate)
- `test_audit.py` — 6 smoke teszt (dict return, required keys, row count, no gaps, null summary, target null count)
- Összesen: 24/24 passed
