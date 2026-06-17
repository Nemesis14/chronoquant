---
epic: epic_6
id: t5
title: Create sampling/create_sample.py and sampling/__init__.py
assignee: modeling_agent
status: pr
blocked_by: [t2, t3, t4]
blocks: [t6]
---

## Goal
Összeilleszteni a sampling modult: az orchestrátor (`create_sample.py`) összefogja
az audit → splits → write lépéseket; az `__init__.py` re-exportálja amit kívülről
kell, hogy a belső struktúra változása ne törje a többi modult.

## Scope
- `src/modeling/quantitative/sampling/create_sample.py` (új)
- `src/modeling/quantitative/sampling/__init__.py` (kitöltése, t2-ben placeholder volt)

## Acceptance Criteria
- [ ] `create_sample(config: SamplingConfig) -> None` implementálva:
  1. `db_path` = `utils.load_asset_config(config.asset_id)["database"]["db_path"]`
  2. `sample_dir` = `Path(f"database/{config.asset_id}/samples/{config.sample_id}")`
  3. `audit = audit_feature_table(db_path, config.target_col)`
  4. `splits = build_expanding_window_splits(data_start=audit["data_start_safe"], data_end=audit["data_end_safe"], ...)`
  5. `metadata` dict összeállítása (relatív `db_relative_path`)
  6. `write_sample_artifacts(sample_dir, metadata, splits, audit)`
- [ ] `embargo_minutes` default logika: `config.embargo_minutes or config.target_horizon_minutes`
- [ ] `__init__.py` re-exportál:
  ```python
  from .config import SamplingConfig
  from .create_sample import create_sample
  from .artifacts import load_sample_definition, validate_sample_definition
  ```
- [ ] Coding standard: modul docstring, Google-style, `# %%` markerek
- [ ] `uv run pyright src/modeling/quantitative/sampling/create_sample.py` hibátlan

## Notes
Az egyetlen hely ahol `utils` importálódik a sampling csomagban — a többi fájl
(splits, audit, artifacts) független marad a project config rendszertől.
