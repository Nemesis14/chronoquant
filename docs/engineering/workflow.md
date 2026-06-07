# ChronoQuant Engineering Workflow

## General

- Keep scripts thin and place reusable logic under `src/`.
- Keep config loading centralized through `src/utils.py`.
- Keep generated artifacts in their existing artifact directories.
- Avoid unrelated refactors while changing task-specific behavior.

## Data Changes

1. Update config and implementation together.
2. Rebuild derived tables on a bounded interval when possible.
3. Validate row ranges, required columns, and duplicate `open_time` values.

## Model Changes

1. Use shared dataset loading and sampling definitions.
2. Train candidates as inactive models first.
3. Generate model comparison artifacts outside the live predictions table.
4. Promote runtime models only after validation and comparison.

New models use LightGBM only (`lgbm_search.py` + `sweep_strategy.py`).
Logistic regression trainers (lasso, p-value) are legacy — do not start new development with them.
See `docs/engineering/lgbm_model_development.md` for the full workflow.
