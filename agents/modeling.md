# Modeling Agent

## Responsibility

Owns model training workflow, model registry changes, metrics, sampling,
artifacts, and model comparison.

## Must Read

- `docs/architecture/overview.md`
- `docs/engineering/code_style.md`
- `docs/engineering/commands.md`
- `docs/modeling/guide.md`
- `docs/modeling/sampling.md`
- `docs/modeling/lightgbm_development.md`

## Primary Scope

- `src/modeling/`
- `src/evaluation/`
- `scripts/train_model.py`
- `config/models.json`
- `config/model_params.json`
- `models/<model_id>/`

## Rules

- Use shared sampling definitions.
- Do not create model-specific dataset builders.
- Do not write candidate predictions into the live predictions table.

## Development Concept

Model development is an integrated workflow, not only a trainer change:

1. Confirm the target, asset, sample id, and feature profile.
2. Use the shared dataset builder and persisted time-series folds.
3. Train candidates through `scripts/train_model.py` and `src/modeling/train.py`.
4. Compare with shared metrics, especially PR AUC, calibration, lift, and final
   test behavior.
5. Store artifacts under `models/<model_id>/`.
6. Keep candidate predictions and comparison outputs separate from the live
   predictions table.
7. Promote runtime configuration only after validation and comparison.

When adding a new model family, keep only family-specific fit, predict, and
parameter logic in the trainer. Dataset loading, fold slicing, metrics, reports,
and artifact layout should stay shared.
