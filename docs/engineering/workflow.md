# ChronoQuant Engineering Workflow

This document is the high-level map for model and strategy work. Keep detailed
rules in the domain guides:

- `docs/engineering/sampling.md`: data range, sample, split, and holdout rules.
- `docs/engineering/lgbm_model_development.md`: LightGBM model development and
  promotion.
- `docs/engineering/strategy_evaluation.md`: trigger sweeps, backtests,
  robustness checks, and strategy config updates.

## General

- Run commands from the repo root.
- Keep scripts thin and place reusable logic under `src/`.
- Keep config loading centralized through `src/utils.py`.
- Keep generated artifacts in their existing artifact directories.
- Avoid unrelated refactors while changing task-specific behavior.

## End-to-End Flow

1. **Data audit:** verify the asset data range, row counts, required columns,
   gaps, duplicate `open_time` values, and target/feature null rates.
2. **Sample and split:** create or reuse a deterministic `sample_id` with
   train/validation folds and a final holdout according to `sampling.md`.
3. **Model search:** train inactive candidates and store model-comparison
   artifacts outside the live predictions table.
4. **Model validation:** compare validation folds, guardrails, calibration,
   lift, and final holdout behavior.
5. **Promotion fit:** after a candidate passes review, refit the production
   artifact on the approved data range using the selected features and params.
6. **Prediction sync:** write only the runtime model predictions needed by the
   application; keep candidate evaluation outputs separate.
7. **Strategy evaluation:** select robust triggers outside the untouched holdout,
   then report unchanged trigger behavior on the holdout.
8. **Model card:** generate `models/<model_id>/model_card.json` for each
   promoted model using `scripts/generate_model_card.py`. This writes CV
   metrics, feature count, and holdout backtest results in a structured file
   that the dashboard reads at startup — no hardcoded stats in UI code.

   ```
   python scripts/generate_model_card.py \
       --model-id  <model_id> \
       --side      long|short \
       --holdout-start "YYYY-MM-DD HH:MM:SS" \
       --holdout-end   "YYYY-MM-DD HH:MM:SS" \
       --entry     <entry_threshold> \
       --max-hold  <max_hold_minutes>
   ```

9. **Report:** document CV metrics, holdout metrics, strategy results,
   artifacts, and config changes.
10. **Config update:** update `config/models.json`, `config/env.json`, and
    `config/strategies.json` only after validation and comparison.
11. **UI verification:** confirm the dashboard loads the intended runtime model,
    active strategy, latest predictions, and backtest/report summaries.

## Data Changes

1. Update config and implementation together.
2. Rebuild derived tables on a bounded interval when possible.
3. Validate row ranges, required columns, duplicate `open_time` values, and
   gaps before using the data for modeling.

## Model Changes

1. Use shared dataset loading and sampling definitions.
2. Train candidates as inactive models first.
3. Generate model comparison artifacts outside the live predictions table.
4. Promote runtime models only after validation, final holdout review, and
   strategy/backtest comparison.

New models use LightGBM only (`lgbm_search.py` + `sweep_strategy.py`).
Logistic regression trainers (lasso, p-value) are legacy; do not start new
development with them.

## Strategy Changes

1. Keep strategy and trigger selection model-independent.
2. Use prediction frames and market facts, not training internals.
3. Treat full-period backtests as diagnostics when they include training data.
4. Update `config/strategies.json` only after trigger robustness and holdout
   reporting are complete.
