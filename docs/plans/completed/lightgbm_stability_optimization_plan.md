# LightGBM Local Optimization Plan

## Context

The SOLUSDT 1h feature expansion is now much wider than the original LightGBM
setup was designed for:

- Current target profile: `solusdt_fw60`
- Long target: `trg_l_fw60_q90`
- Short target: `trg_s_fw60_q10`
- Sample: `base_solusdt_fw60_dev`
- Current champions:
  - `lgbm_solusdt_l_fw60_q90_stable_v1`
  - `lgbm_solusdt_s_fw60_q10_stable_v1`
- Current v1 input feature count: 47
- Feature expansion status from `solusdt_1h_feature_expansion_plan.md`:
  - 190 live `feat_` columns after the second-batch rebuild;
  - 208 `feat_` columns expected after activity backfill.

The current LightGBM trainer is not suitable for this scale of search. It tunes
only one parameter, `num_leaves`, with a small fixed grid. With about 200
features and more than 3M SOLUSDT rows, a broad grid search is too slow and too
fragile for a local PC.

## Goal

Find the best stable SOLUSDT 1h LightGBM candidate on the validation folds, with
**validation log loss as the primary optimization metric**.

Secondary goals:

- keep PR AUC, ROC AUC, Brier score, calibration, and lift in the reports;
- use PR AUC and top-percentile lift as guardrails so the selected model keeps
  useful ranking power for rare q90/q10 events;
- make the search resumable after memory failure or process interruption;
- save every completed and failed trial so bad parameter regions are not
  retried;
- fit within local PC memory by using staged data size, row stride, feature
  filtering, early stopping, and bounded trial budgets;
- keep candidate predictions separate from the live predictions table.

## External References Checked

- LightGBM's tuning guide treats `num_leaves`, `min_data_in_leaf`
  / `min_child_samples`, `max_depth`, `learning_rate`, and bagging/feature
  sampling as core tuning levers.
- LightGBM Python callbacks support `early_stopping`, `log_evaluation`, and
  `record_evaluation`, which are needed for per-trial training curves and
  best-iteration capture.
- Optuna supports probability-distribution-based search spaces through
  `suggest_int`, `suggest_float`, and categorical suggestions. Its `TPESampler`
  starts with random trials, then uses completed trial results to sample more
  promising parameter regions.
- Optuna persistent storage, especially local SQLite `RDBStorage` or
  file-based journal storage, supports save/resume behavior for interrupted
  studies.

## Non-Goals

- Do not run a full Cartesian grid over LightGBM parameters.
- Do not optimize against the live predictions table.
- Do not promote a model from validation log loss alone; promotion still needs
  final holdout and backtest checks.
- Do not add cross-coin features.
- Do not build a model-specific dataset loader.

## Main Direction

Use a staged, distribution-based search:

1. **Smoke stage:** tiny budget, row-strided data, verifies that 190-208 features
   can train without memory failure.
2. **Exploration stage:** Optuna/TPE search on row-strided data, validation
   logloss objective, broad distributions.
3. **Refinement stage:** narrower distributions around the best regions, lower
   row stride or full data if memory allows.
4. **Final CV stage:** retrain top candidates on all folds and a larger row set.
5. **Final model fit:** fit the chosen candidate on the pre-test training range
   and evaluate final holdout.

The search should be **time-budgeted**, not grid-complete. A local run should be
able to stop after a fixed number of trials or hours and resume later without
losing previous trial results.

## Local PC Constraints

Expected pressure points:

- 3M+ rows times ~200 float columns is large before LightGBM internal histogram
  memory is considered.
- Five expanding CV folds multiply training cost.
- Early high-complexity trials can consume too much memory if `num_leaves`,
  `max_bin`, and feature fraction are too loose.
- Saving full validation predictions for every trial would be too large.

Local safeguards:

- Use `row_stride=60` for smoke and exploration.
- Move to `row_stride=10`, `5`, or `1` only for finalists.
- Use `max_bin` no higher than `127` during search; test `255` only for
  finalists if runtime allows.
- Use `force_col_wise=true` and controlled `num_threads`.
- Use feature prefiltering before the search:
  - remove known duplicate columns from the feature expansion summary;
  - remove all-null, high-null, constant, and near-constant columns;
  - optionally cap to the top 120-160 candidates by deterministic audit score
    for the first exploration stage.
- Do not keep trained boosters for failed or non-finalist trials unless needed
  for debugging.
- Save only trial-level metrics and compact training curves during search.

## Search Method

### Preferred Search Engine

Use Optuna with:

- `TPESampler(seed=42, n_startup_trials=20, multivariate=True)` if available;
- local persistent storage:
  - preferred: `sqlite:///models/<model_id>/optuna_study.db`;
  - acceptable fallback: file journal storage under
    `models/<model_id>/optuna_journal.log`;
- `load_if_exists=True` so the study resumes instead of starting from scratch;
- `direction="minimize"` because the primary metric is validation log loss.

Fallback if Optuna is not installed:

- implement seeded random search from the same distributions;
- write every trial to `search_trials.jsonl`;
- skip already-seen parameter hashes on resume.

### Metric Policy

Use validation log loss as the primary search metric because the downstream
strategy uses probability thresholds. A lower log loss generally means the
probability scale is more useful for thresholding and calibration.

Do not optimize PR AUC alone. PR AUC is important for q90/q10 targets because it
measures minority-event ranking quality, but it can improve while probability
calibration gets worse. That would make threshold selection less stable.

Selection policy:

- Primary objective: minimize validation log loss with fold-stability penalties.
- Guardrails:
  - validation PR AUC must not materially regress versus the current champion;
  - top 1%, 5%, and 10% lift must not materially regress;
  - Brier score and calibration buckets must remain acceptable;
  - prediction concentration around strategy thresholds must remain usable.

Initial guardrail thresholds:

- reject a candidate if mean validation PR AUC is more than `5%` below the
  current champion, unless log loss and calibration improve dramatically and
  backtest robustness later confirms the tradeoff;
- reject a candidate if top 5% or top 10% lift collapses on multiple folds;
- prefer the lower-logloss candidate only among models that pass the ranking and
  lift guardrails.

### Objective Function

Primary objective:

```text
mean_valid_log_loss + stability_penalties
```

Recommended score:

```text
score =
    mean(valid_log_loss)
    + 0.25 * std(valid_log_loss)
    + 0.10 * max(0, mean(valid_log_loss - train_log_loss) - allowed_logloss_gap)
```

Initial `allowed_logloss_gap`: `0.03`.

Store these separately:

- raw `mean_valid_log_loss`;
- `std_valid_log_loss`;
- `mean_train_log_loss`;
- `mean_logloss_gap`;
- final `objective_score`.

Tie-breakers:

1. lower raw mean validation log loss;
2. lower validation Brier score;
3. passes PR AUC and lift guardrails with the largest margin;
4. higher validation PR AUC;
5. lower fold-to-fold logloss standard deviation;
6. lower model complexity;
7. smaller feature set.

### Parameter Distributions

Use distributions instead of grids:

```text
num_leaves: int log-ish range [3, 63]
max_depth: categorical [-1, 2, 3, 4, 5, 6, 8]
min_child_samples: int log range [200, 8000]
min_child_weight: float log range [1e-4, 1e-1]
min_split_gain: float log-ish range [1e-5, 0.1], plus exact 0.0 option
reg_alpha: float log range [1e-3, 10.0]
reg_lambda: float log range [1.0, 100.0]
subsample: float uniform [0.45, 0.95]
subsample_freq: categorical [1]
colsample_bytree: float uniform [0.35, 0.95]
learning_rate: float log range [0.005, 0.05]
n_estimators: fixed high cap 2000-4000 with early stopping
max_bin: categorical [63, 127]
path_smooth: float log range [1e-3, 10.0]
extra_trees: categorical [false, true]
```

Constraints:

- If `max_depth > 0`, enforce `num_leaves <= 2^max_depth`.
- For the first local stage, prefer `num_leaves <= 31`.
- Keep `class_weight=null` first; only test class weighting after calibration
  and logloss behavior is understood.
- Use LightGBM metric `binary_logloss` for early stopping.

## Logging And Checkpointing

Every trial must be persisted immediately after completion or failure.

Required artifacts under `models/<model_id>/search/`:

- `optuna_study.db` or `optuna_journal.log`: persistent search state.
- `search_trials.jsonl`: one JSON line per attempted trial.
- `search_best.json`: current best trial after each completed trial.
- `search_summary.csv`: compact table of completed trials.
- `failed_trials.jsonl`: failed trial parameters and exception summary.
- `skipped_trials.jsonl`: parameter hashes already seen and skipped on resume.
- `trial_logs/trial_<number>.json`: parameters, fold metrics, best iterations,
  memory notes, elapsed time.
- `trial_curves/trial_<number>_fold_<fold>.json`: compact LightGBM eval history
  from `record_evaluation`, capped to relevant metric curves.

Do not save full per-row validation predictions for every search trial. Save
full validation predictions only for:

- current best trial;
- top 5 finalists;
- final selected model.

Resume rule:

- On startup, load the persistent study and `search_trials.jsonl`.
- Build a stable hash from resolved LightGBM params, feature profile, target,
  row stride, and fold subset.
- Skip any completed or failed hash unless the user explicitly passes a
  `--retry-failed` option.

Failure handling:

- Catch out-of-memory, LightGBM fit errors, and data-shape errors per trial.
- Mark the trial as failed, persist the failure, release model objects, run
  garbage collection, and continue.
- If three consecutive trials fail for memory, automatically tighten the stage:
  lower `num_leaves`, lower `max_bin`, increase `min_child_samples`, or increase
  `row_stride`.

## Staged Execution Plan

### Stage 0: Data And Feature Audit

Purpose: avoid wasting LightGBM trials on broken or redundant columns.

Actions:

- Run or implement a feature audit for SOLUSDT:
  - null rate;
  - constant / near-constant rate;
  - pairwise duplicate feature detection;
  - memory estimate for selected feature matrix;
  - target positive rate per fold.
- Build a modeling feature list for search:
  - start with all live non-activity features if backfill is not complete;
  - include activity features only after backfill null rates are acceptable;
  - exclude known duplicates from the feature expansion summary.

Acceptance criteria:

- Search feature list is written to `models/<model_id>/search/features_search.json`.
- The audit estimates memory for row strides `60`, `10`, `5`, and `1`.
- No all-null or constant feature enters the search.

### Stage 1: Smoke Search

Purpose: prove the training loop, checkpointing, and memory controls work.

Settings:

- `row_stride=60`
- fold subset: first 2 folds only
- `n_trials=5`
- `timeout_minutes=30`
- feature list: audited search list
- primary metric: validation log loss

Acceptance criteria:

- At least one completed trial is saved.
- Failed trials are logged and not retried on rerun.
- `search_best.json` and `search_summary.csv` are written.
- Trial eval curves include train and validation `binary_logloss`.

### Stage 2: Broad Distribution Search

Purpose: find good parameter regions without a grid.

Settings:

- `row_stride=60`
- all five folds
- `n_trials=60` or `timeout_hours=6`, whichever comes first
- Optuna TPE with random startup trials
- early stopping rounds: `100`
- `n_estimators=3000`

Acceptance criteria:

- Completed trials are persisted continuously.
- The best trial minimizes stability-adjusted validation log loss.
- Top 10 parameter regions are summarized.
- Memory failures do not erase completed work.

### Stage 3: Narrow Refinement

Purpose: spend local compute only near promising regions.

Settings:

- Start from the top 10 Stage 2 trials.
- Narrow parameter distributions around observed good ranges.
- `row_stride=10` or `5`, depending on memory estimate.
- `n_trials=30` or `timeout_hours=8`.

Acceptance criteria:

- The best Stage 3 trial is compared against the best Stage 2 trial on the same
  row stride for a fair check.
- If lower row stride materially changes the ranking, document it.

### Stage 4: Finalist Full-Fold Evaluation

Purpose: evaluate the top candidates more carefully before final fit.

Settings:

- choose top 5 trials by validation log loss score;
- train all five folds;
- use lowest feasible row stride:
  - try `row_stride=5`;
  - try `row_stride=1` only if memory and runtime are acceptable;
- save full validation predictions for these finalist trials only.

Acceptance criteria:

- Finalist comparison includes log loss, Brier score, PR AUC, ROC AUC,
  calibration, lift, prediction concentration, and fold stability.
- The selected finalist is not chosen from a single unusually good fold.

### Stage 5: Final Fit And Holdout

Purpose: produce the candidate model artifact.

Actions:

- Refit the selected params on the pre-test training window.
- Use early stopping only with an internal validation slice from pre-test data,
  not the final test period.
- Evaluate final holdout.
- Save the standard model artifacts:
  - `model.pkl`
  - `features.json`
  - `metrics.json`
  - `params.json`
  - `cv_results.csv`
  - `validation_predictions.csv`
  - `report.html`

Acceptance criteria:

- Final holdout log loss is reported separately from validation log loss.
- Feature list and parameter hash match the selected trial.
- Candidate remains inactive.

## Implementation Tasks

### Task 1: Add Search Artifact Layout

- Add a `search/` directory under each candidate model output directory.
- Implement helper functions for JSONL trial append, best-trial write,
  parameter hashing, and failure logging.

Acceptance criteria:

- Trial logs are flushed after every trial.
- A killed process can resume without losing completed trial records.

### Task 2: Add Distribution-Based Search Runner

- Add a new trainer mode or script entry point, for example:
  - `python scripts/train_model.py --model-id <id> --search`
  - or `python scripts/search_lgbm.py --model-id <id>`
- Prefer keeping reusable logic under `src/modeling/`.
- Use Optuna if available; otherwise use seeded random search fallback.

Acceptance criteria:

- Search distributions live in config, not hard-coded in the script.
- The runner supports `--n-trials`, `--timeout-hours`, `--row-stride`,
  `--fold-limit`, and `--resume`.

### Task 3: Add LightGBM Callbacks

- Use early stopping on validation `binary_logloss`.
- Use `record_evaluation` to capture train/validation logloss curves.
- Use controlled log output through `log_evaluation` or project logging.

Acceptance criteria:

- Every trial records `best_iteration`.
- Training curves are saved in compact JSON.
- Verbose LightGBM output does not flood the console.

### Task 4: Add Local Memory Controls

- Estimate feature matrix memory before training.
- Allow row stride and fold subset control from CLI/config.
- Clean up model objects after every trial.
- Add automatic tightening after repeated memory failures.

Acceptance criteria:

- Memory-related failures are logged as failed trials, not silent crashes when
  catchable.
- The plan can run first on `row_stride=60` and later on lower strides.

### Task 5: Implement Logloss Objective

- Make validation log loss the primary search metric.
- Keep PR AUC and other metrics as secondary diagnostics.
- Add stability penalties for fold-to-fold logloss variance and train-valid
  logloss gap.
- Add PR AUC and top-percentile lift guardrails so low-logloss but weak-ranking
  candidates are rejected.

Acceptance criteria:

- The best trial can be reproduced from `search_summary.csv`.
- Reports clearly separate raw validation log loss from penalized objective.
- Reports show whether each finalist passed or failed the PR AUC/lift
  guardrails.

### Task 6: Create SOLUSDT Local V2 Search Models

- Add inactive long and short candidate IDs if not already present:
  - `lgbm_solusdt_l_fw60_q90_local_v2`
  - `lgbm_solusdt_s_fw60_q10_local_v2`
- Point them at the expanded feature profile and search parameter profile.

Acceptance criteria:

- Candidate models are inactive by default.
- Search output does not touch `solusdt_1m_predictions`.

### Task 7: Run Long And Short Searches Separately

- Run the long and short optimization as separate studies.
- Use identical feature audit and search distribution unless one direction
  proves unstable.
- Keep separate study DBs and summaries.

Acceptance criteria:

- Long and short best trials are independently selected by validation log loss.
- Shared bad parameter regions can be noted, but not blindly copied.

### Task 8: Finalist Backtest And Promotion Review

- Only after validation and final holdout are acceptable, run threshold and
  backtest sweeps.
- Do not choose the model by backtest alone.

Acceptance criteria:

- Promotion requires validation logloss improvement or clear calibration
  improvement, passed PR AUC/lift guardrails, final holdout sanity, and robust
  backtest behavior.

## Recommended First Local Run

1. Run feature audit and produce the search feature list.
2. Run smoke search for long with `row_stride=60`, two folds, five trials.
3. Run smoke search for short with the same settings.
4. Run broad long search with `row_stride=60`, all folds, 60 trials or 6 hours.
5. Run broad short search with the same budget.
6. Refine the top regions with `row_stride=10` or `5`.
7. Evaluate top 5 finalists with full fold diagnostics.
8. Fit final inactive long and short candidates.
9. Compare against current champions.

## Promotion Gate

Do not promote unless the selected candidate:

- improves or preserves final holdout log loss;
- improves validation log loss stability;
- does not materially regress PR AUC and lift;
- has acceptable calibration in high-probability bins;
- survives threshold/backtest robustness checks;
- can be reproduced from persisted search artifacts;
- leaves current champion artifacts available for rollback.
