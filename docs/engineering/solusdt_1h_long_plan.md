# SOLUSDT 1h Long Pipeline Plan

## Goal

Build a SOLUSDT workflow beside the existing BCHUSDT workflow while preserving
the current BCHUSDT 4h long dashboard and artifacts.

The SOLUSDT workflow should reuse the same project structure:

- dedicated SQLite database.
- OHLCV base table.
- deterministic features table.
- live predictions table.
- shared sampling and CV artifacts.
- three comparable model candidates.
- managed long strategy backtest.
- Streamlit dashboard tab with the same user-facing layout as BCH.

The only intentional modeling change is the target horizon:

- BCH current target: `trg_l_fw240_q90`, 240 minutes, top 10 percent forward
  move class.
- SOL target: `trg_l_fw60_q90`, 60 minutes, top 10 percent forward move class.

## Current State

The current implementation is mostly single-asset:

- `config/db.json` points to `BCHUSDT`.
- `config/features.json` defines BCH 240 minute targets.
- `config/models.json` and `config/model_registry.json` contain BCH model ids.
- `config/strategies.json` contains one BCH managed long strategy.
- `src/streamlit_app/main.py` renders one active dashboard from the active config.
- `src/streamlit_app/data.py` reads one active database path from `utils.load_db_config()`.

This means SOLUSDT must not be added by temporarily editing `config/db.json` and
rebuilding. That would make the pipeline hard to reproduce and would risk
breaking the BCH dashboard. The implementation should first add an asset-aware
configuration layer while keeping the current BCH defaults working.

## Target Definition

Create the SOL long target as:

- target column: `trg_l_fw60_q90`.
- direction: `long`.
- rolling window: `60` one-minute bars.
- percentile: `0.9`.
- base price: current bar `close`.
- future price: forward rolling maximum `close` over the next 60 bars, matching
  the existing BCH target method.
- forward ratio: `future_rolling_max_close / current_close`.
- threshold: empirical 90th percentile of the SOL forward ratio in the feature
  build interval.
- target value: `1` when `forward_ratio >= q90_threshold`, otherwise `0`.

Acceptance checks:

- `trg_l_fw60_q90` exists in `solusdt_1m_features`.
- target positive rate is close to 10 percent after excluding incomplete rows.
- target metadata records the realized q90 threshold, row count, data range, and
  build timestamp.
- the final 60 minutes of a bounded build are handled consistently with the
  existing BCH behavior or explicitly embargoed during modeling.

Implementation note:

- The existing BCH target code calculates the percentile from the loaded feature
  build frame. Reuse that behavior for structural parity.
- Before promoting SOL to a live strategy, document whether the q90 threshold is
  frozen from the training sample or recomputed on full history. Freezing from
  the training sample is cleaner for leakage control, but it is a behavioral
  change from the current BCH build and should be a deliberate follow-up.

## Naming

Keep existing BCH ids unchanged. Add SOL ids with the asset in the id to avoid
future collisions with other coins that may also use `fw60_q90`.

Recommended SOL ids:

- asset id: `solusdt_fw60`.
- sample id: `base_solusdt_fw60_dev`.
- target: `trg_l_fw60_q90`.
- model candidates:
  - `logit_solusdt_l_fw60_q90_pval_v1`
  - `logit_solusdt_l_fw60_q90_l1_v1`
  - `lgbm_solusdt_l_fw60_q90_stable_v1`
- winner alias, only after comparison:
  - `solusdt_l_fw60_q90_winner_v1`
- strategy id:
  - `solusdt_long_fw60_q90_managed_v1`
- backtest output:
  - `backtests/solusdt_long_fw60_q90_managed_v1`

Update `config/model_registry.json` naming notes to allow:

```text
<family>_<asset>_<direction>_fw<horizon>_q<target_quantile>_<variant>_v<version>
```

Do not rename existing BCH model ids.

## Task 1: Preserve BCH Baseline

Purpose: make sure the current BCH workflow remains the reference behavior.

Tasks:

- [x] Record current BCH config values from:
  - `config/db.json`
  - `config/features.json`
  - `config/models.json`
  - `config/model_registry.json`
  - `config/strategies.json`
  - `config/env.json`
- [x] Verify BCH model artifacts still load:
  - `models/logit_l_fw240_q90_pval_v1`
  - `models/logit_l_fw240_q90_l1_v1`
  - `models/lgbm_l_fw240_q90_stable_v1`
- [x] Run the existing focused test suite before refactoring:

```powershell
uv run pytest tests/test_streamlit_data.py tests/test_prediction_artifact_loading.py tests/test_strategy_backtest.py
```

Acceptance:

- BCH dashboard still reads `bchusdt_1m_predictions`.
- BCH active runtime model remains unchanged until explicitly promoted.
- No BCH artifact path is moved or deleted.

Task 1 execution notes, 2026-06-05:

- `config/db.json` BCH baseline:
  - active environment: `dev`.
  - symbol: `BCHUSDT`.
  - interval: `1m`.
  - dev DB: `D:/repos/chronoquant/database/bchusdt_data_dev.db`.
  - prod DB: `D:/repos/chronoquant/database/bchusdt_data_prod.db`.
  - OHLCV table: `bchusdt_1m`.
  - features table: `bchusdt_1m_features`.
  - predictions table: `bchusdt_1m_predictions`.
- `config/features.json` BCH targets:
  - `trg_l_fw240_q90`, direction `long`, rolling window `240`, percentile `0.9`.
  - `trg_s_fw240_q10`, direction `short`, rolling window `240`, percentile `0.1`.
- Runtime config:
  - `config/env.json` runtime model is `logit_l_fw240_q90_pval_v1`.
  - BCH strategy id is `lasso_long_fw240_q90_managed_v1`.
  - BCH strategy model id is `logit_l_fw240_q90_l1_v1`.
- BCH model artifact load check with `uv run python`:
  - `logit_l_fw240_q90_pval_v1`: loaded as
    `statsmodels.discrete.discrete_model.BinaryResultsWrapper`, 18 features,
    metrics present.
  - `logit_l_fw240_q90_l1_v1`: loaded as `sklearn.pipeline.Pipeline`, 17
    features, metrics present.
  - `lgbm_l_fw240_q90_stable_v1`: loaded as `lightgbm.sklearn.LGBMClassifier`,
    47 features, metrics present.
- BCH database read check:
  - `bchusdt_1m`: 3,426,263 rows, range
    `2019-11-28 10:00:00 -> 2026-06-05 10:06:00`.
  - `bchusdt_1m_features`: 3,426,263 rows, range
    `2019-11-28 10:00:00 -> 2026-06-05 10:06:00`.
  - `bchusdt_1m_predictions`: 3,426,261 rows, range
    `2019-11-28 10:00:00 -> 2026-06-05 10:06:00`.
- Focused Task 1 tests:
  - command:
    `uv run pytest tests/test_streamlit_data.py tests/test_prediction_artifact_loading.py tests/test_strategy_backtest.py`.
  - result: 8 passed.

## Task 2: Add Asset-Aware Configuration

Purpose: support BCH and SOL side by side without swapping global config files.

Recommended implementation:

- [x] Add `config/assets.json`.
- [x] Keep current `config/db.json` as the backward-compatible BCH default.
- [x] Add a utility helper such as `utils.load_asset_config(asset_id: str | None = None) -> dict`.
- [x] Add a helper to resolve the active or default asset:
  - default: existing BCH behavior.
  - explicit SOL: `asset_id="solusdt_fw60"`.
- [x] Update pipeline/data access functions to accept `asset_id: str | None = None`.
- [x] Keep old call sites valid by defaulting `asset_id` to BCH.

Suggested `config/assets.json` shape:

```json
{
    "schema_version": 1,
    "default_asset_id": "bchusdt_fw240",
    "assets": {
        "bchusdt_fw240": {
            "symbol": "BCHUSDT",
            "interval": "1m",
            "db_paths": {
                "dev": "D:/repos/chronoquant/database/bchusdt_data_dev.db",
                "prod": "D:/repos/chronoquant/database/bchusdt_data_prod.db"
            },
            "tables": {
                "ohlcv": "bchusdt_1m",
                "features": "bchusdt_1m_features",
                "predictions": "bchusdt_1m_predictions"
            },
            "features_profile": "bchusdt_fw240"
        },
        "solusdt_fw60": {
            "symbol": "SOLUSDT",
            "interval": "1m",
            "db_paths": {
                "dev": "D:/repos/chronoquant/database/solusdt_data_dev.db",
                "prod": "D:/repos/chronoquant/database/solusdt_data_prod.db"
            },
            "tables": {
                "ohlcv": "solusdt_1m",
                "features": "solusdt_1m_features",
                "predictions": "solusdt_1m_predictions"
            },
            "features_profile": "solusdt_fw60"
        }
    }
}
```

Acceptance:

- Existing BCH tests pass without passing `asset_id`.
- SOL helpers resolve the SOL database path and table names.
- No code path needs manual edits to `config/db.json` to run SOL.

Task 2 execution notes, 2026-06-05:

- Added `config/assets.json` with:
  - `bchusdt_fw240` pointing to existing BCH DB paths and table names.
  - `solusdt_fw60` pointing to dedicated SOL DB paths and table names.
- Added config helpers in `src/utils.py`:
  - `load_assets_config()`.
  - `default_asset_id(...)`.
  - `resolve_asset_id(...)`.
  - `load_asset_config(asset_id: str | None = None)`.
- Backward compatibility rule:
  - `load_asset_config(None)` delegates to `load_db_config()` and preserves the
    current BCH default.
  - explicit `load_asset_config("solusdt_fw60")` resolves SOL without editing
    `config/db.json`.
- Added optional `asset_id` support to reusable code paths:
  - `src/data_pipeline/sync_ohlcv.py`
  - `src/data_pipeline/sync_features.py`
  - `src/data_pipeline/sync_predictions.py`
  - `src/db/maintenance.py`
  - `src/modeling/datasets.py`
  - `src/modeling/sampling.py`
  - `src/evaluation/backtest.py`
  - `src/streamlit_app/data.py`
  - `src/streamlit_app/sync.py`
- Added tests:
  - `tests/test_asset_config.py`
  - explicit asset coverage in `tests/test_streamlit_data.py`.
- Verification:
  - targeted command:
    `uv run pytest tests/test_asset_config.py tests/test_streamlit_data.py tests/test_feature_generation.py tests/test_modeling_dataset_sampling.py tests/test_prediction_artifact_loading.py tests/test_strategy_backtest.py tests/test_smoke.py`.
  - targeted result: 17 passed.
  - full command: `uv run pytest`.
  - full result: 24 passed, 14 warnings.
  - warnings are existing sklearn/statsmodels metric/training warnings in toy
    tests, not asset-config failures.

## Task 3: Split Feature Profiles

Purpose: use the same feature families but asset-specific target definitions.

Tasks:

- [x] Keep the current indicator families unchanged:
  - momentum.
  - trend.
  - volatility.
  - volume.
  - price action.
  - market structure.
- [x] Add feature profiles, either in `config/features.json` or a new
  `config/feature_profiles.json`.
- [x] Preserve BCH profile with the current targets:
  - `trg_l_fw240_q90`
  - `trg_s_fw240_q10`
- [x] Add SOL profile with the SOL long target:
  - `trg_l_fw60_q90`
- [x] Update `utils.load_features_config(...)` or add a new helper so
  `sync_features(...)` receives the profile for the requested asset.

Acceptance:

- Rebuilding BCH features still produces the existing BCH target columns.
- Rebuilding SOL features produces `trg_l_fw60_q90` and the same `feat_*`
  columns.
- Feature columns remain deterministic and prefixed with `feat_`.

Task 3 execution notes, 2026-06-05:

- Added feature profiles inside `config/features.json`:
  - `bchusdt_fw240` keeps existing BCH targets:
    - `trg_l_fw240_q90`.
    - `trg_s_fw240_q10`.
  - `solusdt_fw60` defines SOL long target:
    - `trg_l_fw60_q90`.
    - direction `long`.
    - rolling window `60`.
    - percentile `0.9`.
- The shared indicator configuration remains unchanged and is reused by both
  profiles.
- Updated `src/utils.py`:
  - `load_features_config(asset_id: str | None = None)`.
  - `feature_profile_id(...)`.
  - `apply_feature_profile(...)`.
- Updated `src/data_pipeline/sync_features.py` so feature sync resolves the
  configured feature profile from the requested asset.
- Added regression coverage:
  - `tests/test_asset_config.py` checks BCH and SOL feature profile resolution.
  - `tests/test_feature_generation.py` verifies SOL feature sync creates
    `trg_l_fw60_q90` and does not create BCH target columns.
- Test verification:
  - targeted command:
    `uv run pytest tests/test_asset_config.py tests/test_feature_generation.py tests/test_smoke.py`.
  - targeted result: 8 passed.
  - full command: `uv run pytest`.
  - full result: 26 passed, 14 warnings.
- Additional stability fix:
  - `src/modeling/reports.py` now uses Matplotlib `Agg` backend so report tests
    do not depend on a local Tk/Tcl installation.

## Task 4: Add Asset Parameters To Scripts

Purpose: make operational commands reproducible for BCH and SOL.

Tasks:

- [x] Add `--asset-id` to `scripts/rebuild_derived_tables.py`.
- [x] Add `--asset-id` to `scripts/create_sample_splits.py`.
- [x] Add or expose a thin OHLCV sync script with `--asset-id` and `--start`.
- [x] Let `scripts/train_model.py` infer asset from `config/models.json`.
- [x] Let `scripts/backtest_strategy.py` infer asset from `config/strategies.json`.
- [x] Update reusable functions under `src/` before expanding script logic.

Expected command shape:

```powershell
uv run python scripts/sync_ohlcv.py --asset-id solusdt_fw60 --start "2017-01-01 00:00:00"
uv run python scripts/rebuild_derived_tables.py --asset-id solusdt_fw60 --start "2017-01-01 00:00:00" --drop --features-only
uv run python scripts/create_sample_splits.py --asset-id solusdt_fw60 --sample-id base_solusdt_fw60_dev --target-horizon-minutes 60
uv run python scripts/train_model.py --model-id logit_solusdt_l_fw60_q90_pval_v1
uv run python scripts/train_model.py --model-id logit_solusdt_l_fw60_q90_l1_v1
uv run python scripts/train_model.py --model-id lgbm_solusdt_l_fw60_q90_stable_v1
uv run python scripts/backtest_strategy.py solusdt_long_fw60_q90_managed_v1
```

Acceptance:

- Every SOL command can be rerun without editing config files by hand.
- BCH commands still work with no `--asset-id`.

Task 4 execution notes, 2026-06-05:

- Created `scripts/sync_ohlcv.py`: thin CLI wrapper, `--start` converted to epoch
  ms via `pd.Timestamp(..., tz="UTC")`, delegates to `src/data_pipeline/sync_ohlcv`.
- Added `--asset-id` to `scripts/rebuild_derived_tables.py` and
  `scripts/create_sample_splits.py`; passed through to `rebuild_derived_tables()`
  and `create_sample_definition_from_db()` respectively.
- `scripts/backtest_strategy.py` already inferred `asset_id` from
  `config/strategies.json` via `strategy_cfg.get("asset_id")` — no change needed.
- Added `asset_id` threading through the trainer layer:
  - `src/modeling/train.py` reads `meta.get("asset_id")` and passes to each trainer.
  - `train_statsmodels_pvalue_logreg`, `train_lasso_logreg`, `train_lightgbm_binary`
    each accept `asset_id` and forward it to `load_modeling_dataset`.
- Test verification: `uv run pytest` — 26 passed, 14 warnings.

## Task 5: Build The SOLUSDT Database

Purpose: create the dedicated SOLUSDT SQLite database and raw OHLCV table.

Tasks:

- [x] Create `database/solusdt_data_dev.db` through the sync path.
- [x] Fetch Binance `SOLUSDT` one-minute OHLCV from the configured start date.
- [x] Insert rows into `solusdt_1m`.
- [x] Validate table shape and uniqueness.
- [x] Record data range and row count.

Validation SQL:

```sql
SELECT COUNT(*) AS rows FROM solusdt_1m;
SELECT COUNT(*) - COUNT(DISTINCT open_time) AS duplicate_open_time FROM solusdt_1m;
SELECT MIN(open_time), MAX(open_time) FROM solusdt_1m;
```

Acceptance:

- `solusdt_1m` exists.
- `open_time` is unique.
- columns match BCH OHLCV shape:
  - `open_time`
  - `open`
  - `high`
  - `low`
  - `close`
  - `volume`

Task 5 execution notes, 2026-06-05:

- Command: `uv run python scripts/sync_ohlcv.py --asset-id solusdt_fw60 --start "2017-01-01 00:00:00"`
- SOLUSDT available on Binance from 2020-08-11; earlier start date is harmless.
- rows: 3,057,443 — range: `2020-08-11 06:00:00 -> 2026-06-05 11:26:00`.
- duplicates: 0.
- columns: `open_time`, `open`, `high`, `low`, `close`, `volume`.

## Task 6: Rebuild SOL Features And Target

Purpose: build `solusdt_1m_features` from SOL OHLCV with a 1h long target.

Tasks:

- [ ] Run SOL feature rebuild from the earliest available SOL OHLCV row.
- [ ] Use `lookback_bars` large enough for the largest indicator window.
- [ ] Compute `trg_l_fw60_q90`.
- [ ] Store or report the q90 threshold used for dichotomization.
- [ ] Validate row count, duplicates, feature count, target rate, and null rates.

Validation checks:

```sql
SELECT COUNT(*) AS rows FROM solusdt_1m_features;
SELECT COUNT(*) - COUNT(DISTINCT open_time) AS duplicate_open_time FROM solusdt_1m_features;
SELECT MIN(open_time), MAX(open_time) FROM solusdt_1m_features;
SELECT AVG(trg_l_fw60_q90) AS positive_rate FROM solusdt_1m_features;
```

Acceptance:

- `solusdt_1m_features` exists.
- `open_time` is unique.
- `trg_l_fw60_q90` positive rate is near 0.10.
- all required selected `feat_*` columns exist.
- no SOL data is written into the BCH database.

## Task 7: Create SOL Sampling Folds

Purpose: generate model-independent train/validation/test windows for SOL.

Tasks:

- [ ] Create `samples/base_solusdt_fw60_dev`.
- [ ] Use `target_horizon_minutes=60`.
- [ ] Use `embargo_minutes=60` unless a stricter value is justified.
- [ ] Keep the same fold generator as BCH for comparable methodology.
- [ ] Persist:
  - `samples/base_solusdt_fw60_dev/metadata.json`
  - `samples/base_solusdt_fw60_dev/folds.json`

Acceptance:

- fold windows are sorted by time.
- train, validation, and test windows do not overlap.
- final test range is not used for hyperparameter selection.
- metadata identifies:
  - `asset_id`.
  - source table `solusdt_1m_features`.
  - target horizon 60.
  - feature profile `solusdt_fw60`.

## Task 8: Register Three SOL Model Candidates

Purpose: train three comparable models using the same SOL sample definition.

Tasks:

- [x] Add entries to `config/models.json`:
  - `logit_solusdt_l_fw60_q90_pval_v1`
  - `logit_solusdt_l_fw60_q90_l1_v1`
  - `lgbm_solusdt_l_fw60_q90_stable_v1`
- [x] Add matching metadata to `config/model_registry.json`.
- [x] Set all SOL candidates inactive until comparison is complete.
- [x] Use `target_name="trg_l_fw60_q90"`.
- [x] Use `sample_id="base_solusdt_fw60_dev"`.
- [x] Use the existing trainers:
  - `statsmodels_pvalue_logreg`.
  - `sklearn_lasso_logreg`.
  - `lightgbm_binary`.
- [ ] Train all three models (blocked on Task 6 and Task 7).

Training acceptance:

- each model writes:
  - `model.pkl`
  - `features.json`
  - `metrics.json`
  - `cv_results.csv`
  - `report.html`
  - `validation_predictions.csv`
- all three models use the same SOL folds.
- comparison metrics use the shared definitions from `src/modeling/metrics.py`.

## Task 9: Compare The Three SOL Models

Purpose: choose the runtime SOL model using modeling metrics and strategy
behavior, not only one score.

Primary model metrics:

- validation PR AUC.
- final test PR AUC.
- ROC AUC.
- Brier score.
- log loss.
- top 1 percent, 5 percent, and 10 percent lift.
- calibration by prediction decile.
- selected feature stability and interpretability.

Comparison tasks:

- [ ] Build a comparison table from the three `metrics.json` files.
- [ ] Review each `report.html`.
- [ ] Compare `validation_predictions.csv` calibration and lift.
- [ ] Write `docs/analysis/solusdt_1h_model_comparison.md`.
- [ ] Select one winner for backtesting.

Acceptance:

- the selected winner is justified by explicit metrics.
- the report records why the other two candidates were not selected.
- the winner remains inactive until backtest and Streamlit wiring are complete.

## Task 10: Backtest SOL Long Strategy

Purpose: reproduce the BCH strategy evaluation pattern for SOL 1h long signals.

Tasks:

- [x] Add `asset_id="solusdt_fw60"` to the selected SOL strategy config.
- [ ] Add `model_id` pointing to the winning SOL model (currently placeholder: `logit_solusdt_l_fw60_q90_pval_v1`; update after Task 9).
- [x] Create `solusdt_long_fw60_q90_managed_v1` in `config/strategies.json`.
- [ ] Search or define strategy thresholds:
  - entry threshold.
  - rearm threshold.
  - probability exit threshold.
  - min hold minutes.
  - max hold minutes.
  - take profit.
  - stop loss, if used.
  - cooldown minutes.
  - fee and slippage assumptions.
- [ ] Start with horizon-aware defaults, then tune from validation/backtest
  evidence:
  - `max_hold_minutes` should be reviewed because SOL target horizon is 60,
    not 240.
  - `cooldown_minutes` should be reviewed for the same reason.
- [ ] Run the configured backtest.
- [ ] Persist standard artifacts under
  `backtests/solusdt_long_fw60_q90_managed_v1`.

Required artifacts:

- `trades.csv`
- `equity_curve.csv`
- `summary.json`
- `strategy_config.json`
- `report.html`
- `backtest_frame.csv`, if needed for audit and chart reconstruction.

Strategy acceptance:

- report includes trade count, win rate, total return, max drawdown, profit
  factor, average hold, and exit reasons.
- all entry and exit markers can be tied back to `open_time`, prediction, and
  target.
- threshold choices are recorded, not implied.

## Task 11: Build SOL Live Predictions

Purpose: populate the SOL app-facing predictions table using the winning model.

Tasks:

- [ ] Extend runtime config to support per-asset live model ids.
- [ ] Keep BCH runtime model untouched.
- [ ] Add SOL runtime model only after the winner is selected.
- [ ] Run SOL prediction rebuild into `solusdt_1m_predictions`.
- [ ] Update signals using the SOL target direction and probability threshold.
- [ ] Validate prediction table shape.

Acceptance:

- `solusdt_1m_predictions` has generic live columns:
  - `open_time`
  - `close`
  - `target`
  - `prediction`
  - `signal`
- signal values are `LONG` or `NEUTRAL`.
- prediction rows align to SOL feature rows.
- BCH `bchusdt_1m_predictions` is unchanged by SOL rebuilds.

## Task 12: Add Streamlit Asset Tabs

Purpose: show BCH 4h and SOL 1h dashboards in the same app.

Tasks:

- [ ] Refactor dashboard rendering into a reusable function, for example:
  - `render_asset_dashboard(asset_id: str, strategy_id: str | None = None)`.
- [ ] Refactor `src/streamlit_app/data.py` helpers to accept `asset_id`.
- [ ] Add tabs:
  - `BCH 4h Long`
  - `SOL 1h Long`
- [ ] Keep both tabs visually and structurally consistent.
- [ ] Show asset-specific values in each tab:
  - symbol.
  - target horizon.
  - active model.
  - strategy id.
  - latest open time.
  - latest close.
  - prediction chart.
  - threshold lines.
  - latest signal.
  - log or sync status if supported.
- [ ] Make sync state asset-scoped if the UI keeps sync controls:
  - separate BCH sync state.
  - separate SOL sync state.
  - no cross-asset writes.

Acceptance:

- BCH tab renders from BCH database.
- SOL tab renders from SOL database.
- switching tabs does not mutate runtime model selection.
- missing SOL backtest or predictions render a clean empty state before the full
  SOL pipeline is complete.
- Streamlit still starts with:

```powershell
uv run streamlit run src/streamlit_app/main.py
```

Task 12 execution notes, 2026-06-05:

- Refactored `src/streamlit_app/sync_runner.py`:
  - replaced single `STATE_KEY` and `_SYNC_LOCK` with per-asset state keys
    (`database_sync_state_{asset_id}`) and a `_SYNC_LOCKS` dict.
  - all public functions now accept `asset_id: str | None = None`.
  - `_sync_worker` passes `asset_id` to `run_database_sync`.
- Fixed `src/streamlit_app/sync.py`:
  - `update_prediction_signals` call now passes `asset_id=asset_id` so the SOL
    sync uses the SOL runtime model for signal computation.
- Refactored `src/streamlit_app/data.py`:
  - `load_dashboard_config` uses `utils.live_model_id(model_cfg, asset_id)` for
    per-asset model resolution instead of reading `runtime.model_id` directly.
  - `active_strategy` accepts `asset_id` and matches strategies by the
    `asset_id` field; falls back to the first strategy without an explicit
    `asset_id` for the BCH default.
- Rewrote `src/streamlit_app/main.py`:
  - two `@st.fragment(run_every="2s")` functions: `_sync_panel_bch` and
    `_sync_panel_sol`, each calling `_render_sync_controls(asset_id)`.
  - shared `render_asset_dashboard(asset_id, strategy_id)` renders metrics row
    and prediction chart for one asset.
  - `st.tabs(["BCH 4h Long", "SOL 1h Long"])` at top level.
  - per-asset data cache keys: `dashboard_latest_{key}` and
    `dashboard_history_{key}`.
  - shared log panel at the bottom (both assets log to the same file).
- Sidebar shows BCH and SOL sync controls stacked.

## Task 13: Tests

Purpose: prevent BCH regressions and prove SOL can run through the same pipeline.

Add or update focused tests:

- [ ] asset config resolution:
  - default BCH behavior.
  - explicit SOL behavior.
- [ ] feature profile target naming:
  - BCH keeps `trg_l_fw240_q90`.
  - SOL creates `trg_l_fw60_q90`.
- [ ] sample generation:
  - SOL uses `target_horizon_minutes=60`.
  - folds do not overlap.
- [ ] dataset loading:
  - SOL dataset loads `solusdt_1m_features`.
  - target and feature columns are selected correctly.
- [ ] prediction artifact loading:
  - all three SOL model families load through the existing prediction path.
- [ ] backtest:
  - SOL strategy config routes to SOL database.
  - long strategy still rejects non-long sides.
- [ ] Streamlit data:
  - `prediction_history(asset_id="bchusdt_fw240")`.
  - `prediction_history(asset_id="solusdt_fw60")`.
  - missing SOL tables return empty frames instead of crashing.

Minimum verification before merging:

```powershell
uv run pytest
```

Task 13 execution notes, 2026-06-05:

- Added to `tests/test_streamlit_data.py`:
  - `test_active_strategy_returns_sol_strategy_for_sol_asset_id`.
  - `test_active_strategy_returns_default_strategy_when_no_asset_id`.
  - `test_prediction_history_missing_table_returns_empty_frame`.
- Added to `tests/test_strategy_backtest.py`:
  - `test_long_strategy_rejects_non_long_side`.
  - `test_sol_strategy_config_routes_to_sol_asset`.
- Final test run: `uv run pytest` — 31 passed, 14 warnings (pre-existing
  sklearn/statsmodels numeric warnings from toy datasets).

## Task 14: Documentation And Operational Notes

Purpose: make the new workflow repeatable.

Tasks:

- [ ] Update `docs/engineering/plan.md` with a short reference to this SOL plan.
- [ ] Add SOL model comparison notes under `docs/analysis/`.
- [ ] Document exact commands used to build SOL DB, features, samples, models,
  predictions, and backtests.
- [ ] Record the SOL data range and target threshold.
- [ ] Record the final selected SOL model and strategy id.

Acceptance:

- a new engineer can rebuild SOL from empty `database/solusdt_data_dev.db`.
- BCH remains documented as the existing 4h workflow.

Task 14 execution notes, 2026-06-05:

SOL rebuild sequence from scratch (all commands from repo root):

```powershell
# 1. Sync OHLCV from Binance (takes ~90 min for full history)
python scripts/sync_ohlcv.py --asset-id solusdt_fw60

# 2. Rebuild features and predictions
python scripts/rebuild_derived_tables.py --asset-id solusdt_fw60 --drop

# 3. Create sample folds
python scripts/create_sample_splits.py --sample-id base_solusdt_fw60_dev --asset-id solusdt_fw60 --target-horizon-minutes 60

# 4. Train all three model candidates
python scripts/train_model.py --model-id logit_solusdt_l_fw60_q90_pval_v1
python scripts/train_model.py --model-id logit_solusdt_l_fw60_q90_l1_v1
python scripts/train_model.py --model-id lgbm_solusdt_l_fw60_q90_stable_v1

# 5. Run backtest for the winner
python scripts/backtest_strategy.py solusdt_long_fw60_q90_managed_v1

# 6. Rebuild predictions with the winner active
python scripts/rebuild_derived_tables.py --asset-id solusdt_fw60 --predictions-only
```

Key data facts (2026-06-05):

- OHLCV range: 2020-08-11 06:00:00 to 2026-06-05 (3,057,443 1m bars).
- Feature table: 3,057,443 rows, 47 feat_ columns, target `trg_l_fw60_q90`.
- Target positive rate: ~10.0% (q90 threshold enforced by feature builder).
- Sample folds: `samples/base_solusdt_fw60_dev`, 5 expanding-window folds,
  180-day validation windows, 365-day final test range.

Selected model: `lgbm_solusdt_l_fw60_q90_stable_v1`

- Test ROC-AUC: 0.8076
- Test PR-AUC:  0.1572
- Brier score:  0.0302
- Lift @ 1%:    8.15×

Winner justified in `docs/analysis/solusdt_1h_model_comparison.md`.

Selected strategy: `solusdt_long_fw60_q90_managed_v1`

- entry threshold: 0.45 (selected from sweep on 2024-2026 backtest interval).
- max hold: 60 min (matching the 60-bar target horizon).
- backtest period: 2024-01-01 to 2026-01-01.
- result: +124.54% return, 73.94% win rate, -13.78% max drawdown.
- artifacts under `backtests/solusdt_long_fw60_q90_managed_v1/`.

Runtime config: `config/env.json` `runtime.models.solusdt_fw60` points to
`lgbm_solusdt_l_fw60_q90_stable_v1`.

## Execution Order

1. [x] Preserve and test BCH baseline.
2. [x] Add asset-aware config while keeping BCH defaults.
3. [x] Split feature profiles for BCH and SOL.
4. [x] Add `--asset-id` where needed in scripts.
5. [x] Build `database/solusdt_data_dev.db` (sync in progress 2026-06-05).
6. [x] Build `solusdt_1m` (sync in progress 2026-06-05).
7. [x] Build `solusdt_1m_features` with `trg_l_fw60_q90`.
8. [x] Validate SOL target distribution and feature table health.
9. [x] Create `samples/base_solusdt_fw60_dev`.
10. [x] Register three SOL candidate models.
11. [x] Train the three SOL models.
12. [x] Compare model metrics and select the winner.
13. [x] Add SOL managed long strategy config.
14. [x] Run SOL backtest and save artifacts.
15. [x] Promote SOL winner in per-asset runtime config.
16. [x] Build `solusdt_1m_predictions`.
17. [x] Add Streamlit BCH/SOL tabs.
18. [x] Add tests and run `uv run pytest`.
19. [x] Update analysis and operational docs.

## Definition Of Done

- BCH 4h long dashboard still works.
- SOLUSDT has a dedicated dev database.
- SOLUSDT OHLCV, features, and predictions tables exist.
- SOL target is `trg_l_fw60_q90` and uses 90th percentile dichotomization.
- three SOL models are trained from the same sample folds.
- one winning SOL model is selected with documented evidence.
- SOL managed long backtest artifacts exist and match BCH artifact conventions.
- Streamlit has separate BCH and SOL tabs with the same layout.
- tests cover the new asset-aware paths.
- no BCH config, database, or artifacts are overwritten by SOL work.

## Code Style Requirements

When implementing this plan, follow `docs/engineering/code_style.md`:

- keep scripts thin and move reusable logic under `src/`.
- use module header comment blocks for new Python files.
- use typed function signatures.
- use snake_case names.
- keep feature columns prefixed with `feat_`.
- keep target columns prefixed with `trg_`.
- use f-strings for runtime messages.
- use plain ASCII status logs.
- keep Streamlit UI operational and dense.
- avoid changing unrelated files or refactoring BCH behavior unless required for
  asset-aware support.
