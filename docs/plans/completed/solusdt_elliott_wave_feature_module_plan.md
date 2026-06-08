# SOLUSDT Elliott Wave Event Study And Feature Module Plan

## Context

This backlog plan defines the first Elliott Wave research module for the
`solusdt_fw60` asset profile.

- Asset: `solusdt_fw60`
- Target horizon: 60 minutes
- Long target: `trg_l_fw60_q90`
- Short target: `trg_s_fw60_q10`
- Source DB: `database/solusdt_data_dev.db`
- Source OHLCV table: `solusdt_1m`
- Current quantitative feature table: `solusdt_1m_features`
- Proposed Elliott event table: `solusdt_elliott_events`
- Proposed Elliott feature table: `solusdt_1m_elliott_features`

The first phase is not a LightGBM task. It is an event-study task:

```text
Can a package or deterministic wave detector identify a wave-3, wave-5, or C-wave
setup early enough that the next 60 minutes show meaningful directional movement?
```

The module should be derived from raw OHLCV data and stored separately from the
current quantitative feature table. This keeps the Elliott parser modular,
auditable, and optional for later model experiments.

## Goals

- Build a historical Elliott event study for SOLUSDT.
- Test whether right-edge Elliott setups predict the existing 60-minute targets.
- Focus first on:
  - bullish wave-3 setup after a detected 1-2 / nested 1-2-1-2 structure;
  - bearish wave-3 setup after a detected bearish 1-2 / nested 1-2-1-2;
  - bullish and bearish wave-5 setup after a detected wave-4 end;
  - bullish and bearish C-wave setup after a detected B-wave end.
- Count how often these setups are detected over a fixed window, for example
  one year.
- Measure whether an event detected at time `t` is followed by meaningful
  directional movement within 60 minutes.
- Keep Elliott event and feature outputs in separate tables.
- Only after the event study passes basic signal gates, test Elliott-only
  LightGBM and ensemble variants.

## Non-Goals

- Do not promote Elliott features directly into the live model before validation.
- Do not use manually labeled expert wave counts as ground truth in the first
  phase.
- Do not use a non-deterministic LLM as the wave counter.
- Do not write candidate Elliott predictions into the live predictions table.
- Do not tune thresholds on the final holdout.
- Do not report retrospective wave labels as predictive signals unless the same
  state could have been detected at the right edge.
- Do not train LightGBM before the raw event study shows measurable signal.

## Package Survey

The Python ecosystem has limited mature Elliott Wave tooling. The practical
approach is to treat Elliott as a deterministic pivot/wave-structure feature
module, then validate whether the features add predictive power.

### `wave-alpha`

Source:

- PyPI: https://pypi.org/project/wave-alpha/

Current notes from PyPI:

- Latest observed release: `0.14.3`, released 2026-05-12.
- Requires Python `>=3.11`, compatible with this repo's `>=3.12`.
- Describes itself as a local-first Elliott Wave analysis engine.
- Provides deterministic rule-validated counts, alternate counts,
  multi-timeframe coherence, derived trade signals, history snapshots, and
  backtest commands.
- Pattern library includes templates such as impulse, zigzag, flat,
  expanded flat, contracting triangle, and ending diagonal.
- It advertises hard lookahead safety in its own backtest harness.
- Optional LLM reranking exists, but should not be used for ChronoQuant first
  pass.

Potential role:

- Best candidate for a third-party Elliott engine proof of concept.
- Use engine-only / LLM-disabled mode.
- Extract structured outputs into deterministic features.

Risks:

- New package, still beta.
- Needs API and output contract review before dependency adoption.
- Must verify local behavior on 1-minute crypto data; package examples are more
  swing-trading oriented.

### `zigzag`

Source:

- PyPI: https://pypi.org/project/zigzag/0.3.2/

Current notes from PyPI:

- Latest observed release: `0.3.2`, released 2022-08-06.
- Requires Python `>=3.8`.
- Provides functions for identifying peaks and valleys of a time series.
- Also provides maximum drawdown functionality.

Potential role:

- Reliable base for pivot extraction.
- Good first dependency if we want a small deterministic module.
- Elliott-like features can be built from pivot sequences without claiming a
  full Elliott count.

Risks:

- It is not an Elliott Wave counter.
- Threshold selection strongly affects output.
- Must ensure point-in-time usage: pivots confirmed only after enough future
  movement has occurred, or explicitly mark confirmation lag.

### `npzigzag`

Source:

- PyPI: https://pypi.org/project/npzigzag/

Potential role:

- Lightweight numpy-style ZigZag candidate.
- Useful for performance comparison against `zigzag` or an in-house pivot
  implementation.

Risks:

- Needs package-quality and maintenance review before use.
- Same lookahead and threshold issues as any ZigZag implementation.

### `ElliottWaveAnalyzer`

Source:

- GitHub: https://github.com/drstevendev/ElliottWaveAnalyzer

Current notes from README:

- Describes itself as a first version of a not-yet-iterative Elliott Wave
  scanner.
- Uses Python 3.9 in its setup notes.
- Examples include monowaves and 1-2-3-4-5 impulsive wave detection.
- The algorithm idea is to try combinations of possible wave patterns and
  validate each against rules.

Potential role:

- Research reference for in-house rule design.
- Could be used to understand monowave and impulse validation logic.

Risks:

- Not a packaged dependency.
- Experimental.
- Not obviously maintained for current Python versions or production use.

### `Elliott_System`

Source:

- GitHub: https://github.com/nowinseason/Elliott_System

Current notes:

- Small repository, no package release.
- README describes Elliott wave detection Python code.

Potential role:

- Research-only reference.

Risks:

- Not suitable as a production dependency without major review.

## Recommended Package Direction

Use a staged approach:

1. Use a package or deterministic engine to map historical pivot/wave structures
   on resampled bars.
2. Start with the timeframes most relevant to a 60-minute target:
   - `1h` bars;
   - `2h` bars;
   - optional `30m` bars if 1h/2h events are too sparse.
3. Use `zigzag` or an in-house pivot detector for the first controlled
   point-in-time setup implementation.
4. Evaluate `wave-alpha` separately as an optional engine because it exposes
   Elliott counts, alternates, and multi-timeframe coherence.
5. Do not depend on experimental GitHub-only repos for the first production
   module.

Rationale:

- Pivot and swing geometry is the measurable base of Elliott analysis.
- Full Elliott counts are subjective and can be unstable at the right edge.
- A right-edge setup detector is more important than a visually correct
  retrospective wave count.
- `wave-alpha` documentation is swing-trading oriented and mentions weekly,
  daily, and 4h coherence. ChronoQuant must explicitly test whether `1h` and
  `2h` wave detection produces enough events for a 60-minute crypto target.

## Core Research Hypothesis

The module should test these hypotheses before any model training:

```text
H1: A detected bullish 1-2 or nested 1-2-1-2 setup predicts that a bullish wave 3
    starts soon enough to create a meaningful 60-minute upside move.

H2: A detected bearish 1-2 or nested 1-2-1-2 setup predicts that a bearish wave 3
    starts soon enough to create a meaningful 60-minute downside move.

H3: A detected bullish wave-4 completion predicts that a bullish wave 5 starts
    soon enough to create a meaningful 60-minute upside move.

H4: A detected bearish wave-4 completion predicts that a bearish wave 5 starts
    soon enough to create a meaningful 60-minute downside move.

H5: A detected B-wave completion predicts that a C-wave starts soon enough to
    create meaningful 60-minute movement in the expected C direction.
```

The main question is not:

```text
Can we draw Elliott waves retrospectively?
```

The main question is:

```text
At time t, can the detector identify a setup before the wave starts, and does
the next 60 minutes validate the expected direction often enough to matter?
```

## Proposed Data Model

Create a separate event table:

```text
solusdt_elliott_events
```

Required event columns:

```text
event_id TEXT PRIMARY KEY
open_time TEXT
asset_id TEXT
timeframe TEXT
engine TEXT
engine_version TEXT
profile_id TEXT
event_type TEXT
direction TEXT
setup_score REAL
expected_wave TEXT
confirmation_lag_bars INTEGER
source_pivots_json TEXT
diagnostics_json TEXT
```

`event_type` examples:

```text
wave3_setup
wave5_setup
c_wave_setup
retrospective_wave3_start
retrospective_wave5_start
retrospective_c_start
```

Then create a separate per-minute feature table:

```text
solusdt_1m_elliott_features
```

Required columns:

```text
open_time TEXT PRIMARY KEY
close REAL
elliott_engine TEXT
elliott_engine_version TEXT
elliott_profile_id TEXT
```

Feature columns should use the `feat_elliott_` prefix:

```text
feat_elliott_...
```

This allows the shared modeling dataset loader to join or select Elliott
features explicitly without mixing them into the current quantitative feature
table by default.

The event table is the first priority. The feature table comes after event
detection is measurable.

## Proposed Source Modules

Suggested implementation layout:

```text
src/analysis/elliott_event_study.py
src/data_pipeline/sync_elliott_features.py
src/modeling/elliott_features.py
scripts/elliott_event_study.py
scripts/sync_elliott_features.py
scripts/audit_elliott_features.py
```

Rules:

- `scripts/sync_elliott_features.py` stays thin.
- Reusable logic belongs under `src/`.
- Config loading must go through `src/utils.py`.
- Store timestamps as UTC strings in `YYYY-MM-DD HH:MM:SS`.
- Generated candidate model outputs stay separate from the live predictions
  table.

## Event Detection Definitions

### Retrospective Wave Map

The retrospective map is allowed to use the full historical window to identify
completed structures. It is used only to answer:

- how many wave-3 / wave-5 / C structures appear in the last year;
- what durations and amplitudes are typical;
- which timeframe produces enough events;
- whether the detector's wave labels are plausible.

Retrospective labels are not predictive features.

### Right-Edge Setup Detection

The right-edge detector is the predictive object. At every event time `t`, it
may only use bars with `open_time <= t`.

The detector should emit a setup when the prior structure is visible and the
next wave is expected but not yet proven.

### Wave-3 Setup

Bullish wave-3 setup:

```text
confirmed bullish wave 1 exists
confirmed wave 2 retracement exists
wave 2 does not invalidate wave 1 origin
wave 2 retracement is inside configured band, e.g. 0.382-0.786
price begins to turn upward from wave 2
optional nested 1-2-1-2 structure appears in the same direction
```

Bearish wave-3 setup mirrors the same logic downward.

Important distinction:

```text
Wrong measurement:
  after full history, label a completed wave 3 and pretend it was known at the start

Correct measurement:
  detect the end of wave 2 / nested 1-2 setup at time t,
  then test whether a wave-3-like directional move appears within the next 60 minutes
```

### Wave-5 Setup

Bullish wave-5 setup:

```text
confirmed bullish 1-2-3 sequence exists
candidate wave 4 retracement exists
wave 4 does not overlap wave 1 in invalidating ways
wave 3 is not the shortest impulse leg
price begins to turn upward from wave 4
```

Bearish wave-5 setup mirrors the same logic downward.

### C-Wave Setup

Short-direction C-wave setup:

```text
corrective A move down is identified
B retracement upward is identified
B appears complete or rejected near a Fibonacci zone
expected C direction is down
```

Long-direction C-wave setup:

```text
corrective A move up is identified
B retracement downward is identified
B appears complete or rejected near a Fibonacci zone
expected C direction is up
```

Naming must be explicit because C-wave direction depends on the preceding
corrective structure. Store `direction` as the expected C move:

```text
direction = long | short
```

For example, if B completes before an expected downward C leg, use
`direction = short`.

## Elliott Feature Families

### 1. Pivot Geometry

Derived from ZigZag or local pivot detection.

Candidate features:

```text
feat_elliott_pivot_dir
feat_elliott_pivot_age_bars
feat_elliott_last_swing_return
feat_elliott_last_swing_duration
feat_elliott_last_swing_slope
feat_elliott_last_swing_atr_norm
feat_elliott_prev_swing_return
feat_elliott_prev_swing_duration
feat_elliott_swing_return_ratio
feat_elliott_swing_duration_ratio
```

Meaning:

- direction of most recent confirmed swing;
- age of current swing;
- amplitude and duration of last swings;
- ratios between adjacent swings.

### 2. Fibonacci Retracement And Extension

Candidate features:

```text
feat_elliott_retrace_last_vs_prev
feat_elliott_retrace_dist_0382
feat_elliott_retrace_dist_0500
feat_elliott_retrace_dist_0618
feat_elliott_extension_dist_1000
feat_elliott_extension_dist_1618
feat_elliott_fib_zone_score
```

Meaning:

- distance from common retracement/extension zones;
- whether current price is near a candidate wave-2 / wave-4 / wave-5 zone.

### 3. Impulse Candidate Structure

Candidate features:

```text
feat_elliott_impulse_score
feat_elliott_impulse_direction
feat_elliott_wave3_setup_score
feat_elliott_wave3_active_flag
feat_elliott_wave3_progress_ratio
feat_elliott_wave5_setup_score
feat_elliott_wave5_active_flag
feat_elliott_wave5_progress_ratio
feat_elliott_wave3_not_shortest_flag
feat_elliott_wave4_overlap_flag
feat_elliott_wave2_retrace
feat_elliott_wave4_retrace
feat_elliott_wave5_extension
feat_elliott_impulse_completion_ratio
```

Meaning:

- rule-based score for a possible 1-2-3-4-5 impulse;
- hard-rule flags;
- normalized progress through candidate impulse.

Primary research focus:

- Detect the likely start of wave 3 after a valid wave 1 and corrective wave 2.
- Detect the likely start of wave 5 after a valid wave 1-2-3-4 structure.
- Encode both bullish and bearish impulse directions.
- Measure first whether these detected states predict `trg_l_fw60_q90` or
  `trg_s_fw60_q10` without LightGBM.

The first Elliott experiment should not try to solve every Elliott pattern.
It should focus on whether wave-3, wave-5, and C-wave setup states are
measurable and predictive.

Candidate wave-3 setup rules:

```text
confirmed wave 1 pivot sequence exists
wave 2 retracement is inside configured Fibonacci band, e.g. 0.382-0.786
price turns back in wave-1 direction
current price has not invalidated wave-1 origin
```

Candidate wave-5 setup rules:

```text
confirmed wave 1-2-3 sequence exists
wave 4 retracement is inside configured band and does not overlap wave 1
price turns back in wave-3 direction
wave 3 is not the shortest impulse leg
```

These rules should produce scores rather than only binary flags. For example:

```text
feat_elliott_wave3_setup_score = 0.0-1.0
feat_elliott_wave5_setup_score = 0.0-1.0
```

The binary active flags are useful for audit and simple backtests, but the
continuous scores are usually better inputs for LightGBM.

### 4. Corrective Candidate Structure

Candidate features:

```text
feat_elliott_correction_score
feat_elliott_correction_type_id
feat_elliott_abc_a_return
feat_elliott_abc_b_retrace
feat_elliott_abc_c_extension
feat_elliott_c_wave_setup_score
feat_elliott_c_wave_active_flag
feat_elliott_b_wave_completion_score
feat_elliott_triangle_score
feat_elliott_flat_score
feat_elliott_zigzag_score
```

Meaning:

- possible ABC / flat / zigzag / triangle structures;
- distance to completion zones;
- likely continuation/reversal state after correction.
- whether a B-wave completion implies a likely C-wave move within the next
  60 minutes.

### 5. Multi-Timeframe Coherence

Compute pivot/wave state on resampled bars, then map the current state back to
1-minute rows.

Candidate timeframes:

```text
5m
15m
30m
1h
```

Candidate features:

```text
feat_elliott_tf5m_dir
feat_elliott_tf15m_dir
feat_elliott_tf30m_dir
feat_elliott_tf1h_dir
feat_elliott_tf_alignment_score
feat_elliott_tf_conflict_score
```

Meaning:

- whether short and higher timeframes agree;
- whether local reversal is against or with the higher-degree swing.

### 6. Right-Edge Stability

Candidate features:

```text
feat_elliott_count_stability_score
feat_elliott_top_count_age_bars
feat_elliott_alternate_count_count
feat_elliott_top_vs_second_score_gap
feat_elliott_recent_relabel_count
```

Meaning:

- whether the current wave interpretation is stable;
- whether many alternates compete with the top count.

This family is especially relevant if using `wave-alpha`, because it exposes
alternate counts and history/stability concepts.

## Leak-Free Computation Rules

Elliott and ZigZag features are high risk for lookahead leakage. Enforce these
rules:

- A pivot can only be used after it is confirmed.
- Store confirmation lag as a feature.
- Do not use future bars to label the current wave state.
- For every row `t`, the feature generator may only read OHLCV rows with
  `open_time <= t`.
- If a library uses future movement to confirm pivots, the feature timestamp
  must be shifted forward to the confirmation time.
- Add a synthetic leakage test where future-only perturbation must not change
  already-emitted historical features.

## Feature Table Build Strategy

### Incremental Sync

Use a bounded lookback because wave features depend on prior pivots:

```text
lookback_bars = 10080  # one week of 1m bars as initial default
```

For large backfills, process in chunks:

```text
6-month chunk -> fetch chunk + lookback -> compute -> write rows inside chunk
```

### Table Columns

Use `ensure_table_columns` like the existing feature pipeline. The module may
start with a small set and add more columns later.

### Idempotency

Use `open_time` as the key and drop existing rows in the target range before
append, matching the current derived-table pattern.

## Event Study Plan

### 1. Timeframe Selection

The event study should evaluate these wave-detection timeframes:

```text
30m
1h
2h
```

Primary candidates:

```text
1h
2h
```

Reasoning:

- The trading target is 60 minutes, so wave events detected on 1h bars are
  naturally aligned with the target.
- 2h bars may produce cleaner structures but fewer events.
- 30m bars may produce more events but more noise.
- `wave-alpha` documentation is swing-trading oriented and mentions higher
  timeframes, so ChronoQuant must test shorter crypto timeframes explicitly.

For each timeframe, report:

- number of detected wave-3 setups over one year;
- number of detected wave-5 setups over one year;
- number of detected C-wave setups over one year;
- median setup duration;
- median pivot confirmation lag;
- median expected move amplitude;
- percentage of events that overlap or cluster.

### 2. Event Outcome Labels

For every setup event at time `t`, compute:

```text
forward_max_up_60m
forward_max_down_60m
forward_close_return_60m
target_long_hit = trg_l_fw60_q90 at t
target_short_hit = trg_s_fw60_q10 at t
```

The event outcome should be aligned to the existing target definition where
possible, but also report raw return thresholds because Elliott events may be
rare.

For long-direction events:

```text
success = target_long_hit == 1
secondary_success = forward_max_up_60m >= configured_threshold
```

For short-direction events:

```text
success = target_short_hit == 1
secondary_success = forward_max_down_60m <= -configured_threshold
```

### 3. Event Report

Create a report artifact:

```text
docs/analysis/solusdt_elliott_event_study_v1.md
```

Minimum contents:

- package/engine used;
- timeframe tested;
- pivot threshold/profile;
- one-year event counts;
- wave-3/wave-5/C setup counts;
- long and short hit rates;
- lift over unconditional target rate;
- event examples;
- failure examples;
- confirmation lag distribution;
- whether events are too sparse for modeling;
- recommendation: reject, refine, or proceed to feature table.

### 4. Acceptance Gate Before Feature Modeling

Proceed to feature-table and LightGBM work only if at least one event type has:

- enough events to measure, initially at least 100 events per year or a clearly
  justified lower count;
- lift above unconditional target rate on chronological slices;
- acceptable confirmation lag relative to the 60-minute target;
- no obvious lookahead dependency.

If these gates fail, do not build an Elliott model yet. Iterate on timeframe,
pivot threshold, and setup definition first.

## Modeling Plan

Only start this section after the event study passes the acceptance gate.

### 1. Elliott-Only Dataset

Add support for selecting `feat_elliott_` columns from
`solusdt_1m_elliott_features`.

Two possible dataset strategies:

1. Join base target columns from `solusdt_1m_features` to Elliott features by
   `open_time`.
2. Store target columns in the Elliott table as copied fields for easier
   standalone audits.

Preferred first implementation:

- Keep target generation in the existing feature table.
- Join by `open_time` in the modeling dataset builder or a new shared join
  helper.
- Do not create a model-specific dataset builder.

### 2. Candidate Model IDs

Example inactive models:

```text
lgbm_solusdt_l_fw60_q90_elliott_v1
lgbm_solusdt_s_fw60_q10_elliott_v1
```

Registry requirements:

- `asset_id`: `solusdt_fw60`
- `target_name`: existing target column
- `family`: `lightgbm`
- `trainer`: `lightgbm_binary`
- `training.sample_id`: `base_solusdt_fw60_dev`
- `active`: `false`

### 3. Search Workflow

Use the same LightGBM search discipline as quantitative models:

```bash
python scripts/search_lgbm.py --model-id lgbm_solusdt_l_fw60_q90_elliott_v1 --stage smoke
python scripts/search_lgbm.py --model-id lgbm_solusdt_l_fw60_q90_elliott_v1 --stage explore --n-trials 60
```

The search code may need a feature-source extension so it can load:

```text
feature_source = elliott
feature_prefix = feat_elliott_
feature_table = solusdt_1m_elliott_features
target_table = solusdt_1m_features
```

Do not duplicate the training loop; only extend shared feature loading.

### 4. Metrics

Compare Elliott-only models with the current quantitative champions:

- mean validation log loss;
- train/valid gap;
- PR AUC;
- ROC AUC;
- Brier score;
- lift at top 1%, 5%, 10%;
- fold-to-fold stability;
- prediction distribution around strategy thresholds;
- final holdout metrics only after research decisions are frozen.

Expected result:

- Elliott-only is unlikely to beat the full quantitative model.
- It can still be useful if it provides uncorrelated errors or improves
  top-percentile lift in specific regimes.

### 5. Feature Importance And Ablation

Review:

- top gain features;
- zero-gain Elliott features;
- per-fold stability of top features;
- whether pivot/Fibonacci features dominate over subjective count flags.

Run ablation groups:

```text
pivot_geometry only
fibonacci only
impulse only
correction only
multi_timeframe only
stability only
all Elliott features
```

### 6. Ensemble Readiness

After Elliott-only model validation, generate OOF prediction artifacts:

```text
models/lgbm_solusdt_l_fw60_q90_elliott_v1/oof_predictions.csv
```

Columns:

```text
open_time,target,prediction,fold,model_id,asset_id,target_name
```

Then test:

```text
quant_lgbm prediction
elliott_lgbm prediction
-> weighted average / logistic meta-model / small LightGBM stacker
```

The ensemble must use OOF predictions only. Never train a meta-model on
in-sample predictions from base models.

## Validation Gates

The Elliott module should proceed from event study to feature/modeling work
only if at least one of these is true:

- wave-3 setup events have stable lift over the unconditional target rate;
- wave-5 setup events have stable lift over the unconditional target rate;
- C-wave setup events have stable lift over the unconditional target rate;
- event counts are high enough for measurement on 1h or 2h bars;
- confirmation lag is short enough that the 60-minute target is still relevant.

After feature/modeling work starts, the module is useful only if at least one
of these is true:

- Elliott-only PR AUC is materially above random baseline;
- Elliott-only top 5% lift is stable across folds;
- Elliott features improve the all-feature quantitative model in ablation;
- Elliott base model improves OOF ensemble performance;
- Elliott model improves calibration or drawdown behavior in strategy tests
  without reducing PR AUC materially.

Reject or park the module if:

- features are unstable under small pivot threshold changes;
- event labels depend on future bars that would not be known at detection time;
- wave-3/wave-5/C detections are too sparse on 1h/2h bars;
- most features are zero-gain;
- validation gains disappear on holdout;
- ensemble improvement comes only from holdout tuning;
- feature generation is too slow for bounded sync.

## Implementation Phases

### Phase 0: Research Spike

- Install no dependencies yet.
- Review `wave-alpha`, `zigzag`, and local pivot implementation options.
- Confirm whether the package can operate on 1h and 2h SOLUSDT bars.
- Prototype retrospective wave mapping on a 3-month slice.
- Prototype right-edge setup detection for:
  - wave-3 after 1-2 / nested 1-2-1-2;
  - wave-5 after wave-4 completion;
  - C-wave after B-wave completion.
- Document output schema and runtime cost.

### Phase 1: Event Detector And Event Table

- Add event-study script and reusable analysis module:
  - `src/analysis/elliott_event_study.py`
  - `scripts/elliott_event_study.py`
- Add `solusdt_elliott_events` table support.
- Emit only point-in-time setup events first:
  - `wave3_setup`;
  - `wave5_setup`;
  - `c_wave_setup`.
- Store event direction, timeframe, setup score, confirmation lag, and source
  pivot diagnostics.

### Phase 2: One-Year Event Study

- Run on a one-year SOLUSDT window.
- Compare `30m`, `1h`, and `2h`; treat `1h` and `2h` as the primary candidates.
- Measure how many events each timeframe detects:
  - bullish wave-3 setup;
  - bearish wave-3 setup;
  - bullish wave-5 setup;
  - bearish wave-5 setup;
  - long-direction C-wave setup;
  - short-direction C-wave setup.
- Measure whether the next 60 minutes hit the existing long/short target or a
  configured raw movement threshold.
- Write `docs/analysis/solusdt_elliott_event_study_v1.md`.

### Phase 3: Event Audit

- Validate:
  - event count;
  - duplicate `open_time`;
  - overlapping events;
  - confirmation lag;
  - setup score distribution;
  - timeframe sensitivity;
  - pivot threshold sensitivity;
  - examples where the setup worked;
  - examples where the setup failed;
  - runtime and memory.

### Phase 4: Feature Table Scaffold

Start only if the event study passes the acceptance gate.

- Add config for Elliott feature table under asset config.
- Add `src/data_pipeline/sync_elliott_features.py`.
- Add `scripts/sync_elliott_features.py`.
- Add `scripts/audit_elliott_features.py`.
- Create deterministic pivot geometry, Fibonacci, wave-3, wave-5, and C-wave
  setup features first.

### Phase 5: Elliott-Only LightGBM

Start only if the event-derived features have measurable raw lift.

- Add inactive model registry entries.
- Extend shared dataset loading for optional feature table joins.
- Run smoke and explore searches.
- Store artifacts under `models/<model_id>/`.
- Write a completed analysis report.

### Phase 6: Extended Elliott Engine

- Evaluate `wave-alpha` engine-only mode.
- Extract candidate count, score, alternate, and coherence features.
- Keep this as a separate profile:

```text
elliott_profile_id = wave_alpha_engine_v1
```

### Phase 7: Ensemble Candidate

- Generate OOF predictions for quantitative and Elliott models.
- Train simple weighted average and logistic stacker.
- Compare against the single quantitative champion.
- Use final holdout only after all model/feature/weight choices are frozen.

## Open Questions

- Which pivot thresholds are stable for 1-minute SOLUSDT?
- Should Elliott features be computed on raw 1m bars, resampled bars, or both?
- Should the first Elliott target be long only, short only, or both?
- Are wave-3 setup states more predictive than wave-5 setup states?
- Do bullish and bearish setups behave symmetrically on SOLUSDT?
- Is wave-5 weaker because it is later in the move and more exhaustion-prone?
- Does Elliott add signal to top-percentile lift, or only duplicate existing
  trend/volatility features?
- Can runtime stay acceptable for dashboard sync, or should Elliott features be
  batch-only until proven?

## References

- `wave-alpha` PyPI: https://pypi.org/project/wave-alpha/
- `zigzag` PyPI: https://pypi.org/project/zigzag/0.3.2/
- `npzigzag` PyPI: https://pypi.org/project/npzigzag/
- `ElliottWaveAnalyzer` GitHub: https://github.com/drstevendev/ElliottWaveAnalyzer
- `Elliott_System` GitHub: https://github.com/nowinseason/Elliott_System
