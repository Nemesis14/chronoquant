# Strategy Evaluation Guide

This guide covers trigger selection, threshold sweeps, backtests, robustness
checks, and strategy config updates. It is intentionally model-independent:
strategy evaluation consumes prediction frames and market facts, not training
internals.

## Purpose

Strategy evaluation answers:

1. Which prediction thresholds create useful trade signals?
2. Are those thresholds stable across time windows and regimes?
3. How does the selected strategy behave on untouched holdout data?
4. Which artifacts and config updates are needed for the dashboard?

## Evaluation Inputs

Use:

- model predictions for the evaluated `model_id`;
- feature/price facts for the same asset and timestamp range;
- strategy parameters from a candidate config or sweep grid;
- sample boundaries from `docs/modeling/sampling.md` when deciding selection
  and holdout windows.

Do not import training internals into strategy evaluation.

## Window Policy

Keep these windows conceptually separate:

1. **Trigger selection window:** pre-holdout validation predictions,
   out-of-fold predictions, or the recent pre-holdout 12-24 months.
2. **Robustness windows:** yearly slices, recent 12-month slices, and the broader
   pre-holdout period.
3. **Untouched holdout report:** run the selected trigger unchanged. Do not tune
   on this result.
4. **Full-period diagnostic:** useful for context, but not sufficient evidence
   if it includes model training data.

The goal is a stable trigger range, not the single best row from one backtest.

## Trigger Sweep

Typical sweep dimensions:

- `entry_threshold`;
- `max_hold_minutes`;
- `take_profit_pct`;
- optional stop-loss or trailing-stop settings when explicitly tested.

Typical fixed values:

- `rearm_threshold`;
- `exit_threshold`;
- `min_hold_minutes`;
- `cooldown_minutes`;
- fees and slippage.

No maintained standalone sweep script is currently documented. Use
`src/modeling/evaluation/backtest.py` from the modeling/evaluation flow or add a
thin CLI wrapper when the sweep contract is finalized.

Choose the sweep dates according to the window policy above, not by convenience.

## Selection Criteria

Prefer candidates that satisfy all of these:

- enough trades for the window length;
- profit factor comfortably above 1.0, usually above 2.0 for promotion-quality
  research;
- acceptable max drawdown for the strategy goal;
- stable win rate and return across slices;
- reasonable exposure and average hold time;
- no dependence on one exceptional market regime;
- nearby thresholds have similar behavior.

Avoid selecting a trigger only because it has the highest score in one sweep.

## Recommended Reports

For each serious candidate, report:

- selected trigger parameters;
- sweep top rows;
- yearly or rolling-window breakdown;
- final holdout result with unchanged trigger;
- full-period diagnostic result;
- trade count, win rate, profit factor, max drawdown, exposure, average hold,
  best/worst trade, and exit reason mix.

## Artifacts

Store outputs under:

```text
backtests/<strategy_id>/summary.json
backtests/<strategy_id>/trades.csv
backtests/<strategy_id>/equity_curve.csv
backtests/<strategy_id>/report.html
backtests/sweep_<model_id>.csv
```

Keep candidate comparison artifacts separate from live prediction tables.

## Strategy Config Updates

Update `config/strategies.json` only after trigger selection, robustness checks,
and holdout reporting.

For the active dashboard strategy:

- use the intended `asset_id`;
- point `model_id` at the promoted runtime model;
- store the exact backtest/evaluation `start` and `end`;
- include fees, slippage, cooldown, thresholds, and holding rules;
- set `output_dir` to `backtests/<strategy_id>`;
- place the active strategy first for the asset if the UI resolver uses first
  match semantics.

## Verification Checklist

- [ ] Strategy evaluation did not use final holdout for trigger selection.
- [ ] Sweep output exists and includes the selected row.
- [ ] Holdout report used the selected trigger unchanged.
- [ ] Full-period results are labeled as diagnostic when they include training
      data.
- [ ] `config/strategies.json` matches the reported parameters.
- [ ] Dashboard config loads the intended runtime model and strategy.
