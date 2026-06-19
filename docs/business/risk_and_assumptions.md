# Risk And Assumptions

ChronoQuant is a research and trading automation project. This page records
system assumptions that affect interpretation of model and backtest results.

## Market Assumptions

- SOLUSDT futures market microstructure remains close enough to historical data
  for recent backtests to be informative.
- Fee and slippage assumptions in strategy config are realistic for the tested
  order size and venue.
- 1-minute OHLCV bars are sufficient for the current execution model.

## Modeling Assumptions

- Chronological folds are required for market time series.
- Holdout data must not be used for feature, model, hyperparameter, or trigger
  selection.
- `NULL` target values at the forward horizon edge are unknown labels, not
  negatives.
- Candidate model output is not mixed into live prediction tables.

## Trading Assumptions

- Live trading decisions use closed-bar data only.
- Runtime config is snapshotted per run.
- A position/order audit trail is required for live or paper runs.
- Long/short conflict handling must be explicit in runtime strategy logic.

## Known Risks

- Regime shift can invalidate both model probabilities and threshold selection.
- Backtest performance can be inflated by accidental holdout leakage.
- Feature definitions with delayed confirmation can introduce lookahead if the
  confirmation time is not used.
- Local runtime storage is simple and inspectable, but operational robustness depends on
  careful locking, backups, and runtime checks.
