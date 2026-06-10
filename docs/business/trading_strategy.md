# Trading Strategy

This page describes the business meaning of the current SOLUSDT strategy. The
technical implementation lives in `docs/evaluation/backtest_engine.md` and
`src/trading/`.

## Prediction Meaning

ChronoQuant currently uses two independent binary classifiers:

| Side | Target | Meaning |
|---|---|---|
| Long | `trg_l_fw60_q90` | Probability that the next 60 minutes contain an unusually strong upward move |
| Short | `trg_s_fw60_q10` | Probability that the next 60 minutes contain an unusually strong downward move |

The target is intentionally not "will price go up/down". It asks whether the
future window reaches a stronger percentile-defined event. This filters small
noise moves.

## Signal Logic

The strategy converts probabilities into trades with:

- entry threshold;
- rearm threshold;
- exit threshold;
- min/max hold time;
- cooldown;
- fee and slippage assumptions;
- optional stop/take-profit/trailing-stop settings.

The active strategy parameters live in `config/strategies.json`.

## Current Artifact Locations

| Artifact | Location |
|---|---|
| Model registry | `config/models.json` |
| Runtime model config | `config/env.json` |
| Strategy config | `config/strategies.json` |
| Long/short model artifacts | `models/<model_id>/` |
| Backtest outputs | `backtests/<strategy_id>/` |
| Live/paper runtime reports | `trading_reports/<run_id>/` |

## Evidence

Use model cards for model evidence and evaluation reports for strategy evidence.

- Model cards: `docs/modeling/model_cards/`
- Strategy reports: `docs/evaluation/reports/`

