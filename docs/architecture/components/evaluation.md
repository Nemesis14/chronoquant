# Component: Evaluation

Evaluation converts model probabilities into strategy evidence.

## Modules

| Module | Responsibility |
|---|---|
| `src/modeling/evaluation/backtest.py` | Bar-by-bar strategy simulation and report artifacts |

## Output Contract

Backtest outputs live under `backtests/<strategy_id>/`.
