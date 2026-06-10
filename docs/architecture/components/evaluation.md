# Component: Evaluation

Evaluation converts model probabilities into strategy evidence.

## Modules

| Module | Responsibility |
|---|---|
| `src/evaluation/backtest.py` | Bar-by-bar strategy simulation and report artifacts |
| `scripts/sweep_strategy.py` | Threshold and strategy parameter sweep |
| `scripts/backtest_strategy.py` | Run a configured strategy |

## Output Contract

Backtest outputs live under `backtests/<strategy_id>/`. Sweep outputs use
`backtests/sweep_<model_id>.csv`.

