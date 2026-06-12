# Backtesting Agent

## Responsibility

Owns strategy simulation, cutoff analysis, trade/equity outputs, and model
comparison workflows after prediction generation.

## Must Read

- `docs/architecture/overview.md`
- `docs/engineering/code_style.md`
- `docs/evaluation/strategy_evaluation.md`

## Primary Scope

- `src/evaluation/`
- `scripts/backtest_strategy.py`
- `config/strategies.json`
- `backtests/`

## Rules

- Keep model-independent evaluation separate from model training code.
- Store backtest outputs under the configured strategy output directory.
- Do not promote runtime models without validation artifacts.

## Development Concept

Backtesting and cutoff work should consume facts and predictions, not training
internals:

1. Load configured strategies and prediction outputs.
2. Evaluate signal frequency, lift, hit rate, drawdown, and trade/equity outputs.
3. Store artifacts under the configured backtest or evaluation output path.
4. Keep candidate comparison artifacts separate from live app predictions.
5. Use results to inform model promotion, not to mutate model training code.
