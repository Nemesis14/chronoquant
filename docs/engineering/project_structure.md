# ChronoQuant Project Structure

## Root

- `AGENTS.md`: Codex entry point.
- `CLAUDE.md`: Claude Code compatibility entry point.
- `.agent/`: shared AI-agent rules, standards, board workflow, MCP reference
  config, and runtime notes.
- `pyproject.toml`, `uv.lock`: Python project and dependency metadata.

## Source

- `src/data_pipeline/`: OHLCV, feature, and prediction sync.
- `src/db/`: SQLite table operations, inspection helpers, and maintenance.
- `src/modeling/`: dataset loading, sampling, trainers, metrics, artifacts, reports.
- `src/evaluation/`: cutoff analysis and backtesting.
- `src/streamlit_app/`: dashboard app, sync runner, data reads, chart components.
- `src/trading/`: live/paper trading service, strategy decisions, state, exchange adapter, journal.
- `src/elliott_waves/`: Elliott Wave detection system (M1-M7).
  - `elliott/config.py`, `elliott/data.py`: ElliottConfig, Pivot, PatternCandidate.
  - `elliott/pivots/`: ZigZag, Fractal, multi-degree pivot motors.
  - `elliott/indicators/`: ATR-14, EMA slope, range expansion, volume.
  - `elliott/validators/`: Impulse, Diagonal, ZigZag, Flat, Triangle, WXY, FullCycle.
  - `elliott/scoring/`: Fibonacci ratios, geometry, momentum context.
  - `elliott/scanners/`: 1212, Wave3, Wave4, Wave5, ABC scanners.
  - `elliott/parser/`: CandidateStore, DynamicParser, OnlineStateMachine.
  - `elliott/viz/`: CandleChart, WavePlot, MultiWavePlot (matplotlib, dark theme).
  - `elliott/backtest/`: LabelGenerator, BacktestEvaluator, ParamSweep, WalkForwardEvaluator.

## Configuration

- `config/`: application, asset, feature, model, strategy, and runtime config.

Business logic should load config through `src/utils.py` instead of reading JSON
files directly.

## Artifacts And Data

- `database/`: SQLite databases.
- `models/`: trained model artifacts under `models/<model_id>/`.
- `samples/`: persisted modeling sample and fold definitions.
- `backtests/`: backtest outputs.
- `trading_reports/`: exported runtime trading reports.
- `logs/`: runtime logs.

## Documentation

- `docs/README.md`: documentation entry point and folder map.
- `docs/business/`: business problem, trading strategy, risk assumptions, glossary.
- `docs/concepts/`: target, feature, and market-pattern concepts.
- `docs/data/`: databases, schemas, dictionaries, lineage, quality checks, datasets.
- `docs/architecture/`: stable architecture documentation.
- `docs/architecture/components/`: component-level architecture notes.
- `docs/modeling/`: model workflow, sampling, validation, LightGBM guide, model cards.
- `docs/evaluation/`: strategy evaluation, backtest engine, decision reports.
- `docs/engineering/`: code style, commands, tooling, testing, workflow, structure, runbooks.
- `docs/reference/`: config, artifact, and script reference.
- `backlog/`: implementation specs and future work.
- `docs/concepts/elliott_waves/`: Elliott Wave research, specs, studies, and examples.

## Agent Rules

- `AGENTS.md`: Codex entry point.
- `.agent/`: shared standards, workflow rules, and runtime references.
- `CLAUDE.md`: Claude compatibility entry point that references `.agent/`.
