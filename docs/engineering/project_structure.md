# ChronoQuant Project Structure

## Root

- `AGENTS.md`: shared coding-agent entry point.
- `CLAUDE.md`: Claude Code compatibility entry point.
- `.mcp.json`: repository MCP server configuration.
- `.claude/`: Claude-specific runtime settings.
- `.codex/`: Codex-specific local notes or settings.
- `pyproject.toml`, `uv.lock`: Python project and dependency metadata.

## Source

- `src/data_pipeline/`: OHLCV, feature, and prediction sync.
- `src/db/`: SQLite table operations, inspection helpers, and maintenance.
- `src/modeling/`: dataset loading, sampling, trainers, metrics, artifacts, reports.
- `src/evaluation/`: cutoff analysis and backtesting.
- `src/streamlit_app/`: dashboard app, sync runner, data reads, chart components.

## Configuration

- `config/`: application, asset, feature, model, strategy, and runtime config.

Business logic should load config through `src/utils.py` instead of reading JSON
files directly.

## Artifacts And Data

- `database/`: SQLite databases.
- `models/`: trained model artifacts under `models/<model_id>/`.
- `samples/`: persisted modeling sample and fold definitions.
- `backtests/`: backtest outputs.
- `logs/`: runtime logs.

## Documentation

- `docs/engineering/`: code style, commands, tooling, testing, workflow, structure.
  - `workflow.md`: high-level engineering flow.
  - `sampling.md`: data range, sample, split, and holdout rules.
  - `lgbm_model_development.md`: LightGBM-specific model workflow.
  - `strategy_evaluation.md`: trigger sweep, backtest, and strategy evaluation rules.
- `docs/architecture/`: stable architecture documentation.
- `docs/plans/active/`: currently active implementation plans.
- `docs/plans/completed/`: completed plans.
- `docs/plans/backlog/`: future work.
- `docs/analysis/`: model and research analysis.

## Agent Definitions

- `agents/`: role-specific agent definitions and development concepts.

Use a role file when the task clearly belongs to that role. For example, model
development should use `agents/modeling.md`, and dashboard work should use
`agents/ui.md`.
