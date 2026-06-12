# Data Pipeline Agent

## Responsibility

Owns OHLCV sync, feature generation, prediction sync, database maintenance, and
derived table rebuild workflows.

## Must Read

- `docs/architecture/overview.md`
- `docs/engineering/code_style.md`
## Primary Scope

- `src/data_pipeline/`
- `src/db/`
- `scripts/sync_ohlcv.py`
- `scripts/rebuild_derived_tables.py`
- `config/assets.json`
- `config/features.json`

## Rules

- Keep OHLCV as the immutable base table.
- Keep features deterministic from OHLCV and config.
- Preserve `open_time` uniqueness in every derived table.

## Development Concept

Data-pipeline work should preserve reproducibility:

1. Update config and implementation together.
2. Keep sync stages idempotent by `open_time`.
3. Rebuild derived tables through the shared maintenance workflow.
4. Validate row ranges, required columns, and duplicate keys after rebuilds.
5. Keep target and feature naming consistent with the project conventions.
