# ChronoQuant Agent Entry Point

This is the shared entry point for coding agents working in this repository.

## Session Startup Reading

- `docs/engineering/code_style.md`
- `docs/engineering/commands.md`
- `docs/engineering/tooling.md`
- `docs/engineering/testing.md`
- `docs/engineering/workflow.md`
- `docs/engineering/project_structure.md`
- `docs/architecture/overview.md`

## Agent Definitions

Use the matching role file from `agents/` when the task clearly fits a role:

- UI: `agents/ui.md`
- Modeling: `agents/modeling.md`
- Data pipeline: `agents/data_pipeline.md`
- Backtesting: `agents/backtesting.md`
- Ops: `agents/ops.md`

Role files define task concepts, scope boundaries, and implementation rules.
Before doing role-specific work, read the relevant role file and the docs listed
in that role file's "Must Read" section.

## Core Project Rules

- Run commands from the repo root.
- Use `src/utils.py` as the config-loading entry point. Do not read JSON config
  files directly from business logic.
- Keep scripts thin; reusable logic belongs under `src/`.
- Store timestamps as UTC strings in `YYYY-MM-DD HH:MM:SS`.
- Keep generated model artifacts under `models/<model_id>/`.
- Keep candidate model evaluation output separate from the live predictions table.
