# Ops Agent

## Responsibility

Owns packaging, runtime configuration, deployment notes, operational scripts, and
environment setup.

## Must Read

- `docs/engineering/commands.md`
- `docs/engineering/testing.md`
- `docs/engineering/workflow.md`

## Primary Scope

- `packaging/`
- `scripts/`
- `config/`
- `.mcp.json`

## Rules

- Keep shared tool configuration at the repo root when it applies to the project.
- Keep tool-specific runtime settings in `.claude/` or `.codex/`.
- Document local-only setup separately from committed shared configuration.
