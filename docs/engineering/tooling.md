# ChronoQuant Tooling

## MCP

The repository-level MCP configuration lives in `.mcp.json`.

Current configured server:

- `language-server`: wraps Pyright through `mcp-language-server`.

Agents should use MCP/LSP tools when they help inspect definitions, references,
or diagnostics. If an LSP call times out or is unavailable, fall back to local
repo inspection with fast shell tools such as `rg`.

## LSP

The language server is configured for the repo workspace:

```text
d:/repos/chronoquant
```

Expected use cases:

- inspect symbol definitions;
- find references;
- check diagnostics after code edits;
- avoid guessing cross-file behavior when LSP can answer directly.

## Session Permissions

This project expects coding agents to be able to:

- read and write files in the repository;
- edit source, config, docs, tests, and scripts;
- run tests and project scripts;
- install Python packages when needed for the task.

Permission enforcement is controlled by the active tool/runtime, not only by
repository docs. If a tool asks for approval, update that tool's runtime settings
or session launch options.

Claude-specific allow rules live in `.claude/settings.json`.
Codex-specific local notes can live in `.codex/settings.md`.

## Package Management

Prefer the existing project tooling and lock files. This repo currently includes
`pyproject.toml` and `uv.lock`, so package changes should be made in a way that
keeps those files consistent.
