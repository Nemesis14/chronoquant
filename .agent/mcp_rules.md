# MCP And Tool Rules

Shared MCP and tool-use rules for agents.

---

## Shared MCP Configuration

The shared MCP reference config is:

- `.agent/.mcp.json`

It defines the `language-server` MCP server for Pyright-backed navigation and
diagnostics.

Important: tool runtimes may not automatically read `.agent/.mcp.json`.
Claude Code may need explicit runtime support for this location. Codex receives
available MCP tools from the active session environment; in this session the
language-server tools are already exposed as `mcp__language_server`.

---

## Expected Language-Server Usage

All agents MUST use the language-server MCP tools as the first choice when they
are available. Do not use CLI Pyright or text search first for tasks that the
language server can answer directly.

Use language-server MCP tools for:

- symbol definitions;
- references before refactors;
- hover/type information;
- diagnostics after editing source files.

If the language server is unavailable, missing from the active runtime, or times
out, fall back to CLI validation and local repo inspection with `pyright`, `rg`,
`sg run`, and targeted file reads.

---

## CLI Tools

- Use `ruff check . --fix` for Ruff auto-fixes.
- Use CLI `pyright` for full-project validation or when language-server MCP
  diagnostics are unavailable.
- Use `pytest` for test validation.
- Use `uv` for Python environment and dependency management.
- Use `rg` for text search.
- Use `sg run` for structural Python searches.

---

## Codex MCP Note

Codex does not rely on repository-local Claude settings for MCP access. For
this repo, Codex should use the MCP tools currently exposed by the session and
treat `.agent/.mcp.json` as the shared reference configuration.
