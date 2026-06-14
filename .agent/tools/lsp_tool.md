# LSP Tool — Language Server (Pyright MCP)

Pyright-backed navigation and diagnostics via MCP. Read this before using
language-server MCP tools or falling back to CLI.

---

## Configuration

MCP config lives in `.mcp.json` at project root (gitignored, machine-specific).

```json
{
  "mcpServers": {
    "language-server": {
      "command": "<go-bin>/mcp-language-server.exe",
      "args": [
        "--workspace", "d:/repos/chronoquant",
        "--lsp", "d:/repos/chronoquant/.venv/Scripts/pyright-langserver.exe",
        "--", "--stdio"
      ]
    }
  }
}
```

Pyright config: `pyrightconfig.json` at repo root — Python 3.12, `.venv`, `typeCheckingMode: basic`.

`.claude/settings.json` must contain `"enableAllProjectMcpServers": true`.

---

## Available MCP Tools

| Tool | When to use |
|------|-------------|
| `mcp__language-server__diagnostics` | Type errors and warnings in a file — use after editing |
| `mcp__language-server__hover` | Symbol type and docstring at a position |
| `mcp__language-server__definition` | Where a symbol is defined |
| `mcp__language-server__references` | All references to a symbol — run before refactoring |
| `mcp__language-server__rename_symbol` | Project-wide rename |
| `mcp__language-server__edit_file` | Apply LSP text edit |

---

## Priority Rules

Use MCP first, CLI as fallback:

| Task | First choice | Fallback |
|------|-------------|---------|
| File diagnostics after edit | `diagnostics` MCP | `uv run pyright src/foo.py` |
| Symbol lookup | `definition` MCP | `rg` / `sg run` |
| Type / docstring | `hover` MCP | read the source file |
| References before refactor | `references` MCP | `rg` for the symbol name |
| Full project validation | CLI `uv run pyright` | — |

---

## Fallback — CLI Pyright

```powershell
# Single file
uv run pyright src/store/duckdb_store.py

# Full project
uv run pyright
```

Use CLI for full-project validation before committing regardless of MCP availability.

---

## When MCP Is Unavailable

If `.mcp.json` is missing: recreate from the template above, adjusting absolute
paths for the machine. If MCP times out or errors: fall back to CLI without
blocking work.
