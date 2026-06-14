# MCP And Tool Rules

Shared MCP and tool-use rules for agents.

---

## Shared MCP Configuration

The MCP configuration lives in `.mcp.json` at the **project root** (gitignored,
machine-specific). It defines one server:

- `language-server`: Pyright-backed navigation and diagnostics.

Runtime notes:

- **Claude Code**: reads `.mcp.json` from the project root. The
  `.claude/settings.json` file must contain `"enableAllProjectMcpServers": true`.
  If `.mcp.json` is missing, recreate it from the template in this file (see
  below) and adjust absolute paths for the machine.
- **Codex**: receives available MCP tools from the active session environment.
  Codex uses its own `~/.codex/config.toml` / project `.codex/config.toml`
  MCP configuration and does not consume Claude's `.mcp.json` directly.

Template for `.mcp.json` (adjust absolute paths for the machine):

```json
{
  "mcpServers": {
    "language-server": {
      "command": "<go-bin>/mcp-language-server.exe",
      "args": ["--workspace", "<repo-root>", "--lsp", "<repo-root>/.venv/Scripts/pyright-langserver.exe", "--", "--stdio"]
    }
  }
}
```

## Atlassian Access — REST Basic Auth Only

All agents access Jira and Confluence via **REST API with Basic Auth**.
Atlassian access in this repository is REST-only.

Credentials from `.agent/.env`:

- `email` + `api_token` → works for both Jira and Confluence REST.

REST base URL: `https://erosszsolt84.atlassian.net`

Authorization header: `Basic base64(email:api_token)`

Never print or commit credential values. See `.agent/atlassian_workflow.md` for
validation commands and full token rules.

---

## Expected Atlassian Usage

Use Jira for active task tracking in project `KAN`. Use Confluence for longer
plans, design notes, and validation writeups, then link those pages from the
related Jira issue.

Use `.agent/.env` credentials and the REST pattern documented in
`.agent/atlassian_workflow.md` for issue creation, transitions, comments,
documentation lookup, page updates, and issue/page linking.

---

## Expected Language-Server Usage

Claude and other agents with a working language-server MCP path should use the
language-server MCP tools as the first choice when they are available. Do not
use CLI Pyright or text search first for tasks that the language server can
answer directly.

Use language-server MCP tools for:

- symbol definitions;
- references before refactors;
- hover/type information;
- diagnostics after editing source files.

If the language server is unavailable, missing from the active runtime, or times
out, fall back to CLI validation and local repo inspection with `pyright`, `rg`,
`sg run`, and targeted file reads.

### Codex Pyright CLI Default

For Codex specifically, treat the integrated `language-server` MCP path as
unreliable in this repository. Do not call `mcp__language_server__*` for routine
navigation, hover/type inspection, references, or diagnostics during task work.
This repository has observed Codex-side MCP timeouts even when the same
`mcp-language-server.exe` plus project `.venv/Scripts/pyright-langserver.exe`
command works in Claude and in standalone stdio tests.

Codex should default to CLI/local inspection:

- use `uv run pyright <file>` for file-level diagnostics;
- use `uv run pyright` for full-project type validation;
- use `rg` for text lookup and `sg run` for structural Python lookup;
- read targeted source files directly for hover-like type/docstring context;
- record in Jira or the final validation notes that the Codex MCP path was
  skipped because the integrated Codex MCP path is known unreliable here.

Claude should continue using the working language-server MCP path when
available.

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
this repo, Codex should not rely on exposed `language-server` tools even when
the active session lists them. Use the CLI Pyright default above instead of
blocking implementation, review, or validation work on MCP repair.

The MCP server template and token rules are documented in this file for agents
where the MCP path works, such as Claude.
