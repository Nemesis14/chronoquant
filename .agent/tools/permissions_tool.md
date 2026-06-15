# Permissions Tool — Agent Permission Profile

Expected capabilities for agents working in ChronoQuant. Actual permissions
are granted by the active runtime — this file documents the expected profile.

---

## Expected Capabilities Per Agent

| Capability | CLAUDE.md | Database Agent | Modeling Agent | UI Agent | Doc Agent |
|-----------|:---------:|:--------------:|:--------------:|:--------:|:---------:|
| File read | yes | yes | yes | yes | yes |
| File edit/write | `_jira_/` only | `src/store/`, `src/data_pipeline/` | `src/modeling/`, `src/evaluation/` | `src/streamlit_app/` | `.agent/`, config |
| Shell / PowerShell | no | yes | yes | yes | yes |
| MCP language-server | no | yes | yes | yes | no |
| Web access | no | on demand | on demand | no | on demand |

---

## Runtime Configuration Files

These files are **toolchain anchors** — they must stay at their fixed locations.
Their content and purpose is documented here; do not move them.

### `.claude/settings.json`
Project-level Claude Code permissions and MCP server enablement.
- `"enableAllProjectMcpServers": true` — required for language-server MCP
- `permissions.allow` — pre-approved tools and PowerShell command patterns

### `.claude/settings.local.json`
Machine-local overrides (gitignored). Extends `settings.json`.
- Skill permissions (e.g. `Skill(code-review)`, `Skill(run)`)
- Local `Bash` / `PowerShell` allow-all entries for dev convenience

### `.mcp.json`
MCP server config (gitignored, machine-specific). See `.agent/tools/lsp_tool.md`
for setup instructions and the template.

### `pyrightconfig.json`
Pyright type-checker config. Python 3.12, `.venv`, `typeCheckingMode: basic`.
See `.agent/tools/lsp_tool.md` for full details.

---

## Safety Rules

- Do not use destructive git or filesystem commands unless the user explicitly asks.
- Do not revert unrelated changes.
- Keep edits scoped to the requesting task.
- Prefer scoped validation (changed files only) before running the full gate.
- Never print, commit, or expose credentials from `.agent/.env`.
