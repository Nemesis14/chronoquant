# Shared Agent Knowledge

Shared rules, skills, and tool docs for AI agents working on ChronoQuant.

---

## Structure

```
.agent/
  general_principles.md         ← every agent reads this
  .env                          ← credentials (gitignored)

  agents/                       ← specialist agent manifests
    database_agent.md           ← src/store/, src/data_pipeline/, DuckDB
    modeling_agent.md           ← src/modeling/, src/evaluation/, features
    ui_agent.md                 ← src/streamlit_app/, trading UI
    doc_agent.md                ← tooling, infra, .agent/ maintenance

  skills/                       ← knowledge and workflows
    coding_skill.md             ← Python coding standards for this project
    jira_skill.md               ← _jira/ local task management workflow
    docs_skill.md               ← _docs/ local documentation workflow

  tools/                        ← tool setup, usage, and config anchors
    lsp_tool.md                 ← Pyright MCP language server + .mcp.json + pyrightconfig.json
    ast_grep_tool.md            ← structural code search (sg run)
    uv_tool.md                  ← Python env and package management
    permissions_tool.md         ← agent permission profile + .claude/ config files
```

---

## How Agents Load This

1. Every agent reads `general_principles.md`.
2. The orchestration layer lives in `CLAUDE.md` (project root) — no separate orchestrator agent.
3. Each specialist manifest (under `agents/`) lists which skills and tools to load.
4. Load only what the manifest requires — skip irrelevant skills/tools.

This keeps token usage minimal and context focused.
