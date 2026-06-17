# Shared Agent Knowledge

Shared rules, skills, and tool docs for AI agents working on ChronoQuant.

---

## Structure

```
.agent/
  general_principles.md         ← every agent reads this
  .env                          ← credentials (gitignored)

  agents/                       ← specialist agent manifests
    database_agent.md           ← src/database/, DuckDB
    modeling_agent.md           ← src/modeling/, features, model artifacts
    ui_agent.md                 ← src/ui/, src/trading/
    code_doc_agent.md           ← tooling, infra, .agent/ maintenance, _doc_/ X110+
    analyst_agent.md            ← _doc_/analysis/ EDA notebooks
    methodology_agent.md        ← _doc_/ X000/X100 módszertani tartalom
    validator_agent.md          ← pr_ ticket validálás, tesztek, javítás

  skills/                       ← knowledge and workflows
    coding_skill.md             ← Python coding standards for this project
    jira_skill.md               ← _jira_/ local task management workflow
    docs_skill.md               ← _doc_/ documentation format, numbering, Mermaid rules
    methodology_doc_skill.md    ← X000/X100 methodology section templates and rules
    analyst_skill.md            ← analysis notebook structure and workflow
    analysis_presentation_skill.md ← table formatting and display for notebooks

  tools/                        ← tool setup, usage, and config anchors
    lsp_tool.md                 ← Pyright MCP language server + .mcp.json + pyrightconfig.json
    ast_grep_tool.md            ← structural code search (sg run)
    uv_tool.md                  ← Python env and package management
    permissions_tool.md         ← agent permission profile + .claude/ config files
```

---

## Agent Ownership Summary

| Agent | Owns |
|-------|------|
| `database_agent` | `src/database/`, DuckDB schema, sync pipeline |
| `modeling_agent` | `src/modeling/`, features, model artifacts |
| `ui_agent` | `src/ui/`, `src/trading/` |
| `code_doc_agent` | `.agent/`, tooling, deps; `_doc_/` X110+ kód-referencia |
| `analyst_agent` | `_doc_/analysis/` — EDA notebooks, empirikus elemzések |
| `methodology_agent` | `_doc_/` X000, X100 — módszertani háttér, üzleti rationale |
| `validator_agent` | `pr_` ticket validálás, static analysis, pytest |

---

## How Agents Load This

1. Every agent reads `general_principles.md`.
2. The orchestration layer lives in `CLAUDE.md` (project root) — no separate orchestrator agent.
3. Each specialist manifest (under `agents/`) lists which skills and tools to load.
4. Load only what the manifest requires — skip irrelevant skills/tools.

This keeps token usage minimal and context focused.
