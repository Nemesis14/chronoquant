# UI Agent

Owns the Streamlit dashboard and user-facing display logic.

---

## Role

Streamlit application: data loading for the UI, sync triggers, chart and table
rendering, and user-facing configuration. Reads from the store via `src/data_handling/store/`
interfaces — does not write to DuckDB directly or modify ML logic.

---

## Required Skills and Tools

Read these before starting work:

- `.agent/general_principles.md`
- `.agent/skills/coding_skill.md`
- `.agent/skills/jira_skill.md`
- `.agent/tools/lsp_tool.md`

Do NOT load:
- `ast_grep_tool.md`, `uv_tool.md` — rarely needed for UI work; load on demand

Load relevant module docs (only for affected modules):

- `_doc_/8000_ui.md` + `_doc_/81xx_*.md` — if touching `src/ui/`
- `_doc_/7000_trading.md` + `_doc_/71xx_*.md` — if touching `src/trading/`

---

## Scope

| Path | Responsibility |
|------|---------------|
| `src/ui/` | Pages, components, data loading, sync wrappers |
| `src/trading/service.py` | Trading service called from UI |
| `_doc_/7xxx*.md`, `_doc_/8xxx*.md` | Trading and UI module documentation |

---

## Out of Scope

Minden egyéb domain: lásd delegation table — `CLAUDE.md`.

---

## Key Patterns

- UI data loading via `src/ui/data.py` — no raw DuckDB queries in page files
- Sync operations via `src/ui/sync.py`
- Store reads via `src/data_handling/store/duckdb_query.py` interfaces
- No `print()` in library code — use `logger.*` or `st.*`

---

## Notes

<!-- UI Agent-specific notes here as the role evolves. -->
