# UI Agent

Owns the FastAPI BFF, the job worker, the chronoquant MCP server, the TV sentiment module, the React frontend and the dev/start scripts.

---

## Role

Az epic_068 utáni felhasználói felület és kiszolgáló réteg: `src/api` (REST + SSE + chat), `src/worker` (hosszú jobok),
`src/cq_mcp` (a chat MCP szervere), `src/sentiment` (TradingView hangulat), `frontend/` (React UI), `scripts/` (PowerShell indítók).
Adatot a store olvasó interfészein át, DuckDB-t csak read-only nyitva olvas; írás kizárólag worker jobbal. Nem módosít ML logikát,
nem ír közvetlenül DuckDB-be az API-ból.

---

## Required Skills and Tools

Read these before starting work:

- `.agent/general_principles.md`
- `.agent/skills/coding_skill.md`
- `.agent/skills/jira_skill.md`
- `.agent/tools/lsp_tool.md`

Do NOT load:
- `ast_grep_tool.md`, `uv_tool.md` — rarely needed; load on demand
- `.agent/skills/tv_sentiment_skill.md` — csak sentiment/chat persona munkához

Load relevant module docs (only for affected modules):

- `_doc_/_plans_/068_bff_ui.md` — az architektúra kontraktusa
- `_doc_/database_and_code_doc/8300_api.md` — `src/api`
- `_doc_/database_and_code_doc/8310_worker.md` — `src/worker`
- `_doc_/database_and_code_doc/8320_cq_mcp.md` — `src/cq_mcp` (tool-lista, jogosultság)
- `_doc_/database_and_code_doc/8330_reports.md` — report-formátum (olvasó oldal)
- `_doc_/database_and_code_doc/8340_sentiment.md` — `src/sentiment`
- `_doc_/database_and_code_doc/8350_frontend.md` — `frontend/`
- `_doc_/database_and_code_doc/8360_scripts.md` — `scripts/`
- `_doc_/database_and_code_doc/7130_trading_journal.md` — trade journal
- `_doc_/database_and_code_doc/5800_scout_runtime.md` — Impulse Scout runtime (worker Scout lépés, `/api/scout/*`, MCP `scout_signals`, Scout panel)

---

## Scope

| Path | Responsibility |
|------|---------------|
| `src/api/` | FastAPI app (`main.py`), `routers/`, `services/`, `schemas.py`, `chat/` (claude CLI runner + `persona_prompt.md`), `tests/` |
| `src/worker/` | `python -m worker`: SQLite job queue, runner, auto-sync scheduler, `handlers/` (`sync_full`, `sync_predictions`, `sentiment_refresh`, `sync_scout`), `scout_runner.py` |
| `src/cq_mcp/` | `chronoquant` MCP szerver (`/mcp/chronoquant`), 20 tool (köztük `scout_signals`) |
| `src/sentiment/` | `tv_report` (fetch, summary, author_backtest) |
| `frontend/` | React 18 + TS + Vite + Tailwind SPA (`pages/`, `components/`, `api/`, `lib/`, `types/`) |
| `src/trading/live/journal.py` | Trade journal (DuckDB); az order-logika törölve |
| `scripts/` | `dev.ps1`, `start.ps1`, `stop.ps1`, `install_autostart.ps1`, `_common.ps1` |
| `.agent/skills/tv_sentiment_skill.md` | A chat hangulat-workflow |

---

## Out of Scope

Minden egyéb domain: lásd delegation table — `CLAUDE.md`. A `src/reports` builderek (riport tartalom) az `analyst_agent` területe; az API csak olvassa őket.

---

## Key Patterns

- Minden REST/SSE végpont `/api/*` alatt; az SPA fallback `frontend/dist`-ből szolgál, de `/api/*` ismeretlen útra JSON 404-et ad
- Az API csak olvas (DuckDB RO); minden írás/hosszú művelet = worker job a `worker.queue.JobQueue`-n át (dedup, `worker_down` jelzés)
- Új író MCP tool csak worker jobot ütemezhet; jelenleg: `enqueue_sync`, `sentiment_refresh` — a többi tool csak olvas
- Chat jogosultság fix: Read, WebSearch, WebFetch + `mcp__chronoquant`; Edit/Write/NotebookEdit/Bash tiltott, a projekt `.claude` settings nem töltődik be
- Config kizárólag `src/utils.py`-n át; port: 8600 (API+prod UI), 5173 (Vite dev)
- Frontend: típusok `src/types/`, fetch `src/api/`, SSE/polling `src/lib/`; a Plotly lazy tölt; Plotly figure téma a frontenden
- Tesztek: `uv run pytest src/api src/worker src/cq_mcp src/sentiment`; frontend: `cd frontend && npx tsc --noEmit && npm run build`
- No `print()` in library code — use `logging`

---

## Notes

<!-- UI Agent-specific notes here as the role evolves. -->
