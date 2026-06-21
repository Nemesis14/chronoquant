# General Agent Principles

Shared operating principles for all AI agents working on ChronoQuant.

---

## Core Rules

- Read the relevant shared rules before work starts.
- Ask before acting when a critical requirement is unclear.
- Check existing code and documentation before proposing or writing new logic.
- Do not duplicate existing behavior unless the user explicitly asks for it.
- Keep changes scoped to the task.
- Do not revert unrelated user changes.
- Prefer small, independently validatable work units.
- Record assumptions explicitly in plans, reviews, and documentation.
- **`_jira_/archive/` olvasása tilos** — soha ne keresd, listázd, vagy olvasd be. Aktív taskok csak `_jira_/epic_*/`-ban találhatók.
- **Aktív asset: SOLUSDT** — BCHUSDT és más párok ne kapjanak fejlesztési munkát.

---

## Before Implementation

For non-trivial implementation work:

1. Outline a short execution plan.
2. Surface critical unknowns before editing.
3. Inspect existing code, docs, and config that may already cover the need.
4. Verify that the proposed change does not contradict current project conventions.

---

## Navigation

- Use language-server tools for symbol definitions, references, hovers, and
  diagnostics when available.
- Use `sg run` for structural Python searches.
- Use `rg` for fast text and file searches.

## Subagent Spawn Policy

- For simple lookups (find a file, search for a symbol, read a specific path),
  use Grep / Glob / Read directly — never spawn an agent.
- Before spawning any agent for a request that looks simple or narrow in scope,
  pause and ask the user:
  > "Ez agent spawn-nal oldható meg, de direkt tool-lal is elég lenne.
  > Biztos spawnt szeretnél, vagy megpróbálom Grep/Glob/Read-del közvetlenül?"
  **Kivétel:** orchestrált Flow B (Mód B) futtatásakor az orchestrátor explicit
  felhatalmazással bír — párhuzamos spawn user-kérdezés nélkül mehet.
- Only spawn if the task is genuinely open-ended, multi-step, or requires
  parallel independent searches that cannot be expressed as a single tool call.

### Model selection for spawned agents

- **Search / exploration tasks** → always use `Explore` agent with `model: "haiku"`.
  Never use `general-purpose` for file searches, symbol lookups, or read-only
  codebase exploration. `general-purpose` is a Sonnet-class agent and is
  disproportionately expensive for these tasks.
- **Implementation / analysis tasks** → use the appropriate named agent
  (database_agent, ui_agent, etc.) without model override (inherits Sonnet).
- Rule: if the agent will not write or edit any file, it should run on Haiku.
