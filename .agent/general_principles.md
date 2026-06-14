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
