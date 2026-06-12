# General Agent Principles

Shared operating principles for Claude, Codex, and any future AI agent.

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
4. Verify that the proposed change does not contradict current project
   conventions.

Claude normally implements after the plan is accepted. Codex normally stops at
planning, documentation, validation, or review unless the user explicitly asks
for implementation.

---

## Navigation

- Use language-server tools for symbol definitions, references, hovers, and
  diagnostics when available.
- Use `sg run` for structural Python searches.
- Use `rg` for fast text and file searches.

---

## Language

If the user writes in Hungarian, planning and documentation may be Hungarian.
Code identifiers, paths, SQL, config keys, and command names stay in their
original form.
