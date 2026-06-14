# Docs Skill — Local Documentation

ChronoQuant documentation lives in `_docs/`. Read this before creating or
updating module documentation.

---

## Structure

`_docs/` mirrors `src/`. Every `src/` module directory has a corresponding
`_docs/` subdirectory:

```
_docs/
  store/             ← mirrors src/store/
  data_pipeline/     ← mirrors src/data_pipeline/
  modeling/          ← mirrors src/modeling/
  evaluation/        ← mirrors src/evaluation/
  streamlit_app/     ← mirrors src/streamlit_app/
  trading/           ← mirrors src/trading/
  app/               ← mirrors src/app/
  elliott_waves/     ← mirrors src/elliott_waves/
  plotting/          ← mirrors src/plotting/
```

---

## When to Create a Doc

- When a module has non-obvious architecture, invariants, or data flow
- When acceptance criteria for a task include documentation
- When onboarding a new agent to a module
- When a design decision is made that is not obvious from the code

Do NOT duplicate what is already readable from the source code. Document
the **why** and **how**, not the **what**.

---

## Doc File Naming

- One file per logical topic within the module directory
- Names: lowercase, underscores, descriptive — `sync_flow.md`, `schema.md`, `partitioning.md`
- No date prefixes; content should be evergreen

---

## Doc File Template

```markdown
# {Topic Title}

One-line summary of what this document covers.

---

## Purpose
Why this module or subsystem exists. What problem it solves.

## Structure
Key files and their roles within this module.

## Key Patterns
Important conventions, data flows, or invariants that are not obvious from code.

## Dependencies
What this module depends on; what depends on it.

## Notes
Open questions, known limitations, future considerations.
```

---

## Agent Responsibilities

Each specialist agent is responsible for the `_docs/` subdirectories within
their scope:

| Agent | _docs/ scope |
|-------|-------------|
| DataEngineer | `_docs/store/`, `_docs/data_pipeline/` |
| MLEngineer | `_docs/modeling/`, `_docs/evaluation/` |
| Prism | `_docs/streamlit_app/`, `_docs/trading/` |
| Steward | `_docs/` root README only |

Conductor links relevant `_docs/` pages from task files when useful.

---

## Relationship to _jira

Task files in `_jira/` may reference `_docs/` pages for context:
```markdown
## Scope
See `_docs/store/schema.md` for current DuckDB schema.
```

This keeps task files concise while pointing to stable reference docs.
