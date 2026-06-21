# Jira Skill — Local Task Management

ChronoQuant uses a local `_jira_/` directory for task tracking. No external
issue tracker. Read this before creating, updating, or referencing tasks.

---

## Directory Structure

```
_jira_/
  epic_{id}_{slug}/
    todo_{tid}_{slug}.md     ← task, active work
    pr_{tid}_{slug}.md       ← task, ready for review
    done_{tid}_{slug}.md     ← task, accepted (delete after sprint)
    todo_{sid}_{slug}.md     ← story, pending (no pr/done state)
```

---

## ID Rules

- IDs are **globally unique** across the entire `_jira_/` directory
- `epic_{n}` — epic number, e.g. `epic_27`
- `t{n}` — task, e.g. `t11`, `t12`
- `s{n}` — story, e.g. `s2`, `s3`
- When creating a new item: scan all `_jira_/` for the highest existing ID and increment

---

## Task Lifecycle

```
todo_t11_slug.md  →  pr_t11_slug.md  →  done_t11_slug.md  →  delete
```

- **Rename the file** as state changes — do not create a new file
- `todo_`: active, in progress
- `pr_`: implementation complete, ready for review
- `done_`: review accepted — delete the file within the sprint

## Story Lifecycle

```
todo_s2_slug.md  →  (broken into tasks)  →  delete when all tasks done
```

- Stories only have `todo_` state
- A story is a spec to be broken into tasks — no pr/done transition
- Delete the story file when all its tasks are done

---

## Task File Template

```markdown
---
epic: epic_{id}
id: t{n}
title: Rövid imperatív cím
assignee: database_agent | modeling_agent | ui_agent | code_doc_agent | methodology_agent | analyst_agent | validator_agent
status: todo | pr | done
blocks: []        # opcionális: ezeket blokkol
blocked_by: []    # opcionális: ezektől függ
---

## Goal
Mit kell csinálni és miért.

## Scope
Érintett fájlok és modulok.

## Acceptance Criteria
- [ ] kritérium 1
- [ ] kritérium 2

## Notes
Progress notes, döntések, blockerek. Append, ne felülírd.
```

---

## Story File Template

```markdown
---
epic: epic_{id}
id: s{n}
title: Short descriptive title
---

## Goal
What this story covers.

## Tasks
- [ ] t{n}: task title
- [ ] t{n}: task title

## Notes
Design decisions, open questions.
```

---

## Epic Folder Convention

- Name: `epic_{id}_{slug}` where `{id}` is a **3-digit zero-padded number** (e.g. `epic_011_slug`) and `{slug}` is 2-5 words, lowercase, underscores
- Create the epic folder before creating child tasks or stories
- Completed epics are moved to `_jira_/archive/` automatically by the Stop hook

### `epic.md` — High-level summary file

Create an `epic.md` inside the epic folder when:
- The request is complex (Flow B), OR
- The user provides a `story_` file as input

`epic.md` is **not a tracked task** — it has no `todo_/pr_/done_` prefix and no lifecycle.
It documents: goal, scope, key decisions, and links to constituent tasks.

```markdown
# Epic {id}: {title}

## Goal
Why this epic exists.

## Scope
Modules and files affected.

## Tasks
- t{n}: title (assignee)
- t{n}: title (assignee)

## Key Decisions
Architectural or design choices made during execution.
```

**Archive condition**: the Stop hook archives an epic when all task files are `done_`
and the only remaining file is `epic.md`. The `epic.md` moves to archive together with the folder.

⚠️ **NEVER read from `_jira_/archive/`** — archived epics are not relevant to active work.

## Epic Counter (`_jira_/jira.json`)

`_jira_/jira.json` holds the global epic counter. **Always read it before creating a new epic.**

```json
{
  "epic_counter": 11
}
```

Workflow:
1. Read `_jira_/jira.json` → get `epic_counter` value (e.g. `11`)
2. Create the epic folder: `_jira_/epic_011_<slug>/`
3. Write new `epic_counter` value back: `12`

The counter is the **source of truth** for the next epic ID. Never derive the ID by scanning folder names.

---

## Agent Responsibilities

**Orchestrator**: creates epics, tasks, stories; renames files on state change;
deletes `done_` files.

**Developer agents** (database_agent, modeling_agent, ui_agent, code_doc_agent, methodology_agent, analyst_agent):
update `## Notes` on their active task; rename `todo_` → `pr_` when done.

**Validator agent**: works only on `pr_` tasks; runs static analysis and tests;
fixes small issues; renames `pr_` → `done_` when clean. If a significant issue
is found: renames back to `todo_` and appends a `[validator]` note explaining why.

## Returning a task to todo_

When the Validator Agent returns a task to `todo_`:

1. Rename `pr_t{n}_slug.md` → `todo_t{n}_slug.md`
2. Append to `## Notes`:

```
[validator] Returned to todo — YYYY-MM-DD
Reason: <what was found>
Failing test / check: <name or assertion>
```

The developer agent that picks it up again must address the noted issue before
moving back to `pr_`.
