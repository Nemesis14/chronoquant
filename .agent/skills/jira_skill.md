# Jira Skill — Local Task Management

ChronoQuant uses a local `_jira/` directory for task tracking. No external
issue tracker. Read this before creating, updating, or referencing tasks.

---

## Directory Structure

```
_jira/
  epic_{id}_{slug}/
    todo_{tid}_{slug}.md     ← task, active work
    pr_{tid}_{slug}.md       ← task, ready for review
    done_{tid}_{slug}.md     ← task, accepted (delete after sprint)
    todo_{sid}_{slug}.md     ← story, pending (no pr/done state)
```

---

## ID Rules

- IDs are **globally unique** across the entire `_jira/` directory
- `epic_{n}` — epic number, e.g. `epic_27`
- `t{n}` — task, e.g. `t11`, `t12`
- `s{n}` — story, e.g. `s2`, `s3`
- When creating a new item: scan all `_jira/` for the highest existing ID and increment

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
title: Short imperative title
assignee: database_agent | modeling_agent | ui_agent | doc_agent | validator_agent
status: todo
blocks: []        # optional: task IDs this blocks
blocked_by: []    # optional: task IDs this depends on
---

## Goal
What needs to be done and why.

## Scope
Which files and modules are affected.

## Acceptance Criteria
- [ ] criterion 1
- [ ] criterion 2

## Notes
Progress notes, blockers, decisions. Append, do not overwrite.
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

- Name: `epic_{id}_{slug}` where slug is 2-5 words, lowercase, underscores
- Create the epic folder before creating child tasks or stories
- No separate epic definition file — the folder name and its contents define the epic

---

## Agent Responsibilities

**Orchestrator**: creates epics, tasks, stories; renames files on state change;
deletes `done_` files.

**Developer agents** (database_agent, modeling_agent, ui_agent, doc_agent):
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
