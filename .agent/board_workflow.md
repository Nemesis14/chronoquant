# Board Workflow

Shared rules for `board.md` and implementation plan files.

---

## Board Purpose

`board.md` is the active task board. It should contain work that is current or
near-term enough that agents need to see it at session start.

Each task row should include:

- task title;
- affected scope;
- short note or dependency.

---

## Plan Folders

Implementation plans live under:

- `docs/plans/active/`: work currently being executed.
- `docs/plans/backlog/`: planned future work.
- `docs/plans/completed/`: completed plans.

Use plan files for work that needs more detail than a single board row.

---

## Task Start

Before starting a board task:

1. Read `board.md`.
2. Check whether a related plan exists under `docs/plans/active/` or
   `docs/plans/backlog/`.
3. Confirm scope, dependencies, and validation expectations.

---

## Task Completion

When a task is finished:

1. Remove the task row from `board.md`; do not mark it done in-place.
2. Move the related plan file from `docs/plans/backlog/` or
   `docs/plans/active/` to `docs/plans/completed/` if one exists.
3. Record the validation performed in the final agent response or in the plan
   file when appropriate.

Do not close a task only because code was edited. Close it only after the
requested behavior and validation criteria are handled.
