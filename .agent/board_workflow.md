# Board Workflow

Shared rules for `board.md` and implementation plan files.

---

## Board Purpose

`board.md` is the active task board. It should contain work that is current or
near-term enough that agents need to see it at session start.

Each task row should include:

- status;
- task title;
- affected scope;
- short note or dependency.

Allowed statuses:

- `Open`: task is recorded and not yet completed.
- `Validation`: an AI agent has completed the requested work and handed it
  over for validation.

When the user asks an AI agent to add or take up a new board task, add it with
`Open` status.

---

## Spec Folders

Implementation specs live under:

- `backlog/`: planned future work and implementation specs.

Use spec files for work that needs more detail than a single board row.

---

## Task Start

Before starting a board task:

1. Read `board.md`.
2. Check whether a related spec exists under `backlog/`.
3. Confirm scope, dependencies, and validation expectations.

---

## Task Completion

When a task is finished:

1. Change the task status in `board.md` from `Open` to `Validation`.
2. Keep or remove the related `backlog/` spec based on whether it remains useful
   as implementation reference.
3. Record the validation performed in the final agent response or in the spec
   file when appropriate.

Do not remove a task only because code was edited or documentation was updated.
Completed work stays on the board in `Validation` until an AI validation pass
checks the requested behavior and validation criteria.

---

## Task Validation

When an AI agent validates a task in `Validation` status:

1. Run or inspect the validation criteria that match the task scope.
2. Record the validation performed in the final response or related spec.
3. Remove the task row from `board.md` only after validation passes.

If validation fails or is incomplete, keep the task on the board and set or keep
its status as `Open`, with the note updated to describe the remaining work.
