# Claude Entry Point

Runtime settings and coding standards live under `.claude/`.

## Session Startup

- `board.md`
- `.claude/coding_standards.md`

## Agent Definitions

- later spec...

## Board and Backlog Management

When a task is finished:
1. Remove the task row from `board.md` — do not mark done, delete the row.
2. Move the backlog plan file from `docs/plans/backlog/` to `docs/plans/completed/` if one exists.

## Task Approach

Before implementing any task:
1. Outline an execution plan — high-level steps, no exhaustive detail.
2. Ask if anything critical is unclear before starting.
3. Check for existing code that already covers the need — do not write redundant logic.
4. Verify integrity: new code must not duplicate or contradict existing modules or conventions.
