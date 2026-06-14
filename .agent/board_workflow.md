# Board Workflow

Shared task lifecycle rules for Jira-based work.

---

## Active Board

Jira is the only active task board for ChronoQuant.

- Project: `KAN`
- Board URL: `https://erosszsolt84.atlassian.net/jira/software/projects/KAN/boards/2`

Do not create or update repository-local task boards.

For detailed Atlassian rules, read `.agent/atlassian_workflow.md`.

When a task key or task request appears in user input, query Jira first. Jira
is the source of truth for task state, task descriptions, comments, and
workflow transitions for every agent.

---

## Status Flow

Use this Jira workflow:

1. `To Do`: task is defined and ready to take.
2. `In Progress`: an implementation agent is actively working on it.
3. `In Review`: implementation is complete and ready for validator review.
4. `Done`: validator confirmed the work.

Implementation agents move tasks to `In Progress` when taking them and to
`In Review` when implementation is complete.

Codex validates tasks in `In Review` and moves them to `Done` only after
validation passes. If validation fails or is incomplete, Codex records the
remaining work on the Jira issue and keeps or moves the issue to a non-done
status.

For validation requests, scope validation to Jira issues currently in
`In Review`. For epic references, inspect the epic's child issues and validate
only the children in `In Review`; see `.agent/atlassian_workflow.md` for the
full rule.

---

## Task Specs

A Jira task description should be clear enough for another agent to execute.
Create a new Confluence spec page only when the task needs longer guidance,
multi-step planning, architecture context, or detailed validation criteria that
would make the Jira issue hard to scan.

When relevant Confluence pages already exist, such as architecture notes,
business specs, module docs, or analysis pages, link them from the Jira issue so
the implementation agent can find the context directly from the task.

Do not create new local backlog specs for Jira tasks. Use Confluence instead.
