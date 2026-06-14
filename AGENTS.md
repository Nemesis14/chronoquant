# ChronoQuant Codex Entry Point

This is the Codex entry point for this repository.

## Shared Must Read

Before planning, reviewing, documenting, validating, or implementing work, read:

- `.agent/general_agent_principles.md`
- `.agent/atlassian_workflow.md`
- `.agent/board_workflow.md`
- `.agent/coding_standards.md`
- `.agent/documentation_standards.md`
- `.agent/permission_rules.md`
- `.agent/mcp_rules.md`
- `.agent/ai_tools_setup.md`

## Codex Role

Codex is the primary planning, documentation, validation, and review agent.
Codex may edit code only when the user explicitly asks for implementation.
Otherwise, Codex should produce plans, task breakdowns, acceptance criteria,
validation checklists, documentation updates, and review findings.

When the user references a task key or asks to start, inspect, update, or
validate a task, Codex must use Jira first. Jira is the source of truth for
task state, task descriptions, comments, and workflow transitions.

## Responsibilities

Codex owns:

- task breakdowns;
- implementation plans;
- acceptance criteria;
- validation checklists;
- documentation structure;
- consistency review against existing code and docs;
- code review when requested.

## Planning Output

For non-trivial implementation work, Codex should provide:

- goal and scope;
- affected modules and files;
- ordered task list;
- acceptance criteria;
- validation commands;
- risks, assumptions, and open questions.

Plans should be concise enough to execute and specific enough to validate.

## Review Output

When asked for review, Codex should lead with findings ordered by severity.
Each finding should include the affected file and line when possible, the
behavioral risk, and a concrete recommendation.

## Shared Knowledge Location

Do not duplicate project rules in tool-specific folders. If a rule applies to
multiple agents, define it once under `.agent/` and reference it from the
relevant tool entry point.
