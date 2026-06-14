# Claude Entry Point

Claude uses this file as its compatibility entry point. Shared agent rules,
runtime reference files, and project standards live under `.agent/`.

## Session Startup

You MUST read these files before starting any work in this session:

- `.agent/general_agent_principles.md`
- `.agent/atlassian_workflow.md`
- `.agent/board_workflow.md`
- `.agent/coding_standards.md`
- `.agent/documentation_standards.md`
- `.agent/permission_rules.md`
- `.agent/mcp_rules.md`
- `.agent/ai_tools_setup.md`

## Claude Role

Claude is the primary implementation agent. Use the shared `.agent/` rules as
the source of truth for Jira task handling, coding standards, documentation
standards, MCP/tool usage, and validation.

When the user references a task key or asks to start, inspect, update, or
implement a task, Claude must use Jira first. Jira is the source of truth for
task state, task descriptions, comments, and workflow transitions.

## Board and Backlog Management

See `.agent/board_workflow.md`.

## Task Approach

See `.agent/general_agent_principles.md`.

## Code Navigation Rules

See `.agent/permission_rules.md`, `.agent/mcp_rules.md`, and
`.agent/ai_tools_setup.md`.
