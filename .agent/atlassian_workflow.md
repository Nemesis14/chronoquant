# Atlassian Workflow

Shared Jira and Confluence operating rules for Codex, Claude, and future
agents.

---

## Source Of Truth

Jira is the only active task board for ChronoQuant.

- Jira project: `KAN`
- Jira board: `https://erosszsolt84.atlassian.net/jira/software/projects/KAN/boards/2`

Do not create or maintain repository-local task boards.

Confluence is the canonical project knowledge base. Agents should look there
for project, module, architecture, planning, and validation documentation before
creating new local documentation.

---

## Agent Startup

Before planning, reviewing, documenting, validating, or implementing work:

1. Read the shared `.agent/` rules listed in `AGENTS.md`.
2. Use Jira for active task state and current work.
3. Use Confluence for longer context, module docs, project docs, and task specs.
4. Use repository files for code and operational agent bootstrap only.

---

## Task Reference Rule

When any agent sees a task reference, issue key, board-status request, or a
request to start, inspect, update, implement, review, or validate a task, Jira
must be queried first. Repository files may provide code and operational
bootstrap context, but they are not an alternative task source of truth.

This rule applies to Codex, Claude, and future agents.

---

## Auth — Basic Auth Only

All agents access Jira and Confluence via **Atlassian REST API with Basic Auth**.
Do not use a scoped token approach for this repository.

Credentials are in `.agent/.env`:

| Key | Value |
|-----|-------|
| `email` | Atlassian account email |
| `api_token` | Atlassian API token — works for both Jira and Confluence |
| `jira_url` | Jira board URL (for reference only) |
| `confluence_url` | Confluence root page URL (for reference only) |

REST base URL for all API calls: `https://erosszsolt84.atlassian.net`

Authorization header:

```
Authorization: Basic base64(email:api_token)
```

Never print, commit, or include the `api_token` value in Jira comments,
Confluence pages, or code.

---

## Token Validation Commands

Run these in a PowerShell terminal from the repo root. Do not validate Jira
access by fetching a hard-coded issue key; deleted or hidden issues return 404
even when authentication and project access work.

The commands below validate the login, the `KAN` project, a safe Jira search,
and Confluence. All status lines should print `200`. The Jira search should
return at least one issue key when the project contains visible issues.

```powershell
# Helper: load .env without printing values
$env_path = "d:\repos\chronoquant\.agent\.env"
$v = @{}
foreach ($l in Get-Content $env_path) {
    $t = $l.Trim()
    if ($t.Length -eq 0 -or $t.StartsWith("#")) { continue }
    $p = $t -split "=", 2
    if ($p.Count -ne 2) { continue }
    $v[$p[0].Trim()] = $p[1].Trim().Trim('"').Trim("'")
}
$h = @{ Authorization = "Basic $([Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes("$($v.email):$($v.api_token)")))" ; Accept = "application/json" }

# Jira login/auth check
"MYSELF_STATUS=$((Invoke-WebRequest -Uri 'https://erosszsolt84.atlassian.net/rest/api/3/myself' -Headers $h -UseBasicParsing).StatusCode)"

# Jira project access check
"PROJECT_KAN_STATUS=$((Invoke-WebRequest -Uri 'https://erosszsolt84.atlassian.net/rest/api/3/project/KAN' -Headers $h -UseBasicParsing).StatusCode)"

# Jira issue visibility/search check
$search = Invoke-WebRequest -Uri 'https://erosszsolt84.atlassian.net/rest/api/3/search/jql?jql=project=KAN&maxResults=1&fields=key,status,summary' -Headers $h -UseBasicParsing
"SEARCH_KAN_STATUS=$($search.StatusCode)"
$search_json = $search.Content | ConvertFrom-Json
if ($search_json.issues.Count -gt 0) {
    "SEARCH_KAN_FIRST=$($search_json.issues[0].key)"
}

# Confluence access check
"CONFLUENCE_STATUS=$((Invoke-WebRequest -Uri 'https://erosszsolt84.atlassian.net/wiki/rest/api/space?limit=1' -Headers $h -UseBasicParsing).StatusCode)"
```

Expected status output:

```text
MYSELF_STATUS=200
PROJECT_KAN_STATUS=200
SEARCH_KAN_STATUS=200
SEARCH_KAN_FIRST=<visible issue key>
CONFLUENCE_STATUS=200
```

If `MYSELF_STATUS` and `PROJECT_KAN_STATUS` are `200` but a direct issue lookup
returns `404`, treat the issue key as deleted, hidden, or mistyped. Do not treat
that as a failed Jira login.

## REST Access Pattern For Agents

Atlassian access uses REST Basic Auth only:

1. Load `.agent/.env` without printing values.
2. Read `email` and `api_token`.
3. Build `Authorization: Basic base64(email:api_token)`.
4. Send Jira requests to `https://erosszsolt84.atlassian.net/rest/api/3/...`.
5. Send Confluence requests to `https://erosszsolt84.atlassian.net/wiki/rest/api/...`.
6. Use `Accept: application/json`.

For the first Jira check in a new session, use `/rest/api/3/myself`,
`/rest/api/3/project/KAN`, and `/rest/api/3/search/jql?jql=project=KAN...`.
Do not use a fixed issue key as the initial connectivity test.

---

## Jira Task Rules

Create new tasks only in Jira. Do not add tasks to local Markdown files.

Expected Jira workflow:

1. `To Do`: task is defined and ready to take.
2. `In Progress`: an implementation agent has taken the task and is working.
3. `In Review`: implementation is complete and ready for validator review.
4. `Done`: validation passed.

Implementation agents, normally Claude:

- move a task from `To Do` to `In Progress` when taking it;
- keep progress and important implementation notes on the Jira issue;
- move the task to `In Review` when the requested work is complete;
- include changed files, commands run, known gaps, and validation already done.

Validation agents, normally Codex:

- take tasks from `In Review`;
- inspect or run validation that matches the task scope;
- move the task to `Done` only after validation passes;
- keep or move the task to a non-done status when validation fails or is
  incomplete, and record the remaining work on the Jira issue.

**Validation request scope rule.** When the user asks to validate a Jira task,
epic, or "new version", validation agents MUST first identify the Jira issues
in `In Review` status that are in scope. If the referenced issue is an epic,
validate only its child issues that are currently `In Review`; do not validate
the epic itself as the implementation unit, and do not validate child issues in
`To Do`, `In Progress`, or `Done` unless the user explicitly asks for a broader
audit. If no scoped issue is in `In Review`, report that there is nothing ready
for validation instead of inferring work from local repository changes.

**`In Review` means verify, not skip.** Any agent that encounters a task in
`In Review` status MUST treat it as a validation task: check that the described
work is actually complete, run the relevant tests or validation commands, and
confirm the acceptance criteria are met. Do not assume the work is done just
because the status is `In Review`.

**When an agent is given an `In Review` task as its goal**, the agent's job is
validation only — not implementation. The agent MUST:

1. Read the task description and acceptance criteria from Jira.
2. Run the specified validation commands or checks.
3. If all acceptance criteria pass → move the task to `Done`, no comment needed.
4. If any acceptance criterion fails → move the task back to `In Progress` and
   add a Jira comment explaining exactly what failed and why.

The agent MUST NOT implement missing work when given an `In Review` task. If
work is missing, the comment must describe the gap so the implementation agent
can fix it.

Use `Done` only for validated work. Do not use `Done` for work merely being
implemented.

---

## Confluence Task Specs

When defining a new Jira task, make the Jira description clear enough for
another agent to execute. A new Confluence spec page is recommended only when
the task is long, ambiguous, cross-module, architecture-sensitive, or otherwise
needs more detail than fits cleanly in the Jira issue.

When a Confluence spec is needed, it should include:

- goal and scope;
- affected modules and files;
- ordered implementation steps;
- acceptance criteria;
- validation commands or checks;
- risks, assumptions, and open questions.

If relevant Confluence pages already exist, such as architecture notes,
business specs, analysis pages, or module documentation, the task creator must
link them from the Jira issue. The goal is that the implementation agent can
understand the task purpose and constraints from Jira without hunting for
related context.

Keep the Jira issue concise and executable. Use Confluence for existing context
and for expanded specs when the Jira description would become too long or too
hard to scan.

---

## Documentation Rules

When the user asks for project documentation, write it to Confluence, not to a
local `.md` file.

Confluence diagrams must be editable draw.io diagrams, not PNG/SVG/Mermaid
fallbacks. The validated ChronoQuant baseline is the `Database` page's
`database-pipeline.drawio` diagram: it renders through the Confluence draw.io
macro and is editable by a user in draw.io. Follow
`.agent/confluence_standards.md` for the macro shape, color palette, attachment
handling, and validation checks.

Use local Markdown only for repository-operational files that agents must read
at startup, such as `AGENTS.md` and `.agent/*.md`. Do not create new local
project docs, module docs, task specs, or boards.

If existing repository documentation conflicts with Confluence, treat
Confluence as the source of truth unless the user explicitly says otherwise.

---
