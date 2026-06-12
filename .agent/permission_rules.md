# Permission Rules

Shared permission expectations for agents working in this repository.

---

## Authority

Actual permissions are granted by the active agent runtime or session. Repository
files cannot force a tool to expose shell, file editing, web, MCP, or plugin
access.

The files below are project-level permission references:

- `.agent/settings.json`
- `.agent/settings.local.json`

Agents must treat them as the expected ChronoQuant permission profile, but must
also obey the permissions actually granted by their current runtime.

---

## Expected Project Capabilities

Agents working on implementation, validation, or documentation should have:

- repository file read access;
- repository file edit/write access when the user asks for changes;
- shell access for project commands;
- PowerShell access on Windows;
- MCP language-server access when available;
- web access only when current external information is required or explicitly
  requested.

---

## Claude

Claude permissions are controlled by Claude Code runtime configuration. The
`.agent/settings.json` and `.agent/settings.local.json` files document the
expected project permissions, including shell/edit/write access and language
server MCP diagnostics.

If Claude Code does not load these files automatically, configure Claude outside
the repo or use the available runtime permissions. Do not create a `.claude/`
folder only to duplicate shared rules.

---

## Codex

Codex permissions are controlled by the Codex session environment. Codex does
not auto-load `.agent/settings.json` or `.agent/settings.local.json` as runtime
permission grants.

For this project, Codex should:

- use the permissions exposed by the current session;
- follow the expected project profile documented in `.agent/settings*.json`;
- report when a required capability is unavailable;
- use MCP language-server tools first when they are available.

---

## Safety

- Do not use destructive git or filesystem commands unless the user explicitly
  asks for them.
- Do not revert unrelated user changes.
- Keep edits scoped to the requested work.
- Prefer validation commands that match the changed scope before running broad
  gates.
