# Confluence Standards

ChronoQuant documentation is moving to Confluence as the canonical knowledge
base.

---

## Canonical Location

New project documentation should be created in Confluence under:

- Space: ChronoQuant personal space
- Root page: `ChronoQuant`
- Agent rules page: `Agents`

Repository `.md` files are no longer the default target for new documentation.

---

## What Still Lives In The Repository

Keep repository-local Markdown only when it is operationally required:

- agent bootstrap rules under `.agent/`;
- active board and backlog files used by local tools;
- code-adjacent README files required before Confluence migration;
- generated or temporary migration source files when explicitly requested.

Do not create new `docs/` or module-local `.md` documentation by default.
Create or update the corresponding Confluence page instead.

---

## Confluence Page Rules

- Use one page per stable topic.
- Use parent/child page structure instead of long flat pages.
- Keep page titles short and searchable.
- Link back to relevant repository paths when discussing code.
- Do not publish secrets, `.env` files, tokens, local credentials, or private
  machine paths that are not useful to the project.

---

## Migration Rule

Existing repository `.md` documentation may be migrated gradually. During the
migration period, prefer updating Confluence first. If a local `.md` file still
exists and the same topic has a Confluence page, the Confluence page is the
source of truth unless a task explicitly says otherwise.
