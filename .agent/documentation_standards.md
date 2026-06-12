# Documentation Standards

ChronoQuant documentation is Confluence-first. New project documentation should
be created or updated in Confluence, not as repository-local `.md` files.

For detailed Confluence rules, see `.agent/confluence_standards.md`.

---

## Default Documentation Target

Use Confluence for:

- feature and module documentation;
- architecture notes and diagrams;
- user-facing or project-level explanations;
- stable engineering decisions;
- migrated content from existing `docs/` or module-local `.md` files.

Repository `.md` files are no longer the default documentation output.

---

## Repository Markdown Exceptions

Create or update local `.md` files only when the file is operationally needed by
the repository or agent workflow:

- `.agent/` bootstrap and shared agent rules;
- `board.md`;
- backlog specs while they are actively used by local implementation workflow;
- code-adjacent README files that have not yet been migrated;
- local migration staging files when explicitly requested.

If the same topic exists both in Confluence and in the repository, Confluence is
the source of truth unless the task explicitly says otherwise.

---

## Migration Guidance

- Do not add new documentation under `docs/` by default.
- When touching existing `.md` documentation, consider whether the right change
  is to migrate or update the Confluence page instead.
- Never publish `.env` files, API tokens, credentials, or local secrets to
  Confluence.
- Keep repository links precise when Confluence pages describe code behavior.

---

## Page Structure

Use clear page hierarchy in Confluence:

- root project page for broad navigation;
- child pages for domains, modules, and workflows;
- subpages for detailed specs, validations, or diagrams.

Prefer short pages with focused scope over large catch-all documents.

---

## Mermaid And Diagrams

When diagrams are needed, use Confluence-supported diagrams or Mermaid blocks if
the current Confluence editor supports them. Keep diagram syntax conservative:

- node IDs should be ASCII-only;
- avoid special Unicode arrows and dashes in diagram syntax;
- keep diagrams small enough to read in the Confluence page.

---

## Language

- If the user communicates in Hungarian, write project documentation in
  Hungarian unless the page already uses English.
- Keep code identifiers, paths, SQL, config keys, and command names in their
  original form.
