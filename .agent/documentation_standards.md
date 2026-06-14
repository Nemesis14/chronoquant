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
- longer task specs and validation plans linked from Jira issues when the Jira
  description is not enough.

Repository `.md` files are no longer the default documentation output.

When the user asks an agent to create project documentation, create or update
the relevant Confluence page. Do not create a local `.md` file unless the user
explicitly asks for a repository-operational Markdown file.

---

## Repository Markdown Exceptions

Create or update local `.md` files only when the file is operationally needed by
the repository or agent workflow:

- `.agent/` bootstrap and shared agent rules;
- `AGENTS.md` or other tool entry points that load shared `.agent/` rules;
- code-adjacent README files that have not yet been migrated;
- local migration staging files when explicitly requested.

Do not create new local task boards, backlog specs, implementation plans,
validation plans, architecture notes, or module docs. Use Jira for task state
and Confluence for documentation/specification.

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

## Diagrams

When diagrams are needed on Confluence pages, use draw.io diagrams directly.

- Architecture maps, pipeline maps, data-flow diagrams, and similar visuals
  must be editable draw.io diagrams embedded with a Confluence draw.io macro.
- The validated baseline is the `Database` Confluence page's
  `database-pipeline.drawio` diagram: it renders in the page, opens in draw.io,
  and is editable by a Confluence user.
- Do not use PNG, SVG, Mermaid, Graphviz, flowchart source, or other rendered
  image fallbacks as the reader-facing diagram artifact.
- Do not leave Mermaid code fences, raw `flowchart ...` blocks, raw SVG/XML, or
  draw.io XML in the visible Confluence page body.
- Store the editable `.drawio` source as the Confluence diagram attachment used
  by the draw.io macro.
- If the draw.io macro renders as `unknown-macro` or otherwise fails, treat that
  as a Confluence/app setup problem to fix before publishing. Do not replace it
  with a PNG fallback.
- Follow `.agent/confluence_standards.md` for draw.io macro shape, sizing,
  validated color palette, attachment naming, and validation rules.

---

## Language

- If the user communicates in Hungarian, write project documentation in
  Hungarian unless the page already uses English.
- Keep code identifiers, paths, SQL, config keys, and command names in their
  original form.
