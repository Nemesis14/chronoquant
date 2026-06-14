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

Before writing new project or module documentation, search Confluence for an
existing page and update it when appropriate.

---

## What Still Lives In The Repository

Keep repository-local Markdown only when it is operationally required:

- agent bootstrap rules under `.agent/`;
- tool entry points such as `AGENTS.md`;
- code-adjacent README files required before Confluence migration;
- generated or temporary migration source files when explicitly requested.

Do not create new `docs/` or module-local `.md` documentation by default.
Create or update the corresponding Confluence page instead.

Do not create new local task specs under `backlog/`. Use Jira descriptions for
straightforward tasks. Create or link Confluence pages only when the task needs
longer specification or when relevant project/module/business context already
exists in Confluence.

When a task key or task request appears, inspect Jira before using Confluence
or repository files for task context.

---

## Confluence Page Rules

- Use one page per stable topic.
- Use parent/child page structure instead of long flat pages.
- Keep page titles short and searchable.
- Link back to relevant repository paths when discussing code.
- Do not publish secrets, `.env` files, tokens, local credentials, or private
  machine paths that are not useful to the project.

---

## Character Encoding

Hungarian project documentation must preserve accented characters correctly in
Confluence and Jira.

### Root cause

PowerShell 5.1 `Invoke-RestMethod` and `Invoke-WebRequest` may send string
`-Body` arguments with the system default encoding instead of UTF-8. This can
corrupt Hungarian accented characters into mojibake stored in Confluence/Jira.

### Required pattern -- always pass bytes, never a string

**Correct (works):**

```powershell
$bodyJson = $bodyObj | ConvertTo-Json -Depth 20 -Compress
$bodyBytes = [System.Text.Encoding]::UTF8.GetBytes($bodyJson)
Invoke-RestMethod -Uri $url -Method POST -Headers $headers -Body $bodyBytes
```

**Alternative -- write JSON to temp file, read as bytes:**

```powershell
$bodyObj | ConvertTo-Json -Depth 20 | Out-File $tmpFile -Encoding utf8
$bodyBytes = [System.IO.File]::ReadAllBytes($tmpFile)
Invoke-RestMethod -Uri $url -Method POST -Headers $headers -Body $bodyBytes
```

**Wrong (corrupts accents) -- do not use:**

```powershell
$body = $bodyObj | ConvertTo-Json -Depth 20
Invoke-RestMethod -Uri $url -Method POST -Headers $headers -Body $body
```

This rule applies to Jira issue create/update, Jira comments, and Confluence
page create/update calls alike.

### Confluence storage entities for Hungarian text

When updating Confluence storage format through REST, do not rely on raw
Hungarian double-acute characters surviving the storage round trip. Encode them
as numeric HTML entities in the storage body before PUT/POST:

| Character name | Storage entity |
|----------------|----------------|
| lowercase o double acute | `&#337;` |
| uppercase O double acute | `&#336;` |
| lowercase u double acute | `&#369;` |
| uppercase U double acute | `&#368;` |

Other accented Hungarian characters may use named entities already common in
Confluence storage, such as `&aacute;`, `&eacute;`, `&iacute;`, `&oacute;`,
`&ouml;`, `&uacute;`, and `&uuml;`.

Do not PUT a page body that already contains mojibake sequences or replacement
characters in Hungarian prose. If a readback contains mojibake marker
characters, the Unicode replacement character, or `?` inside a word where an
accent should be, repair the storage body first or abort the update. Never
round-trip corrupt storage content unchanged.

### Validation after publishing

After writing Hungarian text to Jira or Confluence via REST, read the stored
content back and check for mojibake. If a Hungarian word contains replacement
characters, mojibake marker characters, or `?` where an accent should appear,
the encoding was wrong. Fix and repost before marking the task complete.

---

## Diagram Rules

Architecture maps, pipeline maps, data-flow diagrams, and similar explanatory
documents must be editable in Confluence, not merely valid as source Markdown.

- Confluence pages must use editable draw.io diagrams embedded with the
  Confluence draw.io macro.
- The ChronoQuant Atlassian instance has a validated working draw.io setup:
  the `Database` page renders `database-pipeline.drawio` through a draw.io macro
  and the diagram can be opened and edited by a user in Confluence.
- Do not publish Mermaid code fences or raw `flowchart ...` blocks as the
  visible diagram content on a Confluence page.
- Do not publish PNG, SVG, Mermaid, Graphviz, flowchart source, or other
  rendered image fallbacks as the reader-facing diagram artifact.
- Do not publish raw draw.io XML in the visible page body.
- Store the editable `.drawio` source as the Confluence attachment referenced by
  the draw.io macro.
- Use the storage format of a working UI-created or already validated draw.io
  diagram on the same Confluence instance as the template. Update only the page
  id, diagram name, size, and other clearly understood parameters.
- Prefer several focused diagrams over one large full-system diagram.
- Keep draw.io diagrams compact:
  - default max width: `760 px`;
  - use narrower widths, such as `720 px` or `740 px`, when the diagram does
    not need the full width;
  - set the draw.io macro width explicitly instead of relying on the natural
    diagram size.
- Use the validated ChronoQuant diagram palette by default:
  - page background: `#ffffff`;
  - text: `#172b4d`;
  - connectors and neutral borders: `#344563` or `#6b778c`;
  - source/API nodes: light blue fill `#e8f3ff`, blue border `#2f6f9f`;
  - process nodes: light amber fill `#fff7e6`, amber border `#b36b00`;
  - persisted dataset nodes: light green fill `#e9f7ef`, green border
    `#2e7d32`;
  - query/engine nodes: light violet fill `#f3f0ff`, violet border `#6554c0`;
  - runtime/state nodes: neutral fill `#f4f5f7`, neutral border `#6b778c`.
- Use simple rectangular nodes with small corner radius, consistent spacing,
  and visible borders. Avoid decorative gradients, heavy shadows, oversized
  shapes, and dark canvases.
- Keep labels short and scannable. If text becomes cramped at `760 px`, split
  the content into multiple smaller diagrams.
- When replacing a diagram, update the existing `.drawio` attachment version
  under the same filename when possible. Do not create duplicate diagram files
  unless the old and new diagrams must both remain visible.
- After publishing, read the page back and verify all of the following:
  - storage contains a draw.io macro;
  - storage references the expected `.drawio` filename;
  - storage does not reference PNG/SVG fallback files as the visible diagram;
  - rendered view does not contain `unknown-macro`;
  - rendered view does not contain `Error loading the extension`.
- If the draw.io macro renders as an `unknown-macro` placeholder, shows
  `Error loading the extension`, or cannot find the referenced `.drawio`
  attachment, fix the Confluence app/macro setup or attachment reference before
  marking the documentation complete. Do not replace the diagram with a PNG or
  SVG fallback.

Recommended Confluence draw.io macro shape, based on the validated `Database`
page:

```xml
<ac:structured-macro ac:name="drawio" ac:schema-version="1" data-layout="default">
  <ac:parameter ac:name="diagramName">example.drawio</ac:parameter>
  <ac:parameter ac:name="width">760</ac:parameter>
  <ac:parameter ac:name="simple">0</ac:parameter>
  <ac:parameter ac:name="zoom">1</ac:parameter>
  <ac:parameter ac:name="diagramDisplayName">example.drawio</ac:parameter>
  <ac:parameter ac:name="lbox">1</ac:parameter>
  <ac:parameter ac:name="height">380</ac:parameter>
</ac:structured-macro>
```

---

## Migration Rule

Existing repository `.md` documentation may be migrated gradually. During the
migration period, prefer updating Confluence first. If a local `.md` file still
exists and the same topic has a Confluence page, the Confluence page is the
source of truth unless a task explicitly says otherwise.
